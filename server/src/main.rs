//! The `omp-relayd` binary: bind address resolution, logging setup, listener,
//! and signal handling.
//!
//! The configuration surface is deliberately two values: where to listen, and
//! how much to log. Every protocol limit -- frame size, queue depth, identifier
//! lengths, deadlines -- is a code constant. There is no server-side
//! configuration file.

use std::env;
use std::process::ExitCode;
use std::sync::Arc;

use omp_relayd::relay::{self, ServerState};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// Listen address used when neither an argument nor the environment names one.
///
/// All interfaces, because the intended deployment is a container: restricting
/// exposure is the published-port binding's job, and a container that listened
/// only on loopback would be unreachable even from its own host.
const DEFAULT_LISTEN: &str = "0.0.0.0:7788";

/// Environment variable naming the address to listen on.
///
/// Distinct from `OMP_RELAY_BIND` in `compose.yml`, which selects the *host*
/// interface the container's port is published on. One name for both would
/// conflate the address the process binds with the address an operator reaches.
const LISTEN_ENV: &str = "OMP_RELAY_LISTEN";

const USAGE: &str = "\
omp-relayd -- in-memory TCP relay for OMP peers

Usage:
  omp-relayd [ADDRESS]
  omp-relayd --bind ADDRESS

Arguments:
  ADDRESS   Address to listen on. Defaults to $OMP_RELAY_LISTEN, then 0.0.0.0:7788.

Options:
  -h, --help       Print this message
  -V, --version    Print the version

Environment:
  OMP_RELAY_LISTEN  Address to listen on, when no argument is given
  RUST_LOG          Log filter directives (default: info)
";

/// What the command line asked for.
enum Invocation {
    /// Listen on the resolved address.
    Serve(Option<String>),
    /// Print usage.
    Help,
    /// Print the version.
    Version,
}

#[tokio::main]
async fn main() -> ExitCode {
    let invocation = match parse_args(env::args().skip(1)) {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("omp-relayd: {error}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    match invocation {
        Invocation::Help => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Invocation::Version => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Invocation::Serve(address) => {
            init_tracing();
            let listen = address
                .or_else(|| env::var(LISTEN_ENV).ok().filter(|value| !value.is_empty()))
                .unwrap_or_else(|| DEFAULT_LISTEN.to_owned());
            run(listen).await
        }
    }
}

/// Parses arguments without a command-line parsing dependency: the surface is
/// one address and two informational flags.
fn parse_args<I>(args: I) -> Result<Invocation, String>
where
    I: IntoIterator<Item = String>,
{
    let mut address: Option<String> = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Invocation::Help),
            "-V" | "--version" => return Ok(Invocation::Version),
            "--bind" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--bind requires an address".to_owned())?;
                if address.replace(value).is_some() {
                    return Err("the listen address was given more than once".to_owned());
                }
            }
            unknown if unknown.starts_with('-') => {
                return Err(format!("unknown option {unknown}"));
            }
            _ => {
                if address.replace(arg).is_some() {
                    return Err("the listen address was given more than once".to_owned());
                }
            }
        }
    }

    Ok(Invocation::Serve(address))
}

fn init_tracing() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn run(listen: String) -> ExitCode {
    // Registered before anything else can block, and deliberately not at the
    // point it is awaited. This process is PID 1 in the container, and the
    // kernel discards a signal sent to a PID-namespace init process whose
    // disposition is still the default rather than queueing it. A handler
    // installed after the listener bound would therefore drop a SIGTERM that
    // arrived during startup, and the relay would keep running until Docker's
    // grace period expired and SIGKILL arrived -- which is exactly the
    // behaviour the Dockerfile's "no init shim" note claims this binary does
    // not have.
    let mut termination = Termination::install();

    let listener = match TcpListener::bind(&listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(listen = %listen, %error, "could not bind listener");
            return ExitCode::FAILURE;
        }
    };

    match listener.local_addr() {
        Ok(local_addr) => tracing::info!(%local_addr, "relay listening"),
        Err(error) => tracing::info!(requested = %listen, %error, "relay listening"),
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let accept_loop = tokio::spawn(relay::serve(
        listener,
        Arc::new(ServerState::new()),
        shutdown_rx,
    ));

    termination.recv().await;
    tracing::info!("termination signal received");
    // Ignored: the receiver lives in the task awaited immediately below.
    let _ = shutdown_tx.send(true);

    if let Err(error) = accept_loop.await {
        tracing::error!(%error, "accept loop failed");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// Termination signals, with registration separated from waiting.
///
/// The split is the point: see the comment at the call site in [`run`]. A
/// single `wait_for_termination().await` reads more simply but cannot register
/// a handler before the work it is meant to interrupt.
struct Termination {
    #[cfg(unix)]
    sigterm: Option<tokio::signal::unix::Signal>,
}

impl Termination {
    /// Installs handlers for every termination signal the platform offers.
    fn install() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let sigterm = match signal(SignalKind::terminate()) {
                Ok(sigterm) => Some(sigterm),
                Err(error) => {
                    tracing::warn!(%error, "no SIGTERM handler; only SIGINT will stop the relay");
                    None
                }
            };
            Self { sigterm }
        }

        #[cfg(not(unix))]
        {
            Self {}
        }
    }

    /// Resolves on the first termination signal to arrive.
    ///
    /// `ctrl_c` registers on first poll rather than in [`Self::install`], which
    /// is acceptable because SIGINT reaches PID 1 only from an interactive
    /// terminal, where startup has necessarily already finished.
    ///
    /// Its `io::Result` is matched rather than discarded. A discarded `Err`
    /// resolves the future immediately, which a `_ =` pattern cannot tell apart
    /// from a delivered signal -- so a failure to register the SIGINT handler
    /// would have been reported as "termination signal received" and exited
    /// successfully during startup, abandoning an already-working SIGTERM
    /// listener on the way out.
    async fn recv(&mut self) {
        #[cfg(unix)]
        {
            match self.sigterm.as_mut() {
                Some(sigterm) => {
                    tokio::select! {
                        result = tokio::signal::ctrl_c() => match result {
                            Ok(()) => {}
                            Err(error) => {
                                tracing::warn!(%error, "no SIGINT handler; waiting on SIGTERM only");
                                sigterm.recv().await;
                            }
                        },
                        _ = sigterm.recv() => {}
                    }
                }
                None => wait_for_ctrl_c_forever().await,
            }
        }

        #[cfg(not(unix))]
        {
            wait_for_ctrl_c_forever().await;
        }
    }
}

/// Waits for SIGINT when it is the only termination signal available.
///
/// If it cannot be registered there is nothing left to wait on, and returning
/// would shut the relay down for no reason, so this parks instead. The
/// container's `SIGKILL` remains effective either way.
async fn wait_for_ctrl_c_forever() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(error) => {
            tracing::error!(
                %error,
                "no termination signal handler could be installed; \
                 the relay will run until it is killed"
            );
            std::future::pending::<()>().await;
        }
    }
}
