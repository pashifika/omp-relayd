// @bun
// src/client.ts
import { connect } from "net";
// node_modules/@msgpack/msgpack/dist.esm/utils/prettyByte.mjs
function prettyByte(byte) {
  return `${byte < 0 ? "-" : ""}0x${Math.abs(byte).toString(16).padStart(2, "0")}`;
}

// node_modules/@msgpack/msgpack/dist.esm/ExtData.mjs
class ExtData {
  type;
  data;
  constructor(type, data) {
    this.type = type;
    this.data = data;
  }
}

// node_modules/@msgpack/msgpack/dist.esm/DecodeError.mjs
class DecodeError extends Error {
  constructor(message) {
    super(message);
    const proto = Object.create(DecodeError.prototype);
    Object.setPrototypeOf(this, proto);
    Object.defineProperty(this, "name", {
      configurable: true,
      enumerable: false,
      value: DecodeError.name
    });
  }
}

// node_modules/@msgpack/msgpack/dist.esm/utils/int.mjs
var UINT32_MAX = 4294967295;
function setUint64(view, offset, value) {
  const high = value / 4294967296;
  const low = value;
  view.setUint32(offset, high);
  view.setUint32(offset + 4, low);
}
function setInt64(view, offset, value) {
  const high = Math.floor(value / 4294967296);
  const low = value;
  view.setUint32(offset, high);
  view.setUint32(offset + 4, low);
}
function getInt64(view, offset) {
  const high = view.getInt32(offset);
  const low = view.getUint32(offset + 4);
  return high * 4294967296 + low;
}
function getUint64(view, offset) {
  const high = view.getUint32(offset);
  const low = view.getUint32(offset + 4);
  return high * 4294967296 + low;
}

// node_modules/@msgpack/msgpack/dist.esm/timestamp.mjs
var EXT_TIMESTAMP = -1;
var TIMESTAMP32_MAX_SEC = 4294967296 - 1;
var TIMESTAMP64_MAX_SEC = 17179869184 - 1;
function encodeTimeSpecToTimestamp({ sec, nsec }) {
  if (sec >= 0 && nsec >= 0 && sec <= TIMESTAMP64_MAX_SEC) {
    if (nsec === 0 && sec <= TIMESTAMP32_MAX_SEC) {
      const rv = new Uint8Array(4);
      const view = new DataView(rv.buffer);
      view.setUint32(0, sec);
      return rv;
    } else {
      const secHigh = sec / 4294967296;
      const secLow = sec & 4294967295;
      const rv = new Uint8Array(8);
      const view = new DataView(rv.buffer);
      view.setUint32(0, nsec << 2 | secHigh & 3);
      view.setUint32(4, secLow);
      return rv;
    }
  } else {
    const rv = new Uint8Array(12);
    const view = new DataView(rv.buffer);
    view.setUint32(0, nsec);
    setInt64(view, 4, sec);
    return rv;
  }
}
function encodeDateToTimeSpec(date) {
  const msec = date.getTime();
  const sec = Math.floor(msec / 1000);
  const nsec = (msec - sec * 1000) * 1e6;
  const nsecInSec = Math.floor(nsec / 1e9);
  return {
    sec: sec + nsecInSec,
    nsec: nsec - nsecInSec * 1e9
  };
}
function encodeTimestampExtension(object) {
  if (object instanceof Date) {
    const timeSpec = encodeDateToTimeSpec(object);
    return encodeTimeSpecToTimestamp(timeSpec);
  } else {
    return null;
  }
}
function decodeTimestampToTimeSpec(data) {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  switch (data.byteLength) {
    case 4: {
      const sec = view.getUint32(0);
      const nsec = 0;
      return { sec, nsec };
    }
    case 8: {
      const nsec30AndSecHigh2 = view.getUint32(0);
      const secLow32 = view.getUint32(4);
      const sec = (nsec30AndSecHigh2 & 3) * 4294967296 + secLow32;
      const nsec = nsec30AndSecHigh2 >>> 2;
      return { sec, nsec };
    }
    case 12: {
      const sec = getInt64(view, 4);
      const nsec = view.getUint32(0);
      return { sec, nsec };
    }
    default:
      throw new DecodeError(`Unrecognized data size for timestamp (expected 4, 8, or 12): ${data.length}`);
  }
}
function decodeTimestampExtension(data) {
  const timeSpec = decodeTimestampToTimeSpec(data);
  return new Date(timeSpec.sec * 1000 + timeSpec.nsec / 1e6);
}
var timestampExtension = {
  type: EXT_TIMESTAMP,
  encode: encodeTimestampExtension,
  decode: decodeTimestampExtension
};

// node_modules/@msgpack/msgpack/dist.esm/ExtensionCodec.mjs
class ExtensionCodec {
  static defaultCodec = new ExtensionCodec;
  __brand;
  builtInEncoders = [];
  builtInDecoders = [];
  encoders = [];
  decoders = [];
  constructor() {
    this.register(timestampExtension);
  }
  register({ type, encode, decode }) {
    if (type >= 0) {
      this.encoders[type] = encode;
      this.decoders[type] = decode;
    } else {
      const index = -1 - type;
      this.builtInEncoders[index] = encode;
      this.builtInDecoders[index] = decode;
    }
  }
  tryToEncode(object, context) {
    for (let i = 0;i < this.builtInEncoders.length; i++) {
      const encodeExt = this.builtInEncoders[i];
      if (encodeExt != null) {
        const data = encodeExt(object, context);
        if (data != null) {
          const type = -1 - i;
          return new ExtData(type, data);
        }
      }
    }
    for (let i = 0;i < this.encoders.length; i++) {
      const encodeExt = this.encoders[i];
      if (encodeExt != null) {
        const data = encodeExt(object, context);
        if (data != null) {
          const type = i;
          return new ExtData(type, data);
        }
      }
    }
    if (object instanceof ExtData) {
      return object;
    }
    return null;
  }
  decode(data, type, context) {
    const decodeExt = type < 0 ? this.builtInDecoders[-1 - type] : this.decoders[type];
    if (decodeExt) {
      return decodeExt(data, type, context);
    } else {
      return new ExtData(type, data);
    }
  }
}

// node_modules/@msgpack/msgpack/dist.esm/utils/utf8.mjs
function utf8Count(str) {
  const strLength = str.length;
  let byteLength = 0;
  let pos = 0;
  while (pos < strLength) {
    let value = str.charCodeAt(pos++);
    if ((value & 4294967168) === 0) {
      byteLength++;
      continue;
    } else if ((value & 4294965248) === 0) {
      byteLength += 2;
    } else {
      if (value >= 55296 && value <= 56319) {
        if (pos < strLength) {
          const extra = str.charCodeAt(pos);
          if ((extra & 64512) === 56320) {
            ++pos;
            value = ((value & 1023) << 10) + (extra & 1023) + 65536;
          }
        }
      }
      if ((value & 4294901760) === 0) {
        byteLength += 3;
      } else {
        byteLength += 4;
      }
    }
  }
  return byteLength;
}
function utf8EncodeJs(str, output, outputOffset) {
  const strLength = str.length;
  let offset = outputOffset;
  let pos = 0;
  while (pos < strLength) {
    let value = str.charCodeAt(pos++);
    if ((value & 4294967168) === 0) {
      output[offset++] = value;
      continue;
    } else if ((value & 4294965248) === 0) {
      output[offset++] = value >> 6 & 31 | 192;
    } else {
      if (value >= 55296 && value <= 56319) {
        if (pos < strLength) {
          const extra = str.charCodeAt(pos);
          if ((extra & 64512) === 56320) {
            ++pos;
            value = ((value & 1023) << 10) + (extra & 1023) + 65536;
          }
        }
      }
      if ((value & 4294901760) === 0) {
        output[offset++] = value >> 12 & 15 | 224;
        output[offset++] = value >> 6 & 63 | 128;
      } else {
        output[offset++] = value >> 18 & 7 | 240;
        output[offset++] = value >> 12 & 63 | 128;
        output[offset++] = value >> 6 & 63 | 128;
      }
    }
    output[offset++] = value & 63 | 128;
  }
}
var sharedTextEncoder = new TextEncoder;
var TEXT_ENCODER_THRESHOLD = 50;
function utf8EncodeTE(str, output, outputOffset) {
  sharedTextEncoder.encodeInto(str, output.subarray(outputOffset));
}
function utf8Encode(str, output, outputOffset) {
  if (str.length > TEXT_ENCODER_THRESHOLD) {
    utf8EncodeTE(str, output, outputOffset);
  } else {
    utf8EncodeJs(str, output, outputOffset);
  }
}
var CHUNK_SIZE = 4096;
function utf8DecodeJs(bytes, inputOffset, byteLength) {
  let offset = inputOffset;
  const end = offset + byteLength;
  const units = [];
  let result = "";
  while (offset < end) {
    const byte1 = bytes[offset++];
    if ((byte1 & 128) === 0) {
      units.push(byte1);
    } else if ((byte1 & 224) === 192) {
      const byte2 = bytes[offset++] & 63;
      units.push((byte1 & 31) << 6 | byte2);
    } else if ((byte1 & 240) === 224) {
      const byte2 = bytes[offset++] & 63;
      const byte3 = bytes[offset++] & 63;
      units.push((byte1 & 31) << 12 | byte2 << 6 | byte3);
    } else if ((byte1 & 248) === 240) {
      const byte2 = bytes[offset++] & 63;
      const byte3 = bytes[offset++] & 63;
      const byte4 = bytes[offset++] & 63;
      let unit = (byte1 & 7) << 18 | byte2 << 12 | byte3 << 6 | byte4;
      if (unit > 65535) {
        unit -= 65536;
        units.push(unit >>> 10 & 1023 | 55296);
        unit = 56320 | unit & 1023;
      }
      units.push(unit);
    } else {
      units.push(byte1);
    }
    if (units.length >= CHUNK_SIZE) {
      result += String.fromCharCode(...units);
      units.length = 0;
    }
  }
  if (units.length > 0) {
    result += String.fromCharCode(...units);
  }
  return result;
}
var sharedTextDecoder = new TextDecoder;
var TEXT_DECODER_THRESHOLD = 200;
function utf8DecodeTD(bytes, inputOffset, byteLength) {
  const stringBytes = bytes.subarray(inputOffset, inputOffset + byteLength);
  return sharedTextDecoder.decode(stringBytes);
}
function utf8Decode(bytes, inputOffset, byteLength) {
  if (byteLength > TEXT_DECODER_THRESHOLD) {
    return utf8DecodeTD(bytes, inputOffset, byteLength);
  } else {
    return utf8DecodeJs(bytes, inputOffset, byteLength);
  }
}

// node_modules/@msgpack/msgpack/dist.esm/utils/typedArrays.mjs
function isArrayBufferLike(buffer) {
  return buffer instanceof ArrayBuffer || typeof SharedArrayBuffer !== "undefined" && buffer instanceof SharedArrayBuffer;
}
function ensureUint8Array(buffer) {
  if (buffer instanceof Uint8Array) {
    return buffer;
  } else if (ArrayBuffer.isView(buffer)) {
    return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  } else if (isArrayBufferLike(buffer)) {
    return new Uint8Array(buffer);
  } else {
    return Uint8Array.from(buffer);
  }
}

// node_modules/@msgpack/msgpack/dist.esm/CachedKeyDecoder.mjs
var DEFAULT_MAX_KEY_LENGTH = 16;
var DEFAULT_MAX_LENGTH_PER_KEY = 16;

class CachedKeyDecoder {
  hit = 0;
  miss = 0;
  caches;
  maxKeyLength;
  maxLengthPerKey;
  constructor(maxKeyLength = DEFAULT_MAX_KEY_LENGTH, maxLengthPerKey = DEFAULT_MAX_LENGTH_PER_KEY) {
    this.maxKeyLength = maxKeyLength;
    this.maxLengthPerKey = maxLengthPerKey;
    this.caches = [];
    for (let i = 0;i < this.maxKeyLength; i++) {
      this.caches.push([]);
    }
  }
  canBeCached(byteLength) {
    return byteLength > 0 && byteLength <= this.maxKeyLength;
  }
  find(bytes, inputOffset, byteLength) {
    const records = this.caches[byteLength - 1];
    FIND_CHUNK:
      for (const record of records) {
        const recordBytes = record.bytes;
        for (let j = 0;j < byteLength; j++) {
          if (recordBytes[j] !== bytes[inputOffset + j]) {
            continue FIND_CHUNK;
          }
        }
        return record.str;
      }
    return null;
  }
  store(bytes, value) {
    const records = this.caches[bytes.length - 1];
    const record = { bytes, str: value };
    if (records.length >= this.maxLengthPerKey) {
      records[Math.random() * records.length | 0] = record;
    } else {
      records.push(record);
    }
  }
  decode(bytes, inputOffset, byteLength) {
    const cachedValue = this.find(bytes, inputOffset, byteLength);
    if (cachedValue != null) {
      this.hit++;
      return cachedValue;
    }
    this.miss++;
    const str = utf8DecodeJs(bytes, inputOffset, byteLength);
    const slicedCopyOfBytes = Uint8Array.prototype.slice.call(bytes, inputOffset, inputOffset + byteLength);
    this.store(slicedCopyOfBytes, str);
    return str;
  }
}

// node_modules/@msgpack/msgpack/dist.esm/Decoder.mjs
var STATE_ARRAY = "array";
var STATE_MAP_KEY = "map_key";
var STATE_MAP_VALUE = "map_value";
var mapKeyConverter = (key) => {
  if (typeof key === "string" || typeof key === "number") {
    return key;
  }
  throw new DecodeError("The type of key must be string or number but " + typeof key);
};

class StackPool {
  stack = [];
  stackHeadPosition = -1;
  get length() {
    return this.stackHeadPosition + 1;
  }
  top() {
    return this.stack[this.stackHeadPosition];
  }
  pushArrayState(size) {
    const state = this.getUninitializedStateFromPool();
    state.type = STATE_ARRAY;
    state.position = 0;
    state.size = size;
    state.array = new Array(size);
  }
  pushMapState(size) {
    const state = this.getUninitializedStateFromPool();
    state.type = STATE_MAP_KEY;
    state.readCount = 0;
    state.size = size;
    state.map = {};
  }
  getUninitializedStateFromPool() {
    this.stackHeadPosition++;
    if (this.stackHeadPosition === this.stack.length) {
      const partialState = {
        type: undefined,
        size: 0,
        array: undefined,
        position: 0,
        readCount: 0,
        map: undefined,
        key: null
      };
      this.stack.push(partialState);
    }
    return this.stack[this.stackHeadPosition];
  }
  release(state) {
    const topStackState = this.stack[this.stackHeadPosition];
    if (topStackState !== state) {
      throw new Error("Invalid stack state. Released state is not on top of the stack.");
    }
    if (state.type === STATE_ARRAY) {
      const partialState = state;
      partialState.size = 0;
      partialState.array = undefined;
      partialState.position = 0;
      partialState.type = undefined;
    }
    if (state.type === STATE_MAP_KEY || state.type === STATE_MAP_VALUE) {
      const partialState = state;
      partialState.size = 0;
      partialState.map = undefined;
      partialState.readCount = 0;
      partialState.type = undefined;
    }
    this.stackHeadPosition--;
  }
  reset() {
    this.stack.length = 0;
    this.stackHeadPosition = -1;
  }
}
var HEAD_BYTE_REQUIRED = -1;
var EMPTY_VIEW = new DataView(new ArrayBuffer(0));
var EMPTY_BYTES = new Uint8Array(EMPTY_VIEW.buffer);
try {
  EMPTY_VIEW.getInt8(0);
} catch (e) {
  if (!(e instanceof RangeError)) {
    throw new Error("This module is not supported in the current JavaScript engine because DataView does not throw RangeError on out-of-bounds access");
  }
}
var MORE_DATA = new RangeError("Insufficient data");
var sharedCachedKeyDecoder = new CachedKeyDecoder;

class Decoder {
  extensionCodec;
  context;
  useBigInt64;
  rawStrings;
  maxStrLength;
  maxBinLength;
  maxArrayLength;
  maxMapLength;
  maxExtLength;
  keyDecoder;
  mapKeyConverter;
  totalPos = 0;
  pos = 0;
  view = EMPTY_VIEW;
  bytes = EMPTY_BYTES;
  headByte = HEAD_BYTE_REQUIRED;
  stack = new StackPool;
  entered = false;
  constructor(options) {
    this.extensionCodec = options?.extensionCodec ?? ExtensionCodec.defaultCodec;
    this.context = options?.context;
    this.useBigInt64 = options?.useBigInt64 ?? false;
    this.rawStrings = options?.rawStrings ?? false;
    this.maxStrLength = options?.maxStrLength ?? UINT32_MAX;
    this.maxBinLength = options?.maxBinLength ?? UINT32_MAX;
    this.maxArrayLength = options?.maxArrayLength ?? UINT32_MAX;
    this.maxMapLength = options?.maxMapLength ?? UINT32_MAX;
    this.maxExtLength = options?.maxExtLength ?? UINT32_MAX;
    this.keyDecoder = options?.keyDecoder !== undefined ? options.keyDecoder : sharedCachedKeyDecoder;
    this.mapKeyConverter = options?.mapKeyConverter ?? mapKeyConverter;
  }
  clone() {
    return new Decoder({
      extensionCodec: this.extensionCodec,
      context: this.context,
      useBigInt64: this.useBigInt64,
      rawStrings: this.rawStrings,
      maxStrLength: this.maxStrLength,
      maxBinLength: this.maxBinLength,
      maxArrayLength: this.maxArrayLength,
      maxMapLength: this.maxMapLength,
      maxExtLength: this.maxExtLength,
      keyDecoder: this.keyDecoder
    });
  }
  reinitializeState() {
    this.totalPos = 0;
    this.headByte = HEAD_BYTE_REQUIRED;
    this.stack.reset();
  }
  setBuffer(buffer) {
    const bytes = ensureUint8Array(buffer);
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.pos = 0;
  }
  appendBuffer(buffer) {
    if (this.headByte === HEAD_BYTE_REQUIRED && !this.hasRemaining(1)) {
      this.setBuffer(buffer);
    } else {
      const remainingData = this.bytes.subarray(this.pos);
      const newData = ensureUint8Array(buffer);
      const newBuffer = new Uint8Array(remainingData.length + newData.length);
      newBuffer.set(remainingData);
      newBuffer.set(newData, remainingData.length);
      this.setBuffer(newBuffer);
    }
  }
  hasRemaining(size) {
    return this.view.byteLength - this.pos >= size;
  }
  createExtraByteError(posToShow) {
    const { view, pos } = this;
    return new RangeError(`Extra ${view.byteLength - pos} of ${view.byteLength} byte(s) found at buffer[${posToShow}]`);
  }
  decode(buffer) {
    if (this.entered) {
      const instance = this.clone();
      return instance.decode(buffer);
    }
    try {
      this.entered = true;
      this.reinitializeState();
      this.setBuffer(buffer);
      const object = this.doDecodeSync();
      if (this.hasRemaining(1)) {
        throw this.createExtraByteError(this.pos);
      }
      return object;
    } finally {
      this.entered = false;
    }
  }
  *decodeMulti(buffer) {
    if (this.entered) {
      const instance = this.clone();
      yield* instance.decodeMulti(buffer);
      return;
    }
    try {
      this.entered = true;
      this.reinitializeState();
      this.setBuffer(buffer);
      while (this.hasRemaining(1)) {
        yield this.doDecodeSync();
      }
    } finally {
      this.entered = false;
    }
  }
  async decodeAsync(stream) {
    if (this.entered) {
      const instance = this.clone();
      return instance.decodeAsync(stream);
    }
    try {
      this.entered = true;
      let decoded = false;
      let object;
      for await (const buffer of stream) {
        if (decoded) {
          this.entered = false;
          throw this.createExtraByteError(this.totalPos);
        }
        this.appendBuffer(buffer);
        try {
          object = this.doDecodeSync();
          decoded = true;
        } catch (e) {
          if (!(e instanceof RangeError)) {
            throw e;
          }
        }
        this.totalPos += this.pos;
      }
      if (decoded) {
        if (this.hasRemaining(1)) {
          throw this.createExtraByteError(this.totalPos);
        }
        return object;
      }
      const { headByte, pos, totalPos } = this;
      throw new RangeError(`Insufficient data in parsing ${prettyByte(headByte)} at ${totalPos} (${pos} in the current buffer)`);
    } finally {
      this.entered = false;
    }
  }
  decodeArrayStream(stream) {
    return this.decodeMultiAsync(stream, true);
  }
  decodeStream(stream) {
    return this.decodeMultiAsync(stream, false);
  }
  async* decodeMultiAsync(stream, isArray) {
    if (this.entered) {
      const instance = this.clone();
      yield* instance.decodeMultiAsync(stream, isArray);
      return;
    }
    try {
      this.entered = true;
      let isArrayHeaderRequired = isArray;
      let arrayItemsLeft = -1;
      for await (const buffer of stream) {
        if (isArray && arrayItemsLeft === 0) {
          throw this.createExtraByteError(this.totalPos);
        }
        this.appendBuffer(buffer);
        if (isArrayHeaderRequired) {
          arrayItemsLeft = this.readArraySize();
          isArrayHeaderRequired = false;
          this.complete();
        }
        try {
          while (true) {
            yield this.doDecodeSync();
            if (--arrayItemsLeft === 0) {
              break;
            }
          }
        } catch (e) {
          if (!(e instanceof RangeError)) {
            throw e;
          }
        }
        this.totalPos += this.pos;
      }
    } finally {
      this.entered = false;
    }
  }
  doDecodeSync() {
    DECODE:
      while (true) {
        const headByte = this.readHeadByte();
        let object;
        if (headByte >= 224) {
          object = headByte - 256;
        } else if (headByte < 192) {
          if (headByte < 128) {
            object = headByte;
          } else if (headByte < 144) {
            const size = headByte - 128;
            if (size !== 0) {
              this.pushMapState(size);
              this.complete();
              continue DECODE;
            } else {
              object = {};
            }
          } else if (headByte < 160) {
            const size = headByte - 144;
            if (size !== 0) {
              this.pushArrayState(size);
              this.complete();
              continue DECODE;
            } else {
              object = [];
            }
          } else {
            const byteLength = headByte - 160;
            object = this.decodeString(byteLength, 0);
          }
        } else if (headByte === 192) {
          object = null;
        } else if (headByte === 194) {
          object = false;
        } else if (headByte === 195) {
          object = true;
        } else if (headByte === 202) {
          object = this.readF32();
        } else if (headByte === 203) {
          object = this.readF64();
        } else if (headByte === 204) {
          object = this.readU8();
        } else if (headByte === 205) {
          object = this.readU16();
        } else if (headByte === 206) {
          object = this.readU32();
        } else if (headByte === 207) {
          if (this.useBigInt64) {
            object = this.readU64AsBigInt();
          } else {
            object = this.readU64();
          }
        } else if (headByte === 208) {
          object = this.readI8();
        } else if (headByte === 209) {
          object = this.readI16();
        } else if (headByte === 210) {
          object = this.readI32();
        } else if (headByte === 211) {
          if (this.useBigInt64) {
            object = this.readI64AsBigInt();
          } else {
            object = this.readI64();
          }
        } else if (headByte === 217) {
          const byteLength = this.lookU8();
          object = this.decodeString(byteLength, 1);
        } else if (headByte === 218) {
          const byteLength = this.lookU16();
          object = this.decodeString(byteLength, 2);
        } else if (headByte === 219) {
          const byteLength = this.lookU32();
          object = this.decodeString(byteLength, 4);
        } else if (headByte === 220) {
          const size = this.readU16();
          if (size !== 0) {
            this.pushArrayState(size);
            this.complete();
            continue DECODE;
          } else {
            object = [];
          }
        } else if (headByte === 221) {
          const size = this.readU32();
          if (size !== 0) {
            this.pushArrayState(size);
            this.complete();
            continue DECODE;
          } else {
            object = [];
          }
        } else if (headByte === 222) {
          const size = this.readU16();
          if (size !== 0) {
            this.pushMapState(size);
            this.complete();
            continue DECODE;
          } else {
            object = {};
          }
        } else if (headByte === 223) {
          const size = this.readU32();
          if (size !== 0) {
            this.pushMapState(size);
            this.complete();
            continue DECODE;
          } else {
            object = {};
          }
        } else if (headByte === 196) {
          const size = this.lookU8();
          object = this.decodeBinary(size, 1);
        } else if (headByte === 197) {
          const size = this.lookU16();
          object = this.decodeBinary(size, 2);
        } else if (headByte === 198) {
          const size = this.lookU32();
          object = this.decodeBinary(size, 4);
        } else if (headByte === 212) {
          object = this.decodeExtension(1, 0);
        } else if (headByte === 213) {
          object = this.decodeExtension(2, 0);
        } else if (headByte === 214) {
          object = this.decodeExtension(4, 0);
        } else if (headByte === 215) {
          object = this.decodeExtension(8, 0);
        } else if (headByte === 216) {
          object = this.decodeExtension(16, 0);
        } else if (headByte === 199) {
          const size = this.lookU8();
          object = this.decodeExtension(size, 1);
        } else if (headByte === 200) {
          const size = this.lookU16();
          object = this.decodeExtension(size, 2);
        } else if (headByte === 201) {
          const size = this.lookU32();
          object = this.decodeExtension(size, 4);
        } else {
          throw new DecodeError(`Unrecognized type byte: ${prettyByte(headByte)}`);
        }
        this.complete();
        const stack = this.stack;
        while (stack.length > 0) {
          const state = stack.top();
          if (state.type === STATE_ARRAY) {
            state.array[state.position] = object;
            state.position++;
            if (state.position === state.size) {
              object = state.array;
              stack.release(state);
            } else {
              continue DECODE;
            }
          } else if (state.type === STATE_MAP_KEY) {
            if (object === "__proto__") {
              throw new DecodeError("The key __proto__ is not allowed");
            }
            state.key = this.mapKeyConverter(object);
            state.type = STATE_MAP_VALUE;
            continue DECODE;
          } else {
            state.map[state.key] = object;
            state.readCount++;
            if (state.readCount === state.size) {
              object = state.map;
              stack.release(state);
            } else {
              state.key = null;
              state.type = STATE_MAP_KEY;
              continue DECODE;
            }
          }
        }
        return object;
      }
  }
  readHeadByte() {
    if (this.headByte === HEAD_BYTE_REQUIRED) {
      this.headByte = this.readU8();
    }
    return this.headByte;
  }
  complete() {
    this.headByte = HEAD_BYTE_REQUIRED;
  }
  readArraySize() {
    const headByte = this.readHeadByte();
    switch (headByte) {
      case 220:
        return this.readU16();
      case 221:
        return this.readU32();
      default: {
        if (headByte < 160) {
          return headByte - 144;
        } else {
          throw new DecodeError(`Unrecognized array type byte: ${prettyByte(headByte)}`);
        }
      }
    }
  }
  pushMapState(size) {
    if (size > this.maxMapLength) {
      throw new DecodeError(`Max length exceeded: map length (${size}) > maxMapLengthLength (${this.maxMapLength})`);
    }
    this.stack.pushMapState(size);
  }
  pushArrayState(size) {
    if (size > this.maxArrayLength) {
      throw new DecodeError(`Max length exceeded: array length (${size}) > maxArrayLength (${this.maxArrayLength})`);
    }
    this.stack.pushArrayState(size);
  }
  decodeString(byteLength, headerOffset) {
    if (!this.rawStrings || this.stateIsMapKey()) {
      return this.decodeUtf8String(byteLength, headerOffset);
    }
    return this.decodeBinary(byteLength, headerOffset);
  }
  decodeUtf8String(byteLength, headerOffset) {
    if (byteLength > this.maxStrLength) {
      throw new DecodeError(`Max length exceeded: UTF-8 byte length (${byteLength}) > maxStrLength (${this.maxStrLength})`);
    }
    if (this.bytes.byteLength < this.pos + headerOffset + byteLength) {
      throw MORE_DATA;
    }
    const offset = this.pos + headerOffset;
    let object;
    if (this.stateIsMapKey() && this.keyDecoder?.canBeCached(byteLength)) {
      object = this.keyDecoder.decode(this.bytes, offset, byteLength);
    } else {
      object = utf8Decode(this.bytes, offset, byteLength);
    }
    this.pos += headerOffset + byteLength;
    return object;
  }
  stateIsMapKey() {
    if (this.stack.length > 0) {
      const state = this.stack.top();
      return state.type === STATE_MAP_KEY;
    }
    return false;
  }
  decodeBinary(byteLength, headOffset) {
    if (byteLength > this.maxBinLength) {
      throw new DecodeError(`Max length exceeded: bin length (${byteLength}) > maxBinLength (${this.maxBinLength})`);
    }
    if (!this.hasRemaining(byteLength + headOffset)) {
      throw MORE_DATA;
    }
    const offset = this.pos + headOffset;
    const object = this.bytes.subarray(offset, offset + byteLength);
    this.pos += headOffset + byteLength;
    return object;
  }
  decodeExtension(size, headOffset) {
    if (size > this.maxExtLength) {
      throw new DecodeError(`Max length exceeded: ext length (${size}) > maxExtLength (${this.maxExtLength})`);
    }
    const extType = this.view.getInt8(this.pos + headOffset);
    const data = this.decodeBinary(size, headOffset + 1);
    return this.extensionCodec.decode(data, extType, this.context);
  }
  lookU8() {
    return this.view.getUint8(this.pos);
  }
  lookU16() {
    return this.view.getUint16(this.pos);
  }
  lookU32() {
    return this.view.getUint32(this.pos);
  }
  readU8() {
    const value = this.view.getUint8(this.pos);
    this.pos++;
    return value;
  }
  readI8() {
    const value = this.view.getInt8(this.pos);
    this.pos++;
    return value;
  }
  readU16() {
    const value = this.view.getUint16(this.pos);
    this.pos += 2;
    return value;
  }
  readI16() {
    const value = this.view.getInt16(this.pos);
    this.pos += 2;
    return value;
  }
  readU32() {
    const value = this.view.getUint32(this.pos);
    this.pos += 4;
    return value;
  }
  readI32() {
    const value = this.view.getInt32(this.pos);
    this.pos += 4;
    return value;
  }
  readU64() {
    const value = getUint64(this.view, this.pos);
    this.pos += 8;
    return value;
  }
  readI64() {
    const value = getInt64(this.view, this.pos);
    this.pos += 8;
    return value;
  }
  readU64AsBigInt() {
    const value = this.view.getBigUint64(this.pos);
    this.pos += 8;
    return value;
  }
  readI64AsBigInt() {
    const value = this.view.getBigInt64(this.pos);
    this.pos += 8;
    return value;
  }
  readF32() {
    const value = this.view.getFloat32(this.pos);
    this.pos += 4;
    return value;
  }
  readF64() {
    const value = this.view.getFloat64(this.pos);
    this.pos += 8;
    return value;
  }
}
// node_modules/@msgpack/msgpack/dist.esm/Encoder.mjs
var DEFAULT_MAX_DEPTH = 100;
var DEFAULT_INITIAL_BUFFER_SIZE = 2048;

class Encoder {
  extensionCodec;
  context;
  useBigInt64;
  maxDepth;
  initialBufferSize;
  sortKeys;
  forceFloat32;
  ignoreUndefined;
  forceIntegerToFloat;
  pos;
  view;
  bytes;
  entered = false;
  constructor(options) {
    this.extensionCodec = options?.extensionCodec ?? ExtensionCodec.defaultCodec;
    this.context = options?.context;
    this.useBigInt64 = options?.useBigInt64 ?? false;
    this.maxDepth = options?.maxDepth ?? DEFAULT_MAX_DEPTH;
    this.initialBufferSize = options?.initialBufferSize ?? DEFAULT_INITIAL_BUFFER_SIZE;
    this.sortKeys = options?.sortKeys ?? false;
    this.forceFloat32 = options?.forceFloat32 ?? false;
    this.ignoreUndefined = options?.ignoreUndefined ?? false;
    this.forceIntegerToFloat = options?.forceIntegerToFloat ?? false;
    this.pos = 0;
    this.view = new DataView(new ArrayBuffer(this.initialBufferSize));
    this.bytes = new Uint8Array(this.view.buffer);
  }
  clone() {
    return new Encoder({
      extensionCodec: this.extensionCodec,
      context: this.context,
      useBigInt64: this.useBigInt64,
      maxDepth: this.maxDepth,
      initialBufferSize: this.initialBufferSize,
      sortKeys: this.sortKeys,
      forceFloat32: this.forceFloat32,
      ignoreUndefined: this.ignoreUndefined,
      forceIntegerToFloat: this.forceIntegerToFloat
    });
  }
  reinitializeState() {
    this.pos = 0;
  }
  encodeSharedRef(object) {
    if (this.entered) {
      const instance = this.clone();
      return instance.encodeSharedRef(object);
    }
    try {
      this.entered = true;
      this.reinitializeState();
      this.doEncode(object, 1);
      return this.bytes.subarray(0, this.pos);
    } finally {
      this.entered = false;
    }
  }
  encode(object) {
    if (this.entered) {
      const instance = this.clone();
      return instance.encode(object);
    }
    try {
      this.entered = true;
      this.reinitializeState();
      this.doEncode(object, 1);
      return this.bytes.slice(0, this.pos);
    } finally {
      this.entered = false;
    }
  }
  doEncode(object, depth) {
    if (depth > this.maxDepth) {
      throw new Error(`Too deep objects in depth ${depth}`);
    }
    if (object == null) {
      this.encodeNil();
    } else if (typeof object === "boolean") {
      this.encodeBoolean(object);
    } else if (typeof object === "number") {
      if (!this.forceIntegerToFloat) {
        this.encodeNumber(object);
      } else {
        this.encodeNumberAsFloat(object);
      }
    } else if (typeof object === "string") {
      this.encodeString(object);
    } else if (this.useBigInt64 && typeof object === "bigint") {
      this.encodeBigInt64(object);
    } else {
      this.encodeObject(object, depth);
    }
  }
  ensureBufferSizeToWrite(sizeToWrite) {
    const requiredSize = this.pos + sizeToWrite;
    if (this.view.byteLength < requiredSize) {
      this.resizeBuffer(requiredSize * 2);
    }
  }
  resizeBuffer(newSize) {
    const newBuffer = new ArrayBuffer(newSize);
    const newBytes = new Uint8Array(newBuffer);
    const newView = new DataView(newBuffer);
    newBytes.set(this.bytes);
    this.view = newView;
    this.bytes = newBytes;
  }
  encodeNil() {
    this.writeU8(192);
  }
  encodeBoolean(object) {
    if (object === false) {
      this.writeU8(194);
    } else {
      this.writeU8(195);
    }
  }
  encodeNumber(object) {
    if (!this.forceIntegerToFloat && Number.isSafeInteger(object)) {
      if (object >= 0) {
        if (object < 128) {
          this.writeU8(object);
        } else if (object < 256) {
          this.writeU8(204);
          this.writeU8(object);
        } else if (object < 65536) {
          this.writeU8(205);
          this.writeU16(object);
        } else if (object < 4294967296) {
          this.writeU8(206);
          this.writeU32(object);
        } else if (!this.useBigInt64) {
          this.writeU8(207);
          this.writeU64(object);
        } else {
          this.encodeNumberAsFloat(object);
        }
      } else {
        if (object >= -32) {
          this.writeU8(224 | object + 32);
        } else if (object >= -128) {
          this.writeU8(208);
          this.writeI8(object);
        } else if (object >= -32768) {
          this.writeU8(209);
          this.writeI16(object);
        } else if (object >= -2147483648) {
          this.writeU8(210);
          this.writeI32(object);
        } else if (!this.useBigInt64) {
          this.writeU8(211);
          this.writeI64(object);
        } else {
          this.encodeNumberAsFloat(object);
        }
      }
    } else {
      this.encodeNumberAsFloat(object);
    }
  }
  encodeNumberAsFloat(object) {
    if (this.forceFloat32) {
      this.writeU8(202);
      this.writeF32(object);
    } else {
      this.writeU8(203);
      this.writeF64(object);
    }
  }
  encodeBigInt64(object) {
    if (object >= BigInt(0)) {
      this.writeU8(207);
      this.writeBigUint64(object);
    } else {
      this.writeU8(211);
      this.writeBigInt64(object);
    }
  }
  writeStringHeader(byteLength) {
    if (byteLength < 32) {
      this.writeU8(160 + byteLength);
    } else if (byteLength < 256) {
      this.writeU8(217);
      this.writeU8(byteLength);
    } else if (byteLength < 65536) {
      this.writeU8(218);
      this.writeU16(byteLength);
    } else if (byteLength < 4294967296) {
      this.writeU8(219);
      this.writeU32(byteLength);
    } else {
      throw new Error(`Too long string: ${byteLength} bytes in UTF-8`);
    }
  }
  encodeString(object) {
    const maxHeaderSize = 1 + 4;
    const byteLength = utf8Count(object);
    this.ensureBufferSizeToWrite(maxHeaderSize + byteLength);
    this.writeStringHeader(byteLength);
    utf8Encode(object, this.bytes, this.pos);
    this.pos += byteLength;
  }
  encodeObject(object, depth) {
    const ext = this.extensionCodec.tryToEncode(object, this.context);
    if (ext != null) {
      this.encodeExtension(ext);
    } else if (Array.isArray(object)) {
      this.encodeArray(object, depth);
    } else if (ArrayBuffer.isView(object)) {
      this.encodeBinary(object);
    } else if (typeof object === "object") {
      this.encodeMap(object, depth);
    } else {
      throw new Error(`Unrecognized object: ${Object.prototype.toString.apply(object)}`);
    }
  }
  encodeBinary(object) {
    const size = object.byteLength;
    if (size < 256) {
      this.writeU8(196);
      this.writeU8(size);
    } else if (size < 65536) {
      this.writeU8(197);
      this.writeU16(size);
    } else if (size < 4294967296) {
      this.writeU8(198);
      this.writeU32(size);
    } else {
      throw new Error(`Too large binary: ${size}`);
    }
    const bytes = ensureUint8Array(object);
    this.writeU8a(bytes);
  }
  encodeArray(object, depth) {
    const size = object.length;
    if (size < 16) {
      this.writeU8(144 + size);
    } else if (size < 65536) {
      this.writeU8(220);
      this.writeU16(size);
    } else if (size < 4294967296) {
      this.writeU8(221);
      this.writeU32(size);
    } else {
      throw new Error(`Too large array: ${size}`);
    }
    for (const item of object) {
      this.doEncode(item, depth + 1);
    }
  }
  countWithoutUndefined(object, keys) {
    let count = 0;
    for (const key of keys) {
      if (object[key] !== undefined) {
        count++;
      }
    }
    return count;
  }
  encodeMap(object, depth) {
    const keys = Object.keys(object);
    if (this.sortKeys) {
      keys.sort();
    }
    const size = this.ignoreUndefined ? this.countWithoutUndefined(object, keys) : keys.length;
    if (size < 16) {
      this.writeU8(128 + size);
    } else if (size < 65536) {
      this.writeU8(222);
      this.writeU16(size);
    } else if (size < 4294967296) {
      this.writeU8(223);
      this.writeU32(size);
    } else {
      throw new Error(`Too large map object: ${size}`);
    }
    for (const key of keys) {
      const value = object[key];
      if (!(this.ignoreUndefined && value === undefined)) {
        this.encodeString(key);
        this.doEncode(value, depth + 1);
      }
    }
  }
  encodeExtension(ext) {
    if (typeof ext.data === "function") {
      const data = ext.data(this.pos + 6);
      const size2 = data.length;
      if (size2 >= 4294967296) {
        throw new Error(`Too large extension object: ${size2}`);
      }
      this.writeU8(201);
      this.writeU32(size2);
      this.writeI8(ext.type);
      this.writeU8a(data);
      return;
    }
    const size = ext.data.length;
    if (size === 1) {
      this.writeU8(212);
    } else if (size === 2) {
      this.writeU8(213);
    } else if (size === 4) {
      this.writeU8(214);
    } else if (size === 8) {
      this.writeU8(215);
    } else if (size === 16) {
      this.writeU8(216);
    } else if (size < 256) {
      this.writeU8(199);
      this.writeU8(size);
    } else if (size < 65536) {
      this.writeU8(200);
      this.writeU16(size);
    } else if (size < 4294967296) {
      this.writeU8(201);
      this.writeU32(size);
    } else {
      throw new Error(`Too large extension object: ${size}`);
    }
    this.writeI8(ext.type);
    this.writeU8a(ext.data);
  }
  writeU8(value) {
    this.ensureBufferSizeToWrite(1);
    this.view.setUint8(this.pos, value);
    this.pos++;
  }
  writeU8a(values) {
    const size = values.length;
    this.ensureBufferSizeToWrite(size);
    this.bytes.set(values, this.pos);
    this.pos += size;
  }
  writeI8(value) {
    this.ensureBufferSizeToWrite(1);
    this.view.setInt8(this.pos, value);
    this.pos++;
  }
  writeU16(value) {
    this.ensureBufferSizeToWrite(2);
    this.view.setUint16(this.pos, value);
    this.pos += 2;
  }
  writeI16(value) {
    this.ensureBufferSizeToWrite(2);
    this.view.setInt16(this.pos, value);
    this.pos += 2;
  }
  writeU32(value) {
    this.ensureBufferSizeToWrite(4);
    this.view.setUint32(this.pos, value);
    this.pos += 4;
  }
  writeI32(value) {
    this.ensureBufferSizeToWrite(4);
    this.view.setInt32(this.pos, value);
    this.pos += 4;
  }
  writeF32(value) {
    this.ensureBufferSizeToWrite(4);
    this.view.setFloat32(this.pos, value);
    this.pos += 4;
  }
  writeF64(value) {
    this.ensureBufferSizeToWrite(8);
    this.view.setFloat64(this.pos, value);
    this.pos += 8;
  }
  writeU64(value) {
    this.ensureBufferSizeToWrite(8);
    setUint64(this.view, this.pos, value);
    this.pos += 8;
  }
  writeI64(value) {
    this.ensureBufferSizeToWrite(8);
    setInt64(this.view, this.pos, value);
    this.pos += 8;
  }
  writeBigUint64(value) {
    this.ensureBufferSizeToWrite(8);
    this.view.setBigUint64(this.pos, value);
    this.pos += 8;
  }
  writeBigInt64(value) {
    this.ensureBufferSizeToWrite(8);
    this.view.setBigInt64(this.pos, value);
    this.pos += 8;
  }
}
// src/protocol.ts
var PROTOCOL_VERSION = 1;
var LENGTH_PREFIX_BYTES = 4;
var MAX_FRAME_BYTES = 64 * 1024;
var MAX_IDENTIFIER_BYTES = 64;
var MAX_CORRELATION_BYTES = 128;
var MAX_BODY_BYTES = MAX_FRAME_BYTES - 512;
var SERVER_FRAME_TYPES = [
  "ready",
  "peers",
  "message",
  "receipt",
  "pong",
  "error"
];
function isServerFrameType(value) {
  return SERVER_FRAME_TYPES.includes(value);
}
var encoder = new Encoder({ ignoreUndefined: true });
var decoder = new Decoder({
  maxStrLength: MAX_FRAME_BYTES,
  maxBinLength: 0,
  maxExtLength: 0
});
var textEncoder = new TextEncoder;
function utf8Length(value) {
  return textEncoder.encode(value).length;
}

class EncodeError extends Error {
  name = "EncodeError";
}
function encodePayload(frame) {
  const payload = encoder.encode(frame);
  if (payload.length > MAX_FRAME_BYTES) {
    throw new EncodeError(`${frame.type} frame is ${payload.length} bytes, over the ${MAX_FRAME_BYTES}-byte cap`);
  }
  return payload;
}
function encodeFrame(frame) {
  const payload = encodePayload(frame);
  const framed = new Uint8Array(LENGTH_PREFIX_BYTES + payload.length);
  new DataView(framed.buffer).setUint32(0, payload.length, false);
  framed.set(payload, LENGTH_PREFIX_BYTES);
  return framed;
}
function decodePayload(payload) {
  try {
    return { ok: true, value: decoder.decode(payload) };
  } catch (error) {
    return { ok: false, detail: describe(error) };
  }
}
function describe(error) {
  try {
    return String(error instanceof Error ? error.message : error);
  } catch {
    return "an error that could not be described";
  }
}
var NO_VALUES = [];
var EMPTY = new Uint8Array(0);

class FrameAccumulator {
  #buffer = EMPTY;
  #failure = null;
  get buffered() {
    return this.#buffer.length;
  }
  push(chunk) {
    if (this.#failure !== null) {
      return { values: NO_VALUES, failure: this.#failure };
    }
    const buffer = this.#buffer.length === 0 ? chunk : concat(this.#buffer, chunk);
    const values = [];
    let offset = 0;
    while (buffer.length - offset >= LENGTH_PREFIX_BYTES) {
      const declared = new DataView(buffer.buffer, buffer.byteOffset + offset, LENGTH_PREFIX_BYTES).getUint32(0, false);
      if (declared === 0) {
        return this.#fail("zero_length", "a length prefix declared 0 bytes", values);
      }
      if (declared > MAX_FRAME_BYTES) {
        return this.#fail("oversized", `a length prefix declared ${declared} bytes, over the ${MAX_FRAME_BYTES}-byte cap`, values);
      }
      const end = offset + LENGTH_PREFIX_BYTES + declared;
      if (buffer.length < end) {
        break;
      }
      const decoded = decodePayload(buffer.subarray(offset + LENGTH_PREFIX_BYTES, end));
      if (!decoded.ok) {
        return this.#fail("undecodable", `a ${declared}-byte payload did not decode as one MessagePack value: ${decoded.detail}`, values);
      }
      values.push(decoded.value);
      offset = end;
    }
    this.#buffer = offset === buffer.length ? EMPTY : buffer.subarray(offset).slice();
    return { values, failure: null };
  }
  #fail(reason, detail, values) {
    this.#failure = { reason, detail };
    this.#buffer = EMPTY;
    return { values, failure: this.#failure };
  }
}
function concat(head, tail) {
  const joined = new Uint8Array(head.length + tail.length);
  joined.set(head);
  joined.set(tail, head.length);
  return joined;
}
function validateServerFrame(value) {
  const map = asRecord(value);
  if (map === null) {
    return {
      kind: "invalid",
      reason: `payload is ${describeType(value)}, not a map keyed by field name`
    };
  }
  const type = map["type"];
  if (typeof type !== "string") {
    return {
      kind: "invalid",
      reason: `frame discriminator "type" is ${describeType(type)}, not a string`
    };
  }
  if (!isServerFrameType(type)) {
    return { kind: "ignorable", type };
  }
  switch (type) {
    case "pong":
      return { kind: "frame", frame: { type: "pong" } };
    case "ready": {
      const protocol = map["protocol"];
      if (typeof protocol !== "number" || !Number.isInteger(protocol) || protocol < 0 || protocol > 4294967295) {
        return fieldInvalid("ready", "protocol", protocol, "a u32");
      }
      return { kind: "frame", frame: { type: "ready", protocol } };
    }
    case "peers": {
      const requestId = map["request_id"];
      if (!isNonEmptyString(requestId)) {
        return fieldInvalid("peers", "request_id", requestId);
      }
      const peers = map["peers"];
      if (!Array.isArray(peers) || !peers.every((peer) => typeof peer === "string")) {
        return fieldInvalid("peers", "peers", peers, "an array of strings");
      }
      return {
        kind: "frame",
        frame: {
          type: "peers",
          request_id: requestId,
          peers
        }
      };
    }
    case "message": {
      const id = map["id"];
      if (!isNonEmptyString(id))
        return fieldInvalid("message", "id", id);
      const from = map["from"];
      if (!isNonEmptyString(from))
        return fieldInvalid("message", "from", from);
      const body = map["body"];
      if (typeof body !== "string") {
        return fieldInvalid("message", "body", body, "a string");
      }
      const replyTo = optionalString(map, "reply_to");
      if (replyTo.kind === "invalid") {
        return fieldInvalid("message", "reply_to", map["reply_to"], "a string");
      }
      return {
        kind: "frame",
        frame: replyTo.value === null ? { type: "message", id, from, body } : { type: "message", id, from, body, reply_to: replyTo.value }
      };
    }
    case "receipt": {
      const id = map["id"];
      if (!isNonEmptyString(id))
        return fieldInvalid("receipt", "id", id);
      const to = map["to"];
      if (typeof to !== "string") {
        return fieldInvalid("receipt", "to", to, "a string");
      }
      const status = map["status"];
      if (!isNonEmptyString(status)) {
        return fieldInvalid("receipt", "status", status);
      }
      return { kind: "frame", frame: { type: "receipt", id, to, status } };
    }
    case "error": {
      const code = map["code"];
      if (!isNonEmptyString(code))
        return fieldInvalid("error", "code", code);
      const message = optionalString(map, "message");
      if (message.kind === "invalid") {
        return fieldInvalid("error", "message", map["message"], "a string");
      }
      const requestId = optionalString(map, "request_id");
      if (requestId.kind === "invalid") {
        return fieldInvalid("error", "request_id", map["request_id"], "a string");
      }
      return {
        kind: "frame",
        frame: {
          type: "error",
          code,
          ...message.value === null ? {} : { message: message.value },
          ...requestId.value === null ? {} : { request_id: requestId.value }
        }
      };
    }
    default: {
      const unhandled = type;
      return {
        kind: "invalid",
        reason: `unhandled server frame type: ${String(unhandled)}`
      };
    }
  }
}
function asRecord(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  return value;
}
function fieldInvalid(frame, field, actual, expected = "a non-empty string") {
  return {
    kind: "invalid",
    reason: `${frame}.${field} is ${describeType(actual)}, expected ${expected}`
  };
}
function optionalString(map, field) {
  const value = map[field];
  if (value === undefined || value === null) {
    return { kind: "ok", value: null };
  }
  if (typeof value !== "string") {
    return { kind: "invalid" };
  }
  return { kind: "ok", value };
}
function isNonEmptyString(value) {
  return typeof value === "string" && value.length > 0;
}
function describeType(value) {
  if (value === undefined)
    return "absent";
  if (value === null)
    return "nil";
  if (Array.isArray(value))
    return "an array";
  if (value instanceof Uint8Array)
    return "binary";
  return `a ${typeof value}`;
}
function describeIdentifierProblem(problem) {
  switch (problem.kind) {
    case "empty":
      return "must not be empty";
    case "too_long":
      return `must be at most ${problem.limit} UTF-8 bytes, found ${problem.found}`;
    case "reserved_separator":
      return `must not contain the reserved separator "${problem.separator}"`;
    case "surrounding_whitespace":
      return "must not begin or end with whitespace";
  }
}
function identifierProblem(value) {
  if (value.length === 0) {
    return { kind: "empty" };
  }
  const bytes = utf8Length(value);
  if (bytes > MAX_IDENTIFIER_BYTES) {
    return { kind: "too_long", limit: MAX_IDENTIFIER_BYTES, found: bytes };
  }
  for (const character of value) {
    if (character === "/" || character === "@") {
      return { kind: "reserved_separator", separator: character };
    }
  }
  if (/^\p{White_Space}|\p{White_Space}$/u.test(value)) {
    return { kind: "surrounding_whitespace" };
  }
  return null;
}
function correlationProblem(value) {
  if (value.length === 0) {
    return { kind: "empty" };
  }
  const bytes = utf8Length(value);
  if (bytes > MAX_CORRELATION_BYTES) {
    return { kind: "too_long", limit: MAX_CORRELATION_BYTES, found: bytes };
  }
  return null;
}
function bodyOverBudget(body) {
  const bytes = utf8Length(body);
  return bytes > MAX_BODY_BYTES ? bytes : null;
}

// src/client.ts
var REQUEST_TIMEOUT_MS = 5000;
var HEARTBEAT_INTERVAL_MS = 30000;
var HANDSHAKE_TIMEOUT_MS = 1e4;
var RECONNECT_INITIAL_MS = 500;
var RECONNECT_CAP_MS = 30000;
var RECONNECT_JITTER = 0.2;
var PEER_REPLACED_REPORT = "another session registered this peer name in this room, so the relay displaced this one; " + "reconnecting would displace that session in turn, so OMP Relay has stopped. " + "Give each session its own peer name in its configuration.";
var ambientScheduler = {
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval
};

class RequestFailed extends Error {
  name = "RequestFailed";
  reason;
  code;
  constructor(reason, message, code = null) {
    super(message);
    this.reason = reason;
    this.code = code;
  }
}
function backoffDelay(attempt, random) {
  const exponential = RECONNECT_INITIAL_MS * 2 ** Math.max(0, attempt - 1);
  const base = Math.min(exponential, RECONNECT_CAP_MS);
  const jittered = base * (1 + RECONNECT_JITTER * (random() * 2 - 1));
  return Math.min(Math.round(jittered), RECONNECT_CAP_MS);
}
function asThenable(value) {
  const candidate = value;
  return typeof candidate?.then === "function" ? candidate : null;
}

class RelayClient {
  #config;
  #scheduler;
  #handlers;
  #pending = new Map;
  #timedOut = new Set;
  #state = "stopped";
  #stopped = true;
  #generation = 0;
  #socket = null;
  #accumulator = null;
  #heartbeat = null;
  #reconnect = null;
  #handshake = null;
  #attempt = 0;
  #outageReported = false;
  #statedCause = null;
  constructor(options) {
    this.#config = options.config;
    this.#scheduler = options.scheduler ?? ambientScheduler;
    this.#handlers = options.handlers ?? {};
  }
  get state() {
    return this.#state;
  }
  get pendingRequests() {
    return this.#pending.size;
  }
  start() {
    if (!this.#stopped) {
      return;
    }
    this.#stopped = false;
    this.#attempt = 0;
    this.#outageReported = false;
    this.#openConnection();
  }
  async stop() {
    this.#stopped = true;
    this.#generation += 1;
    this.#state = "stopped";
    this.#cancelReconnect();
    this.#clearHandshakeDeadline();
    this.#clearHeartbeat();
    this.#timedOut.clear();
    this.#failPending(new RequestFailed("stopped", "the client was stopped"));
    const socket = this.#socket;
    this.#socket = null;
    this.#accumulator = null;
    if (socket === null) {
      return;
    }
    socket.removeAllListeners();
    if (socket.destroyed) {
      return;
    }
    const closed = Promise.withResolvers();
    socket.once("close", () => closed.resolve());
    socket.once("error", () => closed.resolve());
    socket.destroy();
    await closed.promise;
  }
  list(requestId = crypto.randomUUID()) {
    const { promise, resolve, reject } = Promise.withResolvers();
    const problem = correlationProblem(requestId);
    if (problem !== null) {
      reject(new RequestFailed("invalid_request", `list request_id ${describeIdentifierProblem(problem)}`));
      return promise;
    }
    this.#issue(requestId, "list", { type: "list", request_id: requestId }, (outcome) => {
      if (!outcome.ok) {
        reject(outcome.error);
      } else if (outcome.frame.type === "peers") {
        resolve(outcome.frame);
      } else {
        reject(new RequestFailed("unexpected_reply", `list ${requestId} was answered with a ${outcome.frame.type} frame`));
      }
    });
    return promise;
  }
  send(request) {
    const id = request.id ?? crypto.randomUUID();
    const { promise, resolve, reject } = Promise.withResolvers();
    const refuse = (message) => {
      reject(new RequestFailed("invalid_request", message));
      return promise;
    };
    const idProblem = correlationProblem(id);
    if (idProblem !== null) {
      return refuse(`send id ${describeIdentifierProblem(idProblem)}`);
    }
    if (request.replyTo !== undefined) {
      const replyProblem = correlationProblem(request.replyTo);
      if (replyProblem !== null) {
        return refuse(`send reply_to ${describeIdentifierProblem(replyProblem)}`);
      }
    }
    const oversized = bodyOverBudget(request.body);
    if (oversized !== null) {
      return refuse(`send body is ${oversized} UTF-8 bytes, over the ${MAX_BODY_BYTES}-byte budget`);
    }
    this.#issue(id, "send", {
      type: "send",
      id,
      to: request.to,
      body: request.body,
      ...request.replyTo === undefined ? {} : { reply_to: request.replyTo }
    }, (outcome) => {
      if (!outcome.ok) {
        reject(outcome.error);
      } else if (outcome.frame.type === "receipt") {
        resolve(outcome.frame);
      } else {
        reject(new RequestFailed("unexpected_reply", `send ${id} was answered with a ${outcome.frame.type} frame`));
      }
    });
    return promise;
  }
  #openConnection() {
    this.#cancelReconnect();
    this.#state = "connecting";
    this.#accumulator = new FrameAccumulator;
    this.#statedCause = null;
    let socket;
    try {
      socket = connect({
        host: this.#config.transport.host,
        port: this.#config.transport.port
      });
    } catch (error) {
      this.#scheduleReconnect(`could not open a connection: ${describe(error)}`);
      return;
    }
    this.#socket = socket;
    socket.setNoDelay(true);
    socket.on("connect", () => {
      this.#guard("connect", () => {
        this.#armHandshakeDeadline(socket);
        this.#write({
          type: "hello",
          protocol: PROTOCOL_VERSION,
          room: this.#config.room,
          peer: this.#config.peer
        });
      });
    });
    socket.on("data", (chunk) => {
      this.#guard("data", () => this.#receive(socket, chunk));
    });
    socket.on("error", (error) => {
      this.#guard("error", () => {
        this.#closeConnection(socket, this.#statedCause ?? describe(error));
      });
    });
    socket.on("close", () => {
      this.#guard("close", () => {
        this.#closeConnection(socket, this.#statedCause ?? "the relay closed the connection");
      });
    });
  }
  #receive(socket, chunk) {
    if (socket !== this.#socket || this.#accumulator === null) {
      return;
    }
    const outcome = this.#accumulator.push(chunk);
    for (const value of outcome.values) {
      const validated = validateServerFrame(value);
      switch (validated.kind) {
        case "frame":
          this.#dispatch(socket, validated.frame);
          break;
        case "ignorable":
          this.#report("info", `ignored an unrecognized ${validated.type} frame`);
          break;
        case "invalid":
          this.#closeConnection(socket, `malformed frame: ${validated.reason}`);
          return;
      }
      if (socket !== this.#socket) {
        return;
      }
    }
    if (outcome.failure !== null) {
      this.#closeConnection(socket, `framing failure (${outcome.failure.reason}): ${outcome.failure.detail}`);
    }
  }
  #dispatch(socket, frame) {
    if (this.#state === "ready") {
      if (frame.type === "ready") {
        this.#closeConnection(socket, "the relay sent a second ready frame");
        return;
      }
    } else if (frame.type !== "ready" && frame.type !== "error") {
      this.#report("warn", `ignored a ${frame.type} frame received before ready`);
      return;
    }
    switch (frame.type) {
      case "ready":
        this.#state = "ready";
        this.#attempt = 0;
        this.#outageReported = false;
        this.#clearHandshakeDeadline();
        this.#armHeartbeat();
        this.#flushUnwritten();
        this.#notify("onReady", () => this.#handlers.onReady?.());
        break;
      case "pong":
        break;
      case "message":
        this.#notify("onMessage", () => this.#handlers.onMessage?.(frame));
        break;
      case "peers":
        this.#settle(frame.request_id, "list", { ok: true, frame });
        break;
      case "receipt":
        this.#settle(frame.id, "send", { ok: true, frame });
        break;
      case "error":
        this.#handleError(socket, frame);
        break;
    }
  }
  #handleError(socket, frame) {
    const detail = frame.message === undefined ? frame.code : `${frame.code}: ${frame.message}`;
    if (frame.code === "peer_replaced") {
      this.#stopped = true;
      this.#report("error", PEER_REPLACED_REPORT);
      this.#closeConnection(socket, PEER_REPLACED_REPORT);
      return;
    }
    if (frame.request_id !== undefined && this.#state === "ready") {
      const settled = this.#settleEither(frame.request_id, {
        ok: false,
        error: new RequestFailed("relay_error", `the relay rejected the request: ${detail}`, frame.code)
      });
      if (settled) {
        return;
      }
    }
    if (this.#state === "ready") {
      this.#statedCause = `the relay reported ${detail}`;
      this.#report("error", `the relay reported ${detail}`);
      return;
    }
    this.#statedCause = `the relay rejected the handshake: ${detail}`;
  }
  #closeConnection(socket, reason) {
    if (socket !== this.#socket) {
      return;
    }
    this.#socket = null;
    this.#accumulator = null;
    this.#clearHandshakeDeadline();
    this.#clearHeartbeat();
    this.#timedOut.clear();
    this.#state = this.#stopped ? "stopped" : "connecting";
    socket.removeAllListeners();
    socket.on("error", () => {});
    socket.destroy();
    this.#failPending(new RequestFailed("disconnected", reason));
    if (!this.#stopped) {
      this.#scheduleReconnect(reason);
    }
    this.#notify("onDisconnect", () => this.#handlers.onDisconnect?.(reason));
  }
  #scheduleReconnect(reason) {
    if (this.#stopped) {
      return;
    }
    this.#attempt += 1;
    const delay = backoffDelay(this.#attempt, Math.random);
    if (!this.#outageReported) {
      this.#outageReported = true;
      this.#report("warn", `${reason}; reconnecting in ${delay} ms`);
    }
    this.#cancelReconnect();
    this.#reconnect = this.#scheduler.setTimeout(() => {
      this.#reconnect = null;
      this.#guard("reconnect", () => {
        if (!this.#stopped) {
          this.#openConnection();
        }
      });
    }, delay);
  }
  #cancelReconnect() {
    if (this.#reconnect !== null) {
      this.#scheduler.clearTimeout(this.#reconnect);
      this.#reconnect = null;
    }
  }
  #armHandshakeDeadline(socket) {
    this.#clearHandshakeDeadline();
    this.#handshake = this.#scheduler.setTimeout(() => {
      this.#handshake = null;
      this.#guard("handshake deadline", () => {
        this.#closeConnection(socket, `no ready within ${HANDSHAKE_TIMEOUT_MS} ms of connecting`);
      });
    }, HANDSHAKE_TIMEOUT_MS);
  }
  #clearHandshakeDeadline() {
    if (this.#handshake !== null) {
      this.#scheduler.clearTimeout(this.#handshake);
      this.#handshake = null;
    }
  }
  #armHeartbeat() {
    this.#clearHeartbeat();
    this.#heartbeat = this.#scheduler.setInterval(() => {
      this.#guard("heartbeat", () => {
        if (this.#state === "ready") {
          this.#write({ type: "ping" });
        }
      });
    }, HEARTBEAT_INTERVAL_MS);
  }
  #clearHeartbeat() {
    if (this.#heartbeat !== null) {
      this.#scheduler.clearInterval(this.#heartbeat);
      this.#heartbeat = null;
    }
  }
  #issue(token, kind, frame, settle) {
    if (this.#stopped) {
      settle({
        ok: false,
        error: new RequestFailed("stopped", "the client is not running")
      });
      return;
    }
    const key = `${kind}:${token}`;
    if (this.#pending.has(key)) {
      settle({
        ok: false,
        error: new RequestFailed("invalid_request", `a ${kind} with correlation token ${JSON.stringify(token)} is already outstanding`)
      });
      return;
    }
    if (this.#timedOut.has(key)) {
      settle({
        ok: false,
        error: new RequestFailed("invalid_request", `a ${kind} with correlation token ${JSON.stringify(token)} timed out on this connection; its late reply would settle this request instead`)
      });
      return;
    }
    let encoded;
    try {
      encoded = encodeFrame(frame);
    } catch (error) {
      settle({
        ok: false,
        error: new RequestFailed("invalid_request", describe(error))
      });
      return;
    }
    const entry = { kind, settle, timer: null, unwritten: encoded };
    this.#pending.set(key, entry);
    entry.timer = this.#scheduler.setTimeout(() => {
      entry.timer = null;
      this.#guard("request timeout", () => {
        if (entry.unwritten === null) {
          this.#timedOut.add(key);
        }
        this.#settleKey(key, {
          ok: false,
          error: new RequestFailed("timeout", `no reply within ${REQUEST_TIMEOUT_MS} ms`)
        });
      });
    }, REQUEST_TIMEOUT_MS);
    if (this.#state === "ready") {
      this.#writeEncoded(encoded);
      entry.unwritten = null;
    }
  }
  #flushUnwritten() {
    for (const entry of this.#pending.values()) {
      if (entry.unwritten !== null) {
        const encoded = entry.unwritten;
        entry.unwritten = null;
        this.#writeEncoded(encoded);
      }
    }
  }
  #settle(token, kind, outcome) {
    if (!this.#settleKey(`${kind}:${token}`, outcome)) {
      this.#report("info", `discarded a ${kind} reply for unknown correlation token ${JSON.stringify(token)}`);
    }
  }
  #settleEither(token, outcome) {
    return this.#settleKey(`send:${token}`, outcome) || this.#settleKey(`list:${token}`, outcome);
  }
  #settleKey(key, outcome) {
    const entry = this.#pending.get(key);
    if (entry === undefined) {
      return false;
    }
    this.#pending.delete(key);
    if (entry.timer !== null) {
      this.#scheduler.clearTimeout(entry.timer);
      entry.timer = null;
    }
    try {
      entry.settle(outcome);
    } catch (error) {
      this.#report("error", `a request callback threw: ${describe(error)}`);
    }
    return true;
  }
  #failPending(error) {
    for (const key of [...this.#pending.keys()]) {
      this.#settleKey(key, { ok: false, error });
    }
  }
  #write(frame) {
    let encoded;
    try {
      encoded = encodeFrame(frame);
    } catch (error) {
      this.#report("error", `refused to send a ${frame.type} frame: ${describe(error)}`);
      return;
    }
    this.#writeEncoded(encoded);
  }
  #writeEncoded(encoded) {
    const socket = this.#socket;
    if (socket === null) {
      return;
    }
    try {
      socket.write(encoded);
    } catch (error) {
      this.#closeConnection(socket, `write failed: ${describe(error)}`);
      return;
    }
    this.#armHeartbeat();
  }
  #guard(site, work) {
    try {
      work();
    } catch (error) {
      this.#report("error", `${site} handler failed: ${describe(error)}`);
    }
  }
  #notify(site, call) {
    const generation = this.#generation;
    try {
      asThenable(call())?.then(undefined, (error) => {
        if (this.#stopped || generation !== this.#generation) {
          return;
        }
        this.#report("error", `the host's ${site} handler rejected: ${describe(error)}`);
      });
    } catch (error) {
      this.#report("error", `the host's ${site} handler failed: ${describe(error)}`);
    }
  }
  #report(level, message) {
    try {
      const handler = this.#handlers.onReport;
      if (handler === undefined) {
        return;
      }
      const returned = handler({ level, message });
      asThenable(returned)?.then(undefined, () => {});
    } catch {}
  }
}

// src/config.ts
import { readFile } from "fs/promises";
import { join } from "path";
var CONFIG_PATH_ENV = "OMP_RELAY_CONFIG";
var DEFAULT_CONFIG_SEGMENTS = [".omp", "agent", "omp-relay.yml"];
function resolveConfigPath(env) {
  const override = env[CONFIG_PATH_ENV];
  if (override !== undefined && override.length > 0) {
    return { ok: true, path: override };
  }
  const home = env["HOME"];
  if (home === undefined || home.length === 0) {
    return {
      ok: false,
      problem: {
        field: null,
        reason: `neither ${CONFIG_PATH_ENV} nor HOME is set, so there is no configuration path to read`
      }
    };
  }
  return { ok: true, path: join(home, ...DEFAULT_CONFIG_SEGMENTS) };
}
async function loadConfig(env) {
  const resolved = resolveConfigPath(env);
  if (!resolved.ok) {
    return { ok: false, path: null, problem: resolved.problem };
  }
  const path = resolved.path;
  let text;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    const missing = typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
    return {
      ok: false,
      path,
      problem: {
        field: null,
        reason: missing ? `configuration file ${path} does not exist` : `configuration file ${path} could not be read: ${describe(error)}`
      }
    };
  }
  let parsed;
  try {
    parsed = Bun.YAML.parse(text);
  } catch (error) {
    return {
      ok: false,
      path,
      problem: {
        field: null,
        reason: `configuration file ${path} is not valid YAML: ${describe(error)}`
      }
    };
  }
  const validated = validateConfig(parsed);
  if (!validated.ok) {
    return { ok: false, path, problem: validated.problem };
  }
  return { ok: true, path, config: validated.config };
}
function validateConfig(document) {
  const root = asRecord(document);
  if (root === null) {
    return problem(null, `configuration must be a mapping, found ${typeName(document)}`);
  }
  const transport = asRecord(root["transport"]);
  if (transport === null) {
    return problem("transport", `transport must be a mapping, found ${typeName(root["transport"])}`);
  }
  const mode = transport["mode"];
  if (mode !== "local") {
    return problem("transport.mode", `transport.mode must be "local", found ${describeValue(mode)}`);
  }
  const rawAddress = transport["address"];
  if (typeof rawAddress !== "string" || rawAddress.length === 0) {
    return problem("transport.address", `transport.address must be a "host:port" string, found ${typeName(rawAddress)}`);
  }
  const address = parseAddress(rawAddress);
  if (address === null) {
    return problem("transport.address", `transport.address ${describeValue(rawAddress)} is not a "host:port" pair with a port in 1-65535`);
  }
  const room = asRecord(root["room"]);
  if (room === null) {
    return problem("room", `room must be a mapping, found ${typeName(root["room"])}`);
  }
  const project = checkIdentifier(room["project"], "room.project");
  if (!project.ok)
    return project;
  const task = checkIdentifier(room["task"], "room.task");
  if (!task.ok)
    return task;
  const peer = checkIdentifier(root["peer"], "peer");
  if (!peer.ok)
    return peer;
  return {
    ok: true,
    config: {
      transport: { mode: "local", host: address.host, port: address.port },
      room: { project: project.value, task: task.value },
      peer: peer.value
    }
  };
}
function parseAddress(value) {
  let host;
  let portText;
  if (value.startsWith("[")) {
    const close = value.indexOf("]");
    if (close < 2 || value[close + 1] !== ":") {
      return null;
    }
    host = value.slice(1, close);
    portText = value.slice(close + 2);
  } else {
    const separator = value.lastIndexOf(":");
    if (separator <= 0) {
      return null;
    }
    host = value.slice(0, separator);
    portText = value.slice(separator + 1);
    if (host.includes(":")) {
      return null;
    }
  }
  if (host.length === 0 || !/^[0-9]+$/.test(portText)) {
    return null;
  }
  const port = Number(portText);
  if (port < 1 || port > 65535) {
    return null;
  }
  return { host, port };
}
function checkIdentifier(value, field) {
  if (typeof value !== "string") {
    return problem(field, `${field} must be a string, found ${typeName(value)}`);
  }
  const broken = identifierProblem(value);
  if (broken !== null) {
    return problem(field, `${field} ${describeIdentifierProblem(broken)}`);
  }
  return { ok: true, value };
}
function problem(field, reason) {
  return { ok: false, problem: { field, reason } };
}
var DIAGNOSTIC_VALUE_CHARS = 40;
function describeValue(value) {
  if (typeof value === "string") {
    return JSON.stringify(value.length > DIAGNOSTIC_VALUE_CHARS ? `${value.slice(0, DIAGNOSTIC_VALUE_CHARS)}\u2026` : value);
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return typeName(value);
}
function typeName(value) {
  if (value === undefined)
    return "nothing";
  if (value === null)
    return "null";
  if (Array.isArray(value))
    return "a list";
  return `a ${typeof value}`;
}

// src/index.ts
var INBOUND_MESSAGE_TYPE = "io.github.pashifika.omp-relay.message";
var CONTROL_CHARACTERS = /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/gu;
var CONTROL_CHARACTERS_OUTSIDE_TEXT = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u2028\u2029]/gu;
var NEUTRALIZED = "\uFFFD";
var BODY_QUOTE = "> ";
function singleLine(value) {
  return value.replace(CONTROL_CHARACTERS, NEUTRALIZED);
}
function quotedBody(value) {
  return value.replace(CONTROL_CHARACTERS_OUTSIDE_TEXT, NEUTRALIZED).split(`
`).map((line) => `${BODY_QUOTE}${line}`);
}
function result(text, details) {
  return { content: [{ type: "text", text }], details };
}
function validationFailure(message) {
  return result(`Invalid mesh arguments: ${message}`, {
    action: "invalid",
    status: "validation_error"
  });
}
function requestFailure(error) {
  if (error instanceof RequestFailed) {
    const text = error.reason === "stopped" ? "OMP Relay is not ready; no request was sent." : error.reason === "disconnected" ? "OMP Relay disconnected before the request completed." : error.reason === "timeout" ? "OMP Relay did not answer before the request deadline." : `OMP Relay request failed: ${error.message}`;
    return result(text, { status: "request_failed", reason: error.reason });
  }
  return result(`OMP Relay request failed: ${describe(error)}`, {
    status: "request_failed",
    reason: "unknown"
  });
}
function receiptResult(receipt) {
  let text;
  switch (receipt.status) {
    case "routed":
      text = `Message ${receipt.id} was queued for ${receipt.to}; this does not mean the recipient has read, accepted, or answered it.`;
      break;
    case "peer_offline":
      text = `Message ${receipt.id} was not queued because peer ${receipt.to} is offline.`;
      break;
    case "recipient_backpressure":
      text = `Message ${receipt.id} was not queued because peer ${receipt.to}'s queue is full; retry later.`;
      break;
    case "invalid_target":
      text = `Message ${receipt.id} was rejected because ${receipt.to} is not a valid target.`;
      break;
    default:
      text = `Relay returned receipt status ${JSON.stringify(receipt.status)} for message ${receipt.id}.`;
      break;
  }
  return result(text, {
    action: "send",
    id: receipt.id,
    to: receipt.to,
    status: receipt.status
  });
}
async function executeMesh(client, args) {
  if (args.action !== "list" && args.action !== "send") {
    return validationFailure('action must be "list" or "send"');
  }
  if (args.action === "send") {
    if (typeof args.to !== "string") {
      return validationFailure("send requires a string to");
    }
    const targetProblem = identifierProblem(args.to);
    if (targetProblem !== null) {
      return validationFailure(`to ${describeIdentifierProblem(targetProblem)}`);
    }
    if (typeof args.message !== "string") {
      return validationFailure("send requires a string message");
    }
    if (args.reply_to !== undefined) {
      if (typeof args.reply_to !== "string") {
        return validationFailure("reply_to must be a string when provided");
      }
      const replyProblem = correlationProblem(args.reply_to);
      if (replyProblem !== null) {
        return validationFailure(`reply_to ${describeIdentifierProblem(replyProblem)}`);
      }
    }
  }
  if (client === null || client.state !== "ready") {
    return result("OMP Relay is not ready; no request was sent.", {
      action: args.action,
      status: "unavailable"
    });
  }
  try {
    if (args.action === "list") {
      const peers = await client.list();
      const text = peers.peers.length === 0 ? "No peers are connected in this room." : `Peers in this room: ${peers.peers.map(singleLine).join(", ")}`;
      return result(text, { action: "list", peers: [...peers.peers] });
    }
    const id = crypto.randomUUID();
    const receipt = await client.send({
      id,
      to: args.to,
      body: args.message,
      ...args.reply_to === undefined ? {} : { replyTo: args.reply_to }
    });
    return receiptResult(receipt);
  } catch (error) {
    return requestFailure(error);
  }
}
function buildInboundInjection(message, room) {
  const details = {
    id: message.id,
    from: message.from,
    project: room.project,
    task: room.task,
    body: message.body,
    ...message.reply_to === undefined ? {} : { reply_to: message.reply_to }
  };
  const lines = [
    `Remote message from ${singleLine(message.from)}`,
    `Project: ${singleLine(room.project)}`,
    `Task: ${singleLine(room.task)}`,
    `Message ID: ${singleLine(message.id)}`,
    ...message.reply_to === undefined ? [] : [`Reply to: ${singleLine(message.reply_to)}`],
    "",
    ...quotedBody(message.body)
  ];
  return { text: lines.join(`
`), details };
}
function schedulerFrom(ctx) {
  return {
    setTimeout(callback, milliseconds) {
      return ctx.setTimeout(callback, milliseconds);
    },
    clearTimeout(handle) {
      ctx.clearTimer(handle);
    },
    setInterval(callback, milliseconds) {
      return ctx.setInterval(callback, milliseconds);
    },
    clearInterval(handle) {
      ctx.clearTimer(handle);
    }
  };
}
function ompRelay(pi) {
  let client = null;
  let generation = 0;
  const notified = new Set;
  const notifyOnce = (ctx, message, type) => {
    if (notified.has(message))
      return;
    notified.add(message);
    try {
      ctx.ui.notify(message, type);
    } catch (error) {
      pi.logger.error("OMP Relay notification failed", { error: describe(error), message });
    }
  };
  const parameters = pi.zod.object({
    action: pi.zod.enum(["list", "send"]).describe("List connected peers or send a message"),
    to: pi.zod.string().optional().describe("Peer name; required for send"),
    message: pi.zod.string().optional().describe("Message body; required for send"),
    reply_to: pi.zod.string().optional().describe("Message identifier being answered")
  });
  pi.registerTool({
    name: "mesh",
    label: "OMP Relay Mesh",
    description: "List peers in the configured relay room or send work to one. A routed result means the message was queued for the recipient; it does not mean the recipient read, accepted, or answered it.",
    parameters,
    async execute(_toolCallId, args) {
      return executeMesh(client, args);
    }
  });
  pi.on("session_start", async (_event, ctx) => {
    const thisGeneration = ++generation;
    notified.clear();
    const previous = client;
    client = null;
    if (previous !== null) {
      await previous.stop();
    }
    if (ctx.mode !== "tui" || thisGeneration !== generation) {
      return;
    }
    const configured = await loadConfig(process.env);
    if (thisGeneration !== generation) {
      return;
    }
    if (!configured.ok) {
      notifyOnce(ctx, configured.problem.reason, "error");
      return;
    }
    const config = configured.config;
    const next = new RelayClient({
      config,
      scheduler: schedulerFrom(ctx),
      handlers: {
        onMessage(message) {
          const injection = buildInboundInjection(message, config.room);
          pi.appendEntry(INBOUND_MESSAGE_TYPE, injection.details);
          pi.sendUserMessage(injection.text, { deliverAs: "steer" });
        },
        onReport(report) {
          const type = report.level === "warn" ? "warning" : report.level;
          notifyOnce(ctx, report.message, type);
        }
      }
    });
    client = next;
    next.start();
  });
  pi.on("session_shutdown", async () => {
    generation += 1;
    const active = client;
    client = null;
    if (active !== null) {
      await active.stop();
    }
  });
}
export {
  executeMesh,
  ompRelay as default,
  buildInboundInjection,
  INBOUND_MESSAGE_TYPE
};
