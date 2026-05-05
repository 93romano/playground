const zlib = require('node:zlib');

// Simple CRC32 (IEEE 802.3) for integrity checks
function makeCRCTable() {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[n] = c >>> 0;
  }
  return table;
}

const CRC_TABLE = makeCRCTable();

function crc32(buf) {
  let crc = 0 ^ -1;
  for (let i = 0; i < buf.length; i++) {
    crc = (crc >>> 8) ^ CRC_TABLE[(crc ^ buf[i]) & 0xff];
  }
  return (crc ^ -1) >>> 0;
}

const MAGIC_V2 = 'BS2\0';
const MAGIC_V1 = 'BS1\0';
const VERSION_1 = 1;

// Compression methods
const METHOD = {
  NONE: 0,
  DEFLATE: 1,
};

// Flags (bitfield)
// bit 0: typed JSON tags enabled
const FLAG_TYPED_TAGS = 1 << 0;

class BinaryStorage {
  constructor(options = {}) {
    this.level = options.level ?? 6; // zlib compression level (0-9)
    this.method = this.#resolveMethod(options.method ?? 'deflate'); // 'none' | 'deflate' | 'auto'
    this.useTags = options.useTags ?? true; // enable Date/BigInt/Buffer tagging
  }

  serialize(data) {
    return this.toBinary(data);
  }

  deserialize(binary) {
    return this.fromBinary(binary);
  }

  stringify(data) {
    const encoded = this.#encode(data);
    return JSON.stringify(encoded);
  }

  parse(jsonStr) {
    const parsed = JSON.parse(jsonStr);
    return this.#decode(parsed);
  }

  toBinary(data) {
    const json = this.stringify(data);
    const jsonBuf = Buffer.from(json, 'utf8');

    let methodUsed = this.method;
    let body;
    if (methodUsed === 'none') {
      body = jsonBuf;
      methodUsed = METHOD.NONE;
    } else if (methodUsed === 'deflate') {
      body = zlib.deflateSync(jsonBuf, { level: this.level });
      methodUsed = METHOD.DEFLATE;
    } else if (methodUsed === 'auto') {
      const deflated = zlib.deflateSync(jsonBuf, { level: this.level });
      if (deflated.length + 0 <= jsonBuf.length) {
        body = deflated;
        methodUsed = METHOD.DEFLATE;
      } else {
        body = jsonBuf;
        methodUsed = METHOD.NONE;
      }
    } else {
      throw new Error(`Unknown method: ${this.method}`);
    }

    const flags = this.useTags ? FLAG_TYPED_TAGS : 0;
    const checksum = crc32(body);

    // Header V2:
    // 0..3   magic 'BS2\0'
    // 4      version (1)
    // 5      method (0=none,1=deflate)
    // 6      flags
    // 7..10  original JSON size (BE u32)
    // 11..14 crc32 over body (BE u32)
    const header = Buffer.alloc(15);
    header.write(MAGIC_V2, 0, 4, 'ascii');
    header.writeUInt8(VERSION_1, 4);
    header.writeUInt8(methodUsed, 5);
    header.writeUInt8(flags, 6);
    header.writeUInt32BE(jsonBuf.length, 7);
    header.writeUInt32BE(checksum, 11);

    return Buffer.concat([header, body]);
  }

  fromBinary(binary) {
    const buf = Buffer.isBuffer(binary) ? binary : Buffer.from(binary);
    if (buf.length < 9) {
      throw new Error('Binary too small: missing header');
    }

    const magic = buf.toString('ascii', 0, 4);

    if (magic === MAGIC_V1) {
      // Backward compatibility for v1 header (no version/flags/crc)
      const method = buf.readUInt8(4);
      const originalSize = buf.readUInt32BE(5);
      const body = buf.subarray(9);

      let decompressed;
      if (method === METHOD.DEFLATE) {
        decompressed = zlib.inflateSync(body);
      } else if (method === METHOD.NONE) {
        decompressed = body;
      } else {
        throw new Error(`Unsupported method: ${method}`);
      }

      if (decompressed.length !== originalSize) {
        throw new Error(
          `Size check failed: expected ${originalSize}, got ${decompressed.length}`
        );
      }
      const jsonStr = decompressed.toString('utf8');
      return this.parse(jsonStr);
    }

    if (magic !== MAGIC_V2) {
      throw new Error('Invalid binary header');
    }

    const version = buf.readUInt8(4);
    if (version !== VERSION_1) {
      throw new Error(`Unsupported version: ${version}`);
    }

    const method = buf.readUInt8(5);
    const flags = buf.readUInt8(6);
    const originalSize = buf.readUInt32BE(7);
    const checksum = buf.readUInt32BE(11);
    const body = buf.subarray(15);

    const actualChecksum = crc32(body);
    if (actualChecksum !== checksum) {
      throw new Error(
        `CRC32 mismatch: expected 0x${checksum.toString(16)}, got 0x${actualChecksum.toString(16)}`
      );
    }

    let decompressed;
    if (method === METHOD.DEFLATE) {
      decompressed = zlib.inflateSync(body);
    } else if (method === METHOD.NONE) {
      decompressed = body;
    } else {
      throw new Error(`Unsupported method: ${method}`);
    }

    if (decompressed.length !== originalSize) {
      throw new Error(
        `Size check failed: expected ${originalSize}, got ${decompressed.length}`
      );
    }

    const jsonStr = decompressed.toString('utf8');
    const prevUseTags = this.useTags;
    this.useTags = (flags & FLAG_TYPED_TAGS) !== 0;
    try {
      return this.parse(jsonStr);
    } finally {
      this.useTags = prevUseTags; // restore
    }
  }

  #resolveMethod(m) {
    const s = String(m).toLowerCase();
    if (s === 'none' || s === 'deflate' || s === 'auto') return s;
    throw new Error(`Invalid method option: ${m}`);
  }

  #encode(value) {
    if (!this.useTags) return value;
    if (value == null) return value;

    // Primitive types
    const t = typeof value;
    if (t === 'bigint') return { __t: 'BigInt', v: value.toString() };
    if (t !== 'object') return value;

    // Buffer
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
      return { __t: 'Buffer', v: value.toString('base64') };
    }

    // Date
    if (value instanceof Date) {
      return { __t: 'Date', v: value.toISOString() };
    }

    // Array
    if (Array.isArray(value)) {
      return value.map((v) => this.#encode(v));
    }

    // Plain object
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = this.#encode(v);
    }
    return out;
  }

  #decode(value) {
    if (!this.useTags) return value;
    if (value == null) return value;
    if (typeof value !== 'object') return value;

    // Tagged
    if (value.__t === 'Buffer') return Buffer.from(value.v, 'base64');
    if (value.__t === 'BigInt') return BigInt(value.v);
    if (value.__t === 'Date') return new Date(value.v);

    // Array
    if (Array.isArray(value)) return value.map((v) => this.#decode(v));

    // Plain object
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = this.#decode(v);
    }
    return out;
  }
}

module.exports = { BinaryStorage };
