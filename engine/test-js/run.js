const assert = require('node:assert');
const { BinaryStorage } = require('./BinaryStorage');

function prettySize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function makeSampleData() {
  return {
    id: 123456,
    idBig: 9007199254740993n, // BigInt beyond JS safe integer
    name: 'Alice',
    active: true,
    balance: 9123.45,
    scores: [10, 20, 30, 40, 50],
    meta: {
      createdAt: new Date('2021-01-01T00:00:00.000Z'),
      tags: ['a', 'b', 'c'],
      flags: { a: true, b: false, c: null },
    },
    payload: Buffer.from('hello world', 'utf8'), // Buffer
    longText:
      'Lorem ipsum dolor sit amet, consectetur adipiscing elit. '.repeat(20),
  };
}

function main() {
  // Use auto to pick the smallest representation automatically
  const storage = new BinaryStorage({ level: 9, method: 'auto', useTags: true });
  const data = makeSampleData();

  const json = storage.stringify(data); // handles BigInt/Buffer/Date via tags
  const jsonBuf = Buffer.from(json, 'utf8');

  const binary = storage.serialize(data);
  const restored = storage.deserialize(binary);

  // Deep equality with native types reconstructed
  assert.deepStrictEqual(restored, data);

  console.log('BinaryStorage test: OK');
  console.log('—');
  console.log('Original (tagged JSON) size:', prettySize(jsonBuf.length));
  console.log('Serialized binary size:', prettySize(binary.length));
  const ratio = (binary.length / jsonBuf.length) * 100;
  console.log(`Size ratio (binary/json): ${ratio.toFixed(2)}%`);

  // Compare with no compression
  const storageNone = new BinaryStorage({ method: 'none', useTags: true });
  const binaryNone = storageNone.serialize(data);
  console.log('No-compress binary size:', prettySize(binaryNone.length));

  // CRC32 integrity check demo
  const tampered = Buffer.from(binary);
  // Flip one byte in the body (after 15-byte header)
  if (tampered.length > 20) tampered[20] ^= 0xff;
  try {
    storage.deserialize(tampered);
    console.log('Unexpected: CRC mismatch not detected');
  } catch (e) {
    console.log('CRC test (expected failure):', e.message);
  }
}

main();
