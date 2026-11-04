#!/usr/bin/env node
// Original synthetic MMDB encoder. No vendor database or third-party data.
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const directory = fileURLToPath(new URL('../crates/edgezero-adapter-axum/tests/fixtures/geoip/', import.meta.url));
const check = process.argv.includes('--check');
const fixtures = new Map();
const join = (...parts) => Buffer.concat(parts.flat().map(part => Buffer.from(typeof part === 'number' ? [part] : part)));
function header(type, size) {
  const extra = [];
  let encoded = size;
  if (size >= 65821) { encoded = 31; extra.push((size - 65821) >> 16, (size - 65821) >> 8, size - 65821); }
  else if (size >= 285) { encoded = 30; extra.push((size - 285) >> 8, size - 285); }
  else if (size >= 29) { encoded = 29; extra.push(size - 29); }
  return Buffer.from(type < 8 ? [(type << 5) | encoded, ...extra] : [encoded, type - 7, ...extra]);
}
const string = value => join(header(2, Buffer.byteLength(value)), Buffer.from(value));
function uint(type, value) {
  let hex = BigInt(value).toString(16);
  if (hex.length % 2) hex = `0${hex}`;
  const bytes = value === 0 ? Buffer.alloc(0) : Buffer.from(hex, 'hex');
  return join(header(type, bytes.length), bytes);
}
const map = entries => join(header(7, entries.length), entries.map(([key, value]) => join(typeof key === 'string' ? string(key) : key, value)));
const array = entries => join(header(11, entries.length), entries);
const pointer = offset => join([0x38], uint32(offset));
function uint32(value) { const bytes = Buffer.alloc(4); bytes.writeUInt32BE(value); return bytes; }
const code = value => map([['iso_code', string(value)]]);
const country = value => map([['country', code(value)]]);
const city = (c, s) => map([['country', code(c)], ['subdivisions', array([code(s)])]]);

function addressBits(address, version) {
  let value;
  if (address.includes('.')) {
    value = address.split('.').reduce((n, octet) => (n << 8n) | BigInt(octet), 0n);
  } else {
    const [left, right] = address.split('::');
    const a = left ? left.split(':') : [];
    const b = right ? right.split(':') : [];
    const words = right === undefined ? a : [...a, ...Array(8 - a.length - b.length).fill('0'), ...b];
    value = words.reduce((n, word) => (n << 16n) | BigInt(`0x${word}`), 0n);
  }
  return value.toString(2).padStart(version === 4 ? 32 : 128, '0');
}

function database(entries, { type = 'GeoLite2-City', version = 4, recordSize = 24, metadataExtra = [], metadataWorkLimit = false, epoch = 1793750400 } = {}) {
  const root = {};
  for (const [cidr, record] of entries) {
    const [address, prefix] = cidr.split('/');
    const bits = addressBits(address, version).slice(0, Number(prefix));
    let node = root;
    for (const bit of bits) node = node[bit] ??= {};
    node.record = record;
  }
  const nodes = [];
  function collect(node) {
    if (!node || node.record !== undefined) return;
    node.index = nodes.length;
    nodes.push(node);
    collect(node[0]); collect(node[1]);
  }
  collect(root);
  const data = [];
  const offsets = new Map();
  function child(node) {
    if (!node) return nodes.length;
    if (node.record === undefined) return node.index;
    if (!offsets.has(node.record)) {
      const offset = data.reduce((sum, bytes) => sum + bytes.length, 0);
      offsets.set(node.record, offset);
      data.push(typeof node.record === 'function' ? node.record(offset) : node.record);
    }
    return nodes.length + 16 + offsets.get(node.record);
  }
  const tree = nodes.map(node => {
    const left = child(node[0]); const right = child(node[1]);
    if (recordSize === 24) return join(uint32(left).subarray(1), uint32(right).subarray(1));
    if (recordSize === 28) return join(uint32(left & 0xffffff).subarray(1), [((left >>> 24) << 4) | (right >>> 24)], uint32(right & 0xffffff).subarray(1));
    return join(uint32(left), uint32(right));
  });
  const metadataEntries = [
    ['binary_format_major_version', uint(5, 2)], ['binary_format_minor_version', uint(5, 0)],
    ['build_epoch', uint(9, epoch)], ['database_type', string(type)],
    ['description', map([['en', string('Original synthetic EdgeZero test data')]])],
    ['ip_version', uint(5, version)], ['languages', array([string('en')])],
    ['node_count', uint(6, nodes.length)], ['record_size', uint(5, recordSize)], ...metadataExtra,
  ];
  let metadata;
  if (metadataWorkLimit) {
    const count = 100;
    const placeholder = array(Array.from({ length: count }, () => pointer(0)));
    const start = map([...metadataEntries, ['unknown', placeholder]]).length;
    metadata = join(
      map([...metadataEntries, ['unknown', array(Array.from({ length: count }, (_, i) => pointer(start + i * 4)))]]),
      Array.from({ length: count }, () => header(2, 65821)), Buffer.alloc(65821, 120),
    );
  } else metadata = map(metadataEntries);
  return join(tree, Buffer.alloc(16), data, Buffer.from('\xab\xcd\xefMaxMind.com', 'latin1'), metadata);
}
const add = (name, entries, options) => fixtures.set(`${name}.mmdb`, database(entries, options));
const cityEntries = [
  ['127.0.0.0/8', city('US', 'CA')],
  ['192.0.2.1/32', city('us', 'ca')],
  ['192.0.2.2/32', country('GB')],
  ['192.0.2.3/32', city('JP', '01')],
  ['192.0.2.4/32', city('US', 'zz')],
  ['192.0.2.5/32', map([['country', code('US')], ['subdivisions', array([map([]), code('NY')])]])],
  ['192.0.2.6/32', map([['country', code('US')], ['subdivisions', array([])]])],
  ['192.0.2.7/32', map([])],
  ['192.0.2.8/32', country('XK')],
  ['192.0.2.9/32', country('')],
  ['192.0.2.10/32', city('US', '')],
  ['192.0.2.11/32', city('US', 'US-CA')],
  ['192.0.2.12/32', map([['subdivisions', array([code('CA')])]])],
  ['192.0.2.13/32', map([['registered_country', code('US')], ['represented_country', code('US')]])],
  ['192.0.2.14/32', map([['country', map([])]])],
  ['192.0.2.15/32', map([['country', code('US')], ['country', code('GB')]])],
  ['192.0.2.16/32', map([['country', code('US')], ['subdivisions', array([code('CA'), map([['iso_code', uint(5, 1)]])])]])],
  ['192.0.2.17/32', city('US', 'a1B')],
  ['192.0.2.18/32', country('UK')],
  ['192.0.2.19/32', country(' U')],
  ['192.0.2.20/32', city('US', 'é')],
];
add('city', cityEntries);
add('city-replacement', [['127.0.0.0/8', city('US', 'NY')], ['192.0.2.1/32', city('US', 'NY')]], { epoch: 1793836800 });
add('country', [['127.0.0.0/8', country('US')], ['192.0.2.1/32', map([['country', code('US')], ['subdivisions', uint(5, 1)]])]], { type: 'GeoLite2-Country' });
// IPv4 embedding uses hexadecimal words in the IPv6 fixture.
add('ipv6', [['::c000:201/128', city('US', 'CA')], ['2001:db8::/32', city('GB', 'ENG')]], { version: 6 });
for (const recordSize of [28, 32]) add(`record-${recordSize}`, [['192.0.2.0/24', city('US', 'CA')]], { recordSize });
for (const [name, record] of [
  ['bad-country-parent', map([['country', string('US')]])],
  ['bad-country-code', map([['country', map([['iso_code', uint(5, 1)]])]])],
  ['bad-subdivisions-parent', map([['country', code('US')], ['subdivisions', string('CA')]])],
  ['bad-first-subdivision', map([['country', code('US')], ['subdivisions', array([string('CA')])]])],
  ['bad-subdivision-code', map([['country', code('US')], ['subdivisions', array([map([['iso_code', uint(5, 1)]])])]])],
  ['bad-unrelated-key', map([['country', code('US')], ['unrelated', map([[uint(5, 0), string('x')]])]])],
  ['bad-offpath-pointer', pointer(0xffffffff)],
]) add(name, [['192.0.2.1/32', city('US', 'CA')], ['203.0.113.1/32', record]]);
// The unknown field points to a pointer, which points to a valid string.
// Upstream verify accepts this; normal deserialize_any rejects the chain.
add('bad-unrelated-chain', [['192.0.2.0/24', offset => {
  const base = map([['country', code('US')], ['unrelated', pointer(0)]]);
  const target = offset + base.length;
  return join(map([['country', code('US')], ['unrelated', pointer(target)]]), pointer(target + 5), string('x'));
}]]);
add('bad-root-chain', [['192.0.2.0/24', offset => join(pointer(offset + 5), pointer(offset + 10), country('US'))]]);
add('unsupported', [['192.0.2.0/24', country('US')]], { type: 'Other-City' });
const cyclicTree = database([['192.0.2.0/24', country('US')]]);
cyclicTree.fill(0, 0, 6);
fixtures.set('bad-tree-cycle.mmdb', cyclicTree);
const badSeparator = database([['192.0.2.0/24', country('US')]]);
badSeparator[24 * 6] = 1;
fixtures.set('bad-separator.mmdb', badSeparator);
fixtures.set('truncated.mmdb', database([['192.0.2.0/24', country('US')]]).subarray(0, -10));
add('bad-metadata', [['192.0.2.0/24', country('US')]], { metadataExtra: [['unknown', pointer(0xffffffff)]] });
// Known upstream metadata-verifier gap. Retained for the ignored acceptance regression.
add('bad-metadata-key', [['192.0.2.0/24', country('US')]], { metadataExtra: [['unknown', map([[uint(5, 0), string('x')]])]] });
// Structural expansion exhaustion. Tiny shared arrays recursively double,
// passing memoized verification but exceeding the normal decoder value budget.
add('expansion-limit', [['192.0.2.0/24', offset => {
  const root = map([['country', code('US')], ['unrelated', pointer(0)]]);
  const parts = [null];
  let current = offset + root.length;
  for (let depth = 0; depth < 17; depth++) {
    const next = current + header(11, 2).length + 10;
    parts.push(join(header(11, 2), pointer(next), pointer(next)));
    current = next;
  }
  parts[0] = map([['country', code('US')], ['unrelated', pointer(offset + root.length)]]);
  parts.push(string('x'));
  return join(parts);
}]]);
// Every normal MMDB scalar type plus ordinary single pointers remains valid.
add('all-values', [['192.0.2.0/24', offset => {
  const fields = [
    ['country', code('US')], ['bytes', join(header(4, 2), [0xff, 0x00])],
    ['u16', uint(5, 1)], ['u32', uint(6, 2)], ['u64', uint(9, 3)], ['u128', uint(10, 4)],
    ['i32', join(header(8, 4), [255, 255, 255, 255])],
    ['bool', header(14, 1)], ['double', join(header(3, 8), Buffer.alloc(8))],
    ['float', join(header(15, 4), Buffer.alloc(4))], ['pointer', pointer(0)],
  ];
  const root = map(fields);
  fields[fields.length - 1][1] = pointer(offset + root.length);
  return join(map(fields), map([['nested', array([string('hello')])]]));
}]]);
// Overlapping valid string targets exhaust verifier work rather than allocating
// their expanded contents. Each ASCII header is itself valid UTF-8 payload.
add('verification-limit', [['192.0.2.0/24', offset => {
  const count = 100;
  const initial = map([['country', code('US')], ['unrelated', array(Array.from({ length: count }, () => pointer(0)))]]);
  const start = offset + initial.length;
  const root = map([['country', code('US')], ['unrelated', array(Array.from({ length: count }, (_, i) => pointer(start + i * 4)))]]);
  return join(root, Array.from({ length: count }, () => header(2, 65821)), Buffer.alloc(65821, 120));
}]]);
add('metadata-verification-limit', [['192.0.2.0/24', country('US')]], { metadataWorkLimit: true });
// An invalid UTF-8 scalar in an unconsumed record proves off-path verification.
add('bad-offpath-utf8', [['192.0.2.1/32', country('US')], ['203.0.113.1/32', map([['unrelated', join(header(2, 1), [255])]])]]);

await mkdir(directory, { recursive: true });
const hashes = [];
for (const [name, bytes] of fixtures) {
  if (check) {
    if (!(await readFile(`${directory}${name}`)).equals(bytes)) throw new Error(`Fixture differs: ${name}`);
  } else await writeFile(`${directory}${name}`, bytes);
  hashes.push(`${createHash('sha256').update(bytes).digest('hex')}  ${name}`);
}
const sums = `${hashes.join('\n')}\n`;
if (check) {
  if ((await readFile(`${directory}SHA256SUMS`, 'utf8')) !== sums) throw new Error('SHA256SUMS differs');
  const existing = (await readdir(directory)).filter(name => name.endsWith('.mmdb'));
  if (existing.some(name => !fixtures.has(name))) throw new Error('Unexpected fixture file');
  console.log(`Verified ${fixtures.size} original fixtures and checksums`);
} else {
  await writeFile(`${directory}SHA256SUMS`, sums);
  console.log(`Wrote ${fixtures.size} original fixtures and checksums`);
}
