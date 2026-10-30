export function tarEntry(name, data = Buffer.alloc(0), type = '0', link = '', options = {}) {
  data = Buffer.from(data);
  const { mode = 0o755, size = data.length, prefix = '' } = options;
  const header = Buffer.alloc(512);
  header.write(name, 0, 100);
  header.write(mode.toString(8).padStart(7, '0') + '\0', 100, 8);
  header.write('0000000\0', 108, 8);
  header.write('0000000\0', 116, 8);
  header.write(size.toString(8).padStart(11, '0') + '\0', 124, 12);
  header.write('00000000000\0', 136, 12);
  header.fill(32, 148, 156);
  header.write(type, 156, 1);
  header.write(link, 157, 100);
  header.write('ustar\0', 257, 6);
  header.write('00', 263, 2);
  header.write(prefix, 345, 155);
  const checksum = header.reduce((sum, byte) => sum + byte, 0);
  header.write(checksum.toString(8).padStart(6, '0') + '\0 ', 148, 8);
  return Buffer.concat([header, data, Buffer.alloc((512 - data.length % 512) % 512)]);
}

export function paxRecord(key, value) {
  const text = `${key}=${value}\n`;
  let length = Buffer.byteLength(text) + 2;
  while (Buffer.byteLength(`${length} ${text}`) !== length) length = Buffer.byteLength(`${length} ${text}`);
  return Buffer.from(`${length} ${text}`);
}
