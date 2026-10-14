// Dependency-free TOML reader for generated Cargo and EdgeZero manifests.
// Keep source rewriting separate so comments, formatting and feature flags survive.
export function parseToml(source) {
  const text = source.replaceAll('\r\n', '\n');
  let offset = 0;
  const root = Object.create(null);
  let table = root;
  const declared = new WeakSet();
  const inlineTables = new WeakSet();
  const fail = (message) => {
    throw new Error(`${message} at TOML line ${text.slice(0, offset).split('\n').length}`);
  };
  function skip(multiline = false) {
    while (offset < text.length) {
      if (text[offset] === ' ' || text[offset] === '\t' || (multiline && text[offset] === '\n')) {
        offset++;
      } else if (text[offset] === '#') {
        while (offset < text.length && text[offset] !== '\n') offset++;
      } else break;
    }
  }
  function string() {
    const quote = text[offset++];
    const multiline = text.slice(offset, offset + 2) === quote.repeat(2);
    if (multiline) {
      offset += 2;
      if (text[offset] === '\n') offset++;
    }
    let result = '';
    while (offset < text.length) {
      if (text[offset] === quote) {
        let count = 1;
        while (text[offset + count] === quote) count++;
        if (!multiline || count >= 3) {
          offset += multiline ? count : 1;
          if (multiline && count > 5) fail('Too many closing string quotes');
          return result + (multiline ? quote.repeat(count - 3) : '');
        }
      }
      const char = text[offset++];
      if ((!multiline && char === '\n') || /[\x00-\x08\x0b-\x1f\x7f]/.test(char)) {
        fail('Invalid string character');
      }
      if (quote === '"' && char === '\\') {
        const escape = text[offset++];
        const escapes = { b: '\b', t: '\t', n: '\n', f: '\f', r: '\r', '"': '"', '\\': '\\' };
        if (Object.hasOwn(escapes, escape)) result += escapes[escape];
        else if (escape === 'u' || escape === 'U') {
          const width = escape === 'u' ? 4 : 8;
          const hex = text.slice(offset, offset + width);
          if (!new RegExp(`^[0-9a-fA-F]{${width}}$`).test(hex)) fail('Invalid Unicode escape');
          const code = Number.parseInt(hex, 16);
          if (code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) fail('Invalid Unicode scalar');
          result += String.fromCodePoint(code);
          offset += width;
        } else if (multiline && /[ \t\n]/.test(escape ?? '')) {
          let whitespace = escape;
          while (/[ \t\n]/.test(text[offset] ?? '')) whitespace += text[offset++];
          if (!whitespace.includes('\n')) fail('Expected multiline string continuation');
        } else fail('Invalid string escape');
      } else result += char;
    }
    fail('Unterminated string');
  }
  function keys() {
    const parts = [];
    while (true) {
      skip();
      if (text[offset] === '"' || text[offset] === "'") {
        if (text.slice(offset, offset + 3) === text[offset].repeat(3)) fail('Multiline key');
        parts.push(string());
      } else {
        const match = /^[A-Za-z0-9_-]+/.exec(text.slice(offset));
        if (!match) fail('Expected key');
        parts.push(match[0]);
        offset += match[0].length;
      }
      skip();
      if (text[offset] !== '.') return parts;
      offset++;
    }
  }
  function descend(object, key) {
    if (!Object.hasOwn(object, key)) object[key] = Object.create(null);
    let child = object[key];
    if (Array.isArray(child)) child = child.at(-1);
    if (!child || typeof child !== 'object' || Array.isArray(child) || inlineTables.has(child)) {
      fail('Key is not a table');
    }
    return child;
  }
  function assign(object, path, value) {
    for (const key of path.slice(0, -1)) object = descend(object, key);
    const key = path.at(-1);
    if (Object.hasOwn(object, key)) fail(`Duplicate key ${key}`);
    object[key] = value;
  }
  function value() {
    skip();
    if (text[offset] === '"' || text[offset] === "'") return string();
    if (text[offset] === '[') {
      offset++;
      const values = [];
      skip(true);
      while (text[offset] !== ']') {
        values.push(value());
        skip(true);
        if (text[offset] !== ',') break;
        offset++;
        skip(true);
      }
      if (text[offset++] !== ']') fail('Expected array closing bracket');
      return values;
    }
    if (text[offset] === '{') {
      offset++;
      const object = Object.create(null);
      skip();
      while (text[offset] !== '}') {
        const path = keys();
        if (text[offset++] !== '=') fail('Expected inline assignment');
        assign(object, path, value());
        skip();
        if (text[offset] !== ',') break;
        offset++;
        skip();
        if (text[offset] === '}') fail('Trailing comma in inline table');
      }
      if (text[offset++] !== '}') fail('Expected inline table closing brace');
      inlineTables.add(object);
      return object;
    }
    const match = /^[^\n,#\]}]+/.exec(text.slice(offset));
    if (!match) fail('Expected value');
    const token = match[0].trim();
    offset += match[0].length;
    if (token === 'true' || token === 'false') return token === 'true';
    if (/^[+-]?(inf|nan)$/.test(token)) return Number(token.replace('inf', 'Infinity'));
    if (/^[+-]?(?:0|[1-9](?:_?\d)*)(?:\.(?:\d(?:_?\d)*))?(?:[eE][+-]?\d(?:_?\d)*)?$/.test(token)) {
      return Number(token.replaceAll('_', ''));
    }
    if (/^0(?:x[\da-fA-F](?:_?[\da-fA-F])*|o[0-7](?:_?[0-7])*|b[01](?:_?[01])*)$/.test(token)) {
      return Number(token.replaceAll('_', ''));
    }
    // Dates are not consumed by these helpers, but may occur elsewhere in manifests.
    if (/^(?:\d{4}-\d{2}-\d{2}(?:[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})?)?|\d{2}:\d{2}:\d{2}(?:\.\d+)?)$/.test(token)) {
      return token;
    }
    fail(`Invalid value ${token}`);
  }
  while (true) {
    skip(true);
    if (offset === text.length) return root;
    if (text[offset] === '[') {
      offset++;
      const array = text[offset] === '[';
      if (array) offset++;
      const path = keys();
      if (text[offset++] !== ']' || (array && text[offset++] !== ']')) fail('Expected table closing bracket');
      table = root;
      for (const key of path.slice(0, -1)) table = descend(table, key);
      const key = path.at(-1);
      if (array) {
        if (!Object.hasOwn(table, key)) table[key] = [];
        if (!Array.isArray(table[key])) fail('Expected array of tables');
        const item = Object.create(null);
        table[key].push(item);
        table = item;
      } else {
        table = descend(table, key);
        if (declared.has(table)) fail('Duplicate table');
      }
      declared.add(table);
    } else {
      const path = keys();
      if (text[offset++] !== '=') fail('Expected assignment');
      assign(table, path, value());
    }
    skip();
    if (offset < text.length && text[offset] !== '\n') fail('Expected end of line');
  }
}
