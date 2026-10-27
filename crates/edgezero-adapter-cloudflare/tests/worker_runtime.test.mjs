import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const installWorkersStreams = new Function(
  readFileSync(new URL('./worker_runtime.js', import.meta.url), 'utf8'),
)

async function exchange(length, chunks) {
  installWorkersStreams()
  const stream = new globalThis.FixedLengthStream(length)
  const writer = stream.writable.getWriter()
  const reading = new Response(stream.readable).arrayBuffer()
  const writing = (async () => {
    try {
      for (const chunk of chunks) await writer.write(chunk)
      await writer.close()
    } finally {
      writer.releaseLock()
    }
  })()
  const [bytes] = await Promise.all([reading, writing])
  return new Uint8Array(bytes)
}

test('buffered stream preserves exact bytes and closes at the declared length', async () => {
  assert.deepEqual(
    await exchange(4, [new Uint8Array([0, 255]), new Uint8Array([1, 128])]),
    new Uint8Array([0, 255, 1, 128]),
  )
})

test('zero-length and bigint declarations use the same byte contract', async () => {
  assert.deepEqual(await exchange(0, []), new Uint8Array())
  assert.deepEqual(await exchange(1n, [new Uint8Array([7])]), new Uint8Array([7]))
})

test('underflow and overflow fail instead of returning truncated or excess bytes', async () => {
  await assert.rejects(exchange(2, [new Uint8Array([1])]), /length/)
  await assert.rejects(exchange(1, [new Uint8Array([1, 2])]), /length/)
})

test('bigint declarations above u32 do not wrap to a short stream', async () => {
  await assert.rejects(exchange((1n << 32n) + 1n, [new Uint8Array([1])]), /length mismatch/)
})

test('invalid lengths and non-byte chunks fail closed', async () => {
  installWorkersStreams()
  for (const length of [-1, 1.5, NaN, Infinity, -1n, 1n << 64n]) {
    assert.throws(() => new globalThis.FixedLengthStream(length), RangeError)
  }
  await assert.rejects(exchange(1, ['not bytes']), TypeError)
})

test('writes retain normal stream backpressure', async () => {
  installWorkersStreams()
  const stream = new globalThis.FixedLengthStream(1)
  const writer = stream.writable.getWriter()
  let settled = false
  const writing = writer.write(new Uint8Array([9])).then(() => { settled = true })
  await Promise.resolve()
  assert.equal(settled, false)
  const reader = stream.readable.getReader()
  assert.deepEqual(await reader.read(), { done: false, value: new Uint8Array([9]) })
  await writing
  await writer.close()
  assert.deepEqual(await reader.read(), { done: true, value: undefined })
  writer.releaseLock()
  reader.releaseLock()
})

test('reader cancellation rejects pending writes and closes the writer', async () => {
  installWorkersStreams()
  const stream = new globalThis.FixedLengthStream(1)
  const writer = stream.writable.getWriter()
  const reader = stream.readable.getReader()
  const outcomes = Promise.allSettled([writer.write(new Uint8Array([1])), writer.closed])
  await reader.cancel('fixture cancellation')
  for (const result of await outcomes) {
    assert.equal(result.status, 'rejected')
    assert.equal(result.reason, 'fixture cancellation')
  }
  writer.releaseLock()
  reader.releaseLock()
})

test('writer abort errors the readable stream without a successful EOF', async () => {
  installWorkersStreams()
  const stream = new globalThis.FixedLengthStream(1)
  const writer = stream.writable.getWriter()
  const outcomes = Promise.allSettled([
    new Response(stream.readable).arrayBuffer(),
    writer.closed,
  ])
  const reason = new Error('fixture writer abort')
  await writer.abort(reason)
  for (const result of await outcomes) {
    assert.equal(result.status, 'rejected')
    assert.equal(result.reason, reason)
  }
  writer.releaseLock()
})

test('installation is idempotent and never replaces an existing host primitive', () => {
  const original = globalThis.FixedLengthStream
  const native = class NativeFixedLengthStream {}
  try {
    globalThis.FixedLengthStream = native
    installWorkersStreams()
    installWorkersStreams()
    assert.equal(globalThis.FixedLengthStream, native)
  } finally {
    if (original === undefined) delete globalThis.FixedLengthStream
    else globalThis.FixedLengthStream = original
  }
})
