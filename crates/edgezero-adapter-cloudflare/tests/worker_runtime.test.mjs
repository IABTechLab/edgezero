import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const installWorkersStreams = new Function(
  readFileSync(new URL('./worker_runtime.js', import.meta.url), 'utf8'),
)

const installRedirectFetch = new Function(
  'wire', readFileSync(new URL('./redirect_runtime.js', import.meta.url), 'utf8'),
)

test('redirect fixture never prefetches an unread body and restores fetch', async () => {
  const original = globalThis.fetch
  const probe = installRedirectFetch(JSON.stringify([
    { status: 302, kind: 'pending', headers: [['location', '/next']] },
    { status: 200, kind: 'bytes', headers: [], bytes: [0, 255, 7] },
  ]))
  const first = new AbortController()
  const init = signal => ({ signal, encodeResponseBody: 'manual' })
  try {
    const reply = await fetch(new Request('https://fixture.invalid/start', { redirect: 'manual' }), init(first.signal))
    await Promise.resolve()
    assert.deepEqual(probe.pulls, [0])
    first.abort()
    assert.deepEqual(probe.aborts, [1])
    await reply.body.cancel()
    assert.deepEqual(probe.cancellations, [1])
    const final = await fetch(new Request('https://fixture.invalid/next', { redirect: 'manual' }), init(new AbortController().signal))
    assert.deepEqual(new Uint8Array(await final.arrayBuffer()), new Uint8Array([0, 255, 7]))
    assert.equal(probe.requests.length, 2)
    assert.deepEqual(probe.aborts, [1, 0])
  } finally {
    probe.restore()
  }
  assert.equal(globalThis.fetch, original)
})

test('redirect fixture rejects implicit redirect or encoding and unexpected fetches', async () => {
  const probe = installRedirectFetch(JSON.stringify([
    { status: 200, kind: 'bytes', headers: [], bytes: [] },
  ]))
  const init = { signal: new AbortController().signal, encodeResponseBody: 'manual' }
  try {
    assert.throws(() => fetch(new Request('https://fixture.invalid/'), init), /manual fetch policy/)
    const request = new Request('https://fixture.invalid/', { redirect: 'manual' })
    assert.throws(() => fetch(request, { signal: init.signal }), /manual fetch policy/)
    assert.equal(probe.requests.length, 0)
    assert.equal((await fetch(request, init)).status, 200)
    assert.throws(() => fetch(request, init), /unexpected fixture fetch/)
    assert.equal(probe.requests.length, 1)
  } finally {
    probe.restore()
  }
})

test('redirect fixture settles pending fetch and body callbacks during cleanup', async () => {
  const probe = installRedirectFetch(JSON.stringify([
    { kind: 'pending-head' },
    { status: 200, kind: 'pending', headers: [] },
  ]))
  const init = { signal: new AbortController().signal, encodeResponseBody: 'manual' }
  const request = new Request('https://fixture.invalid/', { redirect: 'manual' })
  try {
    const head = assert.rejects(fetch(request, init), { name: 'AbortError' })
    const response = await fetch(request, init)
    const reader = response.body.getReader()
    const body = assert.rejects(reader.read(), { name: 'AbortError' })
    await Promise.resolve()
    assert.deepEqual(probe.pulls, [0, 1])
    probe.restore()
    await head
    await body
    reader.releaseLock()
  } finally {
    probe.restore()
  }
})

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
