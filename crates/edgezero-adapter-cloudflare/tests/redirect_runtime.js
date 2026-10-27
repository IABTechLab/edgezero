// Guest-only fetch fixture. This does not model workerd or prove host cancellation.
const responses = JSON.parse(wire)
const original = globalThis.fetch
const probe = { requests: [], pulls: [], aborts: [], cancellations: [] }
const pending = []
const untilCleanup = () => new Promise((resolve, reject) => {
  pending.push(() => reject(new DOMException('fixture cleanup', 'AbortError')))
})
globalThis.fetch = (request, init) => {
  if (request.redirect !== 'manual' || init.encodeResponseBody !== 'manual') {
    throw new TypeError('manual fetch policy lost')
  }
  const index = probe.requests.length
  const response = responses[index]
  if (response === undefined) throw new TypeError('unexpected fixture fetch')
  probe.requests.push({ url: request.url, method: request.method })
  probe.pulls.push(0)
  probe.aborts.push(0)
  probe.cancellations.push(0)
  init.signal.addEventListener('abort', () => { probe.aborts[index]++ }, { once: true })
  if (response.kind === 'pending-head') return untilCleanup()
  let sent = false
  const body = new ReadableStream({
    pull(controller) {
      probe.pulls[index]++
      if (response.kind === 'pending') return untilCleanup()
      if (response.kind === 'error') {
        controller.error(new TypeError('fixture body failure'))
      } else if (response.kind === 'bytes') {
        if (sent || response.bytes.length === 0) controller.close()
        else {
          sent = true
          controller.enqueue(new Uint8Array(response.bytes))
        }
      } else {
        throw new TypeError('unknown fixture body kind')
      }
    },
    cancel() { probe.cancellations[index]++ },
  }, { highWaterMark: 0 })
  return Promise.resolve(new Response(body, {
    status: response.status,
    headers: response.headers,
  }))
}
probe.restore = () => {
  globalThis.fetch = original
  for (const release of pending.splice(0)) release()
}
return probe
