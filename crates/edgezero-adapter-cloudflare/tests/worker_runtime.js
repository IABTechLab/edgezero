// Guest-only fixture; this is not evidence of Workers host behavior.
if (globalThis.FixedLengthStream === undefined) {
  globalThis.FixedLengthStream = class FixedLengthStream extends TransformStream {
    constructor(length) {
      if (typeof length !== 'bigint' && !Number.isSafeInteger(length)) {
        throw new RangeError('invalid stream length')
      }
      const expected = BigInt(length)
      if (expected < 0n || expected >= 1n << 64n) {
        throw new RangeError('invalid stream length')
      }
      let received = 0n
      super({
        transform(chunk, controller) {
          if (!(chunk instanceof ArrayBuffer) && !ArrayBuffer.isView(chunk)) {
            throw new TypeError('expected a byte chunk')
          }
          received += BigInt(chunk.byteLength)
          if (received > expected) throw new TypeError('stream length exceeded')
          controller.enqueue(chunk)
        },
        flush() {
          if (received !== expected) throw new TypeError('stream length mismatch')
        },
      })
    }
  }
}
