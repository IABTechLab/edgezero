#!/usr/bin/env node

import { readFileSync } from 'node:fs'

const capabilityPath = 'docs/guide/capabilities.md'
const adapterOverviewPath = 'docs/guide/adapters/overview.md'
const changelogPath = 'CHANGELOG.md'
const handlersGuidePath = 'docs/guide/handlers.md'
const axumGuidePath = 'docs/guide/adapters/axum.md'
const cloudflareGuidePath = 'docs/guide/adapters/cloudflare.md'
const proxyGuidePath = 'docs/guide/proxying.md'
const routingGuidePath = 'docs/guide/routing.md'
const fastlyGuidePath = 'docs/guide/adapters/fastly.md'
const scaffoldReadmePath =
  'crates/edgezero-cli/src/templates/root/README.md.hbs'
const outboundCorePath = 'crates/edgezero-core/src/outbound.rs'
const outboundCoreProductionPaths = [
  outboundCorePath,
  ...[
    'batch',
    'dispatch',
    'exchange',
    'head',
    'image_optimizer',
    'progress',
    'redirect',
    'scope',
  ].map((name) => `crates/edgezero-core/src/outbound/${name}.rs`),
]
const outboundCoreTestPath = 'crates/edgezero-core/src/outbound/tests.rs'
const compressionCorePath = 'crates/edgezero-core/src/compression.rs'
const outboundAdapterPaths = [
  'crates/edgezero-adapter-axum/src/outbound.rs',
  'crates/edgezero-adapter-cloudflare/src/outbound.rs',
  'crates/edgezero-adapter-fastly/src/outbound.rs',
  'crates/edgezero-adapter-spin/src/outbound.rs',
]
const templateHandlersPath =
  'crates/edgezero-cli/src/templates/core/src/handlers.rs.hbs'
const demoHandlersPath =
  'examples/app-demo/crates/app-demo-core/src/handlers.rs'
const brotliAuditPath =
  'docs/audits/2026-09-27-brotli-decoder-memory-accounting.md'
const outboundSpecPath =
  'docs/superpowers/specs/2026-05-21-outbound-http-design.md'
const platformMetadataDesignPath =
  'docs/superpowers/specs/2026-09-28-platform-resource-metadata-design.md'
const inboundSpecPath =
  'docs/superpowers/specs/2026-08-22-inbound-body-design.md'
const responseEgressSpecPath =
  'docs/superpowers/specs/2026-09-08-response-egress-design.md'
const outboundImplementationIndexPath =
  'docs/superpowers/plans/2026-07-10-outbound-http-implementation.md'
const outboundBatchTerminationPlanPath =
  'docs/superpowers/plans/2026-09-16-outbound-batch-termination.md'
const outboundReviewHardeningPlanPath =
  'docs/superpowers/plans/2026-09-10-outbound-http-review-hardening.md'
const spinPhasePath =
  'docs/superpowers/plans/2026-09-06-outbound-http-phase5-spin.md'
const cloudflarePhasePath =
  'docs/superpowers/plans/2026-09-06-outbound-http-phase4-axum-cloudflare.md'
const fastlyPhasePath =
  'docs/superpowers/plans/2026-09-06-outbound-http-phase6-fastly.md'
const migrationPhasePath =
  'docs/superpowers/plans/2026-09-06-outbound-http-phase7-migration-docs.md'
const sidebarPath = 'docs/.vitepress/config.mts'
const expectedHeader = ['Capability', 'Axum', 'Cloudflare', 'Fastly', 'Spin']
const expectedIngressRows = [
  ['ingress-admission', 'Native', 'Native', 'Native', 'Native'],
  [
    'ingress-admission-abort',
    'Native',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'inbound-read-deadlines',
    'Native',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'raw-ingress-head-limits',
    'Unsupported',
    'Unsupported',
    'Unsupported',
    'Unsupported',
  ],
  [
    'raw-ingress-framing-validation',
    'Unsupported',
    'Unsupported',
    'Unsupported',
    'Unsupported',
  ],
]
const expectedConfigRows = [
  [
    'config-read-allocation-bounds',
    'Unsupported',
    'Unsupported',
    'Unsupported',
    'Unsupported',
  ],
  [
    'config-read-deadlines',
    'BestEffort',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
]
const expectedOutboundRows = [
  ['outbound-http', 'Native', 'Native', 'BestEffort', 'Native'],
  [
    'outbound-authority-override',
    'Native',
    'BestEffort',
    'Native',
    'Unsupported',
  ],
  [
    'outbound-batch-cancellation',
    'Native',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'outbound-batch-completion-order',
    'Native',
    'Native',
    'BestEffort',
    'Native',
  ],
  ['outbound-batch-slot-isolation', 'Native', 'Native', 'BestEffort', 'Native'],
  ['outbound-cache-bypass', 'Native', 'Native', 'Native', 'Native'],
  [
    'outbound-complete-resource-accounting',
    'Unsupported',
    'Unsupported',
    'Unsupported',
    'Unsupported',
  ],
  ['outbound-header-fidelity', 'Native', 'BestEffort', 'Native', 'BestEffort'],
  ['outbound-deadlines', 'Native', 'BestEffort', 'BestEffort', 'BestEffort'],
  [
    'outbound-failure-diagnostics',
    'BestEffort',
    'BestEffort',
    'Native',
    'Native',
  ],
  [
    'outbound-image-optimizer',
    'Unsupported',
    'Unsupported',
    'BestEffort',
    'Unsupported',
  ],
  ['outbound-redirect-following', 'Native', 'Native', 'Native', 'Native'],
  [
    'outbound-flexible-phase-budget',
    'Native',
    'Native',
    'BestEffort',
    'BestEffort',
  ],
  [
    'streamed-upload-deadlines',
    'Native',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'lazy-streamed-response-passthrough',
    'Native',
    'Native',
    'BestEffort',
    'BestEffort',
  ],
]
const expectedResponseEgressRows = [
  [
    'buffered-response-content-length',
    'Native',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'response-egress-abort',
    'BestEffort',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'response-egress-backpressure',
    'BestEffort',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'response-egress-completion',
    'BestEffort',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
  [
    'response-write-deadlines',
    'BestEffort',
    'BestEffort',
    'BestEffort',
    'BestEffort',
  ],
]
const expectedLimitHeader = ['Control', 'Scope', 'Default']
const expectedLimitRows = [
  ['max_request_body_bytes', 'Buffered or streamed request bytes', '8 MiB'],
  [
    'max_encoded_response_bytes',
    'Upstream transport bytes before decoding',
    'Unset',
  ],
  [
    'max_decoded_response_bytes',
    'Identity or EdgeZero-decoded gzip/deflate/Brotli output',
    'Unset',
  ],
  [
    'max_response_bytes',
    'Final buffered response, including raw passthrough',
    '1 MiB',
  ],
  [
    'max_response_header_bytes',
    'Adapter-visible upstream header name/value bytes before normalization, plus `x-edgezero-proxy` when enabled',
    'Unset',
  ],
  [
    'max_response_header_count',
    'Adapter-visible upstream header fields before normalization, plus `x-edgezero-proxy` when enabled',
    'Unset',
  ],
  [
    'max_brotli_window_bits',
    'Brotli stream header checked before decoder allocation',
    '24',
  ],
  [
    'max_decoder_bytes',
    'Pinned policy charge for Brotli/gzip/deflate decoder state',
    '32 MiB',
  ],
  [
    'max_chunk_bytes',
    'Maximum emitted item size after decoding or passthrough',
    'Unset',
  ],
]
const expectedMemoryHeader = [
  'Target',
  'Primary ceiling',
  'Separate stack',
  'Scope',
  'Memory source / reason',
  'Live inbound requests',
  'Population source / reason',
  'Host ingress/framing charge',
  'Accounting source / reason',
]
const expectedMemoryRows = [
  [
    'Axum',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown: Operator configured',
    'Unknown',
    'Unknown: Operator configured',
    'Unknown',
    'Unknown: Operator configured',
  ],
  [
    'Cloudflare Workers',
    '128000000 bytes (128 MB)',
    'None',
    'Per instance',
    'Platform limit: Cloudflare Workers',
    'Unknown',
    'Unknown: Provider unpublished',
    'Unknown',
    'Unknown: Provider unpublished',
  ],
  [
    'Fastly Compute',
    '128000000 bytes (128 MB)',
    '1000000 bytes (1 MB)',
    'Per execution',
    'Platform limit: Fastly Compute',
    '1',
    'Platform limit: Fastly Compute',
    'Unknown',
    'Unknown: Provider unpublished',
  ],
  [
    'Spin (generic)',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown: Runtime configured',
    'Unknown',
    'Unknown: Runtime configured',
    'Unknown',
    'Unknown: Runtime configured',
  ],
  [
    'Akamai Functions (Spin)',
    '134217728 bytes (128 MiB)',
    'None',
    'Per execution',
    'Hosted default: Akamai Functions',
    '1',
    'Hosted default: Akamai Functions',
    'Unknown',
    'Unknown: Provider unpublished',
  ],
]

function fail(message) {
  process.stderr.write(`outbound docs contract: ${message}\n`)
  process.exit(1)
}

function cells(line) {
  const trimmed = line.trim()
  if (!trimmed.startsWith('|') || !trimmed.endsWith('|')) return null
  return trimmed
    .slice(1, -1)
    .split('|')
    .map((cell) => cell.trim())
}

function normalizeCapability(value) {
  return value.startsWith('`') && value.endsWith('`')
    ? value.slice(1, -1)
    : value
}

function normalizeSupport(value) {
  return value
    .replace(/\[\^[^\]]+\]$/u, '')
    .replace(/[¹²³⁴⁵⁶⁷⁸⁹]+$/u, '')
    .trim()
}

const redirectMethodHeader = [
  'Status',
  'Method of answering hop',
  'Next method',
  'Next body',
]
const redirectMethodRows = [
  ['301, 302', 'GET, HEAD', 'same', 'empty'],
  ['301, 302', 'POST', 'GET', 'empty'],
  ['301, 302', 'any other method', 'not followed: MethodNotRewritable', 'none'],
  ['303', 'HEAD', 'HEAD', 'empty'],
  ['303', 'any other method', 'GET', 'empty'],
  ['307, 308', 'any method', 'same', 'replayed'],
]
const redirectGuideFragments = [
  '`OutboundRedirectLimit::new` accepts only 1..=20',
  '`send`, `begin`, and batches never follow redirects',
  'Every other status is final',
  'The initial target is not passed to the redirect policy',
  'An open policy also follows private or link-local addresses',
  'There is no built-in SSRF filter',
  'scheme, host, or effective port',
  'only `Authorization`, `Cookie`, and `host_authority_override`',
  'custom API-key headers and `x-amz-*` signatures remain',
  'does not strip credentials a second time',
  'does not add `Referer` or `Origin`',
  '`Content-Type`, `Content-Length`, `Content-Encoding`, `Content-Language`, `Content-Location`, and `Expect`',
  'settled once before the initial hop',
  'Streamed uploads cannot be replayed',
  'followable status, Location presence, method, replayability, limit, Location resolution, then policy',
  'resolved non-HTTP target reaches the policy with no prepared request',
  'Original Location multiplicity and text eligibility are captured before filtering',
  'fragments never reach the wire',
  'per-call timeout starts afresh at each hop',
  'initial absolute deadline remains an independent chain-wide upper bound',
  'including policy execution and final streamed body consumption',
  'unsupported options are rejected before an expired budget',
  'synchronous policy cannot be preempted',
  'followed response body is discarded unread',
  'Response-header caps and body-independent validation still apply to every head',
  'final response, including stopped or limit-reached 3xx, uses ordinary decoding, caps, body mode and idle policy',
  'Neither the policy nor its error needs `Send` or `Clone`',
  'zero-based hop index (initial request = 0)',
  'one-based redirect number, unchanged',
  'prepared request retains every unchanged option',
  'fresh request is a complete replacement with its own defaults',
  'same supplied client and shared preparation/head-finalization pipeline',
  'supersedes the historical outbound spec',
  'Spin requires plain-HTTP origins and targets outside a narrowed host list to be declared',
  'same native Image Optimizer call path, with no extra fetch, buffering or serialization',
]
const redirectCapabilityFragments = [
  '`outbound-redirect-following` reports shared redirect policy, not native deadlines, cancellation or complete resource bounds',
  'Spin requires plain-HTTP origins and targets outside a narrowed host list to be declared',
  'Local redirect tests are not hosted qualification',
  'Native Image Optimizer qualification remains open',
  'synchronous blocking horizon is unknown',
]

function redirectDocumentationContractError(guide, capability) {
  const section = guide.match(
    /^## Following Redirects\r?\n([\s\S]*?)(?=^## |(?![\s\S]))/mu
  )?.[1]
  if (section === undefined)
    return 'outbound guide is missing Following Redirects'
  const table = section
    .split(/\r?\n/u)
    .map(cells)
    .filter((row) => row !== null)
  const expected = [
    redirectMethodHeader,
    redirectMethodHeader.map(() => '---'),
    ...redirectMethodRows,
  ]
  const normalizedTable = table.map((row, index) =>
    row.map((cell) =>
      index === 1 && /^:?-{3,}:?$/u.test(cell)
        ? '---'
        : normalizeCapability(cell)
    )
  )
  if (JSON.stringify(normalizedTable) !== JSON.stringify(expected)) {
    return 'redirect method/body table disagrees with EZ-9'
  }
  for (const [source, fragments] of [
    [section, redirectGuideFragments],
    [capability, redirectCapabilityFragments],
  ]) {
    const normalized = source.replaceAll(/\s+/gu, ' ')
    for (const fragment of fragments) {
      if (!normalized.includes(fragment))
        return `redirect documentation is missing: ${fragment}`
    }
  }
  for (const source of [guide, capability]) {
    if (
      /redirect following (?:are |is |remains )?(?:not implemented|pending)|begun\/redirect integration remains/iu.test(
        source
      )
    ) {
      return 'redirect documentation still marks implementation pending'
    }
  }
  return null
}

function runRedirectDocumentationContractSelfTests() {
  const guide = [
    '## Following Redirects',
    ...[
      redirectMethodHeader,
      redirectMethodHeader.map(() => '---'),
      ...redirectMethodRows,
    ].map((row) => `| ${row.join(' | ')} |`),
    ...redirectGuideFragments,
  ].join('\n')
  const capability = redirectCapabilityFragments.join('\n')
  const cases = [
    [guide, capability, true],
    [guide.replace('## Following Redirects', '## Other'), capability, false],
  ]
  cases.push([guide + '\n## Next Section\n| Other | Table |', capability, true])
  cases.push([
    guide.replace(redirectGuideFragments[0], '') +
      '\n## Next Section\n' +
      redirectGuideFragments[0],
    capability,
    false,
  ])
  for (const fragment of redirectGuideFragments)
    cases.push([guide.replace(fragment, ''), capability, false])
  for (const fragment of redirectCapabilityFragments)
    cases.push([guide, capability.replace(fragment, ''), false])
  for (const row of redirectMethodRows)
    cases.push([guide.replace(row.join(' | '), 'incorrect'), capability, false])
  cases.push([
    guide + '\nredirect following remains pending',
    capability,
    false,
  ])
  for (const [
    index,
    [candidateGuide, candidateCapability, valid],
  ] of cases.entries()) {
    if (
      (redirectDocumentationContractError(
        candidateGuide,
        candidateCapability
      ) ===
        null) !==
      valid
    ) {
      fail(`redirect documentation checker self-test ${index} failed`)
    }
  }
}

function readRustU64Constant(source, name) {
  const match = source.match(
    new RegExp(`pub const ${name}: u64 = ([^;]+);`, 'u')
  )
  const expression = match?.[1]
  if (expression === undefined) {
    fail(`cannot find Rust constant ${name}`)
  }
  const factors = expression.split('*').map((factor) => factor.trim())
  if (!factors.every((factor) => /^[0-9][0-9_]*$/u.test(factor))) {
    fail(`Rust constant ${name} has an unsupported expression: ${expression}`)
  }
  return factors.reduce(
    (product, factor) => product * BigInt(factor.replaceAll('_', '')),
    1n
  )
}

function formatBinaryBytes(bytes) {
  const mebibyte = 1024n * 1024n
  if (bytes % mebibyte === 0n) {
    return `${bytes / mebibyte} MiB`
  }
  return `${bytes} bytes`
}

const qualifiedDeadline = String.raw`(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*Deadline`
const optionalDeadline = String.raw`Option\s*<\s*${qualifiedDeadline}\s*>`
const deadlineType = String.raw`(?:${qualifiedDeadline}|${optionalDeadline})`

function maskObservationCutoffNonCode(source) {
  const masked = source.split('')
  const blank = (start, end) => {
    for (let index = start; index < end; index += 1) {
      if (!/[\r\n]/u.test(masked[index])) masked[index] = ' '
    }
  }
  let index = 0
  while (index < source.length) {
    if (source.startsWith('//', index)) {
      const newline = source.indexOf('\n', index + 2)
      const end = newline === -1 ? source.length : newline
      blank(index, end)
      index = end
      continue
    }
    if (source.startsWith('/*', index)) {
      let depth = 1
      let end = index + 2
      while (end < source.length && depth > 0) {
        if (source.startsWith('/*', end)) {
          depth += 1
          end += 2
        } else if (source.startsWith('*/', end)) {
          depth -= 1
          end += 2
        } else {
          end += 1
        }
      }
      blank(index, end)
      index = end
      continue
    }
    const rest = source.slice(index)
    const rawStart = rest.match(/^(?:b|c)?r(#{0,255})"/u)
    if (rawStart !== null) {
      const delimiter = `"${rawStart[1]}`
      const closing = source.indexOf(delimiter, index + rawStart[0].length)
      const end = closing === -1 ? source.length : closing + delimiter.length
      blank(index, end)
      index = end
      continue
    }
    const quoted = rest.match(
      /^(?:(?:b|c)?"(?:\\[\s\S]|[^"\\])*"|b?'(?:\\(?:u\{[0-9A-Fa-f_]+\}|x[0-9A-Fa-f]{2}|.)|[^'\\\r\n])')/u
    )
    if (quoted !== null) {
      blank(index, index + quoted[0].length)
      index += quoted[0].length
      continue
    }
    index += 1
  }
  return masked.join('')
}

function markdownFenceOpening(line) {
  const match = /^ {0,3}(`{3,}|~{3,})[ \t]*([^\r\n]*)$/u.exec(line)
  if (match === null) return null
  if (match[1][0] === '`' && match[2].includes('`')) return null
  const language = match[2]
    .trim()
    .split(/[:,\s]/u, 1)[0]
    .toLowerCase()
  return {
    marker: match[1][0],
    minimumLength: match[1].length,
    rust: language === 'rust' || language === 'rs',
  }
}

function markdownFenceClosing(line, fence) {
  let index = 0
  while (index < 3 && line[index] === ' ') index += 1
  let markerLength = 0
  while (line[index + markerLength] === fence.marker) markerLength += 1
  return (
    markerLength >= fence.minimumLength &&
    line.slice(index + markerLength).trim() === ''
  )
}

function markdownViews(source) {
  const prose = source.split('')
  const rust = source.split('')
  const blank = (view) => {
    for (let index = 0; index < source.length; index += 1) {
      if (!/[\r\n]/u.test(source[index])) view[index] = ' '
    }
  }
  const copy = (view, start, end) => {
    for (let index = start; index < end; index += 1) view[index] = source[index]
  }
  blank(prose)
  blank(rust)

  let fence = null
  let lineStart = 0
  while (lineStart < source.length) {
    const newline = source.indexOf('\n', lineStart)
    const lineEnd = newline === -1 ? source.length : newline
    const nextLine = newline === -1 ? source.length : newline + 1
    const line = source.slice(lineStart, lineEnd).replace(/\r$/u, '')

    if (fence === null) {
      const opening = markdownFenceOpening(line)
      if (opening === null) copy(prose, lineStart, nextLine)
      else {
        fence = {
          ...opening,
          contentStart: newline === -1 ? source.length : newline + 1,
        }
      }
    } else if (markdownFenceClosing(line, fence)) {
      if (fence.rust) copy(rust, fence.contentStart, lineStart)
      fence = null
    }

    if (newline === -1) break
    lineStart = nextLine
  }

  if (fence?.rust) copy(rust, fence.contentStart, source.length)
  return { prose: prose.join(''), rust: rust.join('') }
}

function closingDelimiter(source, open) {
  const pairs = new Map([
    ['(', ')'],
    ['[', ']'],
    ['{', '}'],
  ])
  const stack = [pairs.get(source[open])]
  for (let index = open + 1; index < source.length; index += 1) {
    if (pairs.has(source[index])) stack.push(pairs.get(source[index]))
    else if (source[index] === stack.at(-1)) {
      stack.pop()
      if (stack.length === 0) return index
    }
  }
  return -1
}

function topLevelArguments(source, open, close) {
  const argumentsList = []
  let start = open + 1
  for (let index = start; index <= close; index += 1) {
    if (index === close || source[index] === ',') {
      const argument = source.slice(start, index).trim()
      if (argument !== '') argumentsList.push(argument)
      start = index + 1
    } else if ('([{'.includes(source[index])) {
      index = closingDelimiter(source, index)
      if (index === -1) return []
    }
  }
  return argumentsList
}

function signatureContractError(
  source,
  name,
  count,
  typeShape,
  unusedCount = 0
) {
  const code = maskObservationCutoffNonCode(source)
  const signatures =
    code.match(new RegExp(String.raw`\bfn\s+${name}\s*\([^)]*\)`, 'gu')) ?? []
  const expectedType =
    typeShape === 'optional' ? optionalDeadline : qualifiedDeadline
  const parameterCount = (parameter) =>
    signatures.filter((signature) =>
      new RegExp(
        String.raw`(?:^|,)\s*${parameter}\s*:\s*${expectedType}\s*(?=,|\))`,
        'u'
      ).test(signature.slice(signature.indexOf('(') + 1))
    ).length
  const staleParameter = new RegExp(
    String.raw`(?:^|,)\s*(?:cutoff|_cutoff|batch_cutoff)\s*:\s*${deadlineType}\s*(?=,|\))`,
    'u'
  )
  return signatures.some((signature) => staleParameter.test(signature)) ||
    signatures.length !== count ||
    parameterCount('observation_cutoff') !== count - unusedCount ||
    parameterCount('_observation_cutoff') !== unusedCount
    ? `${name} must use ${typeShape} observation_cutoff: Deadline in ${count} signature(s)`
    : null
}

function exampleCodeError(code) {
  const staleName = String.raw`(?:cutoff|_cutoff|batch_cutoff)`
  const staleBinding = new RegExp(
    String.raw`^\s*let\s+(?:mut\s+)?${staleName}\b`,
    'mu'
  )
  if (staleBinding.test(code)) return 'a stale cutoff binding'
  const batchCall = /\b(?:start_batch_until|send_all_until)\s*\(/gu
  for (const match of code.matchAll(batchCall)) {
    const open = match.index + match[0].lastIndexOf('(')
    const close = closingDelimiter(code, open)
    if (close === -1) continue
    const finalArgument = topLevelArguments(code, open, close).at(-1)
    if (new RegExp(String.raw`^${staleName}$`, 'u').test(finalArgument ?? '')) {
      return 'a stale batch cutoff argument'
    }
  }
  return null
}

function exampleContractError(path, source) {
  if (!path.endsWith('.md') && !path.endsWith('.mdx')) {
    return exampleCodeError(maskObservationCutoffNonCode(source))
  }
  const views = markdownViews(source)
  return (
    exampleCodeError(views.prose) ??
    exampleCodeError(maskObservationCutoffNonCode(views.rust))
  )
}

function runObservationCutoffContractSelfTests() {
  const bareCutoff = ['cut', 'off'].join('')
  const ignoredCutoff = `_${bareCutoff}`
  const batchCutoff = `batch_${bareCutoff}`
  for (const [parameters, typeShape, valid] of [
    ['observation_cutoff: Deadline', 'plain', true],
    ['observation_cutoff: edgezero_core :: Deadline', 'plain', true],
    [
      'observation_cutoff: Option < edgezero_core :: Deadline >',
      'optional',
      true,
    ],
    ['observation_cutoff: Option<Deadline>', 'plain', false],
    ['observation_cutoff: Deadline', 'optional', false],
    [`${bareCutoff}: Deadline`, 'plain', false],
    [`${ignoredCutoff}: edgezero_core::Deadline`, 'plain', false],
    [`${batchCutoff}: Option < Deadline >`, 'optional', false],
    [`${bareCutoff}: Deadline, observation_cutoff: Deadline`, 'plain', false],
  ]) {
    const source = `fn dispatch_budget(request: Request, ${parameters}) {}`
    const passed =
      signatureContractError(source, 'dispatch_budget', 1, typeShape) === null
    if (passed !== valid) {
      fail(`observation cutoff signature self-test failed for ${parameters}`)
    }
  }
  for (const [name, source, valid] of [
    ['mutable binding', `let mut ${bareCutoff} = deadline();`, false],
    ['ignored binding', `let ${ignoredCutoff} = deadline();`, false],
    ['batch binding', `let ${batchCutoff} = deadline();`, false],
    [
      'start call',
      `client.start_batch_until(vec![request()], ${bareCutoff});`,
      false,
    ],
    ['ignored call', `client.send_all_until(x, ${ignoredCutoff});`, false],
    ['send call', `client.send_all_until(x, ${batchCutoff});`, false],
    [
      'multiline trailing call',
      `client.start_batch_until(\n    make_requests(),\n    ${bareCutoff},\n);`,
      false,
    ],
    [
      'nested cutoff',
      `client.start_batch_until(make_requests(x, ${bareCutoff}), observation_cutoff);`,
      true,
    ],
    ['comment', `// let mut ${bareCutoff} = deadline();`, true],
    [
      'string',
      `const TEXT: &str = "send_all_until(requests, ${bareCutoff})";`,
      true,
    ],
    ['budget source', 'let source = BudgetSource::BatchCutoff;', true],
    ['constructor', 'let batch = OutboundBatch::cutoff(2);', true],
  ]) {
    if ((exampleContractError('fixture.rs', source) === null) !== valid) {
      fail(`observation cutoff example self-test failed for ${name}`)
    }
  }
  if (
    exampleContractError(
      'fixture.md',
      `Prose: send_all_until(requests, ${bareCutoff})\n` +
        '```rust\nlet observation_cutoff = deadline();\n```'
    ) === null
  ) {
    fail('observation cutoff self-test missed Markdown prose')
  }
  if (
    exampleContractError(
      'fixture.md',
      `\`\`\`rust,no_run\nlet ${bareCutoff} = deadline();\n\`\`\``
    ) === null
  ) {
    fail('observation cutoff self-test missed a modified Rust fence')
  }
  if (
    exampleContractError(
      'fixture.md',
      `\`\`\`rust\`bad\nlet ${bareCutoff} = deadline();\n\`\`\``
    ) === null
  ) {
    fail(
      'observation cutoff self-test treated an invalid backtick fence as a fence'
    )
  }
  for (const [name, fence] of [
    [
      'tilde colon modifier',
      `~~~rust:no_run\nlet ${bareCutoff} = deadline();\n~~~~`,
    ],
    [
      'indented space modifier',
      `   \`\`\`rust no_run\nlet ${bareCutoff} = deadline();\n   \`\`\``,
    ],
    [
      'variable-length rs fence',
      `\`\`\`\`rs:ignore\nlet ${bareCutoff} = deadline();\n\`\`\`\`\``,
    ],
  ]) {
    if (exampleContractError('fixture.md', fence) === null) {
      fail(`observation cutoff self-test missed ${name}`)
    }
  }
  for (const [name, fence] of [
    ['non-Rust fence', `\`\`\`text\nlet ${bareCutoff} = deadline();\n\`\`\``],
    [
      'Rust fence comments and literals',
      `\`\`\`rust\n// let ${bareCutoff} = deadline();\n` +
        `const TEXT: &str = "let ${bareCutoff} = deadline();";\n` +
        `const RAW: &str = r#"let ${bareCutoff} = deadline();"#;\n\`\`\``,
    ],
  ]) {
    if (exampleContractError('fixture.md', fence) !== null) {
      fail(`observation cutoff self-test scanned ${name}`)
    }
  }
  for (const [name, source] of [
    [
      'block comment',
      `fn dispatch_budget(x: Request, observation_cutoff: Deadline) {}\n/*\nlet ${bareCutoff} = deadline();\nfn dispatch_budget(x: Request, ${bareCutoff}: Deadline) {}\n*/`,
    ],
    [
      'raw string',
      `fn dispatch_budget(x: Request, observation_cutoff: Deadline) {}\nconst TEXT: &str = r#"\nlet ${bareCutoff} = deadline();\nfn dispatch_budget(x: Request, ${bareCutoff}: Deadline) {}\n"#;`,
    ],
  ]) {
    if (
      signatureContractError(source, 'dispatch_budget', 1, 'plain') !== null ||
      exampleContractError('fixture.rs', source) !== null
    ) {
      fail(`observation cutoff self-test rejected ${name}`)
    }
  }
}

function testOnlyCondition(condition) {
  const expression = condition.trim()
  if (expression === 'test') return true
  const operation = /^(all|any)\s*\(/u.exec(expression)
  if (operation === null) return false
  const open = operation[0].lastIndexOf('(')
  const close = closingDelimiter(expression, open)
  if (close === -1 || expression.slice(close + 1).trim() !== '') return false
  const inputs = topLevelArguments(expression, open, close)
  return operation[1] === 'all'
    ? inputs.some(testOnlyCondition)
    : inputs.length > 0 && inputs.every(testOnlyCondition)
}

function productionCode(source) {
  const code = maskObservationCutoffNonCode(source)
  const view = code.split('')
  for (const match of code.matchAll(/#\s*\[\s*cfg\s*\(/gu)) {
    const open = match.index + match[0].lastIndexOf('(')
    const close = closingDelimiter(code, open)
    if (close === -1) fail('unclosed cfg condition')
    if (!testOnlyCondition(code.slice(open + 1, close))) continue
    let cursor = code.indexOf(']', close) + 1
    let end = -1
    for (; cursor < code.length; cursor += 1) {
      if (code[cursor] === '(' || code[cursor] === '[') {
        cursor = closingDelimiter(code, cursor)
        if (cursor === -1) fail('unclosed test item')
      } else if (code[cursor] === '{') {
        end = closingDelimiter(code, cursor)
        break
      } else if (code[cursor] === ';') {
        end = cursor
        break
      }
    }
    if (end === -1) fail('unclosed test item')
    for (let index = match.index; index <= end; index += 1) {
      if (code[index] !== '\n') view[index] = ' '
    }
  }
  return view.join('')
}

function declarationBody(source, declaration) {
  const match = declaration.exec(source)
  if (match === null) return null
  const open = match.index + match[0].lastIndexOf('{')
  const close = closingDelimiter(source, open)
  return close === -1 ? null : source.slice(open + 1, close)
}

function liveOutboundContractError(core, batch, runtime, body) {
  const trait = declarationBody(
    core,
    /\bpub\s+trait\s+OutboundHttpClient\s*:\s*Send\s*\+\s*Sync\s*\{/u
  )
  if (trait === null) return 'missing object-safe Send + Sync outbound trait'
  const hooks = [
    /\bfn\s+progress\s*\(\s*&self\s*\)\s*->\s*(?:std\s*::\s*rc\s*::\s*)?Rc\s*<\s*dyn\s+OutboundProgressRuntime\s*>\s*;/u,
    /\bfn\s+time\s*\(\s*&self\s*\)\s*->\s*OutboundTime\s*;/u,
    /\bfn\s+validate_supported_options\s*\(\s*&self\s*,\s*request\s*:\s*&OutboundRequest\s*,\s*mode\s*:\s*OutboundSendMode\s*,?\s*\)\s*->\s*Result\s*<\s*\(\s*\)\s*,\s*EdgeError\s*>\s*;/u,
    /\bfn\s+dispatch_head\s*\(\s*&self\s*,\s*request\s*:\s*OutboundDispatchRequest\s*,?\s*\)\s*->\s*OutboundFuture\s*<\s*OutboundHead\s*>\s*;/u,
    /\bfn\s+begin\s*\(\s*&self\s*,\s*request\s*:\s*OutboundDispatchRequest\s*,?\s*\)\s*->\s*Result\s*<\s*OutboundPending\s*,\s*EdgeError\s*>\s*;/u,
    /\bfn\s+start_batch_until\s*\(\s*&self\s*,\s*batch\s*:\s*OutboundPreparedBatch\s*,?\s*\)\s*->\s*OutboundBatch\s*;/u,
  ]
  if (
    !hooks.every((hook) => hook.test(trait)) ||
    /\bfn\s+(?:send|capture_scope)\s*\(/u.test(trait)
  ) {
    return 'outbound trait must require all six hooks including explicit progress runtime, without defaults or retired hooks'
  }
  if (
    !/\bpub\s+trait\s+OutboundProgressRuntime\b/u.test(runtime) ||
    !/\bpub\s+struct\s+IndependentOutboundProgress\s*;/u.test(runtime) ||
    !/\bimpl\s+OutboundProgressRuntime\s+for\s+IndependentOutboundProgress\b/u.test(
      runtime
    )
  ) {
    return 'outbound progress must expose its runtime trait and one concrete independent implementation, not aliases or test fixtures'
  }
  const scopeOwner = declarationBody(
    runtime,
    /\bpub\s+struct\s+OutboundScopeOwner\s*\{/u
  )
  if (
    scopeOwner === null ||
    /\bpub\s*(?:\([^)]*\))?\s*\w+\s*:/u.test(scopeOwner)
  ) {
    return 'core scope ownership must retain opaque fields'
  }
  const carrier = declarationBody(body, /\bpub\s+struct\s+Body\s*\{/u)
  if (carrier === null || /\bpub\s*(?:\([^)]*\))?\s*\w+\s*:/u.test(carrier)) {
    return 'Body must be an opaque carrier, not the retired public pattern enum'
  }
  const payload = declarationBody(body, /\bpub\s+enum\s+BodyPayload\s*\{/u)
  if (
    payload === null ||
    !/\bOnce\s*\(\s*Bytes\s*\)/u.test(payload) ||
    !/\bStream\s*\(\s*BodyStream\s*\)/u.test(payload) ||
    !/\bpub\s+fn\s+into_parts\s*\(\s*(?:mut\s+)?self\s*\)\s*->\s*\(\s*BodyPayload\s*,\s*Option\s*<\s*OutboundScopeOwner\s*>\s*\)/u.test(
      body
    )
  ) {
    return 'owned Body inspection must expose canonical BodyPayload variants and transfer its optional owner through into_parts'
  }
  for (const name of [
    'OutboundPreparedBatch',
    'OutboundBatchContext',
    'OutboundBatchItem',
    'OutboundBatchResults',
    'OutboundBatchFailure',
  ]) {
    const fields = declarationBody(
      batch,
      new RegExp(String.raw`\bpub\s+struct\s+${name}\s*\{`, 'u')
    )
    if (fields === null || /\bpub\s*(?:\([^)]*\))?\s*\w+\s*:/u.test(fields)) {
      return `${name} must retain private context or identity fields`
    }
    const implementation = declarationBody(
      batch,
      new RegExp(String.raw`\bimpl\s+${name}\s*\{`, 'u')
    )
    if (
      implementation !== null &&
      /\bpub\s+fn\s+(?:new|from_parts)\b/u.test(implementation)
    ) {
      return `${name} must not have a public reconstruction constructor`
    }
  }
  const driver = declarationBody(
    batch,
    /\bpub\s+enum\s+OutboundBatchDriverEvent\s*\{/u
  )
  if (
    driver === null ||
    !/\bItem\s*\{\s*index\s*:\s*usize\s*,\s*completed_at\s*:\s*MonotonicInstant\s*,\s*outcome\s*:\s*Result\s*<\s*OutboundResponse\s*,\s*EdgeError\s*>\s*,?\s*\}/u.test(
      driver
    )
  ) {
    return 'drivers must emit raw index, terminal timestamp and outcome, not consumer receipts'
  }
  if (
    !/\bpub\s+fn\s+cutoff\s*\(\s*context\s*:\s*OutboundBatchContext\s*\)\s*->\s*Self/u.test(
      batch
    ) ||
    !/\bpub\s+fn\s+from_driver\s*<\s*S\s*>\s*\(\s*context\s*:\s*OutboundBatchContext\s*,\s*events\s*:\s*S\s*,\s*owner\s*:\s*OutboundExchangeOwner\s*,?\s*\)/u.test(
      batch
    )
  ) {
    return 'batch constructors must consume opaque context and the driver cancellation owner'
  }
  for (const state of ['AlreadyDelivered', 'Resolved', 'Unresolved']) {
    if (!new RegExp(String.raw`\b${state}\b`, 'u').test(batch))
      return `missing collected ownership state ${state}`
  }
  if (/\b(?:from_futures|finish_batch_item)\b/u.test(core + '\n' + batch)) {
    return 'removed future factories and receipt helpers must not remain in production'
  }
  return null
}

function observationCutoffContract(coreProduction) {
  const facade = declarationBody(coreProduction, /\bimpl\s+HttpClient\s*\{/u)
  if (
    facade === null ||
    !/\bpub\s+fn\s+start_batch_until\s*\(\s*&self\s*,\s*requests\s*:\s*Vec\s*<\s*OutboundRequest\s*>\s*,\s*(?:observation_cutoff|cutoff)\s*:\s*Deadline\s*,?\s*\)/u.test(
      facade
    )
  ) {
    fail(
      'facade batch entry must retain request-vector and absolute observation cutoff'
    )
  }
  const error = signatureContractError(
    readFileSync('crates/edgezero-core/src/time.rs', 'utf8'),
    'dispatch_budget',
    1,
    'optional'
  )
  if (error !== null) fail(error)
  for (const path of [proxyGuidePath]) {
    const exampleError = exampleContractError(path, readFileSync(path, 'utf8'))
    if (exampleError !== null) fail(`${path} contains ${exampleError}`)
  }
}

function headerNormalizationContractError(source, fidelity, headSource = null) {
  let code = productionCode(source)
  if (headSource !== null) {
    if (
      !code.includes('mod head;') ||
      !code.includes(
        'super::head::response_head(status, native_headers, encoded, context)'
      ) ||
      (code.match(/super::head::response_head\s*\(/gu)?.length ?? 0) !== 1
    ) {
      return 'must delegate unchanged native fields, source and context to its single head boundary'
    }
    code += '\n' + productionCode(headSource)
  }
  if (
    (code.match(/OutboundHeaderInput::new\(/gu)?.length ?? 0) !== 1 ||
    (code.match(/OutboundHead::new\(/gu)?.length ?? 0) !== 1 ||
    /\.normalize\s*\(/u.test(code) ||
    /\bOutboundResponse\s*::\s*new\s*\(/u.test(code) ||
    !code.includes(`OutboundHeaderFidelity::${fidelity}`) ||
    /\b(?:normalize_response_headers|insert_proxy_header|apply_response_body_policy)\s*\(/u.test(
      code
    )
  ) {
    return 'must pass original fields and provider fidelity to the single shared head finalizer without adapter normalization'
  }
  return null
}

function runHeaderNormalizationContractSelfTests() {
  const source = `
    let (parts, context) = request.into_parts();
    let headers = OutboundHeaderInput::new(
      "cloudflare", response_headers(&response)?,
      OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
    );
    OutboundHead::new(status, headers, encoded, context, owner)
  `
  const unknown = 'ProviderFieldBoundariesUnknown'
  const delegated =
    'mod head; fn response_head() { super::head::response_head(status, native_headers, encoded, context) }'
  const cases = [
    [source, unknown, true],
    [
      source +
        '\n#[cfg(all(test, feature = "test-utils"))] mod tests { OutboundResponse::new(); }',
      unknown,
      true,
    ],
    [
      source + '\n#[cfg(test)] fn fixture() { OutboundResponse::new(); }',
      unknown,
      true,
    ],
    [source, 'AdapterVisibleFields', false],
    [
      source.replace('OutboundHead::new(', 'OutboundResponse::new('),
      unknown,
      false,
    ],
    [source.replace('OutboundHeaderInput::new(', 'raw_head('), unknown, false],
    [`${source}\nheaders.normalize(limiter);`, unknown, false],
    [`${source}\ninsert_proxy_header(headers);`, unknown, false],
    [
      `${source}\nOutboundResponse::new(method, status, headers, body, clock, provenance);`,
      unknown,
      false,
    ],
    [
      source
        .split('\n')
        .map((line) => `// ${line}`)
        .join('\n'),
      unknown,
      false,
    ],
    [`/* ${source} */`, unknown, false],
    [`const TEXT: &str = r#"${source}"#;`, unknown, false],
    [delegated, unknown, true, source],
    [delegated.replace('native_headers', 'normalized'), unknown, false, source],
    [delegated.replace('mod head;', ''), unknown, false, source],
    [delegated, unknown, false, source + source],
    [delegated, 'AdapterVisibleFields', false, source],
    [delegated + '\nnormalize_response_headers();', unknown, false, source],
  ]
  for (const [index, [fixture, fidelity, valid, head]] of cases.entries()) {
    if (
      (headerNormalizationContractError(fixture, fidelity, head) === null) !==
      valid
    ) {
      fail(`head-first header checker self-test ${index} failed`)
    }
  }
}

function runLiveOutboundContractSelfTests() {
  const core = `
    pub trait OutboundHttpClient: Send + Sync {
      fn progress(&self) -> Rc<dyn OutboundProgressRuntime>;
      fn time(&self) -> OutboundTime;
      fn validate_supported_options(&self, request: &OutboundRequest, mode: OutboundSendMode) -> Result<(), EdgeError>;
      fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead>;
      fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError>;
      fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch;
    }
  `
  const batch = `
    pub struct OutboundPreparedBatch { context: OutboundBatchContext }
    pub struct OutboundBatchContext { original: usize }
    pub struct OutboundBatchItem { identity: Token, index: usize, result: Result }
    pub struct OutboundBatchResults { identity: Token, slots: Slots, termination: State }
    pub struct OutboundBatchFailure { identity: Token, error: EdgeError, slots: Slots }
    pub enum OutboundBatchDriverEvent {
      Item { index: usize, completed_at: MonotonicInstant, outcome: Result<OutboundResponse, EdgeError> },
    }
    pub enum OutboundCollectedSlot { AlreadyDelivered, Resolved(OutboundSlotResult), Unresolved }
    impl OutboundBatch {
      pub fn cutoff(context: OutboundBatchContext) -> Self {}
      pub fn from_driver<S>(context: OutboundBatchContext, events: S, owner: OutboundExchangeOwner) -> Self {}
    }
  `
  const runtime = `
    pub trait OutboundProgressRuntime {}
    pub struct IndependentOutboundProgress;
    impl OutboundProgressRuntime for IndependentOutboundProgress {}
    pub struct OutboundScopeOwner { scope: OutboundScope }
  `
  const body = `
    pub struct Body { payload: BodyPayload, owner: Option<OutboundScopeOwner> }
    pub enum BodyPayload { Once(Bytes), Stream(BodyStream) }
    impl Body {
      pub fn into_parts(self) -> (BodyPayload, Option<OutboundScopeOwner>) {}
    }
  `
  const cases = [
    [core, batch, true],
    [core.replace('Rc<dyn', 'std::rc::Rc<dyn'), batch, true],
    [
      core.replace(
        'fn progress(&self) -> Rc<dyn OutboundProgressRuntime>;',
        ''
      ),
      batch,
      false,
    ],
    [core.replace('fn progress', 'fn capture_scope'), batch, false],
    [
      core.replace(
        '-> Rc<dyn OutboundProgressRuntime>;',
        '-> Rc<dyn OutboundProgressRuntime> { fallback() }'
      ),
      batch,
      false,
    ],
    [
      core.replace(
        '-> Rc<dyn OutboundProgressRuntime>;',
        '-> Option<Rc<dyn OutboundProgressRuntime>>;'
      ),
      batch,
      false,
    ],
    [core.replace('Rc<dyn', 'Arc<dyn'), batch, false],
    [
      core.replace('Rc<dyn OutboundProgressRuntime>', 'ProgressAlias'),
      batch,
      false,
    ],
    [
      core.replace('fn time', 'fn capture_scope(&self) -> Scope; fn time'),
      batch,
      false,
    ],
    [core.replace('fn time(&self) -> OutboundTime;', ''), batch, false],
    [
      core.replace(
        '-> OutboundFuture<OutboundHead>;',
        '-> OutboundFuture<OutboundHead> { lazy() }'
      ),
      batch,
      false,
    ],
    [core.replace('pub trait', 'pub unsafe trait'), batch, false],
    [
      core.replace('OutboundPreparedBatch', 'Vec<OutboundRequest>'),
      batch,
      false,
    ],
    [
      core.replace(
        'fn time',
        'async fn send(&self, request: OutboundRequest); fn time'
      ),
      batch,
      false,
    ],
    [
      core,
      batch.replace('index: usize, result:', 'pub index: usize, result:'),
      false,
    ],
    [
      core,
      batch +
        '\nimpl OutboundBatchItem { pub fn new(index: usize) -> Self {} }',
      false,
    ],
    [core, batch.replace('completed_at: MonotonicInstant,', ''), false],
    [
      core,
      batch.replace(
        'Item { index: usize, completed_at: MonotonicInstant, outcome: Result<OutboundResponse, EdgeError> }',
        'Item(OutboundBatchItem)'
      ),
      false,
    ],
    [
      core,
      batch.replace(
        'context: OutboundBatchContext) -> Self',
        'slot_count: usize) -> Self'
      ),
      false,
    ],
    [core, batch.replace('owner: OutboundExchangeOwner', 'owner: ()'), false],
    [core, batch.replace('AlreadyDelivered,', ''), false],
    [core + '\nfn from_futures() {}', batch, false],
    [core + '\nfn finish_batch_item() {}', batch, false],
  ]
  for (const [
    index,
    [candidateCore, candidateBatch, valid],
  ] of cases.entries()) {
    if (
      (liveOutboundContractError(
        productionCode(candidateCore),
        productionCode(candidateBatch),
        productionCode(runtime),
        productionCode(body)
      ) ===
        null) !==
      valid
    ) {
      fail(`live outbound checker self-test ${index} failed`)
    }
  }
  for (const [index, [candidate, valid]] of [
    [runtime, true],
    [runtime.replace('pub trait OutboundProgressRuntime {}', ''), false],
    [
      runtime.replace(
        'pub struct IndependentOutboundProgress;',
        'pub type IndependentOutboundProgress = OtherRuntime;'
      ),
      false,
    ],
    [
      runtime.replace(
        'impl OutboundProgressRuntime for IndependentOutboundProgress {}',
        ''
      ),
      false,
    ],
    [
      runtime.replace(
        'pub struct OutboundScopeOwner { scope: OutboundScope }',
        ''
      ),
      false,
    ],
    [
      runtime.replace('scope: OutboundScope', 'pub scope: OutboundScope'),
      false,
    ],
    [`#[cfg(test)] mod tests { ${runtime} }`, false],
  ].entries()) {
    if (
      (liveOutboundContractError(
        productionCode(core),
        productionCode(batch),
        productionCode(candidate),
        productionCode(body)
      ) ===
        null) !==
      valid
    ) {
      fail(`outbound runtime checker self-test ${index} failed`)
    }
  }
  for (const [index, [candidate, valid]] of [
    [body, true],
    [body.replace('into_parts(self)', 'into_parts(mut self)'), true],
    [body.replace('pub struct Body', 'pub enum Body'), false],
    [body.replace('payload: BodyPayload', 'pub payload: BodyPayload'), false],
    [body.replace('owner: Option', 'pub owner: Option'), false],
    [
      body.replace(
        'pub enum BodyPayload { Once(Bytes), Stream(BodyStream) }',
        'pub type BodyPayload = OtherBody;'
      ),
      false,
    ],
    [body.replace('Stream(BodyStream)', 'Stream(OtherStream)'), false],
    [body.replace('Once(Bytes),', ''), false],
    [
      body.replace(
        '-> (BodyPayload, Option<OutboundScopeOwner>)',
        '-> BodyPayload'
      ),
      false,
    ],
    [`#[cfg(test)] mod tests { ${body} }`, false],
  ].entries()) {
    if (
      (liveOutboundContractError(
        productionCode(core),
        productionCode(batch),
        productionCode(runtime),
        productionCode(candidate)
      ) ===
        null) !==
      valid
    ) {
      fail(`body carrier checker self-test ${index} failed`)
    }
  }
  const onlyTest = '#[cfg(test)] mod tests { OutboundResponse::new(); }'
  if (/OutboundResponse::new/u.test(productionCode(onlyTest)))
    fail('test fixtures must not satisfy production checks')
  const unknown = 'ProviderFieldBoundariesUnknown'
  const testConstructor = `#[cfg(test)] mod tests {
    OutboundHeaderInput::new("cloudflare", headers, OutboundHeaderFidelity::${unknown});
    OutboundHead::new(status, headers, encoded, context, owner);
  }`
  if (headerNormalizationContractError(testConstructor, unknown) === null) {
    fail('adapter test constructors must not satisfy production head checks')
  }
}

runObservationCutoffContractSelfTests()
runHeaderNormalizationContractSelfTests()
runLiveOutboundContractSelfTests()
runRedirectDocumentationContractSelfTests()
if (
  process.argv.includes('--observation-cutoff-self-test') ||
  process.argv.includes('--redirect-self-test')
) {
  process.exit(0)
}

let capabilitySource
try {
  capabilitySource = readFileSync(capabilityPath, 'utf8')
} catch (error) {
  fail(`cannot read ${capabilityPath}: ${error.message}`)
}

const lines = capabilitySource.split(/\r?\n/u)

function readExactTable(sectionTitle, header, name) {
  const headingIndex = lines.findIndex(
    (line) => line.trim() === `## ${sectionTitle}`
  )
  if (headingIndex === -1) fail(`${name} section is missing`)
  const nextHeadingIndex = lines.findIndex(
    (line, index) => index > headingIndex && line.startsWith('## ')
  )
  const sectionEnd = nextHeadingIndex === -1 ? lines.length : nextHeadingIndex
  const headerIndex = lines.findIndex(
    (line, index) =>
      index > headingIndex &&
      index < sectionEnd &&
      JSON.stringify(cells(line)) === JSON.stringify(header)
  )
  if (headerIndex === -1) fail(`${name} table is missing`)
  const separator = cells(lines[headerIndex + 1] ?? '')
  if (
    separator === null ||
    separator.length !== header.length ||
    !separator.every((value) => /^:?-{3,}:?$/u.test(value))
  ) {
    fail(`${name} table has an invalid separator row`)
  }
  const rows = []
  for (let index = headerIndex + 2; index < sectionEnd; index += 1) {
    const row = cells(lines[index])
    if (row === null) break
    if (row.length !== header.length) {
      fail(
        `${name} row ${index + 1} has ${row.length} cells, expected ${header.length}`
      )
    }
    rows.push(row)
  }
  return rows
}

const actualMemoryRows = readExactTable(
  'Platform Memory Ceilings',
  expectedMemoryHeader,
  'platform memory'
)
if (JSON.stringify(actualMemoryRows) !== JSON.stringify(expectedMemoryRows)) {
  fail(
    `platform memory mismatch\nexpected=${JSON.stringify(expectedMemoryRows)}\nactual=${JSON.stringify(actualMemoryRows)}`
  )
}
const normalizedCapabilitySource = capabilitySource.replaceAll(/\s+/gu, ' ')
for (const fragment of [
  'SDK failure evidence, not stronger transport guarantees',
  'pinned Viceroy can report an internal cause',
  'Framework code does not log provider detail',
  'Local SDK/Node/WASI fixtures are not hosted qualification',
]) {
  if (!normalizedCapabilitySource.includes(fragment)) {
    fail(`provider diagnostic capability is missing its scope: ${fragment}`)
  }
}
for (const requiredFragment of [
  'six simultaneous outbound connections',
  'it is not an inbound population bound',
  'host parser, HPACK, or framing allocations',
  'only `Fits` is a complete validation result',
]) {
  if (!normalizedCapabilitySource.includes(requiredFragment)) {
    fail(`platform memory narrative is missing: ${requiredFragment}`)
  }
}

function tableAfterHeading(source, heading, header) {
  const sourceLines = source.split(/\r?\n/u)
  const headingIndex = sourceLines.findIndex((line) => line.trim() === heading)
  if (headingIndex === -1) fail(`missing heading: ${heading}`)
  const nextHeadingOffset = sourceLines
    .slice(headingIndex + 1)
    .findIndex((line) => /^#{1,6}\s/u.test(line.trim()))
  const sectionEnd =
    nextHeadingOffset === -1
      ? sourceLines.length
      : headingIndex + 1 + nextHeadingOffset
  const headerIndex = sourceLines.findIndex(
    (line, index) =>
      index > headingIndex &&
      index < sectionEnd &&
      JSON.stringify(cells(line)) === JSON.stringify(header)
  )
  if (headerIndex === -1) fail(`missing table under ${heading}`)
  const separator = cells(sourceLines[headerIndex + 1] ?? '')
  if (
    separator === null ||
    separator.length !== header.length ||
    !separator.every((cell) => /^:?-{3,}:?$/u.test(cell))
  ) {
    fail(`invalid table separator under ${heading}`)
  }
  const rows = []
  for (let index = headerIndex + 2; index < sectionEnd; index += 1) {
    const row = cells(sourceLines[index])
    if (row === null) break
    if (row.length !== header.length) {
      fail(`invalid table row width under ${heading}`)
    }
    rows.push(row)
  }
  return rows
}

const migrationHeader = ['Removed or changed API', 'Replacement']
const designMigrationRows = tableAfterHeading(
  readFileSync(platformMetadataDesignPath, 'utf8'),
  '## Migration record',
  migrationHeader
)
const changelogMigrationRows = tableAfterHeading(
  readFileSync(changelogPath, 'utf8'),
  '### Platform resource metadata',
  migrationHeader
)
if (
  designMigrationRows.length !== 8 ||
  JSON.stringify(changelogMigrationRows) !== JSON.stringify(designMigrationRows)
) {
  fail(
    'Unreleased changelog must contain the focused design migration table exactly'
  )
}
if (
  !readFileSync(adapterOverviewPath, 'utf8').includes(
    'CHANGELOG.md#platform-resource-metadata'
  )
) {
  fail('adapter overview must link to the Unreleased migration table')
}

function findMatrixHeader(sectionTitle, name) {
  const headingIndex = lines.findIndex(
    (line) => line.trim() === `## ${sectionTitle}`
  )
  if (headingIndex === -1) {
    fail(`${name} capability section is missing`)
  }
  const nextHeadingIndex = lines.findIndex(
    (line, index) => index > headingIndex && line.startsWith('## ')
  )
  const sectionEnd = nextHeadingIndex === -1 ? lines.length : nextHeadingIndex
  const headerIndex = lines.findIndex(
    (line, index) =>
      index > headingIndex &&
      index < sectionEnd &&
      JSON.stringify(cells(line)) === JSON.stringify(expectedHeader)
  )
  if (headerIndex === -1) {
    fail(`${name} capability matrix is missing`)
  }
  return headerIndex
}

function readMatrix(headerIndex, name) {
  const separator = cells(lines[headerIndex + 1] ?? '')
  if (
    separator === null ||
    separator.length !== expectedHeader.length ||
    !separator.every((value) => /^:?-{3,}:?$/u.test(value))
  ) {
    fail(`${name} capability matrix is missing its five-column separator row`)
  }

  const actualRows = []
  for (let index = headerIndex + 2; index < lines.length; index += 1) {
    const row = cells(lines[index])
    if (row === null) break
    if (row.length !== expectedHeader.length) {
      fail(
        `${name} capability matrix row ${index + 1} has ${row.length} cells, expected 5`
      )
    }
    actualRows.push([
      normalizeCapability(row[0]),
      ...row.slice(1).map(normalizeSupport),
    ])
  }
  return actualRows
}

const matrices = [
  [
    'ingress',
    expectedIngressRows,
    readMatrix(findMatrixHeader('Ingress Matrix', 'ingress'), 'ingress'),
  ],
  [
    'config',
    expectedConfigRows,
    readMatrix(findMatrixHeader('Config Matrix', 'config'), 'config'),
  ],
  [
    'response egress',
    expectedResponseEgressRows,
    readMatrix(
      findMatrixHeader('Response Egress Matrix', 'response egress'),
      'response egress'
    ),
  ],
  [
    'outbound',
    expectedOutboundRows,
    readMatrix(findMatrixHeader('Outbound Matrix', 'outbound'), 'outbound'),
  ],
]
for (const [name, expectedRows, actualRows] of matrices) {
  if (JSON.stringify(actualRows) !== JSON.stringify(expectedRows)) {
    fail(
      `${name} capability matrix mismatch\nexpected=${JSON.stringify(expectedRows)}\nactual=${JSON.stringify(actualRows)}`
    )
  }
}

const hardCutSurfaces = [
  [capabilityPath, capabilitySource],
  [handlersGuidePath, readFileSync(handlersGuidePath, 'utf8')],
  [proxyGuidePath, readFileSync(proxyGuidePath, 'utf8')],
  [fastlyGuidePath, readFileSync(fastlyGuidePath, 'utf8')],
  [scaffoldReadmePath, readFileSync(scaffoldReadmePath, 'utf8')],
]
const redirectDocumentationError = redirectDocumentationContractError(
  hardCutSurfaces[2][1],
  capabilitySource
)
if (redirectDocumentationError !== null) fail(redirectDocumentationError)
for (const [path, source] of hardCutSurfaces) {
  for (const staleFragment of [
    'HttpClient::send_all`',
    '.send_all(',
    'send-all-slot-isolation',
    'run_app_with_request_extensions',
  ]) {
    if (source.includes(staleFragment)) {
      fail(`${path} contains removed outbound API text: ${staleFragment}`)
    }
  }
}
for (const staleFragment of [
  'harvests response bodies in input order',
  'harvests responses in input order',
  'Fastly divides a total budget among provider phase timers',
  'Exact ceiled-millisecond budgets are part of backend identity',
]) {
  if (capabilitySource.includes(staleFragment)) {
    fail(
      `${capabilityPath} contains stale Fastly backend or batch behavior: ${staleFragment}`
    )
  }
}
const handlersGuideSource = hardCutSurfaces[1][1]
for (const requiredFragment of [
  'fn configure_app(app: &mut edgezero_core::app::App) -> Result<(), EdgeError>',
  'completion: ResponseEgressCompletion::empty()',
  'DetachedResponseEgressDecision::Send {',
  'deadline: head.write_deadline_after(DEFAULT_RESPONSE_WRITE_BUDGET),',
  'ResponseEgressCompletion::new(move |report| ... )',
  'Use `left.join(right)`',
  '`AdmissionDecision::Abort`',
  '`App::set_detached_response_egress_decision_factory`',
  'app.set_error_response_renderer(|error|',
  '`DetachedResponseEgressDecision::Abort`',
  'Every `Send` deadline is mandatory and absolute',
  '`Abort` creates no response or egress attempt and invokes no completion callback',
  'sorted, deduplicated `Allow` field',
  'Effective `config_out_of_date` responses retain `Retry-After: 60`',
  'status returned by the callback is ignored',
  'never belongs in response extensions',
]) {
  if (!handlersGuideSource.includes(requiredFragment)) {
    fail(
      `${handlersGuidePath} is missing lifecycle contract: ${requiredFragment}`
    )
  }
}
const cloudflareGuideSource = readFileSync(cloudflareGuidePath, 'utf8')
for (const path of [
  fastlyGuidePath,
  cloudflareGuidePath,
  'docs/guide/adapters/spin.md',
]) {
  const guide = readFileSync(path, 'utf8')
  if (
    guide.includes('`application configuration failed`') ||
    !guide.includes('request::send_assembly_error')
  ) {
    fail(`${path} must use canonical typed assembly-error delivery`)
  }
}
if (
  !cloudflareGuideSource.includes(
    'EdgeZeroApp::build::<App>(CLOUDFLARE_PLATFORM)'
  ) ||
  cloudflareGuideSource.includes('build_app')
) {
  fail(
    `${cloudflareGuidePath} must construct manual applications with Cloudflare platform metadata`
  )
}
const axumGuideSource = readFileSync(axumGuidePath, 'utf8')
for (const fragment of [
  'AxumDevServer::with_config(app, config)',
  'AxumTestServer::start(server)',
  'running.shutdown().await',
  'without initializing its default transport',
  'not a Cloudflare or Spin provider request context',
]) {
  if (!axumGuideSource.includes(fragment)) {
    fail(
      `${axumGuidePath} is missing assembled-app serving contract: ${fragment}`
    )
  }
}
if (axumGuideSource.includes('let router = app.router()')) {
  fail(
    `${axumGuidePath} must test policies through bounded TCP ingress, not router.oneshot`
  )
}
if (
  !axumGuideSource.includes(
    'EdgeZeroApp::build::<App>(edgezero_adapter_axum::AXUM_PLATFORM)'
  ) ||
  axumGuideSource.includes('build_app')
) {
  fail(
    `${axumGuidePath} must construct manual applications with Axum platform metadata`
  )
}
const routingGuideSource = readFileSync(routingGuidePath, 'utf8')
for (const requiredFragment of [
  'Hooks::routes() -> Result<RouterService, EdgeError>',
  'App::from_router::<A>(router, PLATFORM)',
  'App::state::<T>() -> Option<&T>',
  "router's existing state registry",
  'state = crate::load_state()?',
  'App::assembly_error_egress(error, method, request_start, clock)',
  'request::send_assembly_error',
  'No successful app configuration exists on failure',
]) {
  if (!handlersGuideSource.includes(requiredFragment)) {
    fail(
      `handlers guide is missing application assembly contract: ${requiredFragment}`
    )
  }
}
if (
  !routingGuideSource.includes('App::build::<A>(platform)') ||
  routingGuideSource.includes('build_app')
) {
  fail(
    `${routingGuidePath} must document the platform-aware application builder`
  )
}
for (const [path, source] of [
  [capabilityPath, capabilitySource],
  [handlersGuidePath, handlersGuideSource],
]) {
  if (source.includes('build_app_for_platform')) {
    fail(`${path} contains the removed application builder name`)
  }
}
if (
  !hardCutSurfaces[2][1].includes('start_batch_until') ||
  !hardCutSurfaces[2][1].includes('send_all_until')
) {
  fail('outbound guide must document completion-order and ordered batch access')
}
for (const requiredFragment of [
  'OutboundBatchNext::Item',
  'OutboundBatchNext::Finished',
  'OutboundBatchTermination::Completed',
  'OutboundBatchTermination::Cutoff',
  'OutboundBatchFailure',
  'send_all_until(requests, observation_cutoff)',
]) {
  if (!hardCutSurfaces[2][1].includes(requiredFragment)) {
    fail(
      `${proxyGuidePath} is missing typed batch contract: ${requiredFragment}`
    )
  }
}
if (
  !hardCutSurfaces[2][1].includes('.deadline_after(Duration::from_secs(2))') ||
  hardCutSurfaces[2][1].includes(
    'let observation_cutoff = Deadline::after(Duration::from_secs(2))'
  )
) {
  fail(`${proxyGuidePath} must anchor batch cutoffs to the request clock`)
}
if (
  !hardCutSurfaces[3][1].includes('run_app_with_hooks') ||
  !hardCutSurfaces[3][1].includes('send_request_with_hooks')
) {
  fail('Fastly guide must document the closed request/response lifecycle APIs')
}
for (const requiredFragment of [
  'FastlyRequestDelivery<State>',
  'FastlyResponseDelivery::report()',
  'may_send_native_response()',
  '`NotCommitted`',
  '`Unknown`',
  '`Committed`',
  'outside EdgeZero accounting',
]) {
  if (!hardCutSurfaces[3][1].includes(requiredFragment)) {
    fail(
      `Fastly guide is missing delivery-report contract: ${requiredFragment}`
    )
  }
}
if (/Result<Option<State>,\s*fastly::Error>/.test(hardCutSurfaces[3][1])) {
  fail('Fastly guide must not retain the retired custom-send return signature')
}

let outboundSpecSource
try {
  outboundSpecSource = readFileSync(outboundSpecPath, 'utf8')
} catch (error) {
  fail(`cannot read ${outboundSpecPath}: ${error.message}`)
}
if (
  !outboundSpecSource.includes(') -> FastlyRequestDelivery<PostSend>') ||
  outboundSpecSource.includes(') -> Result<Option<PostSend>, fastly::Error>')
) {
  fail(
    'outbound specification must use the owned Fastly custom delivery report'
  )
}
if (
  !outboundSpecSource.includes(
    '| `outbound-failure-diagnostics` | BestEffort | BestEffort | Native | Native |'
  )
) {
  fail(
    'outbound specification is missing the SDK-projection diagnostic matrix row'
  )
}

if (!outboundSpecSource.includes('## 2. Historical implementation baseline')) {
  fail(
    'outbound specification must label section 2 as a historical implementation baseline'
  )
}
if (outboundSpecSource.includes('## 2. Current state (summary)')) {
  fail(
    'outbound specification still labels its historical baseline as current state'
  )
}
if (outboundSpecSource.includes("Today's Fastly client")) {
  fail(
    'outbound specification still describes the historical Fastly client as current'
  )
}
for (const staleFragment of [
  'explicit low-level constructors use a documented default clock',
  'explicit default-clock constructors may select the',
  'Preserve default-clock constructors for low-level use',
  'low-level defaults',
]) {
  if (outboundSpecSource.includes(staleFragment)) {
    fail(
      `outbound specification contains removed clock behavior: ${staleFragment}`
    )
  }
}
const budgetSourceDefinitions = outboundSpecSource.match(
  /pub enum BudgetSource\s*\{/gu
)
if (budgetSourceDefinitions?.length !== 1) {
  fail('outbound specification must define BudgetSource exactly once')
}
for (const staleFragment of [
  'So the collapse today is',
  'The current adapter maps',
  'two real bugs to fix',
  'The current code (`proxy.rs',
  'crates/edgezero-adapter-spin/src/proxy.rs',
  '(`spin/proxy.rs`)',
  'harvests every successfully dispatched `PendingRequest` via blocking `wait()`/`poll()`',
  'cold registration, serial harvest, and streamed-upload cooperative checks',
  'The target-neutral batch probe exercises both still-pending and later-ready harvest branches',
  'send_all_preflight_precedence_and_indices',
  'send_all_dispatches_every_slot_before_wait',
  'ordered harvest',
  'harvest-order',
  'serial harvest',
  'send_one_validated',
  'exact eight outbound',
  'all eight outbound',
  'Fastly send_all_until adapter overhead',
  'pub async fn next(&mut self) -> Option<OutboundBatchItem>',
  'pub async fn collect(self) -> OutboundBatchResults',
  'OutboundBatch::from_stream',
  'Result<OutboundBatchResults, EdgeError>',
  'let Ok((selection, metadata)) = select_pending_slot(&mut pending) else',
  'private failure event',
  'four of the eight outbound capabilities',
  'the eight outbound capabilities',
  'all eight rows/support values',
  'allocation-tracking adversarial',
]) {
  if (outboundSpecSource.includes(staleFragment)) {
    fail(
      `outbound specification contains stale implementation text: ${staleFragment}`
    )
  }
}
for (const requiredFragment of [
  '`BudgetSource::BatchCutoff` attribution alone never emits `OutboundBatchDriverEvent::Cutoff`',
  'monotonic clock shows that the absolute observation cutoff has expired; equality is expired',
  "An earlier Fastly phase timeout remains that slot's terminal `GatewayTimeout` item",
  'pub enum OutboundBatchDriverEvent',
  'pub enum OutboundBatchTermination',
  'pub enum OutboundBatchNext',
  'pub struct OutboundBatchFailure',
  // Historical baseline signatures remain checked here, never as live production APIs.
  'pub fn cutoff(slot_count: usize) -> Self',
  'pub fn finish_batch_item(',
  'pub fn from_driver<StreamValue>',
  'Result<OutboundBatchResults, OutboundBatchFailure>',
  'premature driver EOF',
  'duplicate or out-of-range',
  'Core reports normal completion only at driver EOF',
]) {
  if (!outboundSpecSource.includes(requiredFragment)) {
    fail(
      `outbound specification is missing typed batch contract: ${requiredFragment}`
    )
  }
}
if (
  !/terminal samples follow\s+poll-observation order in the shared monotonic clock domain/u.test(
    outboundSpecSource
  )
) {
  fail(
    'outbound specification is missing the monotonic terminal-observation ordering proof'
  )
}

const cloudflareExactDeadlineRecommendation =
  /\btarget(?:s|ing)?\s+(?:Axum\s+(?:or|and)\s+Cloudflare|Cloudflare\s+(?:or|and)\s+Axum)\b/u
if (cloudflareExactDeadlineRecommendation.test(outboundSpecSource)) {
  fail(
    'outbound specification recommends Cloudflare for exact deadlines despite its BestEffort capability'
  )
}

const inboundSpecSource = readFileSync(inboundSpecPath, 'utf8')
for (const requiredFragment of [
  '    Abort,',
  'pub enum DetachedResponseEgressDecision',
  '    Send {',
  '        completion: ResponseEgressCompletion,',
  '        deadline: Deadline,',
  'DetachedResponseEgressDecision::Send {',
  'deadline: head.write_deadline_after(DEFAULT_RESPONSE_WRITE_BUDGET),',
  'The deadline is mandatory, absolute',
  'Joined completions run left then right with the same borrowed report at most once',
  'Factory `Abort` creates no response or attempt',
  'make no transport-observed reset claim',
  'ResponseEgressCompletion::empty()',
  'Hooks::configure(&mut App) -> Result<(), EdgeError>',
  '`App::set_error_response_renderer(Fn(EdgeError) -> Response)`',
  '`Vec<Method>` rather than flattening protocol data into display text',
  'custom renderer cannot reinterpret the error status',
  'the canonical `Allow` field',
  '`Retry-After: 60` for every effective `config_out_of_date` outcome after custom rendering',
]) {
  if (!inboundSpecSource.includes(requiredFragment)) {
    fail(
      `${inboundSpecPath} is missing lifecycle contract: ${requiredFragment}`
    )
  }
}
for (const staleFragment of [
  'current Tower adapter nevertheless drives',
  'block_in_place` plus a nested runtime `block_on',
  'MethodNotAllowed   { allowed: String',
  'Tokio cannot cancel that blocking closure',
]) {
  if (inboundSpecSource.includes(staleFragment)) {
    fail(
      `inbound specification contains removed Axum bridge text: ${staleFragment}`
    )
  }
}

const outboundReviewHardeningPlanSource = readFileSync(
  outboundReviewHardeningPlanPath,
  'utf8'
)
for (const staleFragment of [
  'Preserve default-clock constructors for low-level use',
  'low-level defaults',
]) {
  if (outboundReviewHardeningPlanSource.includes(staleFragment)) {
    fail(`${outboundReviewHardeningPlanPath} contains removed clock behavior`)
  }
}

const currentDocumentation = [
  [
    outboundImplementationIndexPath,
    readFileSync(outboundImplementationIndexPath, 'utf8'),
  ],
  [
    outboundBatchTerminationPlanPath,
    readFileSync(outboundBatchTerminationPlanPath, 'utf8'),
  ],
  [cloudflarePhasePath, readFileSync(cloudflarePhasePath, 'utf8')],
  [spinPhasePath, readFileSync(spinPhasePath, 'utf8')],
  [fastlyPhasePath, readFileSync(fastlyPhasePath, 'utf8')],
  [migrationPhasePath, readFileSync(migrationPhasePath, 'utf8')],
  [outboundSpecPath, outboundSpecSource],
]
for (const [path, source] of currentDocumentation) {
  for (const staleVersion of [
    'Spin SDK 6.0.0',
    'spin-sdk v6.0.0',
    'Viceroy 0.17.0',
    'viceroy 0.17.0',
    'Fastly SDK 0.12.1',
    'Fastly 0.12.1',
    'fastly v0.12.1',
    'fastly = "=0.12.1"',
    'fastly --precise 0.12.1',
    'Worker 0.8.3',
    'worker = "=0.8.3"',
    'worker --precise 0.8.3',
    'worker v0.8.3',
  ]) {
    if (source.includes(staleVersion)) {
      fail(`${path} contains stale runtime tooling: ${staleVersion}`)
    }
  }
}

const outboundImplementationIndexSource = currentDocumentation[0][1]
if (!outboundImplementationIndexSource.includes('Spin SDK 7 / WASI HTTP 0.3')) {
  fail(
    'outbound implementation index must name the current Spin SDK 7 baseline'
  )
}
if (
  outboundImplementationIndexSource.includes('| Executable:') ||
  outboundImplementationIndexSource.includes('Tasks 1-6 blocked') ||
  outboundImplementationIndexSource.includes('all downstream phases stay') ||
  outboundImplementationIndexSource.includes(
    'exactly seven **outbound** capabilities'
  ) ||
  outboundImplementationIndexSource.includes(
    'exactly twelve **outbound** capabilities'
  ) ||
  outboundImplementationIndexSource.includes(
    'block_in_place` + `Handle::block_on'
  )
) {
  fail('outbound implementation index contains stale implementation state')
}

let brotliAuditSource
try {
  brotliAuditSource = readFileSync(brotliAuditPath, 'utf8')
} catch (error) {
  fail(`cannot read ${brotliAuditPath}: ${error.message}`)
}
const normalizedBrotliAuditSource = brotliAuditSource.replace(/\s+/gu, ' ')
const expectedBrotliDependencies = JSON.parse(
  readFileSync('scripts/brotli_dependency_contract.json', 'utf8')
)
const auditedBrotliDependencies = new Map(
  [
    ...brotliAuditSource.matchAll(
      /^\| `([^`]+)`\s+\| `([^`]+)`\s+\| `([0-9a-f]{64})` \|$/gmu
    ),
  ].map((match) => [match[1], [match[2], match[3]]])
)
if (auditedBrotliDependencies.size !== expectedBrotliDependencies.length) {
  fail(`${brotliAuditPath} must contain exactly the pinned dependency graph`)
}
for (const [name, version, checksum] of expectedBrotliDependencies) {
  const audited = auditedBrotliDependencies.get(name)
  if (audited?.[0] !== version || audited?.[1] !== checksum) {
    fail(
      `${brotliAuditPath} has stale version or checksum evidence for ${name}`
    )
  }
}
for (const requiredFragment of [
  '`brotli-decompressor`',
  '`5.0.1`',
  '`compression-codecs`',
  '`0.4.38`',
  '`alloc-stdlib`',
  '`0.2.4`',
  '3 * 256 * 1080 * 4',
  '3,442,530',
  '16,777,216',
  'Source-audited payload subtotal',
  "The ring buffer's `2^WBITS` bytes are excluded from the fixed term",
]) {
  if (!normalizedBrotliAuditSource.includes(requiredFragment)) {
    fail(`${brotliAuditPath} is missing audited evidence: ${requiredFragment}`)
  }
}

const outboundBatchTerminationPlanSource = currentDocumentation[1][1]
if (outboundBatchTerminationPlanSource.includes('send_all_until(...).await?')) {
  fail(
    `${outboundBatchTerminationPlanPath} contains the stale batch collector error conversion`
  )
}
if (
  outboundBatchTerminationPlanSource.includes('doc-hidden adapter driver event')
) {
  fail(
    `${outboundBatchTerminationPlanPath} still describes the public adapter driver protocol as doc-hidden`
  )
}

const cloudflarePhaseSource = currentDocumentation[2][1]
for (const staleFragment of [
  "Cloudflare's Native timing claim",
  'Cloudflare: Native for HTTP, deadlines',
  '| `outbound-deadlines` | Native | Native |',
  '| `streamed-upload-deadlines` | Native | Native |',
]) {
  if (cloudflarePhaseSource.includes(staleFragment)) {
    fail(
      `${cloudflarePhasePath} contains stale Cloudflare capability text: ${staleFragment}`
    )
  }
}

for (const [path, source] of [
  [cloudflarePhasePath, currentDocumentation[2][1]],
  [spinPhasePath, currentDocumentation[3][1]],
  [fastlyPhasePath, currentDocumentation[4][1]],
]) {
  if (!source.includes('**Superseded batch/lifecycle API:**')) {
    fail(`${path} does not mark its removed batch API as superseded`)
  }
}

const migrationPhaseSource = currentDocumentation[5][1]
for (const currentRow of [
  '| `outbound-deadlines` | Native | BestEffort | BestEffort | BestEffort |',
  '| `streamed-upload-deadlines` | Native | BestEffort | BestEffort | BestEffort |',
  '| `lazy-streamed-response-passthrough` | Native | Native | BestEffort | BestEffort |',
]) {
  if (!migrationPhaseSource.includes(currentRow)) {
    fail(
      `${migrationPhasePath} is missing current capability row: ${currentRow}`
    )
  }
}

for (const dependency of [
  'async-compression 0.4.43',
  'adler2 = "=2.0.1"',
  'brotli 8.0.4',
  'brotli-decompressor 5.0.1',
  'compression-codecs 0.4.38',
  'compression-core 0.4.32',
  'crc32fast = "=1.5.0"',
  'flate2 = "=1.1.9"',
  'miniz_oxide = "=0.8.9"',
  'simd-adler32 = "=0.3.9"',
]) {
  if (!outboundSpecSource.includes(`\`${dependency}\``)) {
    fail(
      `outbound specification is missing audited dependency pin ${dependency}`
    )
  }
}
if (
  !outboundSpecSource.includes(
    "plus EdgeZero's synthetic `x-edgezero-proxy` marker"
  )
) {
  fail(
    'outbound specification does not include the proxy header in response caps'
  )
}

const responseEgressSpecSource = readFileSync(responseEgressSpecPath, 'utf8')
for (const requiredFragment of [
  'pub struct ResponseEgressDeadline { /* private */ }',
  'pub const fn at(deadline: Deadline) -> Self;',
  'pub const fn after(duration: Duration) -> Self;',
  'egress-start-relative deadline',
  'resolves `after(duration)` from that',
  'exact sample before invoking the callback',
  'retired ambiguous `new()` constructor and unresolved',
  'pub struct ResponseEgressCompletion',
  'pub enum DetachedResponseEgressDecision',
  '    Send {',
  '        completion: ResponseEgressCompletion,',
  '        deadline: Deadline,',
  'DetachedResponseEgressDecision::Send {',
  'deadline: head.write_deadline_after(DEFAULT_RESPONSE_WRITE_BUDGET),',
  'mandatory absolute deadline',
  'pub fn join(self, other: Self) -> Self;',
  '`ResponseEgressCompletion::join` consumes a left and right completion',
  'the same borrowed `ResponseEgressReport`; the joined owner executes at most once',
  'existing non-response abort/error boundary',
  'Normalized pre-admission validation invokes the detached-egress decision factory exactly once',
  '`Abort` constructs no response or attempt and invokes no completion, observer, handler, middleware, or body poll',
  'ResponseEgressCompletion::empty()',
  'not attached to a `Response` and never enters `http::Extensions`',
  'ResponseEgressHead::application_deadline()',
  'queue-through-egress',
  'application configuration changed adapter-selected platform metadata',
  'validates source EOF before handing that frame to Hyper',
  'A transmitted zero-length stream is validated asynchronously before Hyper receives the response',
]) {
  if (!responseEgressSpecSource.includes(requiredFragment)) {
    fail(
      `${responseEgressSpecPath} is missing application deadline contract: ${requiredFragment}`
    )
  }
}

if (
  responseEgressSpecSource.includes(
    'Normalized pre-admission validation obtains exactly one completion'
  )
) {
  fail(
    'response-egress evidence still claims every normalized rejection has a completion'
  )
}

for (const requiredFragment of [
  'detached-egress decision factory',
  'this does not promote their `BestEffort` capability cells',
]) {
  if (!capabilitySource.includes(requiredFragment)) {
    fail(
      `${capabilityPath} is missing detached-ingress abort caveat: ${requiredFragment}`
    )
  }
}

const responseEgressHeadingIndex = lines.findIndex(
  (line) => line.trim() === '## Response Egress Matrix'
)
if (responseEgressHeadingIndex === -1) {
  fail(`${capabilityPath} is missing the response-egress section`)
}
const nextCapabilityHeadingIndex = lines.findIndex(
  (line, index) => index > responseEgressHeadingIndex && line.startsWith('## ')
)
const responseEgressSectionEnd =
  nextCapabilityHeadingIndex === -1 ? lines.length : nextCapabilityHeadingIndex
const responseEgressSection = lines
  .slice(responseEgressHeadingIndex, responseEgressSectionEnd)
  .join('\n')
for (const requiredFragment of [
  'The boundaries remain `BestEffort` because none proves end-client receipt:',
  'Provider-owned buffering, SDK allocations, and bytes beyond each stated acceptance boundary are',
]) {
  if (!responseEgressSection.includes(requiredFragment)) {
    fail(
      `${capabilityPath} response-egress section is missing caveat: ${requiredFragment}`
    )
  }
}

function responseEgressProviderBullet(provider) {
  const bulletIndex = lines.findIndex(
    (line, index) =>
      index > responseEgressHeadingIndex &&
      index < responseEgressSectionEnd &&
      line.startsWith(`- ${provider} `)
  )
  if (bulletIndex === -1) {
    fail(`${capabilityPath} is missing the ${provider} response-egress caveat`)
  }
  const bullet = [lines[bulletIndex]]
  for (
    let index = bulletIndex + 1;
    index < responseEgressSectionEnd;
    index += 1
  ) {
    if (!lines[index].startsWith('  ')) break
    bullet.push(lines[index].trim())
  }
  return bullet.join(' ')
}

const responseEgressProviderCaveats = new Map([
  [
    'Axum',
    [
      'commits when Hyper accepts the response head',
      'closes the owned HTTP/1 connection on deadline',
      'reports clean source EOF as `HostHandoff`',
    ],
  ],
  [
    'Cloudflare',
    [
      'awaits JavaScript writer promises',
      'races request abort/deadline signals',
      'coordinator alive with `waitUntil`',
      'deployed disconnect and completion timing remain unproved',
    ],
  ],
  [
    'Fastly',
    [
      'hands buffered heads and complete bodies to the host together',
      'account synchronous body-handle writes through `stream_to_client` close',
      'cannot preempt a blocked source poll or hostcall',
      'loses the body handle if `finish` itself fails',
    ],
  ],
  [
    'Spin',
    [
      'owns the WASI response/body/result writers through handoff',
      'races pending operations against its timer',
      'host teardown and network completion remain provider-observable only',
    ],
  ],
])
for (const [provider, requiredFragments] of responseEgressProviderCaveats) {
  const bullet = responseEgressProviderBullet(provider)
  for (const requiredFragment of requiredFragments) {
    if (!bullet.includes(requiredFragment)) {
      fail(
        `${capabilityPath} ${provider} response-egress caveat is missing: ${requiredFragment}`
      )
    }
  }
}

const limitsHeadingIndex = lines.findIndex(
  (line) => line.trim() === '## Limits And Accounting'
)
if (limitsHeadingIndex === -1) {
  fail('limits and accounting section is missing')
}
const limitsSectionEnd = lines.findIndex(
  (line, index) => index > limitsHeadingIndex && line.startsWith('## ')
)
const limitsHeaderIndex = lines.findIndex(
  (line, index) =>
    index > limitsHeadingIndex &&
    (limitsSectionEnd === -1 || index < limitsSectionEnd) &&
    JSON.stringify(cells(line)) === JSON.stringify(expectedLimitHeader)
)
if (limitsHeaderIndex === -1) {
  fail('outbound limits table is missing')
}
const limitsSeparator = cells(lines[limitsHeaderIndex + 1] ?? '')
if (
  limitsSeparator === null ||
  limitsSeparator.length !== expectedLimitHeader.length ||
  !limitsSeparator.every((value) => /^:?-{3,}:?$/u.test(value))
) {
  fail('outbound limits table is missing its three-column separator row')
}
const actualLimitRows = []
for (let index = limitsHeaderIndex + 2; index < lines.length; index += 1) {
  const row = cells(lines[index])
  if (row === null) break
  if (row.length !== expectedLimitHeader.length) {
    fail(`outbound limits row ${index + 1} has ${row.length} cells, expected 3`)
  }
  actualLimitRows.push([normalizeCapability(row[0]), row[1], row[2]])
}
if (JSON.stringify(actualLimitRows) !== JSON.stringify(expectedLimitRows)) {
  fail(
    `outbound limits mismatch\nexpected=${JSON.stringify(expectedLimitRows)}\nactual=${JSON.stringify(actualLimitRows)}`
  )
}
for (const requiredFragment of [
  'including fields later stripped',
  "charge EdgeZero's synthetic proxy marker",
]) {
  if (!capabilitySource.includes(requiredFragment)) {
    fail(
      `${capabilityPath} is missing the response-header accounting rule: ${requiredFragment}`
    )
  }
}

let outboundCoreSource
try {
  outboundCoreSource = [...outboundCoreProductionPaths, outboundCoreTestPath]
    .map((path) => readFileSync(path, 'utf8'))
    .join('\n')
} catch (error) {
  fail(`cannot read ${outboundCorePath}: ${error.message}`)
}
let compressionCoreSource
try {
  compressionCoreSource = readFileSync(compressionCorePath, 'utf8')
} catch (error) {
  fail(`cannot read ${compressionCorePath}: ${error.message}`)
}
for (const requiredFragment of [
  'Deflate,',
  'Passthrough(PassthroughReason)',
  'pub enum PassthroughReason',
  'PassthroughReason::Stacked',
  'PassthroughReason::Malformed',
  'PassthroughReason::Unsupported(token)',
  'pub fn decode_deflate_stream',
]) {
  if (!compressionCoreSource.includes(requiredFragment)) {
    fail(
      `${compressionCorePath} is missing the content-encoding contract: ${requiredFragment}`
    )
  }
}
for (const requiredFragment of [
  'single `deflate`',
  'Passthrough `Unsupported(token)`',
  'Passthrough `Stacked`',
  'Passthrough `Malformed`',
  'decode_deflate_stream',
]) {
  if (!outboundSpecSource.includes(requiredFragment)) {
    fail(
      `${outboundSpecPath} is missing the content-encoding documentation: ${requiredFragment}`
    )
  }
}
const coreProduction = outboundCoreProductionPaths
  .map((path) => productionCode(readFileSync(path, 'utf8')))
  .join('\n')
const batchProduction = productionCode(
  readFileSync('crates/edgezero-core/src/outbound/batch.rs', 'utf8')
)
const runtimeProduction = productionCode(
  readFileSync('crates/edgezero-core/src/outbound/scope.rs', 'utf8')
)
const bodyProduction = productionCode(
  readFileSync('crates/edgezero-core/src/body.rs', 'utf8')
)
const liveError = liveOutboundContractError(
  coreProduction,
  batchProduction,
  runtimeProduction,
  bodyProduction
)
if (liveError !== null) fail(liveError)
observationCutoffContract(coreProduction)
for (const fragment of [
  'pub enum OutboundProxyMarker',
  'pub fn proxy_marker(',
  'pub fn proxy_marker_setting(',
  'pub fn effective_proxy_marker_enabled(',
  'pub fn proxy_marker_enabled(',
  'pub fn with_proxy_marker(',
  'proxy_marker_off_preserves_upstream_and_consumes_no_header_budget',
]) {
  if (!outboundCoreSource.includes(fragment)) {
    fail(
      `${outboundCorePath} is missing the proxy-marker contract: ${fragment}`
    )
  }
}
for (const fragment of [
  'pub enum ResponseDecoding',
  'pub fn response_decoding(',
  'pub fn response_decoding_setting(',
  'original_coding_skips_decoder_but_keeps_transport_caps',
  'original_coding_preserves_normalization_precedence',
]) {
  if (!outboundCoreSource.includes(fragment)) {
    fail(
      `${outboundCorePath} is missing the original-coding contract: ${fragment}`
    )
  }
}
for (const path of [proxyGuidePath, capabilityPath]) {
  const source = readFileSync(path, 'utf8')
  for (const fragment of [
    'ResponseDecoding::Original',
    'ResponseDecoding::Decode',
    '206',
  ]) {
    if (!source.includes(fragment)) {
      fail(`${path} is missing the original-coding documentation: ${fragment}`)
    }
  }
}
for (const path of [demoHandlersPath, templateHandlersPath]) {
  if (!readFileSync(path, 'utf8').includes('.with_proxy_marker(false)')) {
    fail(`${path} must configure its relay handle with the marker off`)
  }
  if (
    !readFileSync(path, 'utf8').includes(
      '.response_decoding(ResponseDecoding::Original)'
    )
  ) {
    fail(`${path} must select original coding for its relay request`)
  }
}
const retiredClockConstructor = ['new_with_', 'monotonic_clock'].join('')
if (outboundCoreSource.includes(retiredClockConstructor)) {
  fail('OutboundResponse must not retain the hidden clock-paired constructor')
}
const outboundResponseConstructor =
  /impl OutboundResponse \{[\s\S]*?pub fn new\(\s*request_method: Method,\s*status: StatusCode,\s*headers: HeaderMap,\s*body: Body,\s*monotonic_clock: MonotonicClock,\s*content_type_provenance: ContentTypeProvenance,\s*\) -> Self \{/u
if (!outboundResponseConstructor.test(coreProduction)) {
  fail(
    'OutboundResponse::new must require the application clock and original Content-Type provenance'
  )
}
const outboundHeadSource = readFileSync(
  'crates/edgezero-core/src/outbound/head.rs',
  'utf8'
)
const headProduction = productionCode(outboundHeadSource)
if (
  (coreProduction.match(/OutboundResponse::new\s*\(/gu)?.length ?? 0) !== 1 ||
  (headProduction.match(/OutboundResponse::new\s*\(/gu)?.length ?? 0) !== 1 ||
  !/dispatch_head\s*\([^)]*\)\s*\.await\s*\?\s*\.finish\s*\(/u.test(
    coreProduction
  )
) {
  fail(
    'production must construct responses through the single shared head finalizer'
  )
}
if (
  !/if fidelity == OutboundHeaderFidelity::ProviderFieldBoundariesUnknown\s*\{\s*normalized\.content_type_provenance = ContentTypeProvenance::Unknown;/u.test(
    outboundHeadSource
  )
) {
  fail(
    'shared header normalization must preserve unknown provider Content-Type fidelity'
  )
}
for (const adapterPath of outboundAdapterPaths) {
  const adapterSource = readFileSync(adapterPath, 'utf8')
  const adapterCode = productionCode(adapterSource)
  const pairedConstructor = adapterPath.includes('axum')
    ? /pub fn try_with_time\(time: OutboundTime\)/u
    : /pub fn with_time\(time: OutboundTime\)/u
  if (
    !pairedConstructor.test(adapterCode) ||
    /\bfn\s+(?:try_)?with_clock\s*\(/u.test(adapterCode) ||
    !adapterCode.includes('time.wait_until(')
  ) {
    fail(
      `${adapterPath} must hard-migrate to paired time and use its deadline wakeup`
    )
  }
  const fidelity = adapterPath.includes('cloudflare')
    ? 'ProviderFieldBoundariesUnknown'
    : 'AdapterVisibleFields'
  const normalizationError = headerNormalizationContractError(
    adapterSource,
    fidelity,
    adapterPath.includes('spin')
      ? readFileSync(
          'crates/edgezero-adapter-spin/src/outbound/head.rs',
          'utf8'
        )
      : null
  )
  if (normalizationError !== null) fail(`${adapterPath} ${normalizationError}`)
  const clockTests = adapterPath.includes('spin')
    ? readFileSync(
        'crates/edgezero-test-harness/fixtures/reusable-app/crates/fixture-spin-support/src/lib.rs',
        'utf8'
      )
    : adapterSource
  if (!clockTests.includes('converted_response_retains_injected_clock')) {
    fail(`${adapterPath} must behaviorally test the returned response clock`)
  }
}
const documentedDefaults = new Map(
  actualLimitRows.map((row) => [row[0], row[2]])
)
for (const [control, constant] of [
  ['max_request_body_bytes', 'DEFAULT_OUTBOUND_REQUEST_BODY_BYTES'],
  ['max_response_bytes', 'DEFAULT_MAX_RESPONSE_BYTES'],
  ['max_decoder_bytes', 'DEFAULT_MAX_DECODER_BYTES'],
]) {
  const rustDefault = formatBinaryBytes(
    readRustU64Constant(outboundCoreSource, constant)
  )
  const documentedDefault = documentedDefaults.get(control)
  if (documentedDefault !== rustDefault) {
    fail(
      `${control} default mismatch: Rust ${constant} is ${rustDefault}, documentation says ${documentedDefault}`
    )
  }
}

const sidebarSource = readFileSync(sidebarPath, 'utf8')
const sidebarLinks =
  sidebarSource.match(/link:\s*['"]\/guide\/capabilities['"]/gu) ?? []
if (sidebarLinks.length !== 1) {
  fail(
    `expected exactly one /guide/capabilities sidebar link, found ${sidebarLinks.length}`
  )
}
