# Changelog

## Unreleased

### Changed

- Cloudflare now preserves valid extension HTTP method tokens instead of
  coercing them to GET. For example, `PROPFIND /x` no longer invokes a `GET /x`
  handler; normal routing returns 404 or 405 where no matching route exists.
  Malformed method tokens fail conversion with a bounded bad-request error.
