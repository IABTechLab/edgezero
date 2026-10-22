# EdgeZero deployment action adoption

The Fastly lifecycle actions deploy an immutable application release. They do not
build the application or choose its source. This split keeps application bytes
independent from a publisher's credentials and runtime configuration.

## Application release producer

Each application owns a release pipeline that:

1. checks out one approved source revision;
2. builds the application CLI and Fastly package without provider credentials;
3. bundles that CLI, package, `edgezero.toml`, its referenced Fastly manifest,
   and strict `release.json` metadata;
4. publishes the archive under an immutable reference; and
5. publishes the archive's lowercase SHA-256 digest.

The producer does not select a publisher GitHub Environment. One application
release supplies a byte-identical application CLI, Fastly package,
`edgezero.toml`, and Fastly manifest to every publisher and to both production
and staging. Different applications or later releases can have different
manifests. A deployer or runtime environment cannot replace them.

`build-app-cli` and `package-application-release-fastly` retain their GitHub
Artifacts for 14 days. Every lifecycle action, including rollback, needs the
release archive and digest. Copy both to durable immutable storage, such as a
GitHub Release asset, when the required rollback window exceeds 14 days.

### Optional Rust build caching

Call the provider-neutral cache action after checking out the application and
before its Rust build steps. Replace the repository/ref placeholders and pin
EdgeZero actions to one reviewed full commit SHA:

```yaml
- name: Check out application source
  uses: actions/checkout@v7
  with:
    repository: your-org/example-app
    ref: <application-ref>
    path: application
    persist-credentials: false

- name: Capture immutable application revision
  id: source
  shell: bash
  run: |
    set -euo pipefail
    sha=$(git -C application rev-parse HEAD)
    [[ "$sha" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || exit 1
    printf 'sha=%s\n' "$sha" >>"$GITHUB_OUTPUT"

- name: Set up Rust build cache
  id: rust-cache
  uses: IABTechLab/edgezero/.github/actions/setup-rust-build-cache@<ref>
  with:
    app-name: example-app
    cache-target: true
    save-target: ${{ github.ref == 'refs/heads/main' }}
    target-cache-revision: ${{ steps.source.outputs.sha }}
    working-directory: application

- name: Report cache selection
  shell: bash
  env:
    SOURCE_HIT: ${{ steps.rust-cache.outputs.source-cache-hit }}
    TARGET_HIT: ${{ steps.rust-cache.outputs.target-cache-hit }}
    TARGET_DIR: ${{ steps.rust-cache.outputs.cargo-target-dir }}
  run: printf 'Source exact hit=%s; target exact hit=%s; target directory=%s\n' "$SOURCE_HIT" "$TARGET_HIT" "$TARGET_DIR"
```

`app-name` is a stable lowercase cache namespace for one application. Do not
append its source SHA: that would prevent dependency-source reuse across commits.
Resolve `steps.source.outputs.sha` from the checked-out **application** source,
for example with `git -C application rev-parse HEAD`. Do not use the deployment
repository's `github.sha` when the application lives in a separate checkout.
`target-cache-revision` must be a full lowercase Git SHA and is required when
`cache-target: true`; missing or invalid values fail without a compatibility
fallback. An explicitly supplied revision is validated in either mode.

`working-directory` identifies its Cargo workspace containing the committed
`Cargo.lock`. The action restores application-scoped Cargo registry and Git
dependency sources, installs a pinned `sccache`, and enables compiler caching
for every later native or WASM Rust build in the job.

- Dependency source keys include app, runner OS/architecture, and `Cargo.lock`,
  but not source revision. A stable restore prefix also permits reuse when the
  lockfile changes; Cargo fetches missing dependencies.
- With `cache-target: true`, target artifacts are restored using a new v2 key
  containing the complete source revision and lock digest. There is no
  cross-revision or legacy-key fallback. The first build of a new revision has
  a cold target cache; repeating that revision can restore previously saved artifacts.
  Saving is separately opt-in: `save-target: true` enables a success-only
  post-job save after builds complete. Its default is false (restore-only), and
  enabling it without `cache-target: true` fails. Prefer writes from trusted main
  workflow runs; this controls the workflow ref, not which application branch
  can be built. Do not save a full target on every disposable fixture/PR run.
- Compiler results use sccache's compiler/input/environment checks. Unchanged
  compilations can be reused across revisions. Crates that invoke the linker
  (`bin`, `dylib`, `cdylib`, and `proc-macro`) are not cacheable. A cold target
  still requires those compilations, build-script execution, and linking; it
  does not make a new application revision a fully warm build. A hit ratio or
  deployment duration is not guaranteed.

In both modes, the action sets one stable app-specific `CARGO_TARGET_DIR` beneath
`RUNNER_TEMP`, without a SHA or random invocation directory in the path. Native
CLI and WASM builds share it; Cargo keeps target triples/profiles separate.
`cache-target: false` disables remote target operations, not that stable
directory, dependency caching, or sccache. Use **one application per job**: setup
sets job-wide target-directory variables, so setting up another application
redirects subsequent builds into its directory. Put different applications in
separate jobs. Prefer one setup call; an identical repeat skips remote restores
and compiler-cache setup, preserving newer local build outputs. Changing
workspace, source revision, lockfile or cache settings within that directory
fails. With remote targets
disabled, Git HEAD can identify local ownership when no revision was supplied;
a non-Git workspace requires an explicit revision for repeat setup. Symlinks,
control characters in paths and unowned target contents are rejected rather
than deleted.

Outputs `source-cache-hit` and `target-cache-hit` indicate exact primary-key
matches. A false source hit can still mean prefix reuse after a lockfile change;
target hits are false when disabled or missing. Both hit outputs are false on
an identical repeat because that call performs no restore. Read hit status from
the first setup call. `source-cache-key`, `target-cache-key`, and
`cargo-target-dir` expose the selected identities/path,
and sccache logs compiler statistics at job completion. Branch-scoped GitHub
cache access still applies: PR merge-ref caches cannot seed main or unrelated
PRs. Trusted main builds warm the source/compiler caches accessible to later
branch builds; merging may require a cold target build.

#### Breaking cache/build-path migration

This changes the former lockfile-only target policy: only dependency sources and
valid compiler results may cross source revisions, never writable target archives.
It also changes build paths when remote target caching is disabled.

1. Use one stable `app-name` per application, without a source SHA suffix.
2. Resolve the application's full SHA and supply `target-cache-revision` when
   enabling target restores; missing revisions have no compatibility fallback.
3. Opt into target saves explicitly where repeated builds justify archive cost.
   Restore-only callers do not automatically populate new target keys.
4. Remove hard-coded `<application>/target/...` paths. Use `CARGO_TARGET_DIR` or
   `cargo-target-dir`; CLI and adapter builds now share this job-local directory
   even with `cache-target: false`, instead of fresh per-invocation CLI scratch.
5. Use one application per job and prefer one setup call. Repeats must identify
   the same source and use identical workspace, lockfile and cache settings.
6. Expect a cold first target build after migration and for each new source SHA.
   Measure compiler hits and build duration instead of assuming all work is cached.

CLI build invocation, packaging, release verification, credential isolation, and
production/staging deployment semantics are unchanged.

Build and download the configurable application CLI artifact before assembling
the release:

```yaml
- id: app-cli
  uses: IABTechLab/edgezero/.github/actions/build-app-cli@<ref>
  with:
    app-cli-package: example-app-cli
    app-cli-bin: example-app-cli
    app-cli-artifact: example-app-cli-${{ steps.source.outputs.sha }}
    working-directory: application

- name: Download the application CLI archive
  uses: actions/download-artifact@v4
  with:
    name: ${{ steps.app-cli.outputs.app-cli-artifact }}
    path: release-inputs
```

Use the release packager after the application CLI and Fastly package have been
built in the credential-free producer job:

```yaml
- id: release
  uses: IABTechLab/edgezero/.github/actions/package-application-release-fastly@<ref>
  with:
    app-cli-archive: release-inputs/app-cli.tar
    fastly-package: application/pkg/app.tar.gz
    application-manifest: application/edgezero.toml
    adapter-manifest: application/adapters/fastly/fastly.toml
    source-revision: ${{ steps.source.outputs.sha }}
    artifact-name: application-release-${{ steps.source.outputs.sha }}
```

The action verifies the application CLI's lifecycle command surface, writes
strict metadata with `format: 1` and `lifecycle_protocol: 1`, verifies the
assembled archive through the same consumer validator, uploads
`app-release.tar.gz`, and returns its SHA-256. It accepts only prebuilt inputs
beneath `github.workspace` and receives no provider credentials. The explicit
`adapter-manifest` input must be the Fastly manifest referenced by
`edgezero.toml`; the packager stores it at that declared relative path. Other
adapter manifests are not part of a Fastly application release.

The lifecycle actions authenticate the archive with the externally supplied SHA-256 before extraction.
The hashes in `release.json` then prove internal
member consistency and path confinement; `release.json` is not a signature or
an independent source of provenance.

## Migration from the previous deploy action

The lifecycle action no longer builds application source. Move every build input
to the credential-free producer and give the consumer the one immutable release:

| Previous `deploy-fastly` input | Replacement                                                                                                                                                                            |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `app-cli-artifact`             | Build the configurable artifact with `build-app-cli`, bundle its `app-cli.tar`, then pass `app-release-archive`, `app-release-sha256`, and `expected-source-revision` to the consumer. |
| `app-cli-bin`                  | Set it on producer `build-app-cli`; the release metadata records the resulting binary.                                                                                                 |
| `working-directory`            | Use it only in producer build/cache steps.                                                                                                                                             |
| `manifest`                     | Pass the producer path as `application-manifest` to the release packager.                                                                                                              |
| `rust-toolchain`               | Select it in the producer build.                                                                                                                                                       |
| `build-mode`                   | Remove it; the producer builds once before packaging.                                                                                                                                  |
| `build-args`                   | Apply package build arguments in the producer.                                                                                                                                         |
| `cache`                        | Use `setup-rust-build-cache` with `cache-target` in the producer.                                                                                                                      |

The runtime-descriptor architecture has no compatibility fallback. Cut over in
this order:

1. Set and verify the selected physical store for every canonical `__NAME`
   variable in each production and staging Environment.
2. Before staging the new release, run staging config push so the staging-selected
   physical Config Store contains `<ID>` instead of the obsolete `<ID>_staging`
   root.
3. Deploy and verify the new logical resource links, then keep the old stores
   through the rollback window. Remove `edgezero_runtime_env` only after no
   active, staged, or rollback version depends on it.
4. Remove the obsolete `<ID>_staging` root manually after proving no deployed
   version reads it. `config gc` does not delete the obsolete root key. If that
   root referenced chunks, verify the selected physical store, serialize GC
   against config pushes, run a dry-run, and then use a reviewed nonzero
   `--older-than` with `--yes` to reclaim the unreferenced chunks.

Custom application CLI entry points also need the
[Fastly entry-point migration](./adapters/fastly.md#migrating-a-custom-entrypoint).

## Deployment consumer

The deployment repository resolves a trusted application release, downloads its
archive, verifies the pinned digest, and passes it to the lifecycle actions. The
deployer never checks out or rebuilds application source. It supplies only:

- the destination Fastly service and production/staging target;
- runtime variables and selected physical Config/KV/Secret stores;
- a typed config payload when running config push; and
- provider credentials.

Select the source revision and release digest before choosing the publisher
GitHub Environment. A release reference or digest is application-release data and
cannot come from a publisher GitHub Environment. This prevents a publisher or
environment administrator from selecting different executable code or manifests.

Download the selected release once and reuse it:

```yaml
- uses: actions/download-artifact@v4
  with:
    name: application-release-${{ needs.release.outputs.source-revision }}
    path: app-release
    run-id: ${{ needs.release.outputs.run-id }}
    repository: ${{ needs.release.outputs.producer-repository }}
    github-token: ${{ secrets.APPLICATION_RELEASE_TOKEN }}

- id: deploy
  uses: IABTechLab/edgezero/.github/actions/deploy-fastly@<ref>
  with:
    app-release-archive: ${{ github.workspace }}/app-release/app-release.tar.gz
    app-release-sha256: ${{ needs.release.outputs.sha256 }}
    expected-source-revision: ${{ needs.release.outputs.source-revision }}
    fastly-api-token: ${{ secrets.FASTLY_API_TOKEN }}
    fastly-service-id: ${{ vars.FASTLY_SERVICE_ID }}
```

`producer-repository` is the application repository in `owner/name` form. For a
cross-repository release, `APPLICATION_RELEASE_TOKEN` must be a fine-grained
personal access token or GitHub App token with Actions read access to that
repository. The built-in workflow token is sufficient only when producer and
deployer are the same repository.

Deploy, config push, healthcheck, and rollback extract the application CLI from
that release. Deploy uploads its recorded Fastly package. Config push uses its
recorded `edgezero.toml`; Fastly deployment verifies the recorded referenced
manifest. There are no deployer build flags or alternate manifest inputs.

## Environment contract

Publisher GitHub Environments can set canonical runtime variables such as:

```text
EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME
EDGEZERO__STORES__KV__CACHE__NAME
EDGEZERO__STORES__SECRETS__CREDENTIALS__NAME
```

They may choose different values and physical resources for each publisher and
for production/staging. The variable names contain no service ID. Secret Store
selectors contain only a store name, never a secret value. Fastly logging comes
from the immutable application manifest. Every target uses `<logical-id>` as
the config key. The selected Environment chooses the physical
store with `__NAME`; using the same value shares config and using different
values isolates it.

Fastly resolves each selected physical store and attaches it to the target
version under the manifest's logical ID before staging or activation. Failed
resolution or verification withholds publication. See the
[Fastly adapter guide](./adapters/fastly.md#store-selection-and-deployment).

## Example application deployer

The example application has one real hostname, `app.example.com`, and two
publication targets. Production selects the `app.example.com` GitHub
Environment; staging selects `staging.app.example.com`. Both targets still pass
`app.example.com` to Fastly health checks. Validate the domain and target in a
provider-credential-free preflight job, derive the Environment identifier, and
query GitHub before the deploy job references it. This prevents GitHub from
silently creating a missing Environment without its expected protection rules
or secrets. Private repositories use the built-in token with `actions: read`;
the preflight receives no provider secret.

```yaml
name: Deploy Application

on:
  workflow_dispatch:
    inputs:
      domain:
        description: Application hostname
        required: true
        type: string
        default: app.example.com
      deploy-to:
        description: Publication target
        required: true
        type: choice
        options:
          - staging
          - production
      source-revision:
        description: Approved application revision
        required: true
        type: string
      producer-repository:
        description: Repository that produced the application release (owner/name)
        required: true
        type: string
      release-run-id:
        description: Trusted producer workflow run containing the immutable release
        required: true
        type: string
      release-sha256:
        description: Lowercase SHA-256 recorded by the trusted producer
        required: true
        type: string

jobs:
  preflight:
    runs-on: ubuntu-latest
    permissions:
      actions: read
    outputs:
      environment: ${{ steps.environment.outputs.environment-name }}
      concurrency-key: ${{ steps.target.outputs.concurrency-key }}
      deploy-to: ${{ steps.target.outputs.deploy-to }}
      producer-repository: ${{ steps.release.outputs.producer-repository }}
      release-artifact: ${{ steps.release.outputs.artifact }}
      release-run-id: ${{ steps.release.outputs.run-id }}
      sha256: ${{ steps.release.outputs.sha256 }}
      source-revision: ${{ steps.release.outputs.source-revision }}
    steps:
      - id: release
        env:
          SOURCE_REVISION: ${{ inputs.source-revision }}
          PRODUCER_REPOSITORY: ${{ inputs.producer-repository }}
          RELEASE_RUN_ID: ${{ inputs.release-run-id }}
          RELEASE_SHA256: ${{ inputs.release-sha256 }}
        run: |
          {
            [[ "$SOURCE_REVISION" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || {
              echo "::error::source-revision must be a full lowercase Git SHA"; exit 1;
            }
            [[ "$RELEASE_RUN_ID" =~ ^[1-9][0-9]*$ ]] || {
              echo "::error::release-run-id must be a positive integer"; exit 1;
            }
            [[ "$RELEASE_SHA256" =~ ^[0-9a-f]{64}$ ]] || {
              echo "::error::release-sha256 must be 64 lowercase hexadecimal characters"; exit 1;
            }
            [[ "$PRODUCER_REPOSITORY" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || {
              echo "::error::producer-repository must use owner/name syntax"; exit 1;
            }
            echo "artifact=application-release-$SOURCE_REVISION"
            echo "producer-repository=$PRODUCER_REPOSITORY"
            echo "run-id=$RELEASE_RUN_ID"
            echo "sha256=$RELEASE_SHA256"
            echo "source-revision=$SOURCE_REVISION"
          } >>"$GITHUB_OUTPUT"

      - id: target
        env:
          DOMAIN: ${{ inputs.domain }}
          DEPLOY_TO: ${{ inputs.deploy-to }}
        run: |
          case "$DOMAIN" in
            app.example.com) concurrency_key=app-example-com ;;
            *) echo "::error::unsupported application domain"; exit 1 ;;
          esac
          case "$DEPLOY_TO" in
            production) environment="$DOMAIN" ;;
            staging) environment="staging.$DOMAIN" ;;
            *) echo "::error::unsupported publication target"; exit 1 ;;
          esac
          {
            echo "environment=$environment"
            echo "concurrency-key=$concurrency_key"
            echo "deploy-to=$DEPLOY_TO"
          } >>"$GITHUB_OUTPUT"

      - id: environment
        uses: IABTechLab/edgezero/.github/actions/require-github-environment@<ref>
        with:
          environment-name: ${{ steps.target.outputs.environment }}
          repository: ${{ github.repository }}
          github-token: ${{ github.token }}

  deploy:
    needs: preflight
    runs-on: ubuntu-latest
    environment: ${{ needs.preflight.outputs.environment }}
    concurrency:
      group: application-${{ needs.preflight.outputs.concurrency-key }}
      cancel-in-progress: false
    permissions:
      contents: read
      actions: read
    steps:
      - uses: actions/checkout@v4
        with:
          persist-credentials: false

      - name: Download the selected application release
        uses: actions/download-artifact@v4
        with:
          name: ${{ needs.preflight.outputs.release-artifact }}
          path: app-release
          run-id: ${{ needs.preflight.outputs.release-run-id }}
          repository: ${{ needs.preflight.outputs.producer-repository }}
          github-token: ${{ secrets.APPLICATION_RELEASE_TOKEN }}

      - name: Export configured store selectors
        env:
          CONFIG_NAME: ${{ vars.EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME }}
          KV_NAME: ${{ vars.EDGEZERO__STORES__KV__CACHE__NAME }}
          SECRET_NAME: ${{ vars.EDGEZERO__STORES__SECRETS__CREDENTIALS__NAME }}
        run: |
          export_selector() {
            name="$1"
            value="$2"
            [[ -z "$value" ]] && return
            case "$value" in
              *$'\n'* | *$'\r'*) echo "::error::$name contains a newline"; exit 1 ;;
            esac
            printf '%s=%s\n' "$name" "$value" >>"$GITHUB_ENV"
          }
          export_selector EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME "$CONFIG_NAME"
          export_selector EDGEZERO__STORES__KV__CACHE__NAME "$KV_NAME"
          export_selector EDGEZERO__STORES__SECRETS__CREDENTIALS__NAME "$SECRET_NAME"

      - id: config
        uses: IABTechLab/edgezero/.github/actions/config-push-fastly@<ref>
        with:
          app-release-archive: ${{ github.workspace }}/app-release/app-release.tar.gz
          app-release-sha256: ${{ needs.preflight.outputs.sha256 }}
          expected-source-revision: ${{ needs.preflight.outputs.source-revision }}
          fastly-api-token: ${{ secrets.FASTLY_API_TOKEN }}
          working-directory: publisher-config
          app-config: app.toml
          store: app_config
          deploy-to: ${{ needs.preflight.outputs.deploy-to }}

      - name: Record required config reconciliation
        if: ${{ always() && steps.config.outcome == 'failure' && steps.config.outputs.mutation-attempted == 'true' }}
        env:
          RELEASE_SHA256: ${{ needs.preflight.outputs.sha256 }}
          DEPLOY_TO: ${{ needs.preflight.outputs.deploy-to }}
        run: |
          echo "::error::Config mutation was attempted. Re-run config push with release $RELEASE_SHA256 and target $DEPLOY_TO, or verify the selected store and logical key before deploying."
          exit 1

      - id: deploy
        uses: IABTechLab/edgezero/.github/actions/deploy-fastly@<ref>
        with:
          app-release-archive: ${{ github.workspace }}/app-release/app-release.tar.gz
          app-release-sha256: ${{ needs.preflight.outputs.sha256 }}
          expected-source-revision: ${{ needs.preflight.outputs.source-revision }}
          fastly-api-token: ${{ secrets.FASTLY_API_TOKEN }}
          fastly-service-id: ${{ vars.FASTLY_SERVICE_ID }}
          deploy-args: '["--comment","Application release"]'
          deploy-to: ${{ needs.preflight.outputs.deploy-to }}

      - id: health
        uses: IABTechLab/edgezero/.github/actions/healthcheck-fastly@<ref>
        with:
          app-release-archive: ${{ github.workspace }}/app-release/app-release.tar.gz
          app-release-sha256: ${{ needs.preflight.outputs.sha256 }}
          expected-source-revision: ${{ needs.preflight.outputs.source-revision }}
          fastly-api-token: ${{ needs.preflight.outputs.deploy-to == 'staging' && secrets.FASTLY_API_TOKEN || '' }}
          fastly-service-id: ${{ vars.FASTLY_SERVICE_ID }}
          fastly-version: ${{ steps.deploy.outputs.fastly-version }}
          domain: ${{ inputs.domain }}
          path: /health
          deploy-to: ${{ needs.preflight.outputs.deploy-to }}

      - name: Roll back failed deployment
        if: ${{ (failure() || cancelled()) && steps.deploy.outputs.fastly-version != '' && (needs.preflight.outputs.deploy-to == 'staging' || steps.deploy.outputs.previous-version != '') }}
        uses: IABTechLab/edgezero/.github/actions/rollback-fastly@<ref>
        with:
          app-release-archive: ${{ github.workspace }}/app-release/app-release.tar.gz
          app-release-sha256: ${{ needs.preflight.outputs.sha256 }}
          expected-source-revision: ${{ needs.preflight.outputs.source-revision }}
          fastly-api-token: ${{ secrets.FASTLY_API_TOKEN }}
          fastly-service-id: ${{ vars.FASTLY_SERVICE_ID }}
          fastly-version: ${{ steps.deploy.outputs.fastly-version }}
          rollback-to: ${{ steps.deploy.outputs.previous-version }}
          deploy-to: ${{ needs.preflight.outputs.deploy-to }}

      - name: Require config reconciliation after a later failure
        if: ${{ always() && steps.config.outcome == 'success' && (steps.deploy.outcome == 'failure' || steps.health.outcome == 'failure' || cancelled()) }}
        run: |
          echo "::error::Config push succeeded but deployment did not. Restore the previous config value or re-push the previous release's config to the same physical store and logical key before retrying."
          exit 1
```

The example pins the producer run, source revision, and archive digest outside
both publisher Environments. `preflight` validates that identity and proves the
derived GitHub Environment already exists. The deploy job downloads the exact
artifact from that producer repository and run once; each lifecycle action
independently verifies the same archive, digest, and source revision. It receives
environment-scoped runtime values only afterward. The optional `CREDENTIALS` selector is a Secret Store
name from `vars`, never a secret value. `inputs.domain` remains the actual
hostname passed to healthcheck for both targets. The deployer checkout contains
only publisher config; it never checks out or rebuilds application source.

For a staging rollback, omit `rollback-to`; for production, skip rollback when
`previous-version` is empty because a first deployment has no earlier version.
The staging rollback action inspects the exact version and deactivates it only
when staged; it succeeds without mutation when deployment failed while the
version was still an unpublished draft. Use an additional guard in a
production-only workflow if it can perform a first deployment.

## Failed deploy recovery

The action's public `package-digest` becomes available only after the deploy step
emits adapter `package-sha256`; it may be absent when preflight fails before the
application CLI runs. The action emits `fastly-version` as soon as the target
version is known, so it can remain available if link reconciliation or publication
work fails later. The output identifies the exact provider version but does not
prove its current Fastly state.

In a follow-up step guarded with GitHub Actions' `always()` condition, inspect the
failed step's outputs. The rollback action handles the exact staging version
state and refuses incompatible state. If a production version is active and
`previous-version` is present, run production rollback. If its state is unknown,
reconcile provider state manually without guessing. If `mutation-attempted` is
true for config push, re-run the same pinned release, logical store, payload, and
target or verify that exact physical store and logical key before deployment.
Config pushed before deployment must remain backward-compatible with the current
published release until deployment and healthcheck finish. If a later step
fails, restore the prior value or re-push the prior release's config to the same
physical store and logical key; version the physical store when that compatibility
cannot be guaranteed.

Serialize deployments by service. Without serialization, a provider lookup after
failure may observe another run's version.

## Runner requirements

The actions are tested on `ubuntu-latest`. A self-hosted runner must be Linux
x86-64 and provide Bash, `jq`, Mike Farah `yq` v4, `tar`, `curl`, `git`, `base64`,
`realpath`, and either `sha256sum` or `shasum`. The application CLI archive must
contain a Linux x86-64 executable.

## Adoption checklist

- Produce and publish one strict application release without provider credentials.
- Resolve its trusted source revision and SHA-256 outside publisher Environments.
- Download from the explicit producer repository and verify the archive, digest,
  and selected source revision; do not checkout application source in deploy.
- Pass the same archive, digest, and source revision to deploy, config push, healthcheck, and
  rollback.
- Keep runtime variables and credentials in protected publisher Environments.
- Validate that `FASTLY_SERVICE_ID` contains ASCII letters and digits only.
- Thread `deploy-to` consistently through deploy, healthcheck, and rollback.
- Record `package-digest`, `fastly-version`, and `previous-version` for recovery.
- Pin action references and serialize deployments per service.
