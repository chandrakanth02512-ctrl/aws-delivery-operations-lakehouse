# Technical audit of Phases 1–3

Audit date: **27 October 2026 (IST)**. Baseline: `main` at
`82afc56c27edd19ad87a80aa0e9f212d0747dbcc`; fixes: `fix/technical-audit`, package **0.3.1**.
The architecture and existing tests are preserved. No AWS API calls, deployment, IAM changes,
uploads, jobs, queries, state-machine executions or account changes were performed.

## Review scope

All **118 baseline tracked files** were reviewed: 35 package modules, 28 test files, seven scripts,
one Glue adapter, nine SQL files, three CloudFormation templates, two workflows, two configuration
files, 26 documentation/evidence files, README, Git ignore/Python settings, project metadata and lockfile.
The audit adds one regression-test file, this report and a compact verification record: **121 project
files in the reviewed result**. Generated data, environments, JARs and raw logs are excluded from that count.

The source/module review was accompanied by Python syntax compilation and an internal import-graph
check (no cycles found). All 30 lockfile packages and 116 distribution entries were inspected
structurally; every locked distribution entry has a SHA-256 digest. The application/test dependency
versions remain unchanged. Only the project's patch version changes in the lockfile.

Review covered snapshot/CDC dependencies, data contracts and quarantine, Gold formulas and SQL,
source ordering and tombstones, prospective parent/child validation, staged MERGE recovery,
execution state/retries/metrics, CLI gates, packaging, IAM/OIDC, infrastructure and documentation.

## Confirmed defects and focused fixes

| Finding | Severity | Root cause and fix | Regression evidence |
|---|---|---|---|
| Snapshot could overwrite a table outside the requested root or implicitly widen an incoming schema | High correctness risk | Snapshot writer checked only Iceberg version. Reuse the existing CDC schema/location guard in the shared writer; reject mismatches before modifying that table. | Real Iceberg tests assert original rows and snapshot count remain unchanged for both mismatches. |
| CDC watermark shifted with the Python host timezone | Medium | Collected Spark timestamps became host-local naive Python values, then were labeled UTC. Collect an offset-bearing Spark string and parse it explicitly; preserve microseconds. | Tests reproduce and fix the shift under Asia/Kolkata and America/Los_Angeles. Existing late/out-of-order watermark tests remain. |
| Snapshot replay accepted altered Bronze manifest metadata | Medium | Replay checked hashes for whichever files the manifest supplied, without binding its identity, complete dataset set, keys and counts to Landing. Validate those bindings before replay. | Mutated batch ID, removed dataset, changed row count and redirected object key each fail closed. |
| Resumed execution retained an old RUNNING attempt under overall success | Medium | Resume appended a new attempt without closing an interrupted one. Mark the prior attempt INTERRUPTED with an observation timestamp and diagnostic. | Simulated process loss and explicit resume; no fabricated completion time or duration. |
| Bare NaN in a text field passed snapshot JSON validation | Medium | Spark's permissive JSON reader allowed non-standard numeric constants before coercion to strings. Disable that parser option; retain the raw rejected line. | A driver-name NaN is quarantined as MALFORMED_JSON_OBJECT. Existing numeric/string boundary tests remain. |
| Malformed added-column type crashed schema comparison | Medium | A list/dictionary type declaration was used in set membership. Require a string type name before compatibility lookup. | Invalid list/dictionary declarations yield structured breaking-change diagnostics instead of TypeError. |
| Vulnerable uv build-tool pin | Critical, upstream advisory rating | Both workflows pinned 0.12.23. Update to 0.13.4; use that patched version for this audit's final dependency/build checks. | Workflow regression enforces the patched minimum, SHA-pinned Actions and unprivileged PR CI. |

The six correctness defects were reproduced before fixes: **12 deliberately failing regression
cases** (five unit/control, two schema, five Spark). Those development reproductions are distinct
from the passing baseline and final suite. Tests were added, not removed or weakened.

Additional coverage verifies an abrupt interruption after a real committed Silver MERGE, an empty
CDC batch rejected before initialization/publication, and the three portable verification SQL
statements previously excluded together with Athena's metadata query. The `$snapshots` query remains
explicitly unexecuted locally and on AWS. Empty source batches are rejected by the existing protocol;
this audit does not invent empty-batch publication semantics.

## Automated verification

**173 passed, one live AWS test skipped, zero failures/errors in 931.17 seconds.**
Package coverage is **91.92% (92% rounded): 1,525 of 1,659 statements**, with 134 missed.
All existing Phase 1/2/3 tests passed alongside 16 new regression cases; none were removed.
See the [compact verification evidence](evidence/audit-verification.json).

Ruff lint and formatting, all three CloudFormation templates (`cfn-lint`), generated data dictionary,
dependency compatibility, lock consistency and Git whitespace checks passed. Wheel, source distribution
and Glue import ZIP builds passed. The final pip-audit scan checked **31 locked packages/build tools
with zero known vulnerabilities and no skipped entries**. Security-scanner limits and reviewed
Bandit warnings are described below.

The baseline suite passed **157 tests, skipped one live AWS test, and had zero failures/errors in
838.21 seconds**, with 91.83% package line coverage (1,517/1,652 statements). Focused post-fix runs
passed **78 unit/control tests in 1.44 seconds** and **12 Spark/Iceberg tests in 111.45 seconds**.
These supplementary runs are not added to the final suite count.

Runtime: Python 3.13.5, Spark 4.1.1, local Java 21.0.12.1, Iceberg 1.11.0, H2 2.4.240,
Pytest 9.1.1, Ruff 0.17.2, cfn-lint 1.58.0, patched uv 0.13.4. Hosted CI selects Python 3.13
and Temurin Java 17. No claim is made that local Java/JDBC behavior proves managed Glue behavior.

The baseline [main CI run](https://github.com/chandrakanth02512-ctrl/aws-delivery-operations-lakehouse/actions/runs/40369766855)
was confirmed successful. The audit PR's checks provide the hosted result for its exact head commit;
the PR description/final audit handoff records that result after the branch is pushed. This document's
local evidence does not stand in for an unfinished or different-commit hosted run.

## Security findings and boundaries

- **Confirmed and fixed:** [GHSA-hq85-5cpj-mr62](https://github.com/advisories/GHSA-hq85-5cpj-mr62),
  also PYSEC-2026-4321. GitHub's reviewed advisory lists uv `<0.13.4` as affected and **0.13.4** as
  patched. It concerns a bundled trusted CA with compromised intermediate keys. Both CI and manual
  deployment workflow pins were affected. This was independently confirmed beyond scanner output.
- **Secrets:** Gitleaks 8.24.3, with redacted output, scanned all seven reachable commits, their
  messages, all 152 unique historical blobs, and the final source tree. No leaks were found.
  No tracked `.env`, private-key or credential file was found. Dummy AWS strings in Stubber tests
  are test fixtures. This is evidence from the available repository, not a guarantee about deleted
  remote objects or unavailable artifacts.
- **Dependency audit:** pip-audit 2.10.1 checks the locked Python packages, pinned Hatchling backend
  and workflow uv version. The installed-environment scan also checks the actual local packages;
  the editable project is reviewed as source. The compact evidence records exact final counts.
  JVM transitive libraries, native binaries, the OS and managed Glue's bundled dependencies were
  not comprehensively vulnerability-scanned. Isolated build-backend transitive resolution is not
  fully constrained by the application/test lockfile.
- **Bandit 1.9.4:** ten findings (four medium, six low) were reviewed without suppressing rules.
  Two SQL warnings interpolate validated identifiers/fixed internal columns; they do not interpolate
  business values. The download warning uses a fixed HTTPS Maven URL plus pinned SHA-256. The
  benchmark's exec/subprocess warnings concern deliberate execution of a full, operator-reviewed Git
  revision, never source data or CI/Glue input. Its SHA must be trusted before invocation. The other
  warnings concern demonstration assertions and deterministic, non-security random generation.
  No additional confirmed vulnerability was identified from those warnings under these boundaries.
- **Open governance finding, medium:** GitHub reports `main` as unprotected and the repository ruleset list is empty. The owner should require
  PR review and passing `verify` CI before enabling deployment. This audit does not alter repository
  settings. Main-only OIDC trust is not a substitute for protecting who can change main.
- **Workflow/IAM review:** PR CI has contents-read permission, no AWS identity and no
  `pull_request_target` trigger. Checkout does not persist credentials; Actions are SHA-pinned.
  Deployment remains manual, exact-repository/main gated, and requires both approval variable and
  phrase. Its `plan` operation writes release artifacts and creates a change set; it is not read-only.
  Main deployment roles cannot modify IAM or start Glue/Athena workloads. Operations IAM is separate.
  Policy simulation and real authorization remain untested; no template or live IAM permission was changed.

## Correctness, performance and retained limits

The combined suite retains deterministic generation, invalid/duplicate quarantine, foreign keys,
parent-delete restrictions, accepted deletes, version/time/hash ordering, duplicate CDC events,
schema evolution rules, late corrections, empty group cleanup, adoption, and full Gold recomputation
comparisons. Independent Python and hand-calculated KPI expectations remain in place.

Recovery still stages the complete plan before publication, uses per-table checkpoints, and resumes
the frozen plan after partial writes. The new abrupt-loss test bypasses ordinary exception handling
to leave durable RUNNING state after a committed MERGE. It verifies that unrelated batches are
blocked, the prior watermark survives, and recovery converges without duplicate rows.

No speculative performance rewrite was made. Event-only Gold pruning remains covered by snapshot
count assertions. The existing Phase 3 benchmark is retained as historical, narrow evidence; it was
not rerun or presented as an AWS/full-pipeline performance result. Full prospective relationship
scans, affected-date cohort reads, repeated verification actions and unpartitioned small-table
MERGE rewrites remain deliberate correctness/scale tradeoffs. Large-profile Spark throughput,
memory behavior and physical partition pruning remain unmeasured.

Publication is **atomic per table only**. Reader coordination, a distributed writer lease, SCD2,
automated retention/compaction and destructive external-corruption repair remain out of scope.
Saved-plan metadata is checksummed; staged Parquet payload integrity remains schema/row-count and
post-write comparison based, not a cryptographic digest of every payload byte. Pending plans must
finish with their original package/configuration before upgrading to 0.3.1. Successful historical
replay does not continuously re-audit externally modified target tables.

## Documentation and reproduction

README/release/deployment instructions now reflect the public repository and merged phases instead
of telling the owner to repeat publication/retargeting. Existing phase reports remain historical
records. Target guards, strict JSON handling, timezone-independent watermarks, interrupted-attempt
reporting, patched uv and the reported account blocker are documented. Private support-case details
and account credit balances are not committed.

From the repository root, use Python 3.13, Java 17+ and uv 0.13.4:

```bash
uv sync --frozen --extra spark
uv run python scripts/fetch_iceberg.py
ICEBERG_SPARK_JAR=artifacts/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  uv run pytest --cov=delivery_lakehouse --cov-report=term-missing \
  --cov-report=json:artifacts/audit-coverage.json --junitxml=artifacts/audit-pytest.xml
uv run ruff check .
uv run ruff format --check .
uv run cfn-lint infrastructure/*.yaml
uv run python scripts/data_dictionary.py --check
uv pip check
uv lock --check
uv build
uv run python scripts/package_glue.py
git diff --check
```

Security checks used isolated tools, not new application dependencies:

```bash
uv tool run --from pip-audit==2.10.1 pip-audit \
  --path .venv/lib/python3.13/site-packages --skip-editable
uv tool run --from bandit==1.9.4 bandit -r src jobs scripts
# With a verified Gitleaks 8.24.3 binary installed:
gitleaks git --log-opts='--all --full-history' --redact=100
```

Bandit returns a nonzero status for the reviewed warnings above; that is not described as a clean
scanner run. A separate requirements list generated from `uv.lock`, plus `hatchling==1.27.0` and
`uv==0.13.4`, was scanned with pip-audit `--no-deps --disable-pip` to include tools outside `.venv`.
All documented shell blocks are syntax-checked; deployment/upload commands are inspected only.
Cloud commands and destructive/paid examples are never executed merely to test documentation.

## AWS integration readiness verdict

**NOT READY FOR AWS INTEGRATION TESTING in the current account.** Local/static verification can
establish code readiness; it cannot authorize or establish account readiness. The owner reports an
Organizations SCP denying OIDC-provider discovery. Resolve that with AWS Support/the organization
administrator; do not bypass it, create IAM resources as a workaround, or upgrade the Free Plan.

Before a separately approved live test: review/merge the audit PR after passing hosted CI, protect
main, resolve the SCP, establish an authorized temporary session, verify current credits/budget and
Sydney service entitlement, inspect the existing bucket/KMS/Lake Formation configuration, and review
the exact infrastructure change set and regional costs. Uploads, Glue runs, Athena queries and
Step Functions setup/execution each require their own appropriate approval. Managed runtime,
S3/Glue Catalog commits, OIDC, IAM, CloudWatch and Athena remain unverified. No cloud result is inferred
from these local tests.
