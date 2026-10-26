# Phase 1 verification report

Verified on **25 October 2026 (IST)** in Codex Cloud. Scope: local implementation and deployment
preparation. **AWS acceptance is pending review/authorization and AWS authentication.**

## Repository inspection

Started from clean branch `work` at initial commit `cd11d0afc6f1a55d6a17b475cb0b08c43be8dc58`
(`origin/main`). Existing files were a two-line README and Python `.gitignore`; no additional
`AGENTS.md` instructions or implementation were present. Work was isolated on
`feat/phase-1-lakehouse`. Existing history was preserved; no force-push or AWS operation was used.
The complete change inventory is [file-inventory.md](file-inventory.md).

## Executed checks and exact results

| Check | Actual result |
|---|---|
| `pytest tests -q --cov=delivery_lakehouse --cov-report=term-missing --junitxml=artifacts/pytest.xml` with pinned local Iceberg/H2 JARs | **53 passed, 1 skipped in 71.36s**; no failures |
| Python package line coverage | **91%** (576 statements, 53 missed); not branch coverage |
| `ruff check .` | `All checks passed!` |
| `ruff format --check .` | `46 files already formatted` |
| `cfn-lint infrastructure/bootstrap.yaml infrastructure/lakehouse.yaml` | Exit 0; no findings |
| `python scripts/data_dictionary.py --check` | Exit 0; contracts and dictionary match |
| `uv pip check` | 29 packages checked; `All installed packages are compatible` |
| `uv lock --check` | Exit 0; 30 packages resolved, committed lock consistent |
| `python scripts/fetch_iceberg.py` | Both pinned downloads verified against SHA-256 |
| `uv build` | Wheel and source distribution built successfully |
| `python scripts/package_glue.py` | Import ZIP built; deterministic-byte and ZIP integrity tests pass |
| `python scripts/demo.py` | Actual local pipeline SUCCEEDED, identical replay verified, analytical summary written |
| `git diff --check` | Exit 0 |

[Raw Pytest summary](evidence/pytest-summary.txt) · [Static check output](evidence/static-checks.txt) ·
[Reproducible demo evidence](evidence/demo-summary.json)

The one skipped test is `tests/aws/test_readiness.py::test_existing_bucket_region`, explicitly skipped
because live AWS access was not authorized/configured. All **three local Iceberg tests ran**; none
were skipped. Business SQL was also executed against local Gold tables through Spark; Athena-specific
metadata syntax was not executed against AWS.

Runtime: Python **3.13.5**, PySpark **4.1.1**, OpenJDK **21.0.12.1**, Iceberg **1.11.0**,
H2 **2.4.240**, Ruff **0.17.2**, cfn-lint **1.58.0**. CI requests Java 17, matching Glue 6.0's Java
major version. Local Java 21 is supported by Spark but is not the managed Glue runtime. Glue-specific
libraries, AWS service configuration and network behavior remain separate acceptance requirements.

Earlier iterations found and resolved local Python worker/driver mismatch, local catalog compatibility,
and a Decimal-aware test assertion. The final result above is from the verified implementation.
Dependency compatibility/lock checks are not a vulnerability audit.

## Small-profile results

Seed **42**, start date **2026-01-01**, complete synthetic snapshot:
`01bc8423e1ea47518b661f9b4145989ef219ec686deec64953cf790fd37ddfd3`.

| Dataset | Input | Silver accepted | Quarantined |
|---|---:|---:|---:|
| delivery_stations | 3 | 3 | 0 |
| drivers | 18 | 18 | 0 |
| customers | 60 | 60 | 0 |
| shipments | 126 | 120 | 6 |
| delivery_attempts | 140 | 138 | 2 |
| delivery_events | 398 | 396 | 2 |
| **Total** | **745** | **735** | **10** |

All datasets reconcile exactly. Duplicate losers, missing keys, an invalid status, nonnumeric weight,
an orphan customer and an impossible promise timestamp remain visible in quarantine.

| Metric | Result |
|---|---:|
| Total shipments | 120 |
| Delivered shipments | 102 |
| On-time delivered shipments | 48 |
| Terminal failed shipments | 6 |
| Total accepted attempts | 138 |
| Failed attempts | 36 |
| On-time delivery percentage | 47.0588% |
| Average nonnegative delivery delay | 279.3235 minutes |
| First-attempt success percentage | 77.7778% |
| Terminal failed delivery percentage | 5.5556% |

The large delay includes redeliveries a day later. These are synthetic analytical results, not measured
Amazon operational performance. The demo uses Australian afternoon/evening promised times, with
earlier express deadlines. Both local Parquet and local Iceberg pipelines publish six Silver and
nine Gold tables. Iceberg tests inspect metadata format v2, repeat replacement, empty replacement,
v3 rejection and same-batch replay. Quality-gate failure persists evidence, blocks curated writes and
successfully recovers when rerun with the reviewed threshold.

## Not tested / not performed

- No AWS authentication, STS/OIDC exchange or IAM policy simulation against this account.
- No S3 upload/download, Glue job execution, live Glue Catalog registration or Athena query.
- No CloudFormation stack/change-set creation, managed job startup, or Free Plan service eligibility check.
- No bucket encryption/security changes, account-plan upgrade, schedules or chargeable provisioning.
- No medium/large Spark scale benchmark; those generator profile sizes were tested locally.
- No multi-writer concurrency, cross-table atomic publication, CDC, SCD2, late-event handling,
  automated orchestration, monitoring platform or performance optimization from Phases 2/3.
- GitHub hosted CI/OIDC results are not inferred from local results; inspect the PR's actual check status.

## Phase 1 completion checklist

- [x] Repository/history inspection, isolated branch, maintainable src layout and locked dependencies.
- [x] Six deterministic synthetic datasets, three sizes, operational cases and deliberate defects.
- [x] Immutable Landing, traceable Bronze, integrity verification and tested interrupted-batch retry.
- [x] PySpark contracts, typing, deduplication, relationship checks, quarantine and reconciled reports.
- [x] Dimensional Gold model, Spark SQL KPIs, independent oracle and boundary tests.
- [x] Thin Glue 6.0 adapter and real local Iceberg v2 writer validation.
- [x] Existing-bucket CloudFormation, scoped execution/deployment roles and conservative job settings.
- [x] Exact-repository/main OIDC bootstrap and gated manual deployment plan/apply preparation.
- [x] Athena analysis SQL, local CI, cost guide, deployment guide, troubleshooting and evidence.
- [x] Local test/lint/dependency/build checks completed successfully.
- [ ] Owner IAM review, one-time trust bootstrap and explicitly approved resource deployment.
- [ ] Separately approved small AWS Glue run and Athena acceptance evidence.

Recommended next step: review the PR, especially [bootstrap.yaml](../infrastructure/bootstrap.yaml)
and [deployment.md](deployment.md). Only after approval should the owner establish trust and prepare
an AWS change set. Keep the Free Plan and approve at most one small cloud smoke run initially.
