# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A set of Python 3.6 tools for auditing the data quality of a **VAS Experts OIMC / СОРМ** lawful-interception database (PostgreSQL, schema `oims`/`oimc`/`oimm`). The primary tool (`test-ctlc.py`) measures the "binding percentage" (привязка) of captured traffic records (NAT, AAA, logins) to known subscriber/operator data, and dumps the offending records for investigation. Log output and many report strings are in Russian.

The tool ships as an **RPM package for CentOS Stream 8** (see `packaging/test-ctlc.spec`). There is no Python test suite; the scripts run directly against a live database.

## Layout

- `test-ctlc.py` + `lib/` — the main tool.
- `tools/` — standalone helper scripts (`aaachecker.py`, `make_addresses_from_cache.py`, `kpd4.sh`, `first_launch.sh`, `update.sh`), each run on its own and unrelated to the `test-ctlc.py` orchestration.
- `packaging/test-ctlc.spec` — RPM spec.
- `.github/workflows/` — CI: `build-rpm.yml` builds an RPM on every commit/PR; `release.yml` tags + releases on merge of a PR into `master`.

### Installed (RPM) FHS layout, prefix `/opt/vasexperts`

- binary: `/opt/vasexperts/bin/test-ctlc` (shebang points at the bundled venv python)
- config: `/opt/vasexperts/etc/test-ctlc/config.conf` (`%config(noreplace)`)
- lib package + bundled venv (psycopg2, numpy): `/opt/vasexperts/var/lib/test-ctlc/{lib,venv}/`
- logs: `/opt/vasexperts/var/log/test-ctlc/test-ctlc.txt`
- offending-record dumps (`tmp_files_path`): `/opt/vasexperts/var/lib/test-ctlc/tmp/`

The entry point adds `$TEST_CTLC_LIB` (default `/opt/vasexperts/var/lib/test-ctlc`) to `sys.path` so `import lib.*` resolves when installed; running from a source checkout still works because the repo root is already on `sys.path`. If `-f` is omitted, the config defaults to `/opt/vasexperts/etc/test-ctlc/config.conf`.

## Running

```bash
# Installed via RPM
/opt/vasexperts/bin/test-ctlc run -f /opt/vasexperts/etc/test-ctlc/config.conf
/opt/vasexperts/bin/test-ctlc version

# From a source checkout (cp config.conf.defaults config.conf first)
python3 test-ctlc.py run -f config.conf
python3 test-ctlc.py version
```

`config.conf` is created by copying `config.conf.defaults`. The DB connection (host/port/user/password/database) and which scenarios run live in this file — it is **not** the same DB credentials as the standalone `tools/` scripts below, which hardcode their own.

Only the `run` and `version` subcommands are implemented; `list` and `info` are stubs (the `info()` functions in the scenario modules are empty).

## Architecture (test-ctlc.py)

The flow is a linear orchestrator dispatching to self-contained scenario modules:

- **`test-ctlc.py`** — argparse CLI. `run` parses the INI, calls `logger.initialize(...)`, then `processor.run(config)`.
- **`lib/processor.py`** — reads every config value, opens the single psycopg2 connection/cursor, detects schema version, and calls each enabled scenario's `run(cur, ...)` in turn. Finally calls `general.combining_bundles_reports(...)` to print the merged per-operator table.
- **`lib/scenariuos/general.py`** — shared DB helpers: `check_telco_codes` (loads operators), `checking_schema_version`, and `combining_bundles_reports` (numpy-based merge of the three scenario matrices into one table per operator).
- **`lib/scenariuos/oimc_broadband/bundles_{nat,aaa,logins}.py`** — one module per scenario. Each builds SQL via `string.Template`, iterates partitions, computes a percentage per telco/traffic-type, and **returns a `results_matrix`** of rows `[telco, type_code, percent, range_min, range_max, count]`. When a percentage drops below the configured threshold, it writes the offending client addresses / logins to a file in `tmp_files_path`.
- **`lib/logger.py`** — configures the root `logging` to write simultaneously to `<log_path>/test-ctlc.txt` and stdout. Defines a custom `DIAGNOSTIC` level. Scenarios log full SQL at `debug`.

### Key cross-cutting concepts

- **Schema version → partition source.** `general.checking_schema_version` reads `oimm.component_versions`. If the major version is `>= 8`, native partitions are used (`oimc.range_partitions`, `template_partitions_new`); otherwise the legacy pg_pathman view (`public.pathman_partition_list`, `template_partitions_old`). Every scenario branches on this `native_partitions` flag.
- **Traffic types.** A `type_part` dict (duplicated in each scenario and in `aaachecker.py`) maps the seven `oimc.*` tables to 4-char codes: `rawf, htrq, emlc, imcn, vipc, trmc, ftpc`. The same codes key the matrices and the final combined report.
- **Config booleans are strings.** Values like `nat_check`, `exclude_dict_ip_numbering`, etc. are compared against the literal string `'True'` (`if nat_check == 'True'`), not parsed as bools. List-valued options (`aaa_exclude_client_address`) are parsed with `ast.literal_eval` or `json.loads`.
- **Date range logic.** Each scenario's `run` converts `range_hours` into `date_l`/`date_h`; when the range is a whole number of days it snaps to midnight boundaries, otherwise it uses an hour window padded to the next day (partition tables are mostly daily).

### Naming to preserve (intentional misspellings)

These are load-bearing — do not "fix" them or imports/config lookups break:
- Package directory: `lib/scenariuos/` (not `scenarios`).
- Config section: `[scenarious]` (not `[scenarios]`).

## Standalone tools (`tools/`, separate, not part of test-ctlc.py)

Each has its **own hardcoded** psycopg2 connection (`vasexperts` / `oim_admin` @ `127.0.0.1:54321`) and is run on its own:

- **`tools/aaachecker.py`** — cron script (every 3h) checking the AAA bundle percentage against a hardcoded `SUBNETS` list; writes `aaa_report.<ts>.txt`. The `SUBNETS` array must be filled in before it does anything.
- **`tools/make_addresses_from_cache.py`** — utility: reads a dadata cache sqlite (`<db_file>`) and emits a structured-addresses sqlite. Run as `python3 make_addresses_from_cache.py <db_file>`.
- **`tools/kpd4.sh`** — counts the last 24h of dump files under `/opt/vasexperts/var/dump/oim_data/...` per traffic type.
- **`tools/first_launch.sh`** — legacy first-time host setup (installs python3/pip/numpy, copies config); superseded by the RPM for deployment.
- **`tools/update.sh`** — `git pull` from the GitHub remote.

## Conventions

- SQL is assembled with `string.Template.substitute` and f-strings, with telco codes and addresses interpolated directly — this is an internal tool against a trusted DB, but be aware there is no parameterization.
- Scenario modules are deliberately parallel in structure (same partition templates, same `optional()` exclude-clause builder pattern, same matrix shape). When changing one scenario, check whether the same edit applies to the other two.
