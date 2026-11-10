# Reviewer guide for code agents

Instructions for AI code agents (GitHub Copilot, Claude Code, Cursor, etc.) reviewing or
working on this repository. Human readers: start with `README.md`.

## What this repo is

A demo of Apache Airflow 3.x feeding Confluent Platform (Apache Kafka). One DAG,
`file_to_kafka`, watches folders for gzipped tab-delimited files and produces each line as a
JSON record to a Kafka topic. It is a teaching example, not a production service.

## Layout

| Path | What to look at |
|---|---|
| `dags/file_to_kafka.py` | The DAG: `list_new_files` -> `produce_file` (dynamic task mapping, one task per file) |
| `dags/incrementals.py` | `iter_records`: pure parsing, no Airflow imports |
| `Dockerfile` | `apache/airflow:3.0.0` plus the Kafka provider |
| `docker-compose.yaml` | Single-container `standalone` Airflow, mounts `./dags` and `./input` (read-only) |
| `.env.example` | Template for the gitignored `.env` (Kafka connection as one-line JSON) |
| `docker-compose.override.example.yml` | Optional broker host-name mapping |

## Quick start for a review (no Kafka cluster needed)

Static checks, from the repo root:

```bash
python -m py_compile dags/file_to_kafka.py dags/incrementals.py
python -I -c "import sys; sys.path.insert(0,'dags'); from incrementals import iter_records; print('ok')"
docker compose config -q          # needs a .env; use: cp .env.example .env
docker compose build              # confirms the image builds
```

`dags/incrementals.py` has no dependencies beyond the standard library, so it can be tested
with plain `pytest` without Airflow or Kafka. Build a small `.dat.gz` fixture in a temp dir.

Running the full DAG needs a reachable Kafka cluster and files under `input/` (not
committed). Do not try to run it against a cluster you were not given.

## Review focus

1. **Correctness of parsing** (`incrementals.py`): field-count checks, CRLF handling, blank
   lines, keys built from `KEY_COLUMNS`, values kept as strings on purpose.
2. **Delivery semantics** (`produce_file`): flush and ack handling, `BufferError`
   backpressure, ledger written only after a clean flush.
3. **Airflow 3 API usage**: imports from `airflow.sdk`, dynamic task mapping, XCom size of
   the `list_new_files` return value.
4. **Packaging**: pinning and reproducibility in the `Dockerfile`.
5. **Docs vs. code drift**: the README's configuration table and behaviour list must match
   the code and `.env.example`.

## Intentional design choices (do not flag as bugs)

- Delivery is **at-least-once**; the README says so. Duplicates on retry are expected.
- The processed-file ledger is an **Airflow Variable** (demo simplicity).
- Values are **all strings** so leading zeros and decimals survive.
- Input is mounted **read-only**; files are never moved or deleted.
- Malformed lines are **logged and skipped**, not sent to a dead-letter topic.
- The folder name `KEY Incrmentals` matches the real source naming; keep it consistent
  with the README.
- `airflow dags test` fails on the mapped task on Airflow 3.0.0 (`unexpected expand_input`).
  This is a known upstream limitation, not a repo defect.
- `standalone` mode and the lack of healthchecks are demo-only.

## Known open items

Fair to report, or fix if asked: deprecated `airflow.decorators` imports, naive
`datetime.utcnow()`, one `Variable.get` per file per run, no unit tests, unpinned provider
version, placeholder `CHANGELOG.md`.

## Ground rules for changes

- Keep the repo **generic and public-safe**. Never add real broker addresses, hostnames,
  IPs, API keys, customer names or internal links. Use placeholders such as
  `HOST:9092`, `API_KEY`, `bootstrap.example.internal`.
- Never commit `.env`, `input/` or `docker-compose.override.yml` (all gitignored).
- Keep `incrementals.py` free of Airflow imports so it stays unit-testable.
- If you change an environment variable or default, update both `README.md` and
  `.env.example`.
- Keep changes small and focused; this is a demo meant to be read end to end.
- Do not push or open PRs unless the maintainer asks.
