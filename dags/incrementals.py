"""Pure parsing helpers for the gzipped, tab-delimited incremental files (no Airflow imports)."""
import gzip
import json
import logging

log = logging.getLogger(__name__)


def iter_records(path, columns, key_columns, source_name):
    """Yield (key, json_value, line_no) for each well-formed line; skip and log malformed ones.

    All values stay strings so leading zeros (e.g. "0895") and decimals are preserved exactly.
    """
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.rstrip("\r\n")
            if not line:
                continue
            fields = line.split("\t")
            if len(fields) != len(columns):
                log.warning("Reject %s line %d: expected %d fields, got %d",
                            path, line_no, len(columns), len(fields))
                continue
            row = dict(zip(columns, fields))
            key = "|".join(row[c] for c in key_columns)
            value = {**row, "source": source_name,
                     "source_file": path.rsplit("/", 1)[-1], "line_no": line_no}
            yield key, json.dumps(value, separators=(",", ":")), line_no
