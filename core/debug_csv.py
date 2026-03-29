from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

DEBUG_DIR = Path("debug_outputs")
AUDIT_FIELDS = [
    "timestamp", "stage", "company", "source", "source_type", "platform", "keyword", "region_hint",
    "decision", "reason", "search_url", "final_url", "page_title", "result_count_text",
    "title", "url", "canonical_url", "detail_title", "detail_company", "detail_location",
    "employment_type", "matched_keyword", "score", "include_matches", "exclude_matches",
    "hard_excludes", "note", "payload_json",
]


def ensure_debug_dir() -> Path:
    DEBUG_DIR.mkdir(exist_ok=True)
    return DEBUG_DIR


def safe_token(value: str) -> str:
    token = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", str(value or "")).strip("_")
    return token or "unknown"


def write_csv_rows(filename: str, fieldnames: list[str], rows: Iterable[dict], *, append: bool = False) -> Path:
    rows = list(rows)
    if not rows and append:
        return ensure_debug_dir() / filename
    path = ensure_debug_dir() / filename
    mode = "a" if append else "w"
    with path.open(mode, newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not append or f.tell() == 0:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    return path


def _audit_row_from_event(stage: str, payload: dict) -> dict:
    row = {k: "" for k in AUDIT_FIELDS}
    row["timestamp"] = payload.get("timestamp") or datetime.utcnow().isoformat(timespec="seconds")
    row["stage"] = stage
    for key in row:
        if key in payload and key != "payload_json":
            value = payload.get(key, "")
            if isinstance(value, (list, tuple, set)):
                value = ", ".join(str(v) for v in value)
            row[key] = value
    if not payload.get("payload_json"):
        safe_payload = {}
        for k, v in payload.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                safe_payload[k] = v
            else:
                safe_payload[k] = str(v)
        row["payload_json"] = json.dumps(safe_payload, ensure_ascii=False)
    else:
        row["payload_json"] = payload.get("payload_json", "")
    return row


def append_audit_event(stage: str, **payload) -> Path:
    return write_csv_rows("audit_all.csv", AUDIT_FIELDS, [_audit_row_from_event(stage, payload)], append=True)


def append_audit_rows(stage: str, rows: Iterable[dict]) -> Path:
    audit_rows = [_audit_row_from_event(stage, row) for row in rows]
    return write_csv_rows("audit_all.csv", AUDIT_FIELDS, audit_rows, append=True)
