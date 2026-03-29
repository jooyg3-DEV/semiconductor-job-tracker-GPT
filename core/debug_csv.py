from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable

DEBUG_DIR = Path("debug_outputs")


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
