from __future__ import annotations

from core.debug_csv import append_audit_rows, safe_token, write_csv_rows
from core.models import JobRecord

SOURCE_PRIORITY = {
    "공식-국내": 0,
    "공식-글로벌": 1,
    "사람인": 2,
    "잡코리아": 3,
    "잡다": 4,
    "링크드인": 5,
    "하이브레인넷": 6,
    "캐치": 7,
    "링커리어": 8,
    "잡플래닛": 9,
}


def dedupe_records(records: list[JobRecord], *, company_name: str = "", stage_name: str = "dedupe") -> list[JobRecord]:
    by_key: dict[str, JobRecord] = {}
    rows: list[dict[str, str]] = []
    for record in records:
        key = record.unique_key
        if not key:
            continue
        existing = by_key.get(key)
        kept = record
        reason = "unique_key_new"
        if existing:
            if SOURCE_PRIORITY.get(record.source, 999) >= SOURCE_PRIORITY.get(existing.source, 999):
                kept = existing
                reason = "existing_source_priority"
            else:
                reason = "new_source_priority"
        by_key[key] = kept
        rows.append({
            "company": company_name or record.company,
            "stage": stage_name,
            "dedupe_key": key,
            "canonical_url": record.canonical_url,
            "title": record.title,
            "source": record.source,
            "decision": "KEEP" if kept is record else "DROP",
            "reason": reason,
            "kept_title": kept.title,
            "kept_url": kept.url,
        })
    merged = list(by_key.values())
    if company_name:
        fieldnames = ["company", "stage", "dedupe_key", "canonical_url", "title", "source", "decision", "reason", "kept_title", "kept_url"]
        write_csv_rows(f"dedupe_{safe_token(company_name)}.csv", fieldnames, rows, append=False)
        append_audit_rows("dedupe", rows)
    return merged
