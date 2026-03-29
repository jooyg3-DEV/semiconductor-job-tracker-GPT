from __future__ import annotations

from core.debug_csv import safe_token, write_csv_rows
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
    by_soft_key: dict[tuple[str, str, str], JobRecord] = {}
    rows: list[dict[str, str]] = []

    for record in records:
        key = record.unique_key
        if key:
            existing = by_key.get(key)
            kept = record
            dropped = existing
            if existing and SOURCE_PRIORITY.get(record.source, 999) >= SOURCE_PRIORITY.get(existing.source, 999):
                kept = existing
                dropped = record
            by_key[key] = kept
            rows.append({
                "company": company_name or record.company,
                "stage": stage_name,
                "dedupe_key": key,
                "canonical_url": record.canonical_url,
                "title": record.title,
                "source": record.source,
                "decision": "DROP" if dropped is record and existing else "KEEP",
                "reason": "duplicate_unique_key" if existing else "unique_key_new",
            })
            continue

        soft = (
            record.company.lower().strip(),
            record.title.lower().strip(),
            record.location.lower().strip(),
        )
        existing = by_soft_key.get(soft)
        kept = record
        dropped = existing
        if existing and SOURCE_PRIORITY.get(record.source, 999) >= SOURCE_PRIORITY.get(existing.source, 999):
            kept = existing
            dropped = record
        by_soft_key[soft] = kept
        rows.append({
            "company": company_name or record.company,
            "stage": stage_name,
            "dedupe_key": " | ".join(soft),
            "canonical_url": record.canonical_url,
            "title": record.title,
            "source": record.source,
            "decision": "DROP" if dropped is record and existing else "KEEP",
            "reason": "duplicate_soft_key" if existing else "soft_key_new",
        })

    merged = list(by_key.values()) + list(by_soft_key.values())
    if company_name:
        write_csv_rows(
            f"dedupe_{safe_token(company_name)}.csv",
            ["company", "stage", "dedupe_key", "canonical_url", "title", "source", "decision", "reason"],
            rows,
            append=False,
        )
    return merged
