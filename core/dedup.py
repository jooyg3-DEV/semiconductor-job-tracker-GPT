
from __future__ import annotations

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


def dedupe_records(records: list[JobRecord]) -> list[JobRecord]:
    by_key: dict[str, JobRecord] = {}
    by_soft_key: dict[tuple[str, str, str], JobRecord] = {}

    for record in records:
        key = record.unique_key
        if key:
            existing = by_key.get(key)
            if not existing or SOURCE_PRIORITY.get(record.source, 999) < SOURCE_PRIORITY.get(existing.source, 999):
                by_key[key] = record
            continue

        soft = (
            record.company.lower().strip(),
            record.title.lower().strip(),
            record.location.lower().strip(),
        )
        existing = by_soft_key.get(soft)
        if not existing or SOURCE_PRIORITY.get(record.source, 999) < SOURCE_PRIORITY.get(existing.source, 999):
            by_soft_key[soft] = record

    merged = list(by_key.values()) + list(by_soft_key.values())
    return merged
