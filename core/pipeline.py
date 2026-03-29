from __future__ import annotations

from core.debug_csv import safe_token, write_csv_rows
from core.models import JobRecord
from core.utils import canonicalize_job_url, deadline_passed_with_grace, is_valid_record, is_valid_record_payload


def _record_from_payload(sheet_key: str, payload: dict) -> JobRecord:
    return JobRecord(
        company=payload.get("company", sheet_key),
        region=payload.get("region", "글로벌"),
        source=payload.get("source", ""),
        title=payload.get("title", ""),
        url=payload.get("url", ""),
        deadline=payload.get("deadline", "없음"),
        qualification=payload.get("qualification", ""),
        job_function=payload.get("job_function", ""),
        location=payload.get("location", ""),
        employment_type=payload.get("employment_type", ""),
        recruitment_type=payload.get("recruitment_type", "일반"),
        experience_flag=payload.get("experience_flag", "N"),
        masters_flag=payload.get("masters_flag", "N"),
        phd_flag=payload.get("phd_flag", "N"),
        job_id=payload.get("job_id", ""),
        raw_text=payload.get("raw_text", ""),
        metadata=payload.get("metadata", {}) or {},
    )


def _normalize_state_bucket(sheet_key: str, bucket: dict[str, dict]) -> dict[str, dict]:
    normalized: dict[str, dict] = {}
    for _old_key, payload in list(bucket.items()):
        if not is_valid_record_payload(payload):
            continue
        key = (payload.get("job_id") or "").strip() or canonicalize_job_url(payload.get("url", "")) or str(_old_key)
        if not key:
            continue
        normalized[key] = payload
    return normalized


def reconcile_records(sheet_key: str, incoming_records: list[JobRecord], state_manager, today_str: str, miss_threshold: int, *, source_scope: set[str] | None = None):
    source_scope = set(source_scope or set())
    state = state_manager.get_sheet_state(sheet_key)
    normalized_state = _normalize_state_bucket(sheet_key, state)
    state.clear()
    state.update(normalized_state)

    active: list[JobRecord] = []
    closed: list[JobRecord] = []
    rows: list[dict[str, str]] = []
    seen_keys = set()

    for record in incoming_records:
        if not is_valid_record(record):
            rows.append({
                "company": sheet_key,
                "unique_key": record.unique_key,
                "source": record.source,
                "title": record.title,
                "url": record.url,
                "decision": "DROP",
                "reason": "invalid_incoming_record",
            })
            continue
        key = record.unique_key
        seen_keys.add(key)
        state[key] = {
            "job_id": record.job_id,
            "url": record.url,
            "title": record.title,
            "company": record.company,
            "region": record.effective_region,
            "source": record.source,
            "deadline": record.deadline,
            "qualification": record.qualification,
            "job_function": record.job_function,
            "location": record.location,
            "employment_type": record.employment_type,
            "recruitment_type": record.recruitment_type,
            "experience_flag": record.experience_flag,
            "masters_flag": record.masters_flag,
            "phd_flag": record.phd_flag,
            "raw_text": record.raw_text,
            "metadata": record.metadata,
            "miss_count": 0,
            "last_seen": today_str,
        }
        active.append(record)
        rows.append({
            "company": sheet_key,
            "unique_key": key,
            "source": record.source,
            "title": record.title,
            "url": record.url,
            "decision": "ACTIVE",
            "reason": "seen_in_run",
        })

    for key, payload in list(state.items()):
        if key in seen_keys:
            continue
        if not is_valid_record_payload(payload):
            state.pop(key, None)
            rows.append({
                "company": sheet_key,
                "unique_key": key,
                "source": payload.get("source", ""),
                "title": payload.get("title", ""),
                "url": payload.get("url", ""),
                "decision": "DROP",
                "reason": "invalid_state_payload",
            })
            continue
        payload_source = payload.get("source", "")
        if source_scope and payload_source not in source_scope:
            active.append(_record_from_payload(sheet_key, payload))
            rows.append({
                "company": sheet_key,
                "unique_key": key,
                "source": payload_source,
                "title": payload.get("title", ""),
                "url": payload.get("url", ""),
                "decision": "KEEP",
                "reason": "out_of_scope_preserved",
            })
            continue
        miss_count = int(payload.get("miss_count", 0)) + 1
        payload["miss_count"] = miss_count
        payload.setdefault("last_seen", today_str)
        deadline = payload.get("deadline", "")
        if deadline_passed_with_grace(deadline, today_str) or miss_count >= miss_threshold:
            closed.append(_record_from_payload(sheet_key, payload))
            state.pop(key, None)
            rows.append({
                "company": sheet_key,
                "unique_key": key,
                "source": payload_source,
                "title": payload.get("title", ""),
                "url": payload.get("url", ""),
                "decision": "CLOSE",
                "reason": "deadline_or_miss_threshold",
            })
            continue
        active.append(_record_from_payload(sheet_key, payload))
        rows.append({
            "company": sheet_key,
            "unique_key": key,
            "source": payload_source,
            "title": payload.get("title", ""),
            "url": payload.get("url", ""),
            "decision": "KEEP",
            "reason": f"miss_count={miss_count}",
        })

    write_csv_rows(
        f"reconcile_{safe_token(sheet_key)}.csv",
        ["company", "unique_key", "source", "title", "url", "decision", "reason"],
        rows,
        append=False,
    )
    return active, closed
