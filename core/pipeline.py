from __future__ import annotations

from core.models import JobRecord
from core.utils import deadline_passed_with_grace


def reconcile_records(sheet_key: str, incoming_records: list[JobRecord], state_manager, today_str: str, miss_threshold: int):
    state = state_manager.get_sheet_state(sheet_key)
    active: list[JobRecord] = []
    closed: list[JobRecord] = []

    seen_keys = set()

    for record in incoming_records:
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
            "miss_count": 0,
            "last_seen": today_str,
        }
        active.append(record)

    for key, payload in list(state.items()):
        if key in seen_keys:
            continue
        miss_count = int(payload.get("miss_count", 0)) + 1
        payload["miss_count"] = miss_count
        payload.setdefault("last_seen", today_str)
        deadline = payload.get("deadline", "")
        if deadline_passed_with_grace(deadline, today_str) or miss_count >= miss_threshold:
            closed.append(JobRecord(
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
            ))
            state.pop(key, None)

    return active, closed
