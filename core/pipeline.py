
from __future__ import annotations

from core.models import JobRecord
from core.utils import deadline_passed_with_grace, to_deadline_sort_key


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
            "region": record.region,
            "source": record.source,
            "deadline": record.deadline,
            "qualification": record.qualification,
            "job_function": record.job_function,
            "location": record.location,
            "employment_type": record.employment_type,
            "phd_preferred": record.phd_preferred,
            "raw_text": record.raw_text,
            "miss_count": 0,
            "closed": False,
        }

        if deadline_passed_with_grace(record.deadline, today_str):
            state[key]["closed"] = True
            closed.append(record)
        else:
            active.append(record)

    for key, item in list(state.items()):
        if key in seen_keys:
            continue
        item["miss_count"] = int(item.get("miss_count", 0)) + 1
        if item["miss_count"] >= miss_threshold:
            item["closed"] = True
            closed.append(JobRecord(
                company=item["company"],
                region=item["region"],
                source=item["source"],
                title=item["title"],
                url=item["url"],
                deadline=item.get("deadline", "없음"),
                qualification=item.get("qualification", ""),
                job_function=item.get("job_function", ""),
                location=item.get("location", ""),
                employment_type=item.get("employment_type", ""),
                phd_preferred=item.get("phd_preferred", "N"),
                job_id=item.get("job_id", ""),
                raw_text=item.get("raw_text", ""),
            ))

    active = [r for r in active if not deadline_passed_with_grace(r.deadline, today_str)]
    active.sort(key=lambda r: to_deadline_sort_key(r.deadline))
    closed.sort(key=lambda r: (r.company, r.title))
    state_manager.set_sheet_state(sheet_key, state)
    return active, closed
