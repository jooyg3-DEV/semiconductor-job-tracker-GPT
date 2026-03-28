from __future__ import annotations

from core.models import JobRecord
from core.utils import deadline_passed_with_grace

RECRUITMENT_ORDER = {"인재풀": 0, "채용시 마감": 1, "상시": 2, "일반": 3}

def reconcile_records(sheet_key: str, incoming_records: list[JobRecord], state_manager, today_str: str, miss_threshold: int):
    state = state_manager.get_sheet_state(sheet_key)
    active=[]
    closed=[]
    seen_keys=set()
    for record in incoming_records:
        key=record.unique_key
        seen_keys.add(key)
        state[key]={"job_id": record.job_id, "url": record.url, "title": record.title, "company": record.company, "region": record.effective_region, "source": record.source, "deadline": record.deadline, "qualification": record.qualification, "job_function": record.job_function, "location": record.location, "employment_type": record.employment_type, "recruitment_type": record.recruitment_type, "experience_flag": record.experience_flag, "masters_flag": record.masters_flag, "phd_flag": record.phd_flag, "miss_count": 0, "closed": False}
        if deadline_passed_with_grace(record.deadline, today_str):
            state[key]["closed"]=True
            closed.append(record)
        else:
            active.append(record)
    for key,item in list(state.items()):
        if key in seen_keys:
            continue
        item["miss_count"]=int(item.get("miss_count",0))+1
        rec = JobRecord(company=item["company"], region=item.get("region",""), source=item.get("source",""), title=item.get("title",""), url=item.get("url",""), deadline=item.get("deadline","없음"), qualification=item.get("qualification",""), job_function=item.get("job_function",""), location=item.get("location",""), employment_type=item.get("employment_type",""), recruitment_type=item.get("recruitment_type","일반"), experience_flag=item.get("experience_flag","N"), masters_flag=item.get("masters_flag","N"), phd_flag=item.get("phd_flag","N"), job_id=item.get("job_id",""), raw_text="")
        if item["miss_count"] >= miss_threshold:
            item["closed"]=True
            closed.append(rec)
        else:
            active.append(rec)
    state_manager.set_sheet_state(sheet_key, state)
    active.sort(key=lambda r: (RECRUITMENT_ORDER.get(r.recruitment_type, 9), r.effective_region != "국내", r.deadline not in ("", "없음"), r.deadline or "", r.title))
    closed.sort(key=lambda r: (RECRUITMENT_ORDER.get(r.recruitment_type, 9), r.company, r.deadline not in ("", "없음"), r.deadline or "", r.title))
    return active, closed
