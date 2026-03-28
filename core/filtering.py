from __future__ import annotations

from core.models import JobRecord
from core.utils import (
    has_experience,
    has_masters,
    has_phd,
    summarize_requirements,
    shorten_cell,
)

STRONG_ROLE_PATTERNS = [
    "process engineer", "process support engineer", "process integration", "field application engineer",
    "application engineer", "applications development engineer", "customer engineer", "metrology",
    "deposition", "lithography", "packaging", "yield", "integration", "advanced packaging",
    "반도체", "공정",
]
TITLE_EXCLUDE_TERMS = ["software", "ai", "it", "cybersecurity", "logistics", "finance", "hr", "legal", "buyer", "cloud", "security"]

def _job_corpus(record: JobRecord, source_type: str) -> str:
    # avoid raw full blob for official pages
    base = " ".join([record.title or "", record.job_function or "", record.qualification or "", record.metadata.get("summary", "")])
    if source_type == "platform":
        base = " ".join([base, (record.raw_text or "")[:3000]])
    return base.lower()

def filter_records(company_name: str, source_name: str, source_type: str, records: list[JobRecord], include_keywords: list[str], exclude_keywords: list[str]) -> list[JobRecord]:
    out=[]
    include_keywords_l=[k.lower() for k in include_keywords if k]
    exclude_keywords_l=[k.lower() for k in exclude_keywords if k]
    for record in records:
        if not (record.title and record.url):
            continue
        title_l=(record.title or "").lower()
        title_job_l=f"{record.title} {record.job_function}".lower()
        corpus=_job_corpus(record, source_type)
        if any(tok in title_job_l for tok in TITLE_EXCLUDE_TERMS):
            continue
        strong_role_hit=any(p in title_job_l for p in STRONG_ROLE_PATTERNS) or any(p in corpus for p in STRONG_ROLE_PATTERNS)
        include_hits=sum(1 for k in include_keywords_l if k in title_job_l or k in corpus)
        exclude_hits=sum(1 for k in exclude_keywords_l if k in title_l or (source_type=='platform' and k in corpus))
        if not strong_role_hit and include_hits == 0:
            continue
        if exclude_hits > 0 and not strong_role_hit and include_hits < 2:
            continue
        summary=summarize_requirements(record.qualification, record.raw_text if source_type=='platform' else '', record.metadata.get('summary',''))
        record.qualification=shorten_cell(summary or record.qualification or '')
        flags_text=f"{record.qualification} {record.raw_text if source_type=='platform' else record.metadata.get('summary','')}"
        record.experience_flag=has_experience(flags_text)
        record.masters_flag=has_masters(flags_text)
        record.phd_flag=has_phd(flags_text)
        out.append(record)
    return out
