from __future__ import annotations

from core.models import JobRecord
from core.utils import has_experience, has_masters, has_phd, shorten_cell, summarize_requirements

STRONG_ROLE_PATTERNS = [
    "process engineer",
    "process support engineer",
    "process integration",
    "field application engineer",
    "application engineer",
    "customer engineer",
    "metrology",
    "deposition",
    "lithography",
    "packaging",
    "yield",
    "integration",
    "반도체",
    "공정",
]
TITLE_EXCLUDE_TERMS = ["software", "ai", "it", "cybersecurity", "logistics", "finance", "hr", "legal", "buyer", "cloud", "security"]
ALLOWED_SPECIAL_RECRUITMENT = {"인재풀", "상시", "채용시 마감"}


def _job_corpus(record: JobRecord, source_type: str) -> str:
    if source_type == "official":
        return " ".join([
            record.title or "",
            record.job_function or "",
            record.qualification or "",
            record.metadata.get("summary", ""),
            record.recruitment_type or "",
        ]).lower()
    return " ".join([
        record.title or "",
        record.job_function or "",
        record.qualification or "",
        record.metadata.get("summary", ""),
        record.recruitment_type or "",
    ]).lower()


def filter_records(company_name: str, source_name: str, source_type: str, records: list[JobRecord], include_keywords: list[str], exclude_keywords: list[str]) -> list[JobRecord]:
    out: list[JobRecord] = []
    include_keywords_l = [k.lower() for k in include_keywords if k]
    exclude_keywords_l = [k.lower() for k in exclude_keywords if k]

    for record in records:
        if not (record.title and record.url):
            continue

        title_l = (record.title or "").lower()
        title_job_l = f"{record.title} {record.job_function}".lower()
        corpus = _job_corpus(record, source_type)
        special_recruitment = (record.recruitment_type or "") in ALLOWED_SPECIAL_RECRUITMENT

        if any(tok in title_job_l for tok in TITLE_EXCLUDE_TERMS) and not special_recruitment:
            continue

        strong_role_hit = any(p in title_job_l for p in STRONG_ROLE_PATTERNS) or any(p in corpus for p in STRONG_ROLE_PATTERNS)
        include_hits = sum(1 for k in include_keywords_l if k in title_job_l or k in corpus)
        exclude_hits = sum(1 for k in exclude_keywords_l if k in corpus)

        if not strong_role_hit and include_hits == 0 and not special_recruitment:
            continue
        if exclude_hits > 0 and not strong_role_hit and include_hits < 2 and not special_recruitment:
            continue

        summary = summarize_requirements(record.qualification, record.metadata.get("summary", ""), record.raw_text)
        record.qualification = shorten_cell(summary or record.qualification or "")
        text_for_flags = f"{record.qualification} {record.metadata.get('summary', '')} {record.raw_text}"
        record.experience_flag = has_experience(text_for_flags)
        record.masters_flag = has_masters(text_for_flags)
        record.phd_flag = has_phd(text_for_flags)
        out.append(record)

    return out
