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
    "semiconductor",
    "etch",
    "thin film",
    "unit process",
    "module engineer",
    "반도체",
    "공정",
]
HARD_EXCLUDE_TERMS = [
    "software", "firmware", "it", "information technology", "cloud", "cybersecurity", "security engineer",
    "machine learning", "data engineer", "data scientist", "circuit", "circuit design", "analog", "digital",
    "mixed-signal", "rtl", "verification", "design verification", "physical design", "layout", "asic", "soc", "fpga",
    "finance", "accounting", "hr", "human resources", "legal", "procurement", "buyer", "logistics",
    "supply chain", "sales", "marketing", "facility", "facilities", "site operations", "plant", "maintenance",
    "utility", "utilities", "ehs", "environmental health safety", "factory operations",
]
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


def evaluate_record(record: JobRecord, source_type: str, include_keywords: list[str], exclude_keywords: list[str]):
    include_keywords_l = [k.lower() for k in include_keywords if k]
    exclude_keywords_l = [k.lower() for k in exclude_keywords if k]
    title_job_l = f"{record.title} {record.job_function}".lower()
    corpus = _job_corpus(record, source_type)
    special_recruitment = (record.recruitment_type or "") in ALLOWED_SPECIAL_RECRUITMENT

    hard_excludes = [tok for tok in HARD_EXCLUDE_TERMS if tok in title_job_l]
    if hard_excludes and not special_recruitment:
        return False, "hard_exclude_title", [], [], hard_excludes

    strong_role_hit = any(p in title_job_l for p in STRONG_ROLE_PATTERNS) or any(p in corpus for p in STRONG_ROLE_PATTERNS)
    include_matches = [k for k in include_keywords_l if k in title_job_l or k in corpus]
    exclude_matches = [k for k in exclude_keywords_l if k in title_job_l or k in corpus]

    if not strong_role_hit and not include_matches and not special_recruitment:
        return False, "no_role_signal", include_matches, exclude_matches, hard_excludes
    if exclude_matches and not strong_role_hit and len(include_matches) < 2 and not special_recruitment:
        return False, "exclude_keyword", include_matches, exclude_matches, hard_excludes
    return True, "accepted", include_matches, exclude_matches, hard_excludes


def filter_records(company_name: str, source_name: str, source_type: str, records: list[JobRecord], include_keywords: list[str], exclude_keywords: list[str]) -> list[JobRecord]:
    out: list[JobRecord] = []
    for record in records:
        if not (record.title and record.url):
            continue
        accepted, reason, include_matches, exclude_matches, hard_excludes = evaluate_record(record, source_type, include_keywords, exclude_keywords)
        record.metadata["filter_reason"] = reason
        record.metadata["include_matches"] = include_matches
        record.metadata["exclude_matches"] = exclude_matches
        record.metadata["hard_excludes"] = hard_excludes
        if not accepted:
            continue
        summary = summarize_requirements(record.qualification, record.metadata.get("summary", ""), record.raw_text)
        record.qualification = shorten_cell(summary or record.qualification or "")
        text_for_flags = f"{record.qualification} {record.metadata.get('summary', '')} {record.raw_text}"
        record.experience_flag = has_experience(text_for_flags)
        record.masters_flag = has_masters(text_for_flags)
        record.phd_flag = has_phd(text_for_flags)
        out.append(record)

    return out
