
from __future__ import annotations

from core.models import JobRecord


def filter_records(
    company_name: str,
    source_name: str,
    records: list[JobRecord],
    include_keywords: list[str],
    exclude_keywords: list[str],
    education_rule: str,
) -> list[JobRecord]:
    out: list[JobRecord] = []
    include_keywords_l = [k.lower() for k in include_keywords]
    exclude_keywords_l = [k.lower() for k in exclude_keywords]

    for record in records:
        corpus = " ".join(
            [
                record.title or "",
                record.job_function or "",
                record.qualification or "",
                record.location or "",
                record.raw_text or "",
            ]
        ).lower()

        include_hits = sum(1 for k in include_keywords_l if k in corpus)
        exclude_hits = sum(1 for k in exclude_keywords_l if k in corpus)
        education_ok = any(token in corpus for token in ["master", "phd", "ph.d", "석사", "박사"])

        if include_hits == 0:
            continue
        if exclude_hits > 0 and include_hits < 2:
            continue
        if education_rule and not education_ok:
            continue

        out.append(record)

    return out
