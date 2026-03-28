from __future__ import annotations

import re

from core.models import JobRecord


PHD_PATTERNS = ["phd preferred", "ph.d", "ph.d.", "박사 우대", "박사학위 우대"]
EDU_PATTERNS = ["master", "master's", "phd", "ph.d", "석사", "박사"]
STRONG_ROLE_PATTERNS = [
    "process engineer",
    "process integration",
    "advanced process engineering",
    "field application engineer",
    "application engineer",
    "applications development engineer",
    "product applications",
    "process support engineer",
    "customer engineer",
    "metrology",
    "deposition",
    "lithography",
    "packaging",
    "advanced packaging",
    "yield",
    "integration",
    "반도체",
    "공정",
]


def filter_records(
    company_name: str,
    source_name: str,
    records: list[JobRecord],
    include_keywords: list[str],
    exclude_keywords: list[str],
    education_rule: str,
) -> list[JobRecord]:
    out: list[JobRecord] = []
    include_keywords_l = [k.lower() for k in include_keywords if k]
    exclude_keywords_l = [k.lower() for k in exclude_keywords if k]

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
        strong_role_hit = any(p in corpus for p in STRONG_ROLE_PATTERNS)
        education_ok = any(token in corpus for token in EDU_PATTERNS)

        if include_hits == 0 and not strong_role_hit:
            continue
        if education_rule and not education_ok:
            continue
        if exclude_hits > 0 and not (include_hits >= 2 or strong_role_hit):
            continue

        # 최소 품질: 제목 또는 링크가 있어야 함
        if not (record.title and record.url):
            continue

        out.append(record)

    return out
