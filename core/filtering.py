from __future__ import annotations

from collections import Counter, defaultdict

from core.models import JobRecord
from core.search_plan import get_search_plan
from core.utils import contains_term, find_matches, has_experience, has_masters, has_phd, normalize_text_for_match, shorten_cell, summarize_requirements

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
COMPANY_POSITIVE_OVERRIDES = {
    "ASML": ["applications engineer", "application engineer", "customer support engineer", "field service", "metrology"],
    "Applied Materials": ["process support engineer", "application engineer", "customer engineer", "field application engineer"],
    "Lam Research": ["process engineer", "field process", "customer support engineer", "etch", "deposition"],
    "KLA": ["application engineer", "metrology", "yield", "inspection"],
    "TEL": ["process engineer", "application engineer", "customer engineer", "etch", "deposition"],
    "Micron": ["yield", "integration", "process engineer", "manufacturing engineer"],
    "ASM": ["process engineer", "application engineer", "deposition"],
    "TSMC": ["process engineer", "module engineer", "integration", "yield"],
    "NVIDIA": ["process engineer", "packaging", "yield", "integration"],
    "AMD": ["process engineer", "packaging", "yield", "integration"],
}
COMPANY_NEGATIVE_OVERRIDES = {
    "ASML": ["software engineer", "cloud", "security"],
    "Applied Materials": ["software engineer", "supply chain"],
    "Lam Research": ["software engineer", "supply chain", "procurement"],
    "KLA": ["software engineer", "digital design", "finance"],
    "TEL": ["procurement", "sales", "marketing"],
    "Micron": ["data scientist", "software engineer", "finance"],
    "ASM": ["software engineer", "it", "finance"],
    "TSMC": ["software engineer", "digital design", "accounting"],
    "NVIDIA": ["software engineer", "asic", "rtl", "verification"],
    "AMD": ["software engineer", "asic", "rtl", "verification"],
}


def _job_corpus(record: JobRecord, source_type: str) -> str:
    parts = [
        record.title or "",
        record.job_function or "",
        record.qualification or "",
        record.metadata.get("summary", ""),
        record.recruitment_type or "",
        record.location or "",
    ]
    if source_type == "platform":
        parts.append(record.raw_text or "")
    return normalize_text_for_match(" ".join(parts))


def _first_search_keyword(company_name: str, text: str) -> tuple[str, int]:
    for idx, keyword in enumerate(get_search_plan(company_name)):
        if contains_term(text, keyword):
            return keyword, idx
    return "", 999


def evaluate_record(company_name: str, record: JobRecord, source_type: str, include_keywords: list[str], exclude_keywords: list[str]):
    include_keywords_l = [k.lower() for k in include_keywords if k]
    exclude_keywords_l = [k.lower() for k in exclude_keywords if k]
    title_job_text = normalize_text_for_match(f"{record.title} {record.job_function}")
    corpus = _job_corpus(record, source_type)
    special_recruitment = (record.recruitment_type or "") in ALLOWED_SPECIAL_RECRUITMENT

    title_strong_hits = find_matches(title_job_text, STRONG_ROLE_PATTERNS)
    corpus_strong_hits = [t for t in find_matches(corpus, STRONG_ROLE_PATTERNS) if t not in title_strong_hits]
    include_matches = [k for k in include_keywords_l if contains_term(title_job_text, k) or contains_term(corpus, k)]
    exclude_matches = [k for k in exclude_keywords_l if contains_term(title_job_text, k) or contains_term(corpus, k)]
    hard_excludes = [tok for tok in HARD_EXCLUDE_TERMS if contains_term(title_job_text, tok)]
    company_positive = find_matches(corpus, COMPANY_POSITIVE_OVERRIDES.get(company_name, []))
    company_negative = find_matches(corpus, COMPANY_NEGATIVE_OVERRIDES.get(company_name, []))
    matched_keyword, keyword_rank = _first_search_keyword(company_name, f"{title_job_text} {corpus}")

    score = 0
    score += min(len(title_strong_hits), 3) * 4
    score += min(len(corpus_strong_hits), 3) * 2
    score += min(len(include_matches), 3) * 2
    score += min(len(company_positive), 2) * 2
    score -= min(len(exclude_matches), 3) * 2
    score -= min(len(company_negative), 2) * 2
    score -= min(len(hard_excludes), 2) * 4
    if matched_keyword:
        score += 2
    if special_recruitment:
        score += 1

    meta = {
        "keyword": matched_keyword,
        "keyword_rank": keyword_rank,
        "score": score,
        "title_strong_hits": title_strong_hits,
        "corpus_strong_hits": corpus_strong_hits,
        "company_positive": company_positive,
        "company_negative": company_negative,
    }

    if hard_excludes:
        return False, "hard_exclude_title", include_matches, exclude_matches, hard_excludes, meta

    positive_signal = bool(title_strong_hits or include_matches or company_positive or matched_keyword)
    if not positive_signal and not special_recruitment:
        return False, "no_role_signal", include_matches, exclude_matches, hard_excludes, meta

    threshold = 2 if source_type == "official" else 3
    if score < threshold and not (special_recruitment and positive_signal):
        return False, "low_score", include_matches, exclude_matches, hard_excludes, meta

    if exclude_matches and score < threshold + 2 and not (special_recruitment and positive_signal):
        return False, "exclude_keyword", include_matches, exclude_matches, hard_excludes, meta

    return True, "accepted", include_matches, exclude_matches, hard_excludes, meta


def filter_records(company_name: str, source_name: str, source_type: str, records: list[JobRecord], include_keywords: list[str], exclude_keywords: list[str]) -> list[JobRecord]:
    out: list[JobRecord] = []
    stats = Counter()
    keyword_buckets: defaultdict[int, list[JobRecord]] = defaultdict(list)
    for record in records:
        if not (record.title and record.url):
            stats["missing_title_or_url"] += 1
            continue
        accepted, reason, include_matches, exclude_matches, hard_excludes, meta = evaluate_record(company_name, record, source_type, include_keywords, exclude_keywords)
        record.metadata["filter_reason"] = reason
        record.metadata["include_matches"] = include_matches
        record.metadata["exclude_matches"] = exclude_matches
        record.metadata["hard_excludes"] = hard_excludes
        record.metadata["matched_search_keyword"] = meta["keyword"]
        record.metadata["matched_search_rank"] = meta["keyword_rank"]
        record.metadata["score"] = meta["score"]
        record.metadata["title_strong_hits"] = meta["title_strong_hits"]
        record.metadata["corpus_strong_hits"] = meta["corpus_strong_hits"]
        record.metadata["company_positive"] = meta["company_positive"]
        record.metadata["company_negative"] = meta["company_negative"]
        if not accepted:
            stats[reason] += 1
            continue
        summary = summarize_requirements(record.qualification, record.metadata.get("summary", ""), record.raw_text)
        record.qualification = shorten_cell(summary or record.qualification or "")
        text_for_flags = f"{record.qualification} {record.metadata.get('summary', '')} {record.raw_text}"
        record.experience_flag = has_experience(text_for_flags)
        record.masters_flag = has_masters(text_for_flags)
        record.phd_flag = has_phd(text_for_flags)
        keyword_buckets[int(record.metadata.get("matched_search_rank", 999))].append(record)
        stats["accepted"] += 1

    for rank in sorted(keyword_buckets):
        out.extend(keyword_buckets[rank])

    ordered_stats = ", ".join(f"{k}={v}" for k, v in sorted(stats.items())) if stats else "none"
    print(f"[INFO] filter summary {company_name}/{source_name}: {ordered_stats}")
    return out
