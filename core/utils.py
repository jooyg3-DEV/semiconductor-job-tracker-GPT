from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup


KOREA_TOKENS = [
    "korea", "대한민국", "한국", "seoul", "suwon", "hwaseong", "icheon", "cheonan", "daejeon", "gumi", "pyeongtaek",
]
EXPERIENCE_TOKENS = [
    "experience", "experienced", "경력", "신입", "경력무관", "years", "year", "5+", "3+", "2+",
]
MASTERS_TOKENS = ["master", "master's", "masters", "msc", "m.s.", "석사"]
PHD_TOKENS = ["phd", "ph.d", "doctorate", "박사"]


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def join_nonempty(*parts: str) -> str:
    return " | ".join([p for p in [clean_text(p) for p in parts] if p])


def parse_json_ld(soup: BeautifulSoup) -> list[dict[str, Any]]:
    results = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.get_text(strip=True))
        except Exception:
            continue
        if isinstance(data, dict):
            results.append(data)
        elif isinstance(data, list):
            results.extend([x for x in data if isinstance(x, dict)])
    return results


def to_deadline_sort_key(deadline: str) -> tuple[int, str]:
    if not deadline or deadline == "없음":
        return (0, "")
    return (1, deadline)


def detect_flag(text: str, tokens: list[str]) -> str:
    t = (text or "").lower()
    return "Y" if any(token.lower() in t for token in tokens) else "N"


def is_phd_preferred(text: str) -> str:
    return detect_flag(text, PHD_TOKENS)


def has_phd(text: str) -> str:
    return detect_flag(text, PHD_TOKENS)


def has_masters(text: str) -> str:
    return detect_flag(text, MASTERS_TOKENS)


def has_experience(text: str) -> str:
    return detect_flag(text, EXPERIENCE_TOKENS)


def summarize_requirements(*texts: str, limit: int = 140) -> str:
    source = clean_text(" ".join([t for t in texts if t]))
    if not source:
        return ""
    patterns = [
        r"(신입[^.;\n]{0,40}|경력무관[^.;\n]{0,40}|경력\s*\d+년[^.;\n]{0,40}|\d+\+?\s*years?[^.;\n]{0,40})",
        r"(석사[^.;\n]{0,40}|박사[^.;\n]{0,40}|master[^.;\n]{0,40}|ph\.?d[^.;\n]{0,40})",
        r"(preferred qualifications?[^.;\n]{0,80}|minimum qualifications?[^.;\n]{0,80})",
    ]
    hits = []
    low = source.lower()
    for pat in patterns:
        for m in re.finditer(pat, low, flags=re.I):
            hits.append(clean_text(source[m.start():m.end()]))
    if not hits:
        hits = [source[:limit]]
    dedup = []
    for h in hits:
        if h and h not in dedup:
            dedup.append(h)
    result = ", ".join(dedup)[:limit]
    return result


def extract_education_and_experience(text: str) -> str:
    return summarize_requirements(text)


def infer_job_function(title: str, raw_text: str) -> str:
    text = f"{title} {raw_text}".lower()
    mapping = [
        ("field application engineer", "Field Application Engineer"),
        ("application engineer", "Application Engineer"),
        ("process support engineer", "Process Support Engineer"),
        ("process engineer", "Process Engineer"),
        ("customer engineer", "Customer Engineer"),
        ("metrology", "Metrology"),
        ("deposition", "Deposition"),
        ("lithography", "Lithography"),
        ("packaging", "Packaging"),
        ("yield", "Yield"),
        ("integration", "Integration"),
        ("반도체", "반도체"),
        ("공정", "공정"),
    ]
    for needle, label in mapping:
        if needle in text:
            return label
    return clean_text(title)


def infer_region_from_location(location: str, fallback_region: str) -> str:
    loc = (location or "").lower()
    if any(tok in loc for tok in KOREA_TOKENS):
        return "국내"
    return fallback_region or "글로벌"


def normalize_location(location: str) -> str:
    return clean_text(location).replace(" | ", ", ")


def normalize_employment_type(value: str) -> str:
    v = clean_text(value)
    mapping = {
        "full time": "Full-time",
        "full-time": "Full-time",
        "part time": "Part-time",
        "part-time": "Part-time",
        "intern": "Intern",
        "contract": "Contract",
        "regular": "Regular",
        "temporary": "Temporary",
    }
    low = v.lower()
    for k, out in mapping.items():
        if k in low:
            return out
    return v or "없음"


def deadline_passed_with_grace(deadline: str, today_str: str) -> bool:
    if not deadline or deadline == "없음":
        return False
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d"):
        try:
            dd = datetime.strptime(deadline[:10].replace(".", "-").replace("/", "-"), "%Y-%m-%d")
            today = datetime.strptime(today_str, "%Y-%m-%d")
            return today > dd + timedelta(days=1)
        except Exception:
            continue
    return False


def explicit_company_match(text: str, aliases: list[str]) -> bool:
    t = (text or "").lower()
    return any(alias.lower() in t for alias in aliases)


def looks_like_listing_or_search_page(url: str, title: str, raw: str) -> bool:
    text = f"{url} {title} {raw}".lower()
    tokens = ["search", "recruitsearch", "listfiltermode", "sort=", "keyword=", "전체 공고", "채용공고 검색", "인기 top"]
    return any(tok in text for tok in tokens)


def shorten_cell(value: str, limit: int = 1000) -> str:
    v = clean_text(value)
    return v[:limit] if len(v) > limit else v
