from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from bs4 import BeautifulSoup

KOREA_TOKENS = [
    "korea", "대한민국", "한국", "seoul", "suwon", "hwaseong", "icheon", "cheonan", "daejeon", "gumi", "pyeongtaek",
]
EXPERIENCE_TOKENS = [
    "experience", "experienced", "경력", "신입", "경력무관", "years", "year", "5+", "3+", "2+",
]
MASTERS_TOKENS = ["master", "master's", "masters", "msc", "m.s.", "석사"]
PHD_TOKENS = ["phd", "ph.d", "doctorate", "박사"]
TALENT_POOL_TOKENS = [
    "인재풀", "인재 등록", "talent pool", "talent community", "introduce yourself", "talent network", "join our talent network",
]
UNTIL_FILLED_TOKENS = ["채용시", "채용 시 마감", "until filled", "untilfilled"]
ROLLING_TOKENS = ["상시채용", "상시 모집", "수시 채용", "수시채용", "rolling", "rolling basis", "always hiring"]

ASCII_WORD_RE = re.compile(r"[a-z0-9]+")


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return " ".join(stringify(v) for v in value if stringify(v))
    if isinstance(value, dict):
        return " ".join(stringify(v) for v in value.values() if stringify(v))
    return str(value)


def clean_text(text: Any) -> str:
    if text is None:
        return ""
    s = stringify(text)
    return re.sub(r"\s+", " ", s).strip()


def normalize_text_for_match(text: Any) -> str:
    return clean_text(text).lower()


def tokenize_ascii(text: Any) -> list[str]:
    return ASCII_WORD_RE.findall(normalize_text_for_match(text))


def contains_term(text: Any, term: str) -> bool:
    src = normalize_text_for_match(text)
    needle = normalize_text_for_match(term)
    if not needle:
        return False
    if re.search(r"[가-힣]", needle):
        return needle in src
    if not re.search(r"[a-z0-9]", needle):
        return needle in src
    pattern = r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])"
    return re.search(pattern, src) is not None


def find_matches(text: Any, terms: list[str]) -> list[str]:
    out: list[str] = []
    for term in terms:
        if term and contains_term(text, term) and term not in out:
            out.append(term)
    return out


def join_nonempty(*parts: Any) -> str:
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


def detect_flag(text: Any, tokens: list[str]) -> str:
    t = clean_text(text).lower()
    return "Y" if any(token.lower() in t for token in tokens) else "N"


def has_phd(text: Any) -> str:
    return detect_flag(text, PHD_TOKENS)


def has_masters(text: Any) -> str:
    return detect_flag(text, MASTERS_TOKENS)


def has_experience(text: Any) -> str:
    return detect_flag(text, EXPERIENCE_TOKENS)


def infer_recruitment_type(title: Any, raw_text: Any, deadline: Any) -> str:
    text = clean_text(join_nonempty(title, raw_text, deadline)).lower()
    if any(tok in text for tok in [t.lower() for t in TALENT_POOL_TOKENS]):
        return "인재풀"
    if any(tok in text for tok in [t.lower() for t in UNTIL_FILLED_TOKENS]):
        return "채용시 마감"
    if any(tok in text for tok in [t.lower() for t in ROLLING_TOKENS]):
        return "상시"
    return "일반"


def summarize_requirements(*texts: Any, limit: int = 160) -> str:
    source = clean_text(" ".join(clean_text(t) for t in texts if clean_text(t)))
    if not source:
        return ""
    hits = []
    patterns = [
        r'(신입[^.;\n]{0,40}|경력무관[^.;\n]{0,40}|경력\s*\d+년[^.;\n]{0,40}|\d+\+?\s*years?[^.;\n]{0,40})',
        r'(석사[^.;\n]{0,40}|박사[^.;\n]{0,40}|master[^.;\n]{0,40}|ph\.?d[^.;\n]{0,40})',
        r'(preferred qualifications?[^.;\n]{0,80}|minimum qualifications?[^.;\n]{0,80})',
        r'(full[- ]time|part[- ]time|intern|contract|regular|temporary)',
        r'(상시채용[^.;\n]{0,20}|수시 채용[^.;\n]{0,20}|채용시[^.;\n]{0,20}|인재풀[^.;\n]{0,20})',
    ]
    low = source.lower()
    for pat in patterns:
        for m in re.finditer(pat, low, flags=re.I):
            hits.append(clean_text(source[m.start():m.end()]))
    if not hits:
        return source[:limit]
    dedup = []
    for h in hits:
        if h and h not in dedup:
            dedup.append(h)
    return ", ".join(dedup)[:limit]


def extract_education_and_experience(text: Any) -> str:
    return summarize_requirements(text)


def infer_job_function(title: Any, raw_text: Any) -> str:
    text = f"{clean_text(title)} {clean_text(raw_text)}".lower()
    mapping = [
        ("field application engineer", "Field Application Engineer"),
        ("application engineer", "Application Engineer"),
        ("process support engineer", "Process Support Engineer"),
        ("process integration", "Process Integration"),
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


def infer_region_from_location(location: Any, fallback_region: str) -> str:
    loc = clean_text(location).lower()
    if any(tok in loc for tok in KOREA_TOKENS):
        return "국내"
    return fallback_region or "글로벌"


def normalize_location(location: Any) -> str:
    return clean_text(location).replace(" | ", ", ")


def normalize_employment_type(value: Any) -> str:
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


def deadline_passed_with_grace(deadline: Any, today_str: str) -> bool:
    dd_text = clean_text(deadline)
    if not dd_text or dd_text == "없음":
        return False
    try:
        dd = datetime.strptime(dd_text[:10].replace(".", "-").replace("/", "-"), "%Y-%m-%d")
        today = datetime.strptime(today_str, "%Y-%m-%d")
        return today > dd + timedelta(days=1)
    except Exception:
        return False


def explicit_company_match(text: Any, aliases: list[str]) -> bool:
    t = clean_text(text).lower()
    return any(alias.lower() in t for alias in aliases)


def looks_like_listing_or_search_page(url: str, title: Any, raw: Any) -> bool:
    text = f"{url} {clean_text(title)} {clean_text(raw)}".lower()
    tokens = ["search", "recruitsearch", "listfiltermode", "sort=", "keyword=", "전체 공고", "채용공고 검색", "인기 top"]
    return any(tok in text for tok in tokens)


def shorten_cell(value: Any, limit: int = 1000) -> str:
    v = clean_text(value)
    return v[:limit] if len(v) > limit else v
