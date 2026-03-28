from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from bs4 import BeautifulSoup

KOREA_TOKENS = [
    "korea", "대한민국", "한국", "seoul", "suwon", "hwaseong", "icheon", "cheonan", "daejeon", "gumi", "pyeongtaek",
    "yongin", "osan", "asan", "gyeonggi", "incheon", "부천", "평택", "수원", "화성", "이천", "천안", "대전"
]
EXPERIENCE_TOKENS = ["experience", "experienced", "경력", "신입", "경력무관", "years", "year", "5+", "3+", "2+", "entry level"]
MASTERS_TOKENS = ["master", "master's", "masters", "msc", "m.s.", "석사", "ms degree"]
PHD_TOKENS = ["phd", "ph.d", "doctorate", "doctoral", "박사"]

TALENT_POOL_TOKENS = ["talent community", "talent network", "introduce yourself", "인재 등록", "인재풀", "talent pool"]
UNTIL_FILLED_TOKENS = ["채용시", "채용 시 마감", "until filled", "open until filled"]
ONGOING_TOKENS = ["상시채용", "상시 모집", "수시 채용", "rolling basis", "ongoing"]

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
        return (1, "9999-99-99")
    return (0, deadline)

def detect_flag(text: str, tokens: list[str]) -> str:
    t = (text or "").lower()
    return "Y" if any(token.lower() in t for token in tokens) else "N"

def has_phd(text: str) -> str:
    return detect_flag(text, PHD_TOKENS)

def has_masters(text: str) -> str:
    return detect_flag(text, MASTERS_TOKENS)

def has_experience(text: str) -> str:
    return detect_flag(text, EXPERIENCE_TOKENS)

def summarize_requirements(*texts: str, limit: int = 160) -> str:
    source = clean_text(" ".join([t for t in texts if t]))
    if not source:
        return ""
    low = source.lower()
    parts = []
    # career
    m = re.search(r"(경력\s*무관|신입(?:\s*포함)?|경력\s*\d+년\s*이상|\d+\+?\s*years?(?:\s*of)?(?:\s*experience)?)", low, re.I)
    if m:
        parts.append(clean_text(source[m.start():m.end()]))
    # degree
    degs=[]
    if has_masters(source)=="Y":
        degs.append("석사")
    if has_phd(source)=="Y":
        degs.append("박사")
    if degs:
        parts.append("/".join(degs))
    # preference
    pref = []
    if 'preferred' in low or '우대' in source:
        if has_masters(source)=='Y': pref.append('석사 우대')
        if has_phd(source)=='Y': pref.append('박사 우대')
    parts.extend(pref)
    if not parts:
        snippets=[]
        for pat in [r"minimum qualifications?[^.;\n]{0,80}", r"preferred qualifications?[^.;\n]{0,80}", r"지원자격[^.;\n]{0,80}"]:
            for m in re.finditer(pat, low, re.I):
                snippets.append(clean_text(source[m.start():m.end()]))
        if snippets:
            parts.extend(snippets[:2])
        else:
            parts = [source[:limit]]
    out = ", ".join(dict.fromkeys([p for p in parts if p]))
    return out[:limit]

def extract_education_and_experience(text: str) -> str:
    return summarize_requirements(text)

def infer_job_function(title: str, raw_text: str) -> str:
    text = f"{title} {raw_text}".lower()
    mapping = [
        ("field application engineer", "Field Application Engineer"),
        ("application engineer", "Application Engineer"),
        ("applications development engineer", "Applications Development Engineer"),
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

def infer_region_from_location(location: str, fallback_region: str) -> str:
    loc = (location or "").lower()
    if any(tok in loc for tok in KOREA_TOKENS):
        return "국내"
    return fallback_region or "글로벌"

def normalize_location(location: str) -> str:
    v = clean_text(location).replace(" | ", ", ")
    return v or "없음"

def normalize_employment_type(value: str) -> str:
    v = clean_text(value)
    mapping = {"full time": "Full-time", "full-time": "Full-time", "part time": "Part-time", "part-time": "Part-time", "intern": "Intern", "contract": "Contract", "regular": "Regular", "temporary": "Temporary"}
    low = v.lower()
    for k, out in mapping.items():
        if k in low:
            return out
    return v or "없음"

def infer_recruitment_type(title: str, raw_text: str, deadline: str) -> str:
    text = f"{title} {raw_text} {deadline}".lower()
    if any(tok in text for tok in TALENT_POOL_TOKENS):
        return "인재풀"
    if any(tok in text for tok in UNTIL_FILLED_TOKENS):
        return "채용시 마감"
    if any(tok in text for tok in ONGOING_TOKENS):
        return "상시"
    return "일반"

def deadline_passed_with_grace(deadline: str, today_str: str) -> bool:
    if not deadline or deadline == "없음" or deadline in ("채용시", "채용 시 마감"):
        return False
    try:
        dd = datetime.strptime(deadline[:10].replace('.', '-').replace('/', '-'), '%Y-%m-%d')
        today = datetime.strptime(today_str, '%Y-%m-%d')
        return today > dd + timedelta(days=1)
    except Exception:
        return False

def explicit_company_match(text: str, aliases: list[str]) -> bool:
    t = (text or "").lower()
    return any(alias.lower() in t for alias in aliases)

def looks_like_listing_or_search_page(url: str, title: str, raw: str) -> bool:
    text = f"{url} {title} {raw}".lower()
    tokens = ["search", "recruitsearch", "listfiltermode", "sort=", "keyword=", "전체 공고", "채용공고 검색", "인기 top", "공고리스트를 불러오고 있습니다"]
    return any(tok in text for tok in tokens)

def shorten_cell(value: str, limit: int = 1000) -> str:
    v = clean_text(value)
    return v[:limit] if len(v) > limit else v
