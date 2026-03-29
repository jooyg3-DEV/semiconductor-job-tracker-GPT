from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

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

LOCATION_PATTERNS = [
    "hwaseong", "pyeongtaek", "icheon", "cheongju", "yongin", "suwon", "seoul", "daejeon",
    "shanghai", "wuhan", "beijing", "shenzhen", "hsinchu", "linkou", "tainan", "taichung",
    "singapore", "kitakami", "hiroshima", "phoenix", "austin", "boise", "korea", "taiwan",
    "japan", "china", "netherlands", "united states", "arizona", "oregon", "milpitas",
]

INVALID_ROW_REQUIRED_FIELDS = ("source", "title", "url", "company")
INTERNSHIP_TERMS = ["intern", "internship", "apprentice", "co-op", "co op", "thesis internship", "인턴"]
CORP_SUFFIX_PATTERNS = [
    r"\(주\)", r"주식회사", r"co\.?\s*,?\s*ltd\.?", r"ltd\.?", r"inc\.?", r"corp\.?", r"corporation", r"limited", r"gmbh", r"llc"
]


def canonicalize_job_url(url: Any) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    parsed = urlparse(raw)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if "saramin.co.kr" in netloc:
        qs = parse_qs(parsed.query)
        rec_idx = (qs.get("rec_idx") or [""])[0]
        if rec_idx:
            return f"{scheme}://{netloc}/zf_user/jobs/relay/view?rec_idx={rec_idx}"
    if "jobkorea.co.kr" in netloc:
        m = re.search(r"/Recruit/GI_Read/(\d+)", path, flags=re.I)
        if m:
            return f"{scheme}://{netloc}/Recruit/GI_Read/{m.group(1)}"
        gid = (parse_qs(parsed.query).get("GI_No") or [""])[0]
        if gid:
            return f"{scheme}://{netloc}/Recruit/GI_Read/{gid}"
    if "linkedin.com" in netloc:
        m = re.search(r"/jobs/view/(\d+)", path)
        if m:
            return f"{scheme}://{netloc}/jobs/view/{m.group(1)}"
    if any(host in netloc for host in ["asml.com", "appliedmaterials.com", "lamresearch.com", "careers.amd.com"]):
        return f"{scheme}://{netloc}{path}"
    if parsed.query:
        return f"{scheme}://{netloc}{path}"
    return raw.rstrip("/")


def infer_location_from_text(*texts: Any) -> str:
    corpus = normalize_text_for_match(" ".join(clean_text(t) for t in texts if clean_text(t)))
    if not corpus:
        return ""
    hits: list[str] = []
    for token in LOCATION_PATTERNS:
        if contains_term(corpus, token):
            hits.append(token)
    if not hits:
        return ""
    pretty = []
    for token in hits[:3]:
        pretty.append(token.title() if re.search(r"[a-z]", token) else token)
    return ", ".join(dict.fromkeys(pretty))


def extract_match_snippet(text: Any, term: str, window: int = 32) -> str:
    src = clean_text(text)
    if not src:
        return ""
    low = src.lower()
    needle = clean_text(term).lower()
    idx = low.find(needle)
    if idx < 0:
        return ""
    start = max(0, idx - window)
    end = min(len(src), idx + len(needle) + window)
    return src[start:end]


def internship_matches_by_field(title: Any = "", employment_type: Any = "", qualification: Any = "", summary: Any = "") -> list[dict[str, str]]:
    # Keep internship detection conservative to avoid false positives from noisy raw descriptions.
    # We only trust explicit internship signals in title and employment type.
    fields = {
        "title": title,
        "employment_type": employment_type,
    }
    matches: list[dict[str, str]] = []
    for field_name, value in fields.items():
        original = clean_text(value)
        normalized = normalize_text_for_match(original)
        if not normalized:
            continue
        for term in INTERNSHIP_TERMS:
            if contains_term(normalized, term):
                matches.append({
                    "field": field_name,
                    "term": term,
                    "snippet": extract_match_snippet(original, term) or original[:120],
                })
    deduped = []
    for item in matches:
        if item not in deduped:
            deduped.append(item)
    return deduped


def looks_like_internship(*texts: Any) -> bool:
    title = texts[0] if len(texts) > 0 else ""
    employment_type = texts[1] if len(texts) > 1 else ""
    qualification = texts[2] if len(texts) > 2 else ""
    summary = texts[3] if len(texts) > 3 else ""
    return bool(internship_matches_by_field(title, employment_type, qualification, summary))


def is_valid_record_payload(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    for field in INVALID_ROW_REQUIRED_FIELDS:
        if not clean_text(payload.get(field, "")):
            return False
    return True


def is_valid_record(record: Any) -> bool:
    if record is None:
        return False
    return all(clean_text(getattr(record, field, "")) for field in INVALID_ROW_REQUIRED_FIELDS)


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
    value = clean_text(location).replace(" | ", ", ")
    return value or "미기재"


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


def _normalize_company_text(text: Any) -> str:
    t = clean_text(text).lower()
    for pat in CORP_SUFFIX_PATTERNS:
        t = re.sub(pat, " ", t, flags=re.I)
    t = re.sub(r"[^a-z0-9가-힣]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def explicit_company_match(text: Any, aliases: list[str]) -> bool:
    raw = clean_text(text)
    if not raw:
        return False
    t = _normalize_company_text(raw)
    compact = t.replace(" ", "")
    for alias in aliases:
        alias_norm = _normalize_company_text(alias)
        if not alias_norm:
            continue
        if contains_term(t, alias_norm) or alias_norm in t or alias_norm.replace(" ", "") in compact:
            return True
    return False


def looks_like_listing_or_search_page(url: str, title: Any, raw: Any) -> bool:
    text = f"{url} {clean_text(title)} {clean_text(raw)}".lower()
    tokens = ["search", "recruitsearch", "listfiltermode", "sort=", "keyword=", "전체 공고", "채용공고 검색", "인기 top"]
    return any(tok in text for tok in tokens)


def shorten_cell(value: Any, limit: int = 1000) -> str:
    v = clean_text(value)
    return v[:limit] if len(v) > limit else v
