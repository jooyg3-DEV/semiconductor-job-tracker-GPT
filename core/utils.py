
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any
from bs4 import BeautifulSoup


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


def is_phd_preferred(text: str) -> str:
    t = (text or "").lower()
    tokens = ["phd preferred", "ph.d.", "ph.d", "박사 우대", "박사학위 우대"]
    return "Y" if any(token.lower() in t for token in tokens) else "N"


def extract_education_and_experience(text: str) -> str:
    t = clean_text(text)
    edu_match = re.search(r"(master[^.;\n]*|ph\.?d[^.;\n]*|석사[^.;\n]*|박사[^.;\n]*)", t, flags=re.I)
    exp_match = re.search(r"((?:\d+\+?\s*years?|신입|경력 무관|experience[^.;\n]*|경력[^.;\n]*))", t, flags=re.I)
    parts = []
    if edu_match:
        parts.append(clean_text(edu_match.group(1)))
    if exp_match:
        parts.append(clean_text(exp_match.group(1)))
    return " / ".join(parts)


def infer_job_function(title: str, raw_text: str) -> str:
    text = f"{title} {raw_text}".lower()
    mapping = [
        ("field application engineer", "Field Application Engineer"),
        ("application engineer", "Application Engineer"),
        ("process support engineer", "Process Support Engineer"),
        ("process engineer", "Process Engineer"),
        ("metrology", "Metrology"),
        ("lithography", "Lithography"),
        ("deposition", "Deposition"),
        ("packaging", "Packaging"),
    ]
    for needle, label in mapping:
        if needle in text:
            return label
    return clean_text(title)


def deadline_passed_with_grace(deadline_str: str, today_str: str) -> bool:
    if not deadline_str or deadline_str == "없음":
        return False
    try:
        d = datetime.strptime(deadline_str, "%Y-%m-%d")
        today = datetime.strptime(today_str, "%Y-%m-%d")
    except ValueError:
        return False
    return today >= d + timedelta(days=1)


_KOREA_LOCATION_TERMS = [
    "korea", "대한민국", "한국", "seoul", "서울", "경기", "suwon", "수원",
    "hwaseong", "화성", "pyeongtaek", "평택", "icheon", "이천", "cheonan", "천안",
    "daejeon", "대전", "gumi", "구미", "osan", "오산", "gihung", "기흥",
    "yongin", "용인", "incheon", "인천", "pangyo", "판교", "청주", "cheongju",
]


def infer_region_from_location(location: str, fallback_region: str = "") -> str:
    text = (location or "").strip().lower()
    if text:
        if any(term in text for term in _KOREA_LOCATION_TERMS):
            return "국내"
        return "글로벌"
    return fallback_region or "글로벌"
