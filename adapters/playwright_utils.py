from __future__ import annotations

import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import Page

from core.models import JobRecord
from core.utils import (
    clean_text,
    extract_education_and_experience,
    infer_job_function,
    infer_location_from_text,
    infer_recruitment_type,
    normalize_employment_type,
    normalize_location,
    parse_json_ld,
)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"


def safe_goto(page: Page, url: str, timeout_ms: int = 120000) -> None:
    last_err = None
    for wait_until in ["domcontentloaded", "load", "commit"]:
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            page.wait_for_timeout(1500)
            return
        except Exception as exc:
            last_err = exc
    if last_err:
        raise last_err


def build_record_from_detail(
    *,
    company: str,
    region: str,
    source_label: str,
    title: str,
    url: str,
    raw_text: str,
    deadline: str = "없음",
    location: str = "",
    employment_type: str = "",
    qualification: str = "",
    job_function: str = "",
    recruitment_type: str = "",
    job_id: str = "",
) -> JobRecord:
    return JobRecord(
        company=company,
        region=region,
        source=source_label,
        title=clean_text(title),
        url=url,
        deadline=clean_text(deadline) or "없음",
        qualification=qualification or extract_education_and_experience(raw_text),
        job_function=job_function or infer_job_function(title, raw_text),
        location=normalize_location(location or infer_location_from_text(title, raw_text)),
        employment_type=normalize_employment_type(employment_type),
        recruitment_type=recruitment_type or infer_recruitment_type(title, raw_text, deadline),
        job_id=job_id,
        raw_text=clean_text(raw_text),
    )


def parse_jobposting_json_ld(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    for item in parse_json_ld(soup):
        if (item.get("@type") or "").lower() == "jobposting":
            return item
    return {}


def extract_job_id_from_url(url: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if qs.get("jobId"):
        return qs["jobId"][0]
    m = re.search(r"(?:jobId=|/jobs?/|/jobdetail/|j)(\d{4,})", url, flags=re.I)
    return m.group(1) if m else ""


def absolutize(base_url: str, href: str) -> str:
    if href.startswith("http"):
        return href
    return urljoin(base_url, href)


def collect_candidate_links(page: Page, base_url: str, limit: int = 80) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    selectors = [
        "a[href*='job']",
        "a[href*='career']",
        "a[href*='recruit']",
        "a[href*='opening']",
        "a[href*='position']",
        "a.link",
        "a[data-automation-id*='job']",
        "a[data-ph-at-id*='job']",
    ]
    for sel in selectors:
        loc = page.locator(sel)
        count = min(loc.count(), 200)
        for i in range(count):
            node = loc.nth(i)
            try:
                href = node.get_attribute("href") or ""
                text = clean_text(node.inner_text())
            except Exception:
                continue
            if not href:
                continue
            full = absolutize(base_url, href)
            if full in seen:
                continue
            if not text:
                text = full.rsplit("/", 1)[-1]
            seen.add(full)
            candidates.append((text, full))
            if len(candidates) >= limit:
                return candidates
    return candidates
