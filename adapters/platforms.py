from __future__ import annotations

import re
from urllib.parse import quote_plus

from adapters.playwright_utils import USER_AGENT, build_record_from_detail, collect_candidate_links, extract_job_id_from_url, safe_goto
from adapters.base import BaseAdapter
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import requests

from core.utils import clean_text, is_phd_preferred

COMMON_TERMS = "반도체 공정 석사 박사 field application engineer process engineer metrology deposition lithography"
TARGET_COMPANIES = [
    "삼성전자DS", "SK하이닉스", "ASML", "Applied Materials", "KLA", "Lam Research", "TEL", "Micron", "ASM", "TSMC", "NVIDIA", "AMD"
]
ALIASES = {
    "삼성전자DS": ["삼성전자", "samsung"],
    "SK하이닉스": ["sk hynix", "sk하이닉스", "하이닉스"],
    "ASML": ["asml"],
    "Applied Materials": ["applied materials", "applied", "어플라이드머티어리얼즈"],
    "KLA": ["kla"],
    "Lam Research": ["lam research", "lam"],
    "TEL": ["tokyo electron", "tel", "도쿄일렉트론"],
    "Micron": ["micron"],
    "ASM": ["asm"],
    "TSMC": ["tsmc"],
    "NVIDIA": ["nvidia"],
    "AMD": ["amd"],
}


class SearchPlatformAdapter(BaseAdapter):
    max_details_per_company = 8

    def _search_url(self, company_name: str) -> str:
        query = f"{company_name} {COMMON_TERMS}"
        base = self.source_cfg.url
        if "saramin" in base:
            return f"{base}?searchword={quote_plus(query)}"
        if "jobkorea" in base:
            return f"https://www.jobkorea.co.kr/Search/?stext={quote_plus(query)}"
        if "linkedin" in base:
            return f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(query)}"
        if "jobda" in base:
            return f"{base}"
        return base

    def _infer_company(self, text: str) -> str | None:
        t = text.lower()
        for company, aliases in ALIASES.items():
            if any(alias.lower() in t for alias in aliases):
                return company
        return None

    def fetch(self):
        records = []
        headers = {"User-Agent": USER_AGENT}
        platform_region = self.source_cfg.meta.get("platform_region", self.source_cfg.region)

        for target_company in TARGET_COMPANIES:
            search_url = self._search_url(target_company)
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(user_agent=USER_AGENT)
                    safe_goto(page, search_url)
                    page.wait_for_timeout(3000)
                    candidates = collect_candidate_links(page, page.url, limit=25)
                    browser.close()
            except Exception:
                continue

            kept = 0
            for text, url in candidates:
                if kept >= self.max_details_per_company:
                    break
                try:
                    r = requests.get(url, headers=headers, timeout=30)
                    r.raise_for_status()
                except Exception:
                    continue
                soup = BeautifulSoup(r.text, "lxml")
                raw = clean_text(soup.get_text(" ", strip=True))
                title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else text)
                company = self._infer_company(f"{title} {raw} {url}") or target_company
                if company != target_company:
                    continue
                records.append(build_record_from_detail(
                    company=company,
                    region=platform_region,
                    source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                    title=title,
                    url=url,
                    raw_text=raw,
                    phd_preferred=is_phd_preferred(raw),
                    job_id=extract_job_id_from_url(url),
                ))
                kept += 1
        return records
