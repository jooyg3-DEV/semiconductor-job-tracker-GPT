from __future__ import annotations

from urllib.parse import quote_plus

from adapters.playwright_utils import USER_AGENT, build_record_from_detail, collect_candidate_links, extract_job_id_from_url, parse_jobposting_json_ld, safe_goto
from adapters.base import BaseAdapter
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import requests

from core.utils import clean_text, is_phd_preferred

SEARCH_TERMS = "반도체 공정 석사 박사 field application engineer process engineer metrology deposition lithography"


class SearchPlatformAdapter(BaseAdapter):
    max_details = 20

    def _search_url(self) -> str:
        base = self.source_cfg.url
        if "saramin" in base:
            return f"{base}?searchword={quote_plus(SEARCH_TERMS)}"
        if "jobkorea" in base:
            return f"https://www.jobkorea.co.kr/Search/?stext={quote_plus(SEARCH_TERMS)}"
        if "linkedin" in base:
            return f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(SEARCH_TERMS)}"
        return base

    def fetch(self):
        records = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self._search_url())
            page.wait_for_timeout(4000)
            candidates = collect_candidate_links(page, page.url, limit=60)
            browser.close()
        headers = {"User-Agent": USER_AGENT}
        for text, url in candidates[: self.max_details]:
            try:
                r = requests.get(url, headers=headers, timeout=30)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else text)
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                phd_preferred=is_phd_preferred(raw),
                job_id=extract_job_id_from_url(url),
            ))
        return records
