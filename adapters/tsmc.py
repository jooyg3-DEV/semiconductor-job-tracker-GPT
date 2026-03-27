from __future__ import annotations

from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, extract_job_id_from_url, parse_jobposting_json_ld, safe_goto
from core.utils import clean_text


class TSMCAdapter(BaseAdapter):
    def fetch(self):
        job_links: list[str] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(3500)

            seen = set()
            for _ in range(4):
                links = page.locator("a.link, a[href*='JobDetail']")
                for i in range(min(links.count(), 100)):
                    a = links.nth(i)
                    try:
                        href = a.get_attribute("href") or ""
                    except Exception:
                        continue
                    if not href:
                        continue
                    full = urljoin(page.url, href)
                    if "JobDetail" not in full or full in seen:
                        continue
                    seen.add(full)
                    job_links.append(full)
                next_button = page.locator("a:has-text('Next'), button:has-text('Next')")
                if next_button.count() == 0:
                    break
                try:
                    next_button.first.click(timeout=5000)
                    page.wait_for_timeout(2000)
                except Exception:
                    break
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        records = []
        for url in job_links[:40]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            html = r.text
            soup = BeautifulSoup(html, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            data = parse_jobposting_json_ld(html)
            title = clean_text(data.get("title") or (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else ""))
            location = ""
            jl = data.get("jobLocation")
            if isinstance(jl, dict):
                location = clean_text((jl.get("address") or {}).get("streetAddress") or (jl.get("address") or {}).get("addressLocality"))
            employment = clean_text(data.get("employmentType") or "")
            qualification = clean_text(data.get("educationRequirements"))
            exp = clean_text(data.get("experienceRequirements"))
            qual = " / ".join([x for x in [qualification, exp] if x])
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                location=location,
                employment_type=employment,
                qualification=qual,
                phd_preferred="Y" if any(token in raw.lower() for token in ["phd preferred", "ph.d", "ph.d.", "박사 우대"]) else "N",
                job_id=extract_job_id_from_url(url),
            ))
        return records
