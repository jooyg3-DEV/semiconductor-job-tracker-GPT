from __future__ import annotations

import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, safe_goto


class SamsungDSAdapter(BaseAdapter):
    def fetch(self):
        candidates: list[tuple[str, str]] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(3000)
            sels = ["a[href*='?no=']", "a[href*='/hr/?no=']", "a[href*='notice']"]
            seen = set()
            for sel in sels:
                loc = page.locator(sel)
                for i in range(min(loc.count(), 100)):
                    a = loc.nth(i)
                    try:
                        href = a.get_attribute("href") or ""
                        text = a.inner_text().strip()
                    except Exception:
                        continue
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href in seen:
                        continue
                    seen.add(href)
                    candidates.append((text, href))
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        records = []
        for text, url in candidates[:30]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            raw = " ".join(soup.stripped_strings)
            if not any(token in raw for token in ["DS", "반도체", "메모리", "파운드리", "Semiconductor"]):
                continue
            title = (soup.find("h1") or soup.find("title"))
            title_text = title.get_text(" ", strip=True) if title else text
            m = re.search(r"no=(\d+)", url)
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title_text,
                url=url,
                raw_text=raw,
                job_id=m.group(1) if m else "",
            ))
        return records
