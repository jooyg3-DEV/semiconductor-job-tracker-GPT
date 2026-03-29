from __future__ import annotations

import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, safe_goto
from core.utils import clean_text


class SamsungDSAdapter(BaseAdapter):
    def _collect_from_html(self, html: str) -> list[str]:
        urls = []
        for m in re.finditer(r'https://www\.samsungcareers\.com/hr/\?no=\d+', html):
            urls.append(m.group(0))
        for m in re.finditer(r'(/hr/\?no=\d+)', html):
            urls.append(f"https://www.samsungcareers.com{m.group(1)}")
        return urls

    def fetch(self):
        seen: set[str] = set()
        candidates: list[tuple[str, str]] = []

        # requests first: often page embeds links in raw HTML/scripts
        try:
            r = requests.get(self.source_cfg.url, timeout=45, headers={"User-Agent": USER_AGENT})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("a[href*='?no='], a[href*='/hr/?no=']"):
                href = a.get('href') or ''
                if href.startswith('/'):
                    href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                if href and href not in seen:
                    seen.add(href)
                    candidates.append((clean_text(a.get_text(' ', strip=True)), href))
            for href in self._collect_from_html(r.text):
                if href not in seen:
                    seen.add(href)
                    candidates.append(("", href))
        except Exception:
            pass

        # playwright fallback
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=USER_AGENT)
                safe_goto(page, self.source_cfg.url)
                page.wait_for_timeout(3500)
                html = page.content()
                for href in self._collect_from_html(html):
                    if href not in seen:
                        seen.add(href)
                        candidates.append(("", href))
                loc = page.locator("a[href*='?no='], a[href*='/hr/?no=']")
                for i in range(min(loc.count(), 120)):
                    a = loc.nth(i)
                    try:
                        href = a.get_attribute('href') or ''
                        text = clean_text(a.inner_text())
                    except Exception:
                        continue
                    if href.startswith('/'):
                        href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href and href not in seen:
                        seen.add(href)
                        candidates.append((text, href))
                browser.close()
        except Exception:
            pass

        headers = {"User-Agent": USER_AGENT}
        records = []
        for text, url in candidates[:50]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            if "DS부문" not in raw and not any(token in raw for token in ["메모리", "반도체", "파운드리", "Semiconductor"]):
                continue
            title = (soup.find("h1") or soup.find("title"))
            title_text = clean_text(title.get_text(" ", strip=True) if title else text)
            m = re.search(r"no=(\d+)", url)
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title_text or text or "삼성전자 DS 채용공고",
                url=url,
                raw_text=raw,
                job_id=m.group(1) if m else "",
            ))
        return records
