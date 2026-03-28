from __future__ import annotations

import re
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
        seen = set()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(5000)
            for _ in range(8):
                html = page.content()
                patterns = [
                    r'https://careers\.tsmc\.com/en_US/careers/JobDetail\?jobId=\d+[^"\']*',
                    r'https://careers\.tsmc\.com/en_US/careers/JobDetail/[^"\']+',
                    r'/en_US/careers/JobDetail\?jobId=\d+[^"\']*',
                    r'/en_US/careers/JobDetail/[^"\']+',
                ]
                for pat in patterns:
                    for m in re.finditer(pat, html):
                        href = m.group(0)
                        full = urljoin(page.url, href)
                        if 'JobDetail' in full and full not in seen:
                            seen.add(full)
                            job_links.append(full)
                links = page.locator("a.link, a[href*='JobDetail']")
                for i in range(min(links.count(), 120)):
                    a = links.nth(i)
                    try:
                        href = a.get_attribute('href') or ''
                    except Exception:
                        continue
                    if not href:
                        continue
                    full = urljoin(page.url, href)
                    if 'JobDetail' in full and full not in seen:
                        seen.add(full)
                        job_links.append(full)
                next_button = page.locator("a[aria-label*='Next'], a:has-text('Next'), button:has-text('Next'), button[title*='Next']")
                if next_button.count() == 0:
                    break
                try:
                    next_button.first.click(timeout=5000)
                    page.wait_for_timeout(2500)
                except Exception:
                    break
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        records = []
        for url in job_links[:120]:
            html = ""
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
                html = r.text
            except Exception:
                try:
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=True)
                        page = browser.new_page(user_agent=USER_AGENT)
                        safe_goto(page, url)
                        page.wait_for_timeout(2000)
                        html = page.content()
                        browser.close()
                except Exception:
                    continue
            soup = BeautifulSoup(html, 'lxml')
            raw = clean_text(soup.get_text(' ', strip=True))
            data = parse_jobposting_json_ld(html)
            title = clean_text(data.get('title') or (soup.find('h1').get_text(' ', strip=True) if soup.find('h1') else ''))
            if not title:
                continue
            location = ''
            jl = data.get('jobLocation')
            if isinstance(jl, dict):
                location = clean_text((jl.get('address') or {}).get('streetAddress') or (jl.get('address') or {}).get('addressLocality'))
            employment = clean_text(data.get('employmentType') or '')
            qualification = clean_text(data.get('educationRequirements') or '')
            exp = clean_text(data.get('experienceRequirements') or '')
            qual = ' / '.join([x for x in [qualification, exp] if x])
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get('source_label', self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                location=location,
                employment_type=employment,
                qualification=qual,
                job_id=extract_job_id_from_url(url),
            ))
        return records
