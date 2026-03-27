
from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, urlparse, urlencode

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from core.models import JobRecord
from core.utils import clean_text, extract_education_and_experience, infer_job_function, is_phd_preferred, parse_json_ld


class TSMCAdapter(BaseAdapter):
    def fetch(self) -> list[JobRecord]:
        job_links: list[str] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(self.source_cfg.url, wait_until="domcontentloaded", timeout=120000)

            # 링크 패턴: JobDetail?jobId=...
            for _ in range(3):
                page.wait_for_timeout(1500)
                for a in page.locator("a.link").all():
                    try:
                        href = a.get_attribute("href") or ""
                    except Exception:
                        continue
                    if "JobDetail" in href:
                        job_links.append(href)
                # 다음 페이지 시도
                next_button = page.locator("a:has-text('Next'), button:has-text('Next')")
                if next_button.count() == 0:
                    break
                try:
                    next_button.first.click()
                except Exception:
                    break
            browser.close()

        dedup_links = []
        seen = set()
        for href in job_links:
            if href not in seen:
                seen.add(href)
                dedup_links.append(href)

        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            for href in dedup_links[:100]:
                page.goto(href, wait_until="domcontentloaded", timeout=120000)
                soup = BeautifulSoup(page.content(), "lxml")
                jsonlds = parse_json_ld(soup)
                payload = {}
                for item in jsonlds:
                    if item.get("@type") == "JobPosting":
                        payload = item
                        break
                if not payload:
                    raw = clean_text(soup.get_text(" ", strip=True))
                    title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
                    job_id = parse_qs(urlparse(href).query).get("jobId", [""])[0]
                    records.append(JobRecord(
                        company=self.company_cfg.name,
                        region=self.source_cfg.region,
                        source=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                        title=title or "TSMC Job",
                        url=href,
                        deadline="없음",
                        qualification=extract_education_and_experience(raw),
                        job_function=infer_job_function(title, raw),
                        location="",
                        employment_type="",
                        phd_preferred=is_phd_preferred(raw),
                        job_id=job_id,
                        raw_text=raw,
                    ))
                    continue

                title = clean_text(payload.get("title"))
                job_id = str(((payload.get("identifier") or {}).get("value")) or parse_qs(urlparse(href).query).get("jobId", [""])[0])
                employment_type = clean_text(payload.get("employmentType"))
                location = clean_text((((payload.get("jobLocation") or {}).get("address") or {}).get("streetAddress")))
                qualification_text = clean_text(payload.get("educationRequirements")) + " / " + clean_text(payload.get("experienceRequirements"))
                raw = clean_text(" ".join([
                    title,
                    clean_text(payload.get("qualifications")),
                    clean_text(payload.get("responsibilities")),
                    clean_text(payload.get("skills")),
                    qualification_text,
                    location,
                ]))
                records.append(JobRecord(
                    company=self.company_cfg.name,
                    region=self.source_cfg.region,
                    source=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                    title=title,
                    url=href,
                    deadline="없음",
                    qualification=extract_education_and_experience(qualification_text or raw),
                    job_function=infer_job_function(title, raw),
                    location=location,
                    employment_type=employment_type,
                    phd_preferred=is_phd_preferred(raw),
                    job_id=job_id,
                    raw_text=raw,
                ))
            browser.close()
        return records
