
from __future__ import annotations

from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from core.models import JobRecord
from core.utils import clean_text, extract_education_and_experience, infer_job_function, is_phd_preferred


class GenericPlaywrightAdapter(BaseAdapter):
    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(self.source_cfg.url, wait_until="domcontentloaded", timeout=90000)

            links = []
            for locator in page.locator("a").all():
                try:
                    href = locator.get_attribute("href") or ""
                    text = clean_text(locator.inner_text())
                except Exception:
                    continue
                if href and text and ("job" in href.lower() or "career" in href.lower()):
                    links.append((text, href))
            browser.close()

        for text, href in links[:50]:
            raw = text
            records.append(JobRecord(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=text,
                url=href,
                deadline="없음",
                qualification=extract_education_and_experience(raw),
                job_function=infer_job_function(text, raw),
                location="",
                employment_type="",
                phd_preferred=is_phd_preferred(raw),
                raw_text=raw,
            ))
        return records
