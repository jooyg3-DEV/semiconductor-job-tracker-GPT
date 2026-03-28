
from __future__ import annotations

import re
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

from adapters.base import BaseAdapter
from core.models import JobRecord
from core.utils import clean_text, extract_education_and_experience, infer_job_function, normalize_location

SITEMAP_URL = "https://www.asml.com/en/job_posting-sitemap.xml"


class ASMLGlobalAdapter(BaseAdapter):
    def fetch(self) -> list[JobRecord]:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(SITEMAP_URL, headers=headers, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [
            loc.text.strip()
            for loc in root.findall(".//sm:url/sm:loc", ns)
            if loc.text and "/en/careers/find-your-job/" in loc.text
        ]
        records: list[JobRecord] = []
        for url in urls[:120]:
            try:
                rr = requests.get(url, headers=headers, timeout=60)
                rr.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(rr.text, "lxml")
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
            raw = clean_text(soup.get_text(" ", strip=True))
            if not title:
                continue
            location = ""
            m = re.search(r"Location\s+(.*?)\s+Team\b", raw, flags=re.I)
            if m:
                location = clean_text(m.group(1))
            deadline = "없음"
            records.append(JobRecord(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                deadline=deadline,
                qualification=extract_education_and_experience(raw),
                job_function=infer_job_function(title, raw),
                location=normalize_location(location),
                employment_type="없음",
                raw_text=raw,
            ))
        return records
