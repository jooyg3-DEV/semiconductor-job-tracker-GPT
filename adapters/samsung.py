
from __future__ import annotations

import re
import requests
from bs4 import BeautifulSoup

from adapters.base import BaseAdapter
from core.models import JobRecord
from core.utils import clean_text, extract_education_and_experience, infer_job_function, is_phd_preferred, join_nonempty


class SamsungDSAdapter(BaseAdapter):
    def fetch(self) -> list[JobRecord]:
        r = requests.get(self.source_cfg.url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        records: list[JobRecord] = []

        for a in soup.select('a[href*="?no="], a[href*="/hr/?no="]'):
            title = clean_text(a.get_text(" ", strip=True))
            href = a.get("href", "")
            if not title or not href:
                continue
            url = href if href.startswith("http") else f"https://www.samsungcareers.com{href}"
            parent = a.find_parent(["tr", "li", "div"])
            raw = clean_text(parent.get_text(" ", strip=True)) if parent else title
            if "DS" not in raw and "반도체" not in raw and "메모리" not in raw:
                continue
            job_id_match = re.search(r"no=(\d+)", url)
            records.append(JobRecord(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                deadline="없음",
                qualification=extract_education_and_experience(raw),
                job_function=infer_job_function(title, raw),
                location="",
                employment_type="",
                phd_preferred=is_phd_preferred(raw),
                job_id=job_id_match.group(1) if job_id_match else "",
                raw_text=raw,
            ))
        return records
