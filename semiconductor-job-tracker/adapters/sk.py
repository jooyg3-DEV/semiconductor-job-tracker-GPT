
from __future__ import annotations

import requests
from bs4 import BeautifulSoup

from adapters.base import BaseAdapter
from core.models import JobRecord


class SKHynixAdapter(BaseAdapter):
    def fetch(self) -> list[JobRecord]:
        r = requests.get(self.source_cfg.url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(" ", strip=True)
        if "No job opening is found" in text:
            return []
        return []
