from __future__ import annotations

import requests
from bs4 import BeautifulSoup

from adapters.base import BaseAdapter


class SKHynixAdapter(BaseAdapter):
    def fetch(self):
        r = requests.get(self.source_cfg.url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(" ", strip=True)
        if "No job opening is found" in text or "진행중인 채용 공고가 없습니다" in text:
            return []
        # 현재 페이지 구조는 동적이며 본 프로젝트 초기 버전에서는 공고 없음 상태만 안정적으로 처리.
        return []
