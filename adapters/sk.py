from __future__ import annotations

import requests
from bs4 import BeautifulSoup

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail
from core.utils import clean_text


class SKHynixAdapter(BaseAdapter):
    def fetch(self):
        session = requests.Session()
        headers = {"User-Agent": USER_AGENT}
        list_urls = [self.source_cfg.url, "https://www.skcareers.com/Recruit"]
        detail_urls: list[str] = []
        seen = set()
        for list_url in list_urls:
            try:
                r = session.get(list_url, timeout=45, headers=headers)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("a[href*='/Recruit/Detail/']"):
                href = a.get('href') or ''
                if not href:
                    continue
                if href.startswith('/'):
                    href = f"https://www.skcareers.com{href}"
                if href not in seen:
                    seen.add(href)
                    detail_urls.append(href)
        records = []
        for url in detail_urls[:60]:
            try:
                r = session.get(url, timeout=45, headers=headers)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, 'lxml')
            raw = clean_text(soup.get_text(' ', strip=True))
            if 'SK hynix' not in raw and 'SK하이닉스' not in raw:
                continue
            title = clean_text((soup.find('h1') or soup.find('title')).get_text(' ', strip=True))
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get('source_label', self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
            ))
        return records
