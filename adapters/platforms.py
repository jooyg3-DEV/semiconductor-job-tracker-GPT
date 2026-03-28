from __future__ import annotations

from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, collect_candidate_links, extract_job_id_from_url, safe_goto
from core.utils import clean_text, explicit_company_match, looks_like_listing_or_search_page

COMMON_TERMS = "반도체 공정 field application engineer process engineer process support engineer application engineer customer engineer metrology deposition lithography packaging yield integration"
TARGET_COMPANIES = ["삼성전자DS", "SK하이닉스", "ASML", "Applied Materials", "KLA", "Lam Research", "TEL", "Micron", "ASM", "TSMC", "NVIDIA", "AMD"]
ALIASES = {
    "삼성전자DS": ["삼성전자", "samsung", "device solutions", "ds division", "삼성 ds", "samsung electronics"],
    "SK하이닉스": ["sk hynix", "sk하이닉스", "하이닉스"],
    "ASML": ["asml"],
    "Applied Materials": ["applied materials", "어플라이드머티어리얼즈", "어플라이드 머티어리얼즈", "어플라이드머티어리얼즈코리아", "amat"],
    "KLA": ["kla", "kla corporation"],
    "Lam Research": ["lam research", "램리서치", "lamresearch"],
    "TEL": ["tokyo electron", "tel", "도쿄일렉트론", "tokyoelectron", "tokyo electron korea"],
    "Micron": ["micron", "마이크론"],
    "ASM": ["asm international", "asm", "에이에스엠"],
    "TSMC": ["tsmc", "taiwan semiconductor", "taiwan semiconductor manufacturing", "대만반도체"],
    "NVIDIA": ["nvidia", "엔비디아"],
    "AMD": ["amd", "advanced micro devices"],
}
PLATFORM_URL_REQUIRES = {
    "사람인": ["rec_idx=", "/jobs/relay/view", "/job-search/view"],
    "링크드인": ["/jobs/view/"],
    "잡코리아": ["/Recruit/GI_Read/", "/Recruit/GI_Read"],
    "링커리어": ["/activity/view", "/recruit/", "/jobs/"],
}
PLATFORM_PATH_REJECTS = {
    "사람인": ["/zf_user/search/recruit"],
    "잡코리아": ["/Search/", "/Recruit/Home"],
    "링크드인": ["/jobs/search/"],
    "링커리어": ["/search/", "?q="],
}
JOBISH_TERMS = [
    "채용", "공고", "recruit", "position", "role", "job", "지원자격", "responsibilities", "qualifications",
    "employment type", "minimum qualifications", "preferred qualifications", "full-time", "경력", "신입"
]


def _extract_candidates_from_html(base_url: str, html: str, limit: int = 80) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        text = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        full = urljoin(base_url, href)
        low = full.lower()
        if not any(tok in low for tok in ["job", "career", "recruit", "opening", "position", "view"]):
            continue
        if full in seen:
            continue
        seen.add(full)
        candidates.append((text or full.rsplit("/", 1)[-1], full))
        if len(candidates) >= limit:
            break
    return candidates


class SearchPlatformAdapter(BaseAdapter):
    def __init__(self, company_cfg, source_cfg):
        super().__init__(company_cfg, source_cfg)
        self.max_candidates = int(self.source_cfg.meta.get("max_candidates", 20))
        self.max_details_per_company = int(self.source_cfg.meta.get("max_details", 10))

    def _search_url(self, company_name: str) -> str:
        query = f"{company_name} {COMMON_TERMS}"
        platform = self.source_cfg.meta.get("source_label", self.source_cfg.name)
        base = self.source_cfg.url
        if platform == "사람인":
            return f"{base}?searchword={quote_plus(query)}"
        if platform == "잡코리아":
            return f"https://www.jobkorea.co.kr/Search/?stext={quote_plus(query)}"
        if platform == "링크드인":
            linkedin_query = company_name + " process engineer semiconductor metrology lithography deposition packaging field application engineer"
            return f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(linkedin_query)}"
        if platform == "링커리어":
            return f"https://linkareer.com/search?query={quote_plus(query)}"
        return base

    def _infer_company(self, text: str) -> str | None:
        t = text.lower()
        for company, aliases in ALIASES.items():
            if any(alias.lower() in t for alias in aliases):
                return company
        return None

    def _url_allowed(self, platform: str, url: str) -> bool:
        requires = PLATFORM_URL_REQUIRES.get(platform, [])
        return any(token in url for token in requires) if requires else True

    def _title_allowed(self, title: str) -> bool:
        tl = (title or "").lower()
        blocked = ["회원가입", "로그인", "검색", "전체 채용공고"]
        return not any(x in tl for x in blocked)

    def _reject_candidate(self, platform: str, url: str, title: str, raw: str, target_company: str) -> bool:
        if looks_like_listing_or_search_page(url, title, raw):
            return True
        if any(token in url for token in PLATFORM_PATH_REJECTS.get(platform, [])):
            return True
        if not self._url_allowed(platform, url):
            return True
        if not self._title_allowed(title):
            return True
        aliases = ALIASES[target_company]
        if not explicit_company_match(f"{title} {raw} {url}", aliases):
            return True
        raw_l = raw.lower()
        if not any(tok.lower() in raw_l or tok.lower() in (title or "").lower() for tok in JOBISH_TERMS):
            return True
        if platform == "링크드인":
            linkedin_positive = ["job function", "industries", "employment type", "minimum qualifications", "preferred qualifications", "seniority level"]
            if not any(tok in raw_l for tok in linkedin_positive):
                return True
        return False

    def _detail_text(self, platform: str, url: str) -> tuple[str, str]:
        headers = {"User-Agent": USER_AGENT}
        if platform == "링크드인":
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=USER_AGENT)
                safe_goto(page, url)
                page.wait_for_timeout(2000)
                html = page.content()
                browser.close()
            soup = BeautifulSoup(html, "lxml")
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
            raw = clean_text(soup.get_text(" ", strip=True))
            return title, raw
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
        raw = clean_text(soup.get_text(" ", strip=True))
        if not title:
            og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
            if og and og.get("content"):
                title = clean_text(og["content"])
        return title, raw

    def fetch(self):
        records = []
        platform_region = self.source_cfg.meta.get("platform_region", self.source_cfg.region)
        platform = self.source_cfg.meta.get("source_label", self.source_cfg.name)
        target_companies = self.source_cfg.meta.get("target_companies") or TARGET_COMPANIES
        debug = bool(self.source_cfg.meta.get("debug"))

        headers = {"User-Agent": USER_AGENT}

        for target_company in target_companies:
            search_url = self._search_url(target_company)
            candidates: list[tuple[str, str]] = []
            if platform != "링크드인":
                try:
                    r = requests.get(search_url, headers=headers, timeout=30)
                    r.raise_for_status()
                    candidates = _extract_candidates_from_html(search_url, r.text, limit=self.max_candidates)
                except Exception:
                    candidates = []

            if platform == "링크드인" or not candidates:
                try:
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=True)
                        page = browser.new_page(user_agent=USER_AGENT)
                        safe_goto(page, search_url, timeout_ms=45000)
                        page.wait_for_timeout(2000)
                        if debug:
                            print(f"[DEBUG] linkedin search_url={search_url}")
                            print(f"[DEBUG] linkedin page_title={page.title()}")
                        candidates = collect_candidate_links(page, page.url, limit=self.max_candidates)
                        if debug:
                            print(f"[DEBUG] {platform}/{target_company} candidate_links={len(candidates)}")
                        browser.close()
                except Exception as exc:
                    if debug:
                        print(f"[DEBUG] {platform}/{target_company} search_failed={exc}")
                    continue

            alias_candidates = []
            aliases = ALIASES[target_company]
            for text, url in candidates:
                combined = f"{text} {url}"
                if explicit_company_match(combined, aliases) or platform == "링크드인":
                    alias_candidates.append((text, url))
            if debug:
                print(f"[DEBUG] {platform}/{target_company} alias_candidates={len(alias_candidates)}")

            kept = 0
            for text, url in alias_candidates:
                if kept >= self.max_details_per_company:
                    break
                try:
                    title, raw = self._detail_text(platform, url)
                except Exception as exc:
                    if debug:
                        print(f"[DEBUG] {platform}/{target_company} detail_failed={url} err={exc}")
                    continue
                title = title or clean_text(text)
                if self._reject_candidate(platform, url, title, raw, target_company):
                    continue
                company = self._infer_company(f"{title} {raw} {url}")
                if company != target_company:
                    continue
                records.append(build_record_from_detail(
                    company=company,
                    region=platform_region,
                    source_label=platform,
                    title=title,
                    url=url,
                    raw_text=raw,
                    job_id=extract_job_id_from_url(url),
                ))
                kept += 1
            if debug:
                print(f"[DEBUG] {platform}/{target_company} kept_details={kept}")
        return records
