from __future__ import annotations

from urllib.parse import quote_plus

from adapters.playwright_utils import USER_AGENT, build_record_from_detail, collect_candidate_links, extract_job_id_from_url, safe_goto
from adapters.base import BaseAdapter
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import requests

from core.utils import clean_text, explicit_company_match, looks_like_listing_or_search_page

COMMON_TERMS = "반도체 공정 process engineer process support engineer field application engineer application engineer customer engineer metrology deposition lithography packaging yield integration"
TARGET_COMPANIES = [
    "삼성전자DS", "SK하이닉스", "ASML", "Applied Materials", "KLA", "Lam Research", "TEL", "Micron", "ASM", "TSMC", "NVIDIA", "AMD"
]
ALIASES = {
    "삼성전자DS": ["삼성전자", "samsung", "device solutions", "ds division", "삼성 ds", "samsung electronics"],
    "SK하이닉스": ["sk hynix", "sk하이닉스", "하이닉스"],
    "ASML": ["asml"],
    "Applied Materials": ["applied materials", "어플라이드머티어리얼즈", "어플라이드 머티어리얼즈", "어플라이드머티어리얼즈코리아", "amat"],
    "KLA": ["kla", "k l a", "kla corporation"],
    "Lam Research": ["lam research", "램리서치", "lamresearch"],
    "TEL": ["tokyo electron", "tel", "도쿄일렉트론", "tokyoelectron", "tokyo electron korea", "도쿄일렉트론코리아"],
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
    "잡다": ["/recruit/", "/jobs/"],
    "하이브레인넷": ["/job/", "/hiring/", "/recruit/"],
    "캐치": ["/NCS/RecruitInfo", "/RecruitInfo", "/NCS/RecruitDetail", "/RecruitDetail"],
    "링커리어": ["/activity/view", "/recruit/", "/jobs/"],
    "잡플래닛": ["/job/", "/jobs/"]
}
PLATFORM_PATH_REJECTS = {
    "캐치": ["RecruitSearch", "theme=", "sort=", "scrollTo=searchDetail", "firstTab=category"],
    "사람인": ["/zf_user/search/recruit"],
    "잡코리아": ["/Search/", "/Recruit/Home"],
    "링크드인": ["/jobs/search/"],
    "링커리어": ["/search/", "?q="],
}
PLATFORM_CONTENT_REJECTS = {
    "캐치": [
        "공고리스트를 불러오고 있습니다", "최근검색기록", "검색조건초기화", "오늘 뜬 인기 top 50",
        "경력 인기 top 50", "신입 인기 top 50", "top중견중소", "인턴·교육생", "반도체·전기·전자",
        "전체 공고 이동하기", "조건 추가 각각 최대", "캐치tv", "채용설명회", "인재pick"
    ],
    "링크드인": [],
    "하이브레인넷": ["대학원", "신입생 모집", "진학상담", "대학원카페", "교수", "연구실정보"],
}
JOBISH_TERMS = [
    "채용", "공고", "recruit", "position", "role", "job", "지원자격", "responsibilities", "qualifications",
    "employment type", "minimum qualifications", "preferred qualifications", "full-time", "경력", "신입"
]


class SearchPlatformAdapter(BaseAdapter):
    max_details_per_company = 4

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
        if platform == "잡플래닛":
            return f"https://www.jobplanet.co.kr/search?query={quote_plus(query)}"
        if platform == "캐치":
            return f"https://www.catch.co.kr/NCS/RecruitSearch?search={quote_plus(company_name)}"
        if platform == "하이브레인넷":
            return f"https://www.hibrain.net/search/?q={quote_plus(query)}"
        if platform == "잡다":
            return f"https://www.jobda.im/search?query={quote_plus(query)}"
        return base

    def _infer_company(self, text: str) -> str | None:
        t = text.lower()
        for company, aliases in ALIASES.items():
            if any(alias.lower() in t for alias in aliases):
                return company
        return None

    def _url_allowed(self, platform: str, url: str) -> bool:
        requires = PLATFORM_URL_REQUIRES.get(platform, [])
        if not requires:
            return True
        return any(token in url for token in requires)

    def _title_allowed(self, platform: str, title: str) -> bool:
        tl = (title or "").lower()
        blocked_prefixes = [
            "top중견중소", "신입 인기 top", "경력 인기 top", "오늘 뜬 인기 top", "반도체·전기·전자", "인턴·교육생",
            "회원가입", "로그인", "검색", "전체 채용공고"
        ]
        if any(x in tl for x in blocked_prefixes):
            return False
        if platform == "링크드인" and not tl:
            return False
        return True

    def _reject_candidate(self, platform: str, url: str, title: str, raw: str, target_company: str) -> bool:
        if looks_like_listing_or_search_page(url, title, raw):
            return True
        if any(token in url for token in PLATFORM_PATH_REJECTS.get(platform, [])):
            return True
        if not self._url_allowed(platform, url):
            return True
        if not self._title_allowed(platform, title):
            return True

        raw_l = raw.lower()
        if any(token.lower() in raw_l for token in PLATFORM_CONTENT_REJECTS.get(platform, [])):
            return True

        aliases = ALIASES[target_company]
        if not explicit_company_match(f"{title} {raw} {url}", aliases):
            return True

        if not any(tok.lower() in raw_l or tok.lower() in (title or "").lower() for tok in JOBISH_TERMS):
            return True

        if platform == "링크드인":
            linkedin_positive = ["job function", "industries", "employment type", "minimum qualifications", "preferred qualifications", "show more jobs", "seniority level"]
            if not any(tok in raw_l for tok in linkedin_positive):
                return True
            if "join now" in raw_l and not any(tok in raw_l for tok in linkedin_positive):
                return True

        if platform == "캐치":
            catch_positive = ["채용공고", "오늘마감", "d-", "지원하기", "정규직", "신입/경력"]
            if not any(tok in raw_l for tok in catch_positive):
                return True

        return False

    def _detail_text(self, platform: str, url: str) -> tuple[str, str]:
        headers = {"User-Agent": USER_AGENT}
        if platform == "링크드인":
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(user_agent=USER_AGENT)
                    safe_goto(page, url)
                    page.wait_for_timeout(2500)
                    html = page.content()
                    browser.close()
                soup = BeautifulSoup(html, "lxml")
                title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
                raw = clean_text(soup.get_text(" ", strip=True))
                return title, raw
            except Exception:
                pass
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
        for target_company in target_companies:
            search_url = self._search_url(target_company)
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(user_agent=USER_AGENT)
                    safe_goto(page, search_url)
                    page.wait_for_timeout(2500)
                    candidates = collect_candidate_links(page, page.url, limit=40)
                    browser.close()
            except Exception:
                continue

            kept = 0
            for text, url in candidates:
                if kept >= self.max_details_per_company:
                    break
                try:
                    title, raw = self._detail_text(platform, url)
                except Exception:
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
        return records

