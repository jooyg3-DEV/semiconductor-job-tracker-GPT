from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, extract_job_id_from_url, safe_goto
from core.models import JobRecord
from core.search_plan import LINKEDIN_COMPANY_SLUGS, get_company_keywords, get_company_regions
from core.utils import clean_text, explicit_company_match

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
JOBISH_TERMS = [
    "채용", "공고", "recruit", "position", "role", "job", "지원자격", "responsibilities", "qualifications",
    "employment type", "minimum qualifications", "preferred qualifications", "full-time", "경력", "신입",
]
DEBUG_DIR = Path("debug_outputs")
DEBUG_FIELDS = [
    "timestamp", "platform", "company", "stage", "keyword", "region_hint", "search_url", "page_title", "result_count_text",
    "candidate_links_count", "alias_candidates_count", "title", "url", "detail_title", "decision", "reason",
    "include_matches", "exclude_matches", "hard_excludes", "note",
]

PLATFORM_CONFIG = {
    "사람인": {
        "result_zero_pattern": r"총\s*0건",
        "candidate_selectors": ["a[href*='rec_idx=']", "a[href*='/zf_user/jobs/relay/view']", "a[href*='/job-search/view']"],
        "required_url_tokens": ["rec_idx=", "/jobs/relay/view", "/job-search/view"],
        "reject_url_tokens": ["/zf_user/search/recruit", "/zf_user/member", "javascript:"],
        "search_url": lambda company, keyword, region: f"https://www.saramin.co.kr/zf_user/search/recruit?searchword={quote_plus(_join_query(company, keyword, region))}",
    },
    "잡코리아": {
        "result_zero_pattern": r"검색결과\s*0건|총\s*0건",
        "candidate_selectors": ["a[href*='/Recruit/GI_Read/']"],
        "required_url_tokens": ["/Recruit/GI_Read/", "/Recruit/GI_Read"],
        "reject_url_tokens": ["/Search/", "/Recruit/Home", "javascript:"],
        "search_url": lambda company, keyword, region: f"https://www.jobkorea.co.kr/Search/?stext={quote_plus(_join_query(company, keyword, region))}",
    },
    "링크드인": {
        "result_zero_pattern": r"\b0\b.*jobs?",
        "candidate_selectors": ["a[href*='/jobs/view/']", "a.base-card__full-link", "div.base-card a[href*='/jobs/view/']"],
        "required_url_tokens": ["/jobs/view/"],
        "reject_url_tokens": ["/jobs/search/", "/signup", "/login", "privacy", "about", "ms-windows-store://", "guest-controls", "legal"],
        "search_url": lambda company, keyword, region: _linkedin_search_url(company, keyword, region),
    },
    "링커리어": {
        "result_zero_pattern": r"검색 결과\s*0건|0개의 결과",
        "candidate_selectors": ["a[href*='/recruit/']", "a[href*='/jobs/']", "a[href*='/activity/view']"],
        "required_url_tokens": ["/recruit/", "/jobs/", "/activity/view"],
        "reject_url_tokens": ["/search/", "?q=", "javascript:"],
        "search_url": lambda company, keyword, region: f"https://linkareer.com/search?query={quote_plus(_join_query(company, keyword, region))}",
    },
}


def _join_query(company: str, keyword: str, region: str) -> str:
    return " ".join(x for x in [company, keyword, region] if x).strip()


def _linkedin_search_url(company: str, keyword: str, region: str) -> str:
    slug = LINKEDIN_COMPANY_SLUGS.get(company, "")
    if slug:
        region_part = f"&location={quote_plus(region)}" if region else ""
        keyword_part = f"?keywords={quote_plus(keyword)}" if keyword else "?keywords="
        return f"https://www.linkedin.com/company/{slug}/jobs/{keyword_part}{region_part}"
    query = _join_query(company, keyword, region)
    return f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(query)}"


class SearchPlatformAdapter(BaseAdapter):
    def __init__(self, company_cfg, source_cfg):
        super().__init__(company_cfg, source_cfg)
        self.max_candidates = int(self.source_cfg.meta.get("max_candidates", 20))
        self.max_details_per_company = int(self.source_cfg.meta.get("max_details", 10))
        self.debug = bool(self.source_cfg.meta.get("debug"))
        self.debug_rows: list[dict[str, str]] = []

    def _log_debug(self, platform: str, company: str, stage: str, *, keyword: str = "", region_hint: str = "", search_url: str = "", page_title: str = "", result_count_text: str = "", candidate_links_count: int | str = "", alias_candidates_count: int | str = "", title: str = "", url: str = "", detail_title: str = "", decision: str = "", reason: str = "", include_matches=None, exclude_matches=None, hard_excludes=None, note: str = ""):
        if not self.debug:
            return
        row = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "platform": platform,
            "company": company,
            "stage": stage,
            "keyword": keyword,
            "region_hint": region_hint,
            "search_url": search_url,
            "page_title": clean_text(page_title),
            "result_count_text": clean_text(result_count_text),
            "candidate_links_count": str(candidate_links_count),
            "alias_candidates_count": str(alias_candidates_count),
            "title": clean_text(title),
            "url": url,
            "detail_title": clean_text(detail_title),
            "decision": decision,
            "reason": reason,
            "include_matches": ", ".join(include_matches or []),
            "exclude_matches": ", ".join(exclude_matches or []),
            "hard_excludes": ", ".join(hard_excludes or []),
            "note": clean_text(note),
        }
        self.debug_rows.append(row)
        msg = f"[DEBUG] {platform}/{company} {stage} decision={decision} reason={reason} keyword={keyword} title={clean_text(title)[:120]}"
        if search_url:
            msg += f" search_url={search_url[:160]}"
        if page_title:
            msg += f" page_title={clean_text(page_title)[:100]}"
        if result_count_text:
            msg += f" result_count={clean_text(result_count_text)[:80]}"
        if note:
            msg += f" note={clean_text(note)[:120]}"
        print(msg)

    def write_debug_csv(self) -> None:
        if not self.debug:
            return
        DEBUG_DIR.mkdir(exist_ok=True)
        platform = self.source_cfg.meta.get("source_label", self.source_cfg.name)
        companies = self.source_cfg.meta.get("target_companies") or ["all"]
        company_token = companies[0] if len(companies) == 1 else "multi"
        file_path = DEBUG_DIR / f"debug_{company_token}_{platform}.csv"
        with file_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=DEBUG_FIELDS)
            writer.writeheader()
            for row in self.debug_rows:
                writer.writerow(row)
        print(f"[DEBUG] wrote debug csv {file_path}")

    def _iter_search_inputs(self, company_name: str) -> list[tuple[str, str, str]]:
        platform = self.source_cfg.meta.get("source_label", self.source_cfg.name)
        build_url = PLATFORM_CONFIG[platform]["search_url"]
        search_terms = [""] + get_company_keywords(company_name)
        region_hints = [""] + get_company_regions(company_name)[:3]
        pairs: list[tuple[str, str, str]] = []
        seen: set[tuple[str, str]] = set()
        for keyword in search_terms:
            regions = [""] if keyword == "" else region_hints[:2]
            for region in regions:
                key = (keyword, region)
                if key in seen:
                    continue
                seen.add(key)
                pairs.append((keyword, region, build_url(company_name, keyword, region)))
        return pairs

    def _title_allowed(self, title: str) -> bool:
        tl = (title or "").lower()
        blocked = [
            "회원가입", "로그인", "join now", "sign in", "about", "privacy", "cookie", "copyright",
            "brand policy", "community guidelines", "guest controls", "user agreement", "forgot password",
            "공고 등록", "채용정보", "직업별", "역세권별", "hot100", "헤드헌팅", "채용관", "파견대행",
            "커뮤니티", "면접후기", "기업·연봉", "스크랩", "실시간 공고", "본문 바로가기",
        ]
        return not any(x in tl for x in blocked)

    def _candidate_from_soup(self, platform: str, base_url: str, html: str) -> tuple[list[tuple[str, str]], str, str]:
        cfg = PLATFORM_CONFIG[platform]
        soup = BeautifulSoup(html, "lxml")
        page_title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        page_text = clean_text(soup.get_text(" ", strip=True))
        result_count_text = ""
        zero_pat = cfg.get("result_zero_pattern", "")
        if zero_pat:
            m = re.search(zero_pat, page_title, flags=re.I) or re.search(zero_pat, page_text[:3000], flags=re.I)
            if m:
                result_count_text = m.group(0)
        candidates: list[tuple[str, str]] = []
        seen: set[str] = set()
        for selector in cfg["candidate_selectors"]:
            for a in soup.select(selector):
                href = a.get("href") or ""
                if not href:
                    continue
                full = urljoin(base_url, href)
                text = clean_text(a.get_text(" ", strip=True))
                if full in seen:
                    continue
                seen.add(full)
                candidates.append((text or full.rsplit("/", 1)[-1], full))
                if len(candidates) >= self.max_candidates:
                    return candidates, page_title, result_count_text
        return candidates, page_title, result_count_text

    def _url_allowed(self, platform: str, url: str) -> tuple[bool, str]:
        low = url.lower()
        cfg = PLATFORM_CONFIG[platform]
        if any(token.lower() in low for token in cfg["reject_url_tokens"]):
            return False, "blocked_url_pattern"
        if not any(token.lower() in low for token in cfg["required_url_tokens"]):
            return False, "invalid_url_pattern"
        return True, "allowed"

    def _detail_text(self, platform: str, url: str) -> tuple[str, str, str]:
        headers = {"User-Agent": USER_AGENT}
        if platform == "링크드인":
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=USER_AGENT)
                safe_goto(page, url, timeout_ms=45000)
                page.wait_for_timeout(2000)
                html = page.content()
                final_url = page.url
                browser.close()
            soup = BeautifulSoup(html, "lxml")
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
            raw = clean_text(soup.get_text(" ", strip=True))
            return title, raw, final_url
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
        if not title:
            og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
            if og and og.get("content"):
                title = clean_text(og["content"])
        raw = clean_text(soup.get_text(" ", strip=True))
        return title, raw, url

    def _company_alias_match(self, company: str, title: str, raw: str, url: str) -> bool:
        return explicit_company_match(f"{title} {raw} {url}", ALIASES[company])

    def _has_job_signal(self, title: str, raw: str) -> bool:
        low = f"{title} {raw}".lower()
        return any(tok.lower() in low for tok in JOBISH_TERMS)

    def fetch(self):
        records: list[JobRecord] = []
        platform_region = self.source_cfg.meta.get("platform_region", self.source_cfg.region)
        platform = self.source_cfg.meta.get("source_label", self.source_cfg.name)
        target_companies = self.source_cfg.meta.get("target_companies") or TARGET_COMPANIES
        headers = {"User-Agent": USER_AGENT}

        for target_company in target_companies:
            accepted_candidates: list[tuple[str, str, str, str, str]] = []
            seen_candidate_urls: set[str] = set()
            search_inputs = self._iter_search_inputs(target_company)
            for keyword, region_hint, search_url in search_inputs:
                html = ""
                page_title = ""
                result_count_text = ""
                candidates: list[tuple[str, str]] = []
                try:
                    if platform != "링크드인":
                        r = requests.get(search_url, headers=headers, timeout=30)
                        r.raise_for_status()
                        html = r.text
                        candidates, page_title, result_count_text = self._candidate_from_soup(platform, search_url, html)
                    else:
                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=True)
                            page = browser.new_page(user_agent=USER_AGENT)
                            safe_goto(page, search_url, timeout_ms=45000)
                            page.wait_for_timeout(2000)
                            html = page.content()
                            page_title = page.title()
                            browser.close()
                        candidates, _, result_count_text = self._candidate_from_soup(platform, search_url, html)
                except Exception as exc:
                    self._log_debug(platform, target_company, "search_meta", keyword=keyword, region_hint=region_hint, search_url=search_url, decision="WARN", reason="search_failed", note=str(exc))
                    continue

                self._log_debug(platform, target_company, "search_meta", keyword=keyword, region_hint=region_hint, search_url=search_url, page_title=page_title, result_count_text=result_count_text, candidate_links_count=len(candidates), decision="PASS", reason="search_ok")
                if result_count_text and "0" in result_count_text:
                    continue

                alias_count = 0
                for title, url in candidates:
                    self._log_debug(platform, target_company, "candidate_raw", keyword=keyword, region_hint=region_hint, search_url=search_url, page_title=page_title, result_count_text=result_count_text, title=title, url=url, decision="CANDIDATE", reason="raw_candidate")
                    allowed, reason = self._url_allowed(platform, url)
                    if not allowed:
                        self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, decision="REJECT", reason=reason)
                        continue
                    if not self._title_allowed(title):
                        self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, decision="REJECT", reason="blocked_title")
                        continue
                    if url in seen_candidate_urls:
                        self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, decision="REJECT", reason="duplicate_candidate")
                        continue
                    seen_candidate_urls.add(url)
                    accepted_candidates.append((keyword, region_hint, title, url, search_url))
                    alias_count += 1
                    self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, decision="PASS", reason="candidate_kept", alias_candidates_count=alias_count)
                    if len(accepted_candidates) >= self.max_candidates:
                        break
                if len(accepted_candidates) >= self.max_candidates:
                    break

            kept_for_company = 0
            for keyword, region_hint, title, url, source_search_url in accepted_candidates:
                if kept_for_company >= self.max_details_per_company:
                    break
                try:
                    detail_title, raw, final_url = self._detail_text(platform, url)
                except Exception as exc:
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, decision="REJECT", reason="detail_fetch_failed", note=str(exc))
                    continue
                if platform == "링크드인" and "/jobs/view/" not in final_url:
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, detail_title=detail_title, decision="REJECT", reason="not_linkedin_job_detail", note=final_url)
                    continue
                if not self._company_alias_match(target_company, detail_title or title, raw, final_url):
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, detail_title=detail_title, decision="REJECT", reason="company_alias_miss")
                    continue
                if not self._has_job_signal(detail_title or title, raw):
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, detail_title=detail_title, decision="REJECT", reason="no_job_signal")
                    continue
                record = build_record_from_detail(
                    company=target_company,
                    region=platform_region,
                    source_label=platform,
                    title=detail_title or title,
                    url=final_url,
                    raw_text=raw,
                    job_id=extract_job_id_from_url(final_url),
                )
                record.metadata["search_keyword"] = keyword
                record.metadata["search_region_hint"] = region_hint
                self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, detail_title=detail_title, decision="PASS", reason="detail_ok")
                records.append(record)
                kept_for_company += 1

        self.write_debug_csv()
        return records
