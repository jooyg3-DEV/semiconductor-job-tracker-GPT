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
from core.debug_csv import append_audit_rows
from core.models import JobRecord
from core.search_plan import LINKEDIN_COMPANY_SLUGS, get_company_keywords, get_company_regions
from core.utils import canonicalize_job_url, clean_text, explicit_company_match, infer_location_from_text, normalize_employment_type, parse_json_ld

TARGET_COMPANIES = ["삼성전자DS", "SK하이닉스", "ASML", "Applied Materials", "KLA", "Lam Research", "TEL", "Micron", "ASM", "TSMC", "NVIDIA", "AMD"]
ALIASES = {
    "삼성전자DS": ["삼성전자", "samsung", "device solutions", "ds division", "삼성 ds", "samsung electronics"],
    "SK하이닉스": ["sk hynix", "sk하이닉스", "하이닉스"],
    "ASML": ["asml", "asml korea"],
    "Applied Materials": [
        "applied materials", "applied materials korea", "applied materials korea ltd",
        "어플라이드 머티어리얼즈", "어플라이드 머티어리얼즈 코리아", "어플라이드머티어리얼즈", "어플라이드머티어리얼즈코리아", "amat",
    ],
    "KLA": ["kla", "kla corporation", "kla-tencor"],
    "Lam Research": ["lam research", "램리서치", "lamresearch"],
    "TEL": ["tokyo electron", "tel", "도쿄일렉트론", "tokyoelectron", "tokyo electron korea"],
    "Micron": ["micron", "마이크론", "micron technology"],
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
    "candidate_links_count", "alias_candidates_count", "title", "url", "detail_title", "final_url", "canonical_url",
    "selector", "detail_company", "detail_location", "employment_type", "decision", "reason", "include_matches",
    "exclude_matches", "hard_excludes", "note",
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


def _parse_company_from_title(platform: str, title: str) -> str:
    title = clean_text(title)
    if not title:
        return ""
    if platform == "사람인":
        m = re.match(r"^\[(.*?)\]", title)
        if m:
            return clean_text(m.group(1))
    if platform == "잡코리아":
        m = re.match(r"^(.*?)\s*채용\s*-", title)
        if m:
            return clean_text(m.group(1))
    if platform == "링크드인":
        parts = [p.strip() for p in title.split("|") if p.strip()]
        if len(parts) >= 2:
            return parts[-2] if parts[-1].lower() == "linkedin" else parts[-1]
    return ""


def _extract_company_from_jsonld(soup: BeautifulSoup) -> str:
    for item in parse_json_ld(soup):
        for key in ["hiringOrganization", "organization"]:
            value = item.get(key)
            if isinstance(value, dict) and clean_text(value.get("name")):
                return clean_text(value.get("name"))
        if clean_text(item.get("name")) and item.get("@type") in {"Organization", "Corporation"}:
            return clean_text(item.get("name"))
    return ""


def _extract_company_from_soup(platform: str, soup: BeautifulSoup, title: str) -> tuple[str, str]:
    selectors = {
        "사람인": [".company_nm", ".recruit_company_name", ".company a", "meta[property='og:site_name']", "meta[property='og:title']"],
        "잡코리아": [".coName", ".tplJobView .tit_company", ".company .name", "meta[property='og:title']", "meta[property='og:description']"],
        "링크드인": ["a.topcard__org-name-link", ".topcard__flavor-row a", ".topcard__flavor"],
    }.get(platform, [])
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            content = node.get("content") if node.name == "meta" else node.get_text(" ", strip=True)
            value = clean_text(content)
            if value:
                return value, selector
    jsonld_value = _extract_company_from_jsonld(soup)
    if jsonld_value:
        return jsonld_value, "jsonld"
    parsed = _parse_company_from_title(platform, title)
    if parsed:
        return parsed, "title"
    return "", ""


class SearchPlatformAdapter(BaseAdapter):
    def __init__(self, company_cfg, source_cfg):
        super().__init__(company_cfg, source_cfg)
        self.max_candidates = int(self.source_cfg.meta.get("max_candidates", 20))
        self.max_details_per_company = int(self.source_cfg.meta.get("max_details", 10))
        self.debug = bool(self.source_cfg.meta.get("debug"))
        self.light_mode = bool(self.source_cfg.meta.get("light_mode"))
        self.debug_rows: list[dict[str, str]] = []

    def _log_debug(self, platform: str, company: str, stage: str, **kwargs):
        row = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "platform": platform,
            "company": company,
            "stage": stage,
            "keyword": kwargs.get("keyword", ""),
            "region_hint": kwargs.get("region_hint", ""),
            "search_url": kwargs.get("search_url", ""),
            "page_title": clean_text(kwargs.get("page_title", "")),
            "result_count_text": clean_text(kwargs.get("result_count_text", "")),
            "candidate_links_count": str(kwargs.get("candidate_links_count", "")),
            "alias_candidates_count": str(kwargs.get("alias_candidates_count", "")),
            "title": clean_text(kwargs.get("title", "")),
            "url": kwargs.get("url", ""),
            "detail_title": clean_text(kwargs.get("detail_title", "")),
            "final_url": kwargs.get("final_url", ""),
            "canonical_url": kwargs.get("canonical_url", ""),
            "selector": kwargs.get("selector", ""),
            "detail_company": clean_text(kwargs.get("detail_company", "")),
            "detail_location": clean_text(kwargs.get("detail_location", "")),
            "employment_type": clean_text(kwargs.get("employment_type", "")),
            "decision": kwargs.get("decision", ""),
            "reason": kwargs.get("reason", ""),
            "include_matches": ", ".join(kwargs.get("include_matches") or []),
            "exclude_matches": ", ".join(kwargs.get("exclude_matches") or []),
            "hard_excludes": ", ".join(kwargs.get("hard_excludes") or []),
            "note": clean_text(kwargs.get("note", "")),
        }
        self.debug_rows.append(row)
        if self.debug:
            msg = f"[DEBUG] {platform}/{company} {stage} decision={row['decision']} reason={row['reason']} keyword={row['keyword']} title={row['title'][:120]}"
            if row["search_url"]:
                msg += f" search_url={row['search_url'][:160]}"
            if row["page_title"]:
                msg += f" page_title={row['page_title'][:100]}"
            if row["result_count_text"]:
                msg += f" result_count={row['result_count_text'][:80]}"
            if row["note"]:
                msg += f" note={row['note'][:120]}"
            print(msg)

    def write_debug_csv(self) -> None:
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
        append_audit_rows("platform_debug", self.debug_rows)
        print(f"[DEBUG] wrote debug csv {file_path}")

    def _iter_search_inputs(self, company_name: str) -> list[tuple[str, str, str]]:
        platform = self.source_cfg.meta.get("source_label", self.source_cfg.name)
        build_url = PLATFORM_CONFIG[platform]["search_url"]
        search_terms = [""] + get_company_keywords(company_name)
        region_hints = [""] + get_company_regions(company_name)[: (2 if self.light_mode else 3)]
        if platform == "링크드인":
            search_terms = ["", "field application engineer", "process engineer", "customer engineer"]
        elif self.light_mode:
            search_terms = search_terms[:5]
        pairs: list[tuple[str, str, str]] = []
        seen: set[tuple[str, str]] = set()
        for keyword in search_terms:
            regions = [""] if keyword == "" else region_hints[: (1 if self.light_mode else 2)]
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

    def _candidate_from_soup(self, platform: str, base_url: str, html: str) -> tuple[list[tuple[str, str, str]], dict[str, str]]:
        cfg = PLATFORM_CONFIG[platform]
        soup = BeautifulSoup(html, "lxml")
        page_title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        h1_text = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
        page_text = clean_text(soup.get_text(" ", strip=True))
        result_count_text = ""
        zero_pat = cfg.get("result_zero_pattern", "")
        if zero_pat:
            m = re.search(zero_pat, page_title, flags=re.I) or re.search(zero_pat, page_text[:3000], flags=re.I)
            if m:
                result_count_text = m.group(0)
        anchors = soup.select("a[href]")
        jobs_view_links = [urljoin(base_url, a.get("href") or "") for a in anchors if "/jobs/view/" in (a.get("href") or "")]
        login_wall = "sign up | linkedin" in page_title.lower() or any(tok in page_text.lower()[:1000] for tok in ["join now", "sign in", "continue to linkedin"])
        json_ld_count = len(soup.select("script[type='application/ld+json']"))
        metrics = {
            "page_title": page_title,
            "h1_text": h1_text,
            "result_count_text": result_count_text,
            "total_anchor_count": str(len(anchors)),
            "jobs_view_href_count": str(len(jobs_view_links)),
            "json_ld_jobposting_count": str(json_ld_count),
            "login_wall_detected": "Y" if login_wall else "N",
            "canonical_url": clean_text((soup.find("link", rel="canonical") or {}).get("href", "")),
        }
        candidates: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for selector in cfg["candidate_selectors"]:
            for a in soup.select(selector):
                href = a.get("href") or ""
                if not href:
                    continue
                full = urljoin(base_url, href)
                canonical = canonicalize_job_url(full)
                text = clean_text(a.get_text(" ", strip=True))
                if canonical in seen:
                    continue
                seen.add(canonical)
                candidates.append((text or full.rsplit("/", 1)[-1], full, selector))
                if len(candidates) >= self.max_candidates:
                    return candidates, metrics
        return candidates, metrics

    def _url_allowed(self, platform: str, url: str) -> tuple[bool, str]:
        low = url.lower()
        cfg = PLATFORM_CONFIG[platform]
        if any(token.lower() in low for token in cfg["reject_url_tokens"]):
            return False, "blocked_url_pattern"
        if not any(token.lower() in low for token in cfg["required_url_tokens"]):
            return False, "invalid_url_pattern"
        return True, "allowed"

    def _detail_payload(self, platform: str, url: str) -> dict[str, str]:
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
        else:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            html = r.text
            final_url = url
        soup = BeautifulSoup(html, "lxml")
        title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
        if not title:
            og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
            if og and og.get("content"):
                title = clean_text(og["content"])
        raw = clean_text(soup.get_text(" ", strip=True))
        detail_company, company_selector = _extract_company_from_soup(platform, soup, title)
        detail_location = ""
        for selector in [".job-location", ".location", ".workplace", ".subTitle .loc", ".topcard__flavor--bullet", "meta[property='place:location:address']"]:
            node = soup.select_one(selector)
            if node:
                detail_location = clean_text(node.get("content") if node.name == "meta" else node.get_text(" ", strip=True))
                if detail_location:
                    break
        if not detail_location:
            detail_location = infer_location_from_text(title, raw)
        employment_text = ""
        for token in ["Full time", "Full-time", "Part time", "Part-time", "Intern", "Contract", "Regular", "Temporary"]:
            if token.lower() in raw.lower() or token.lower() in title.lower():
                employment_text = token
                break
        qualification = ""
        for selector in [".qualification", ".summary", ".recruitment-detail__description", ".job-description"]:
            node = soup.select_one(selector)
            if node:
                qualification = clean_text(node.get_text(" ", strip=True))
                if qualification:
                    break
        if not qualification:
            qualification = raw[:500]
        return {
            "title": title,
            "raw": raw,
            "final_url": final_url,
            "detail_company": detail_company,
            "detail_location": detail_location or "미기재",
            "employment_type": normalize_employment_type(employment_text),
            "qualification": qualification,
            "page_title": clean_text(soup.title.get_text(" ", strip=True) if soup.title else ""),
            "company_selector": company_selector,
        }

    def _company_alias_match(self, company: str, extracted_company: str, title: str, raw: str, url: str) -> tuple[bool, str]:
        if extracted_company:
            return explicit_company_match(extracted_company, ALIASES[company]), extracted_company
        whole = f"{title} {raw[:500]} {url}"
        return explicit_company_match(whole, ALIASES[company]), extracted_company

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
            accepted_candidates: list[tuple[str, str, str, str, str, str]] = []
            seen_candidate_urls: set[str] = set()
            gated_streak = 0
            for keyword, region_hint, search_url in self._iter_search_inputs(target_company):
                try:
                    if platform == "링크드인":
                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=True)
                            page = browser.new_page(user_agent=USER_AGENT)
                            safe_goto(page, search_url, timeout_ms=45000)
                            page.wait_for_timeout(2000)
                            html = page.content()
                            browser.close()
                    else:
                        r = requests.get(search_url, headers=headers, timeout=30)
                        r.raise_for_status()
                        html = r.text
                    candidates, metrics = self._candidate_from_soup(platform, search_url, html)
                except Exception as exc:
                    self._log_debug(platform, target_company, "search_meta", keyword=keyword, region_hint=region_hint, search_url=search_url, decision="WARN", reason="search_failed", note=str(exc))
                    continue

                note = f"anchors={metrics.get('total_anchor_count','')} jobs_view={metrics.get('jobs_view_href_count','')} jsonld={metrics.get('json_ld_jobposting_count','')} login_wall={metrics.get('login_wall_detected','N')} h1={metrics.get('h1_text','')[:60]}"
                gated = platform == "링크드인" and metrics.get("login_wall_detected") == "Y" and int(metrics.get("jobs_view_href_count", "0") or 0) == 0
                self._log_debug(platform, target_company, "search_meta", keyword=keyword, region_hint=region_hint, search_url=search_url, page_title=metrics.get("page_title", ""), result_count_text=metrics.get("result_count_text", ""), candidate_links_count=len(candidates), decision=("REJECT" if gated else "PASS"), reason=("gated_page" if gated else "search_ok"), final_url=metrics.get("canonical_url", ""), note=note)
                if gated:
                    gated_streak += 1
                    if gated_streak >= 3:
                        break
                    continue
                gated_streak = 0
                if metrics.get("result_count_text") and "0" in metrics.get("result_count_text", ""):
                    continue
                if not candidates:
                    continue

                alias_count = 0
                for title, url, selector in candidates:
                    canonical = canonicalize_job_url(url)
                    self._log_debug(platform, target_company, "candidate_raw", keyword=keyword, region_hint=region_hint, search_url=search_url, page_title=metrics.get("page_title", ""), result_count_text=metrics.get("result_count_text", ""), title=title, url=url, canonical_url=canonical, selector=selector, decision="CANDIDATE", reason="raw_candidate")
                    allowed, why = self._url_allowed(platform, url)
                    if not allowed:
                        self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, canonical_url=canonical, selector=selector, decision="REJECT", reason=why)
                        continue
                    if not self._title_allowed(title):
                        self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, canonical_url=canonical, selector=selector, decision="REJECT", reason="blocked_title")
                        continue
                    if canonical in seen_candidate_urls:
                        self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, canonical_url=canonical, selector=selector, decision="REJECT", reason="duplicate_candidate")
                        continue
                    seen_candidate_urls.add(canonical)
                    accepted_candidates.append((keyword, region_hint, title, url, canonical, search_url))
                    alias_count += 1
                    self._log_debug(platform, target_company, "candidate_pruned", keyword=keyword, region_hint=region_hint, search_url=search_url, title=title, url=url, canonical_url=canonical, selector=selector, decision="PASS", reason="candidate_kept", alias_candidates_count=alias_count)
                    if len(accepted_candidates) >= self.max_candidates:
                        break
                if len(accepted_candidates) >= self.max_candidates:
                    break

            kept_for_company = 0
            for keyword, region_hint, title, url, canonical, source_search_url in accepted_candidates:
                if kept_for_company >= self.max_details_per_company:
                    break
                try:
                    payload = self._detail_payload(platform, url)
                except Exception as exc:
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, canonical_url=canonical, decision="REJECT", reason="detail_fetch_failed", note=str(exc))
                    continue
                final_url = payload["final_url"]
                final_canonical = canonicalize_job_url(final_url)
                company_ok, company_signal = self._company_alias_match(target_company, payload["detail_company"], payload["title"] or title, payload["raw"], final_url)
                if platform == "링크드인" and "/jobs/view/" not in final_url:
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, final_url=final_url, canonical_url=final_canonical, detail_title=payload["title"], detail_company=payload["detail_company"], detail_location=payload["detail_location"], decision="REJECT", reason="not_linkedin_job_detail", note=f"company_selector={payload.get('company_selector','')}")
                    continue
                if not company_ok:
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, final_url=final_url, canonical_url=final_canonical, detail_title=payload["title"], detail_company=company_signal or payload["detail_company"], detail_location=payload["detail_location"], decision="REJECT", reason="company_alias_miss", note=f"company_selector={payload.get('company_selector','')}")
                    continue
                if not self._has_job_signal(payload["title"] or title, payload["raw"]):
                    self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, final_url=final_url, canonical_url=final_canonical, detail_title=payload["title"], detail_company=payload["detail_company"], detail_location=payload["detail_location"], decision="REJECT", reason="no_job_signal", note=f"company_selector={payload.get('company_selector','')}")
                    continue
                record = build_record_from_detail(
                    company=target_company,
                    region=platform_region,
                    source_label=platform,
                    title=payload["title"] or title,
                    url=final_url,
                    raw_text=payload["raw"],
                    qualification=payload["qualification"],
                    location=payload["detail_location"] or "미기재",
                    employment_type=payload["employment_type"],
                    job_id=extract_job_id_from_url(final_url),
                )
                record.metadata["search_keyword"] = keyword
                record.metadata["search_region_hint"] = region_hint
                record.metadata["search_url"] = source_search_url
                record.metadata["page_title"] = payload.get("page_title", "")
                record.metadata["detail_company"] = payload["detail_company"]
                record.metadata["canonical_url"] = final_canonical
                record.metadata["company_selector"] = payload.get("company_selector", "")
                self._log_debug(platform, target_company, "detail_parse", keyword=keyword, region_hint=region_hint, search_url=source_search_url, title=title, url=url, final_url=final_url, canonical_url=final_canonical, detail_title=payload["title"], detail_company=payload["detail_company"], detail_location=payload["detail_location"], employment_type=payload["employment_type"], decision="PASS", reason="detail_ok", note=f"company_selector={payload.get('company_selector','')}")
                records.append(record)
                kept_for_company += 1

        self.write_debug_csv()
        return records
