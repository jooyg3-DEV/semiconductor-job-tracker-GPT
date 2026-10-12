from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

VERSION = "2.2"
ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config.json"
DATA = ROOT / "data"
ARTIFACTS = Path("debug_outputs")

DETAIL_TOKENS = (
    "/job/", "/jobs/", "/career/", "/careers/", "/recruit/", "/recruitment/",
    "/position/", "/vacanc", "/opening", "/notification/", "/view", "/detail", "/apply/"
)
JOB_WORDS = re.compile(
    r"(engineer|scientist|research|process|application|field|device|metrology|yield|equipment|system|"
    r"연구|개발|공정|소자|장비|채용|모집|평가|시험|설계)", re.I
)
KOREA = (
    "korea", "south korea", "대한민국", "한국", "seoul", "서울", "hwaseong", "화성",
    "icheon", "이천", "yongin", "용인", "pyeongtaek", "평택", "suwon", "수원",
    "cheongju", "청주", "daejeon", "대전", "dongtan", "동탄", "gyeonggi", "경기",
    "pangyo", "판교", "bundang", "분당", "ochang", "오창"
)
IGNORE_TEXT = {
    "home", "careers", "career", "jobs", "채용", "채용공고", "지원하기", "apply",
    "login", "로그인", "privacy", "terms", "about", "faq", "news", "contact",
    "채용정보", "전체 채용정보", "인재채용"
}
GLOBAL_DENY_PATH = (
    "/help/", "/privacy", "/terms", "/people/people/", "/subsid/detail/",
    "/company-info/sri-certification", "/company-info/view", "/faq", "/notice/view"
)
DYNAMIC_COMPANIES = {
    "KLA Korea", "Lam Research Korea", "Applied Materials Korea", "ASML Korea",
    "Samsung Electronics DS", "SK hynix", "Samsung Display", "KIST", "ETRI",
    "NNFC", "LG Electronics", "Hanwha Systems", "Hanwha Aerospace", "KAI",
    "Tokyo Electron Korea", "Tesla Korea", "Micron", "Onto Innovation Korea",
    "DB HiTek", "Magnachip", "LX Semicon", "KBSI"
}
KOREA_REQUIRED_GLOBAL = {
    "KLA Korea", "Lam Research Korea", "Applied Materials Korea", "ASML Korea",
    "ASM Korea", "Micron", "Onto Innovation Korea"
}
HANWHA_HINTS = {
    "Hanwha Systems": ("한화시스템", "hanwha systems"),
    "Hanwha Aerospace": ("한화에어로스페이스", "hanwha aerospace"),
}
BROWSER_MORE_TEXT = (
    "load more", "show more", "view more", "more jobs", "see more",
    "더보기", "더 보기", "채용공고 더보기", "공고 더보기"
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def canonical(url: str) -> str:
    if not url:
        return ""
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        return ""
    keep = []
    for key, value in parse_qsl(p.query, keep_blank_values=True):
        kl = key.lower()
        if kl.startswith("utm_") or kl in {"source", "src", "ref", "referrer", "gh_src", "_ga", "pvs"}:
            continue
        keep.append((key, value))
    path = re.sub(r"/{2,}", "/", p.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", urlencode(sorted(keep)), ""))


def extract_job_id(url: str, text: str = "") -> str:
    p = urlparse(url)
    q = {k.lower(): v for k, v in parse_qsl(p.query)}
    for key in (
        "gh_jid", "jobid", "job_id", "reqid", "req_id", "requisitionid",
        "postingid", "wantedauthno", "wr_id", "seq", "idx", "titleid", "b_idx"
    ):
        if clean(q.get(key)):
            return clean(q[key])

    target = f"{p.path} {text}"

    # ASML uses slugs such as "...-j00353351". Normalize to the master form.
    match = re.search(r"(?:^|[-_/])j(\d{8})\b", target, re.I)
    if match:
        return f"J-{match.group(1)}"

    # Workday job slugs normally end in "_2639689-5" or "_2635637".
    match = re.search(r"_(\d{6,10})(?:-\d+)?(?:$|[/?#\s])", target)
    if match:
        return match.group(1)

    for pattern in (
        r"\bJ-\d{6,}\b", r"\bR-?\d{4,}\b", r"\bRC\d{8,}\b",
        r"(?:/job/|/jobs/|job-)(\d{5,12})(?:\b|/|-)",
        r"(?:requisition|req)[-_ /:]?(\d{5,12})\b",
    ):
        match = re.search(pattern, target, re.I)
        if match:
            value = match.group(1) if match.lastindex else match.group(0)
            if re.fullmatch(r"R\d{4,}", value, re.I):
                return value.upper()
            return value
    return ""


def korea_signal(text: str) -> str:
    lowered = clean(text).lower()
    return next((signal for signal in KOREA if signal in lowered), "")


def parse_dates(text: str):
    found = []
    for y, m, d in re.findall(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})", text):
        try:
            found.append(date(int(y), int(m), int(d)))
        except ValueError:
            pass
    return found


def obviously_closed(text: str) -> bool:
    lowered = clean(text).lower()
    if any(token in lowered for token in ("no longer accepting", "closed", "접수마감", "마감된 공고")):
        return True
    if "마감" in lowered:
        dates = parse_dates(lowered)
        if dates and max(dates) < date.today():
            return True
    return False


def load_config(company_filter: str = ""):
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    companies = [c["name"] for c in cfg["companies"]]
    if company_filter:
        companies = [c for c in companies if company_filter.lower() in c.lower()]
    allowed = set(companies)
    sources = [s for s in cfg["sources"] if s["company"] in allowed]
    sources.sort(key=lambda x: (companies.index(x["company"]), x["priority"], x["source_type"]))
    return cfg, companies, sources


def anchor_context(anchor, max_levels: int = 4, max_chars: int = 1200) -> str:
    """Return the smallest useful card-like ancestor text without swallowing a whole page."""
    best = clean(anchor.parent.get_text(" ", strip=True) if anchor.parent else "")
    node = anchor.parent
    for _ in range(max_levels):
        if node is None:
            break
        text_value = clean(node.get_text(" ", strip=True))
        if 20 <= len(text_value) <= max_chars:
            best = text_value
        if len(text_value) > max_chars:
            break
        node = node.parent
    return best[:max_chars]


def source_needs_browser(source: dict, request_count: int) -> bool:
    if request_count == 0:
        return True
    if source["company"] in DYNAMIC_COMPANIES:
        return True
    host = urlparse(source["url"]).netloc.lower()
    return any(token in host for token in (
        "myworkdayjobs.com", "careerlink.kr", "careers.", "recruiter.co.kr",
        "mokahr.com", "fairy.im"
    ))


def url_allowed(source: dict, url: str, title: str, context: str) -> bool:
    company = source["company"]
    p = urlparse(url)
    path = p.path.lower()
    query = {k.lower(): v for k, v in parse_qsl(p.query)}
    combined = clean(f"{title} {context} {url}")

    if any(token in path for token in GLOBAL_DENY_PATH):
        return False

    if company == "LG Electronics":
        return "/apply/detail" in path

    if company == "Hyundai Mobis":
        return "/jobs-view" in path and "seq" in query

    if company == "KETI":
        return "employment.php" in path and query.get("at") == "view" and bool(query.get("idx"))

    if company == "KIMS":
        return "board.php" in path and bool(query.get("wr_id"))

    if company == "ETRI" and "etri.re.kr" in p.netloc.lower():
        return "/kor/bbs/view.etri" in path and query.get("b_board_id") == "ETRI39"

    if company == "KBSI":
        return "/board/view" in path and bool(query.get("titleid"))

    if company == "NNFC":
        return bool(re.search(r"/B[0-9A-Za-z]+\.do$", p.path))

    if company == "ASM Korea":
        if "/open-vacancies/" not in path:
            return False
        return bool(korea_signal(combined))

    if company == "ASML Korea":
        base = "/en/careers/find-your-job/"
        if not path.startswith(base) or path.rstrip("/") == base.rstrip("/"):
            return False
        return bool(re.search(r"-j\d{8}$", path, re.I))

    if company == "SK hynix":
        if "talent.skhynix.com" in p.netloc.lower():
            return "/hub/ko/apply/job" in path and path.rstrip("/") != "/hub/ko/apply/job"
        if "skcareers.com" in p.netloc.lower():
            if "/recruit/detail/" not in path:
                return False
            lower = combined.lower()
            return "sk hynix" in lower or "sk하이닉스" in lower
        return False

    if company in ("Samsung Electronics DS", "Samsung Display"):
        if "/subsid/" in path:
            return False
        return any(token in path for token in ("/recruit/", "/apply/", "/posting/", "/job/"))

    if company in HANWHA_HINTS and source["source_type"].startswith("보조-"):
        host = p.netloc.lower()
        if "saramin.co.kr" in host and "/zf_user/jobs/relay/view" not in path:
            return False
        if "jobkorea.co.kr" in host and "/recruit/gi_read/" not in path:
            return False
        hints = HANWHA_HINTS[company]
        if not any(h in combined.lower() for h in hints):
            return False

    if "글로벌" in source["source_type"] and company in KOREA_REQUIRED_GLOBAL:
        if not korea_signal(combined):
            return False

    return True


def score_link(source: dict, url: str, title: str, context: str, jid: str) -> float:
    p = urlparse(url)
    query_keys = {k.lower() for k, _ in parse_qsl(p.query)}
    score = 0.0
    if any(token in p.path.lower() for token in DETAIL_TOKENS):
        score += 2.0
    if query_keys & {
        "idx", "seq", "jobid", "job_id", "reqid", "req_id", "wr_id",
        "titleid", "wantedauthno", "gh_jid", "postingid", "b_idx"
    }:
        score += 2.5
    if jid:
        score += 2.5
    if JOB_WORDS.search(title):
        score += 1.0
    if re.search(r"\d", url):
        score += 0.5
    if korea_signal(f"{title} {context} {url}"):
        score += 1.0
    if source["source_type"].startswith("공식-"):
        score += 0.5

    if source["company"] == "NNFC" and re.search(r"/B[0-9A-Za-z]+\.do$", p.path):
        score += 3.0

    return score


class Fetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/154 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        })

    def __enter__(self):
        self.pw = sync_playwright().start()
        self.browser = self.pw.chromium.launch(headless=True, args=["--no-sandbox"])
        return self

    def __exit__(self, *args):
        self.browser.close()
        self.pw.stop()

    def request_html(self, url: str):
        response = self.session.get(url, timeout=20, allow_redirects=True)
        response.raise_for_status()
        return response.text or ""

    def browser_html(self, url: str):
        page = self.browser.new_page(viewport={"width": 1440, "height": 1200})
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=40000)
            page.wait_for_timeout(1400)

            for _ in range(5):
                clicked = False
                for label in BROWSER_MORE_TEXT:
                    try:
                        locator = page.get_by_text(label, exact=False)
                        count = min(locator.count(), 3)
                        for idx in range(count):
                            item = locator.nth(idx)
                            if item.is_visible():
                                item.click(timeout=1200)
                                page.wait_for_timeout(500)
                                clicked = True
                                break
                        if clicked:
                            break
                    except Exception:
                        continue
                if not clicked:
                    break

            for _ in range(8):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(350)
            return page.content()
        finally:
            page.close()


def extract(source: dict, html: str, parser: str):
    soup = BeautifulSoup(html, "html.parser")
    results = {}
    for anchor in soup.find_all("a", href=True):
        url = canonical(urljoin(source["url"], clean(anchor.get("href"))))
        if not url or url == canonical(source["url"]):
            continue

        title = clean(anchor.get_text(" ", strip=True)) or clean(anchor.get("title"))
        if not title or len(title) < 3 or title.lower() in IGNORE_TEXT:
            continue

        context = anchor_context(anchor)
        combined_context = f"{title} {context}"
        if obviously_closed(combined_context):
            continue

        # Public research-institute boards often retain years of closed notices.
        # When a card exposes an explicit application period, reject it only when
        # at least two dates are visible and the latest date is already past.
        if source["company"] in {"KETI", "KIMS", "KIST", "KRISS", "ETRI", "NNFC", "KBSI"}:
            period_dates = parse_dates(combined_context)
            if len(period_dates) >= 2 and max(period_dates) < date.today():
                continue

        if not url_allowed(source, url, title, context):
            continue

        jid = extract_job_id(url, f"{title} {context}")
        score = score_link(source, url, title, context, jid)
        if score < 3.0:
            continue

        confidence = round(max(0.1, min(0.99, 0.42 + score * 0.075)), 2)
        key_seed = jid or hashlib.sha1(url.encode()).hexdigest()[:16]
        job_key = f"{source['company']}|{key_seed}"
        item = {
            "company": source["company"],
            "source_type": source["source_type"],
            "source_url": source["url"],
            "source_priority": source["priority"],
            "job_key": job_key,
            "job_id": jid,
            "title": title[:350],
            "location_signal": korea_signal(f"{title} {context} {url}"),
            "detail_url": url,
            "parser": parser,
            "confidence": confidence,
            "raw_hash": hashlib.sha1(f"{title}|{url}".encode()).hexdigest()[:16],
        }
        old = results.get(job_key)
        if old is None or confidence > old["confidence"]:
            results[job_key] = item
    return list(results.values())[:500]


def merge_candidates(*groups):
    merged = {}
    for group in groups:
        for item in group:
            old = merged.get(item["job_key"])
            if old is None or item["confidence"] > old["confidence"]:
                merged[item["job_key"]] = item
    return list(merged.values())


def build_candidate(
    source: dict,
    title: str,
    url: str,
    job_id: str = "",
    location: str = "",
    parser: str = "adapter",
    confidence: float = 0.95,
):
    url = canonical(url)
    title = clean(title)
    job_id = clean(job_id) or extract_job_id(url, title)
    if not url or not title:
        return None
    key_seed = job_id or hashlib.sha1(url.encode()).hexdigest()[:16]
    return {
        "company": source["company"],
        "source_type": source["source_type"],
        "source_url": source["url"],
        "source_priority": source["priority"],
        "job_key": f"{source['company']}|{key_seed}",
        "job_id": job_id,
        "title": title[:350],
        "location_signal": clean(location)[:200],
        "detail_url": url,
        "parser": parser,
        "confidence": confidence,
        "raw_hash": hashlib.sha1(f"{title}|{url}".encode()).hexdigest()[:16],
    }


def workday_spec(source: dict):
    p = urlparse(source["url"])
    host = p.netloc.lower()
    segments = [x for x in p.path.split("/") if x]

    if "myworkdayjobs.com" in host:
        tenant = host.split(".")[0]
        site = next((x for x in segments if x.lower() not in {"en-us", "ko-kr"}), "")
        if not site:
            return None
        api_host = host
        ui_base = f"https://{host}/en-US/{site}"
        return tenant, site, api_host, ui_base

    if "myworkdaysite.com" in host and len(segments) >= 3 and segments[0].lower() == "recruiting":
        tenant, site = segments[1], segments[2]
        api_root = host.replace("myworkdaysite.com", "myworkdayjobs.com")
        api_host = f"{tenant}.{api_root}"
        ui_base = f"https://{api_host}/en-US/{site}"
        return tenant, site, api_host, ui_base

    return None


def collect_workday_api(fetcher, source: dict):
    spec = workday_spec(source)
    if not spec:
        return [], ""
    tenant, site, api_host, ui_base = spec
    endpoint = f"https://{api_host}/wday/cxs/{tenant}/{site}/jobs"
    results = {}
    offset = 0
    limit = 20
    total = None

    try:
        while offset < 500:
            response = fetcher.session.post(
                endpoint,
                json={
                    "appliedFacets": {},
                    "limit": limit,
                    "offset": offset,
                    "searchText": "",
                },
                timeout=20,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            postings = payload.get("jobPostings") or []
            if total is None:
                total = int(payload.get("total") or 0)

            for posting in postings:
                title = clean(posting.get("title"))
                external = clean(posting.get("externalPath"))
                location = clean(posting.get("locationsText"))
                bullets = " ".join(clean(x) for x in (posting.get("bulletFields") or []))
                combined = f"{title} {location} {bullets} {external}"
                if source["company"] in {"KLA Korea", "Onto Innovation Korea"} and not korea_signal(combined):
                    continue
                if not external:
                    continue
                url = canonical(ui_base.rstrip("/") + "/" + external.lstrip("/"))
                jid = extract_job_id(url, combined)
                item = build_candidate(
                    source, title, url, jid, location or korea_signal(combined),
                    "workday-api", 0.99,
                )
                if item:
                    results[item["job_key"]] = item

            offset += len(postings)
            if not postings or len(postings) < limit or (total and offset >= total):
                break

        return list(results.values()), ""
    except Exception as exc:
        return [], f"workday-api {type(exc).__name__}: {exc}"[:1000]


def collect_asm_greenhouse(fetcher, source: dict):
    if source["company"] != "ASM Korea":
        return [], ""
    endpoint = "https://boards-api.greenhouse.io/v1/boards/asm/jobs?content=true"
    results = {}
    try:
        response = fetcher.session.get(endpoint, timeout=20)
        response.raise_for_status()
        jobs = response.json().get("jobs") or []
        for job in jobs:
            location = clean((job.get("location") or {}).get("name"))
            if not korea_signal(location):
                continue
            jid = clean(job.get("id"))
            title = clean(job.get("title"))
            url = clean(job.get("absolute_url"))
            item = build_candidate(
                source, title, url, jid, location, "greenhouse-api", 0.99
            )
            if item:
                results[item["job_key"]] = item
        return list(results.values()), ""
    except Exception as exc:
        return [], f"greenhouse-api {type(exc).__name__}: {exc}"[:1000]


LAM_LOCATION_TERMS = (
    "korea",
    "south korea",
    "yongin",
    "icheon",
    "yongin or icheon",
    "pyeongtaek",
    "cheongju",
    "seoul",
)


def lam_heading(soup, fallback: str) -> str:
    for tag in soup.find_all(["h1", "h2", "h3"]):
        value = clean(tag.get_text(" ", strip=True))
        if value and value.lower() not in {"jobs", "single position", "careers at lam research"}:
            if JOB_WORDS.search(value):
                return value
    return fallback


def collect_lam_korea(fetcher, source: dict):
    if source["company"] != "Lam Research Korea" or "글로벌" not in source["source_type"]:
        return [], ""

    listing_links = {}
    errors = []
    base = "https://careers.lamresearch.com/careers"

    for term in LAM_LOCATION_TERMS:
        consecutive_empty = 0
        for start in (0, 10, 20, 30, 40):
            url = f"{base}?filter_rmk_location={quote_plus(term)}&sort_by=timestamp&start={start}"
            try:
                response = fetcher.session.get(url, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                found_here = 0
                for anchor in soup.find_all("a", href=True):
                    href = clean(anchor.get("href"))
                    full = canonical(urljoin(base, href))
                    path = urlparse(full).path.lower()
                    if "/careers/job/" not in path and not (
                        "/careers/apply" in path and "pid=" in full
                    ):
                        continue
                    title = clean(anchor.get_text(" ", strip=True)) or clean(anchor.get("title"))
                    if not title or title.lower() in IGNORE_TEXT:
                        continue
                    context = anchor_context(anchor, max_levels=5, max_chars=1600)
                    combined = f"{title} {context} {full}"
                    if not korea_signal(combined) and not re.search(r"\bKR[-\s]", combined, re.I):
                        continue
                    listing_links.setdefault(full, title)
                    found_here += 1

                if found_here == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
                if consecutive_empty >= 2:
                    break
            except Exception as exc:
                errors.append(f"{term}:{start}:{type(exc).__name__}")
                break

    results = {}
    for url, fallback_title in list(listing_links.items())[:100]:
        try:
            response = fetcher.session.get(url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            text_value = clean(soup.get_text(" ", strip=True))
            if "no longer accepting applications" in text_value.lower():
                continue
            if not korea_signal(text_value) and not re.search(r"\bKR[-\s]", text_value, re.I):
                continue

            match = re.search(r"Requisition ID\s*([0-9]{5,7})\b", text_value, re.I)
            jid = match.group(1) if match else extract_job_id(url, text_value)
            title = lam_heading(soup, fallback_title)
            loc_match = re.search(
                r"Work Location\s+(.{1,120}?)(?:Work Country|The group|The impact|Job description)",
                text_value,
                re.I,
            )
            location = clean(loc_match.group(1)) if loc_match else korea_signal(text_value)
            item = build_candidate(
                source, title, url, jid, location, "lam-korea-search", 0.98
            )
            if item:
                results[item["job_key"]] = item
        except Exception as exc:
            errors.append(f"detail:{type(exc).__name__}")

    error = ",".join(errors[:8])
    return list(results.values()), error


def collect_platform_adapter(fetcher, source: dict):
    groups = []
    errors = []

    workday_items, workday_error = collect_workday_api(fetcher, source)
    if workday_items:
        groups.append(workday_items)
    if workday_error:
        errors.append(workday_error)

    asm_items, asm_error = collect_asm_greenhouse(fetcher, source)
    if asm_items:
        groups.append(asm_items)
    if asm_error:
        errors.append(asm_error)

    lam_items, lam_error = collect_lam_korea(fetcher, source)
    if lam_items:
        groups.append(lam_items)
    if lam_error:
        errors.append(lam_error)

    return merge_candidates(*groups), "; ".join(errors)[:1500]


def previous_snapshot() -> dict[str, int]:
    path = DATA / "latest_snapshot.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        counts = {}
        for item in payload.get("candidates", []):
            counts.setdefault(item["company"], set()).add(item["job_key"])
        return {company: len(keys) for company, keys in counts.items()}
    except Exception:
        return {}


def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def collect_one(fetcher: Fetcher, source: dict):
    request_error = ""
    request_items = []
    browser_items = []
    adapter_items = []
    request_loaded = False
    browser_loaded = False

    adapter_error = ""
    try:
        adapter_items, adapter_error = collect_platform_adapter(fetcher, source)
    except Exception as exc:
        adapter_error = f"adapter {type(exc).__name__}: {exc}"[:1500]

    try:
        html = fetcher.request_html(source["url"])
        request_loaded = True
        if html:
            request_items = extract(source, html, "requests")
    except Exception as exc:
        request_error = f"{type(exc).__name__}: {exc}"

    browser_error = ""
    # The browser remains an independent fallback even when an API adapter exists.
    # This helps detect platform changes where one path silently becomes incomplete.
    if source_needs_browser(source, len(request_items)):
        try:
            html = fetcher.browser_html(source["url"])
            browser_loaded = True
            if html:
                browser_items = extract(source, html, "playwright")
        except Exception as exc:
            browser_error = f"{type(exc).__name__}: {exc}"

    final_items = merge_candidates(adapter_items, request_items, browser_items)
    parsers = []
    if adapter_items:
        parsers.append("adapter")
    if request_loaded:
        parsers.append("requests")
    if browser_loaded:
        parsers.append("playwright")
    parser = "+".join(parsers)

    success = bool(adapter_items) or request_loaded or browser_loaded
    error = "; ".join(
        x for x in (adapter_error, request_error, browser_error) if x
    )[:2000]

    return final_items, {
        "company": source["company"],
        "source_type": source["source_type"],
        "url": source["url"],
        "success": success,
        "parser": parser,
        "adapter_candidate_count": len(adapter_items),
        "request_candidate_count": len(request_items),
        "browser_candidate_count": len(browser_items),
        "candidate_count": len(final_items),
        "error": error,
    }


def main():
    parser = argparse.ArgumentParser(description="Independent GitHub blind-spot collector v2.2")
    parser.add_argument("--company", default="", help="Optional company substring for manual debug")
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    started_at = now_iso()
    cfg, companies, sources = load_config(args.company)
    previous = previous_snapshot()

    merged = {}
    statuses = []

    with Fetcher() as fetcher:
        for index, source in enumerate(sources, 1):
            print(f"[{index}/{len(sources)}] {source['company']} | {source['source_type']}", flush=True)
            items, status = collect_one(fetcher, source)
            statuses.append(status)
            print(
                f"  adapter={status['adapter_candidate_count']} "
                f"requests={status['request_candidate_count']} "
                f"browser={status['browser_candidate_count']} final={status['candidate_count']} "
                f"parser={status['parser'] or 'none'}",
                flush=True,
            )
            for item in items:
                old = merged.get(item["job_key"])
                if old is None or item["confidence"] > old["confidence"]:
                    merged[item["job_key"]] = item

    candidates = sorted(merged.values(), key=lambda x: (x["company"], x["job_key"]))
    counts = {}
    for candidate in candidates:
        counts[candidate["company"]] = counts.get(candidate["company"], 0) + 1

    signals = []
    for status in statuses:
        if not status["success"]:
            signals.append({
                "company": status["company"],
                "signal": "SOURCE_ERROR",
                "severity": "HIGH",
                "source_type": status["source_type"],
                "url": status["url"],
                "detail": status["error"],
            })
        elif status["candidate_count"] == 0:
            detail = "Source loaded but no accepted job-detail candidates were extracted."
            if status["error"]:
                detail += f" Partial fetch error: {status['error']}"
            signals.append({
                "company": status["company"],
                "signal": "SOURCE_EMPTY",
                "severity": "LOW",
                "source_type": status["source_type"],
                "url": status["url"],
                "detail": detail + " This is not proof of zero openings.",
            })

    for company in companies:
        old_count = previous.get(company, 0)
        new_count = counts.get(company, 0)
        if old_count >= 5 and new_count < max(2, old_count // 2):
            signals.append({
                "company": company,
                "signal": "COUNT_DROP",
                "severity": "MEDIUM",
                "source_type": "",
                "url": "",
                "detail": f"Candidate count dropped from {old_count} to {new_count}.",
            })

    finished_at = now_iso()
    snapshot = {
        "schema_version": 2,
        "collector_version": VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "company_filter": args.company,
        "company_count": len(companies),
        "source_count": len(sources),
        "candidate_count": len(candidates),
        "candidate_count_by_company": counts,
        "candidates": candidates,
    }
    status_payload = {
        "schema_version": 2,
        "collector_version": VERSION,
        "run_id": run_id,
        "generated_at": finished_at,
        "statuses": statuses,
        "signals": signals,
    }
    run_payload = {
        "schema_version": 2,
        "collector_version": VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "company_count": len(companies),
        "source_count": len(sources),
        "candidate_count": len(candidates),
        "signal_count": len(signals),
        "error_count": sum(1 for s in statuses if not s["success"]),
        "config_generated_at": cfg.get("generated_at", ""),
        "github_sha": os.environ.get("GITHUB_SHA", ""),
        "github_event": os.environ.get("GITHUB_EVENT_NAME", "local"),
    }

    write_json(DATA / "latest_snapshot.json", snapshot)
    write_json(DATA / "latest_status.json", status_payload)
    write_json(DATA / "latest_run.json", run_payload)
    write_json(ARTIFACTS / "snapshot.json", snapshot)
    write_json(ARTIFACTS / "status.json", status_payload)

    print(json.dumps(run_payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
