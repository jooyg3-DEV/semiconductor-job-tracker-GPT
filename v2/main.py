from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

VERSION = "2.1"
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
    for pattern in (
        r"\bJ-\d{6,}\b", r"\bR\d{5,}\b", r"\bRC\d{8,}\b",
        r"(?:/job/|/jobs/|job-)(\d{5,12})(?:\b|/|-)",
        r"(?:requisition|req)[-_ /:]?(\d{5,12})\b",
    ):
        match = re.search(pattern, target, re.I)
        if match:
            return match.group(1) if match.lastindex else match.group(0)
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

    if company == "ASM Korea":
        if "/open-vacancies/" not in path:
            return False
        return bool(korea_signal(combined))

    if company == "SK hynix":
        if "talent.skhynix.com" in p.netloc.lower():
            return "/hub/ko/apply/job" in path and path.rstrip("/") != "/hub/ko/apply/job"
        if "skcareers.com" in p.netloc.lower():
            return "/recruit/detail/" in path
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

        parent = anchor.parent
        context = clean(parent.get_text(" ", strip=True) if parent else "")[:700]
        if obviously_closed(f"{title} {context}"):
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
    request_loaded = False
    browser_loaded = False

    try:
        html = fetcher.request_html(source["url"])
        request_loaded = True
        if html:
            request_items = extract(source, html, "requests")
    except Exception as exc:
        request_error = f"{type(exc).__name__}: {exc}"

    browser_error = ""
    if source_needs_browser(source, len(request_items)):
        try:
            html = fetcher.browser_html(source["url"])
            browser_loaded = True
            if html:
                browser_items = extract(source, html, "playwright")
        except Exception as exc:
            browser_error = f"{type(exc).__name__}: {exc}"

    final_items = merge_candidates(request_items, browser_items)
    parser = (
        "requests+playwright" if request_loaded and browser_loaded
        else "playwright" if browser_loaded
        else "requests" if request_loaded
        else ""
    )
    success = request_loaded or browser_loaded
    error = "; ".join(x for x in (request_error, browser_error) if x)[:1500]
    return final_items, {
        "company": source["company"],
        "source_type": source["source_type"],
        "url": source["url"],
        "success": success,
        "parser": parser,
        "request_candidate_count": len(request_items),
        "browser_candidate_count": len(browser_items),
        "candidate_count": len(final_items),
        "error": error,
    }


def main():
    parser = argparse.ArgumentParser(description="Independent GitHub blind-spot collector v2.1")
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
                f"  requests={status['request_candidate_count']} "
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
