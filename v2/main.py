from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

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
    r"연구|개발|공정|소자|장비|채용|모집)", re.I
)
KOREA = (
    "korea", "south korea", "대한민국", "한국", "seoul", "서울", "hwaseong", "화성",
    "icheon", "이천", "yongin", "용인", "pyeongtaek", "평택", "suwon", "수원",
    "cheongju", "청주", "daejeon", "대전", "dongtan", "동탄"
)
IGNORE = {
    "home", "careers", "career", "jobs", "채용", "채용공고", "지원하기", "apply",
    "login", "로그인", "privacy", "terms", "about", "faq", "news", "contact"
}


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
        "postingid", "wantedauthno", "wr_id", "seq", "idx", "titleid"
    ):
        if clean(q.get(key)):
            return clean(q[key])
    target = f"{p.path} {text}"
    for pattern in (
        r"\bJ-\d{6,}\b", r"\bR-\d{3,}\b", r"\bRC\d{8,}\b",
        r"(?:/job/|/jobs/|job-)(\d{6,12})(?:\b|/|-)",
        r"(?:requisition|req)[-_ /:]?(\d{5,12})\b",
    ):
        match = re.search(pattern, target, re.I)
        if match:
            return match.group(1) if match.lastindex else match.group(0)
    return ""


def korea_signal(text: str) -> str:
    lowered = clean(text).lower()
    return next((signal for signal in KOREA if signal in lowered), "")


def load_config(company_filter: str = ""):
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    companies = [c["name"] for c in cfg["companies"]]
    if company_filter:
        companies = [c for c in companies if company_filter.lower() in c.lower()]
    allowed = set(companies)
    sources = [s for s in cfg["sources"] if s["company"] in allowed]
    sources.sort(key=lambda x: (companies.index(x["company"]), x["priority"], x["source_type"]))
    return cfg, companies, sources


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

    def get(self, url: str):
        request_error = ""
        try:
            response = self.session.get(url, timeout=25, allow_redirects=True)
            response.raise_for_status()
            body = response.text or ""
            if len(body) >= 3000 and "javascript is required" not in body.lower():
                return body, "requests"
            request_error = f"requests content insufficient: {len(body)} chars"
        except Exception as exc:
            request_error = f"requests {type(exc).__name__}: {exc}"

        page = self.browser.new_page(viewport={"width": 1440, "height": 1000})
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(1200)
            for _ in range(5):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(450)
            return page.content(), "playwright"
        except Exception as exc:
            raise RuntimeError(f"{request_error}; playwright {type(exc).__name__}: {exc}") from exc
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
        if not title or len(title) < 3 or title.lower() in IGNORE:
            continue
        context = clean(anchor.parent.get_text(" ", strip=True) if anchor.parent else "")[:500]
        jid = extract_job_id(url, f"{title} {context}")
        parsed = urlparse(url)
        query_keys = {k.lower() for k, _ in parse_qsl(parsed.query)}

        score = 0.0
        if any(token in parsed.path.lower() for token in DETAIL_TOKENS):
            score += 2.0
        if query_keys & {
            "idx", "seq", "jobid", "job_id", "reqid", "req_id", "wr_id",
            "titleid", "wantedauthno", "gh_jid", "postingid"
        }:
            score += 2.5
        if jid:
            score += 2.5
        if JOB_WORDS.search(title):
            score += 1.0
        if re.search(r"\d", url):
            score += 0.5

        combined = f"{title} {context} {url}"
        if "글로벌" in source["source_type"] and not korea_signal(combined):
            score -= 3.0
        if score < 2.5:
            continue

        confidence = round(max(0.1, min(0.98, 0.45 + score * 0.08)), 2)
        key_seed = jid or hashlib.sha1(url.encode()).hexdigest()[:16]
        job_key = f"{source['company']}|{key_seed}"
        item = {
            "company": source["company"],
            "source_type": source["source_type"],
            "source_url": source["url"],
            "source_priority": source["priority"],
            "job_key": job_key,
            "job_id": jid,
            "title": title[:300],
            "location_signal": korea_signal(combined),
            "detail_url": url,
            "parser": parser,
            "confidence": confidence,
            "raw_hash": hashlib.sha1(f"{title}|{url}".encode()).hexdigest()[:16],
        }
        if job_key not in results or confidence > results[job_key]["confidence"]:
            results[job_key] = item
    return list(results.values())[:500]


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


def main():
    parser = argparse.ArgumentParser(description="Independent GitHub blind-spot collector v2")
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
            try:
                html, parser_name = fetcher.get(source["url"])
                candidates = extract(source, html, parser_name)
                statuses.append({
                    "company": source["company"],
                    "source_type": source["source_type"],
                    "url": source["url"],
                    "success": True,
                    "parser": parser_name,
                    "candidate_count": len(candidates),
                    "error": "",
                })
                for candidate in candidates:
                    old = merged.get(candidate["job_key"])
                    if old is None or candidate["confidence"] > old["confidence"]:
                        merged[candidate["job_key"]] = candidate
            except Exception as exc:
                statuses.append({
                    "company": source["company"],
                    "source_type": source["source_type"],
                    "url": source["url"],
                    "success": False,
                    "parser": "",
                    "candidate_count": 0,
                    "error": f"{type(exc).__name__}: {exc}"[:1000],
                })

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
            signals.append({
                "company": status["company"],
                "signal": "SOURCE_EMPTY",
                "severity": "LOW",
                "source_type": status["source_type"],
                "url": status["url"],
                "detail": "Source loaded but no detail candidates were extracted. This is not proof of zero openings.",
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
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "company_filter": args.company,
        "company_count": len(companies),
        "source_count": len(sources),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    status_payload = {
        "schema_version": 2,
        "run_id": run_id,
        "generated_at": finished_at,
        "statuses": statuses,
        "signals": signals,
    }
    run_payload = {
        "schema_version": 2,
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
