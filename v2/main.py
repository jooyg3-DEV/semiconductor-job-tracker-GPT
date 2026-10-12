from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import gspread
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
OUT = Path("debug_outputs")
SNAPSHOT = "GITHUB_SNAPSHOT"
GAPS = "GITHUB_GAPS"
RUNLOG = "GITHUB_RUN_LOG"

DETAIL_TOKENS = ("/job/", "/jobs/", "/career/", "/careers/", "/recruit/", "/recruitment/",
                 "/position/", "/vacanc", "/opening", "/notification/", "/view", "/detail", "/apply/")
KOREA = ("korea", "south korea", "대한민국", "한국", "seoul", "서울", "hwaseong", "화성",
         "icheon", "이천", "yongin", "용인", "pyeongtaek", "평택", "suwon", "수원",
         "cheongju", "청주", "daejeon", "대전", "dongtan", "동탄")
JOB_WORDS = re.compile(
    r"(engineer|scientist|research|process|application|field|device|metrology|yield|equipment|system|"
    r"연구|개발|공정|소자|장비|채용|모집)", re.I
)
IGNORE_TEXT = {"home", "careers", "career", "jobs", "채용", "채용공고", "지원하기", "apply",
               "login", "로그인", "privacy", "terms", "about", "faq", "news", "contact"}


@dataclass(frozen=True)
class Source:
    company: str
    source_type: str
    url: str
    priority: int


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def canonical(url):
    if not url:
        return ""
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        return ""
    keep = []
    for key, value in parse_qsl(p.query, keep_blank_values=True):
        k = key.lower()
        if k.startswith("utm_") or k in {"source", "src", "ref", "referrer", "gh_src", "_ga", "pvs"}:
            continue
        keep.append((key, value))
    path = re.sub(r"/{2,}", "/", p.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", urlencode(sorted(keep)), ""))


def job_id(url, text=""):
    p = urlparse(url)
    q = {k.lower(): v for k, v in parse_qsl(p.query)}
    for key in ("gh_jid", "jobid", "job_id", "reqid", "req_id", "requisitionid",
                "postingid", "wantedauthno", "wr_id", "seq", "idx", "titleid"):
        if clean(q.get(key)):
            return clean(q[key])
    target = f"{p.path} {text}"
    for pattern in (r"\bJ-\d{6,}\b", r"\bR-\d{3,}\b", r"\bRC\d{8,}\b",
                    r"(?:/job/|/jobs/|job-)(\d{6,12})(?:\b|/|-)",
                    r"(?:requisition|req)[-_ /:]?(\d{5,12})\b"):
        m = re.search(pattern, target, re.I)
        if m:
            return m.group(1) if m.lastindex else m.group(0)
    return ""


def korea_signal(text):
    lowered = clean(text).lower()
    return next((x for x in KOREA if x in lowered), "")


class Store:
    def __init__(self):
        info = json.loads(SERVICE_ACCOUNT_JSON)
        self.book = gspread.service_account_from_dict(info).open_by_key(SHEET_ID)

    def rows(self, name):
        return self.book.worksheet(name).get_all_records(default_blank="")

    def append(self, name, rows):
        if rows:
            self.book.worksheet(name).append_rows(rows, value_input_option="USER_ENTERED",
                                                  insert_data_option="INSERT_ROWS", table_range="A1")


class Fetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/154 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        })
        self.pw = None
        self.browser = None

    def __enter__(self):
        self.pw = sync_playwright().start()
        self.browser = self.pw.chromium.launch(headless=True, args=["--no-sandbox"])
        return self

    def __exit__(self, *args):
        if self.browser:
            self.browser.close()
        if self.pw:
            self.pw.stop()

    def html(self, url):
        try:
            r = self.session.get(url, timeout=25, allow_redirects=True)
            r.raise_for_status()
            body = r.text or ""
            if len(body) >= 3000 and "javascript is required" not in body.lower():
                return body, "requests"
        except Exception:
            pass
        page = self.browser.new_page(viewport={"width": 1440, "height": 1000})
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(1200)
            for _ in range(4):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(400)
            return page.content(), "playwright"
        finally:
            page.close()


def load_sources(store, company_filter=""):
    companies = [clean(r.get("회사")) for r in store.rows("COMPANIES")
                 if clean(r.get("감시")).upper() == "Y"]
    if company_filter:
        companies = [c for c in companies if company_filter.lower() in c.lower()]
    allowed = set(companies)
    sources = []
    for r in store.rows("SOURCES"):
        company = clean(r.get("회사"))
        if company not in allowed or clean(r.get("활성")).upper() != "Y":
            continue
        url = clean(r.get("URL"))
        if not url:
            continue
        try:
            priority = int(r.get("우선순위") or 99)
        except Exception:
            priority = 99
        sources.append(Source(company, clean(r.get("Source Type")), url, priority))
    sources.sort(key=lambda s: (companies.index(s.company), s.priority, s.source_type))
    return companies, sources


def master_index(store):
    ids, urls, active = set(), set(), {}
    for r in store.rows("JOBS_ALL"):
        jid = clean(r.get("Job ID"))
        url = canonical(clean(r.get("공식 URL")))
        aux = canonical(clean(r.get("보조 URL")))
        if jid:
            ids.add(jid)
        if url:
            urls.add(url)
        if aux:
            urls.add(aux)
        if clean(r.get("공고상태")).lower() == "active":
            active.setdefault(clean(r.get("회사")), []).append(r)
    return ids, urls, active


def previous_counts(store):
    counts, latest = {}, {}
    for r in store.rows(SNAPSHOT):
        company, run, key = clean(r.get("Company")), clean(r.get("Run ID")), clean(r.get("Job Key"))
        if not company or not run or not key:
            continue
        counts.setdefault((company, run), set()).add(key)
        latest[company] = max(run, latest.get(company, ""))
    return {c: len(counts.get((c, run), set())) for c, run in latest.items()}


def extract_candidates(source, html, parser):
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for a in soup.find_all("a", href=True):
        url = canonical(urljoin(source.url, clean(a.get("href"))))
        if not url or url == canonical(source.url):
            continue
        title = clean(a.get_text(" ", strip=True)) or clean(a.get("title"))
        if not title or title.lower() in IGNORE_TEXT or len(title) < 3:
            continue
        context = clean(a.parent.get_text(" ", strip=True) if a.parent else "")[:500]
        p = urlparse(url)
        qkeys = {k.lower() for k, _ in parse_qsl(p.query)}
        jid = job_id(url, f"{title} {context}")
        score = 0.0
        if any(t in p.path.lower() for t in DETAIL_TOKENS):
            score += 2
        if qkeys & {"idx", "seq", "jobid", "job_id", "reqid", "req_id", "wr_id", "titleid",
                    "wantedauthno", "gh_jid", "postingid"}:
            score += 2.5
        if jid:
            score += 2.5
        if JOB_WORDS.search(title):
            score += 1
        if re.search(r"\d", url):
            score += .5
        combined = f"{title} {context} {url}"
        if "글로벌" in source.source_type and not korea_signal(combined):
            score -= 3
        if score < 2.5:
            continue
        confidence = round(max(.1, min(.98, .45 + score * .08)), 2)
        keyseed = jid or hashlib.sha1(url.encode()).hexdigest()[:16]
        key = f"{source.company}|{keyseed}"
        item = {
            "company": source.company, "source_type": source.source_type, "source_url": source.url,
            "priority": source.priority, "job_key": key, "job_id": jid, "title": title[:300],
            "location": korea_signal(combined), "detail_url": url, "parser": parser,
            "confidence": confidence,
            "raw_hash": hashlib.sha1(f"{title}|{url}".encode()).hexdigest()[:16],
        }
        if key not in out or confidence > out[key]["confidence"]:
            out[key] = item
    return list(out.values())[:400]


def csv_out(name, rows, fields):
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / name).open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--company", default="")
    args = ap.parse_args()

    start = now_iso()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    store = Store()
    companies, sources = load_sources(store, args.company)
    known_ids, known_urls, active = master_index(store)
    prev = previous_counts(store)

    merged, status = {}, []
    with Fetcher() as fetch:
        for i, source in enumerate(sources, 1):
            print(f"[{i}/{len(sources)}] {source.company} | {source.source_type}", flush=True)
            try:
                html, parser = fetch.html(source.url)
                items = extract_candidates(source, html, parser)
                status.append((source, True, parser, len(items), ""))
                for x in items:
                    if x["job_key"] not in merged or x["confidence"] > merged[x["job_key"]]["confidence"]:
                        merged[x["job_key"]] = x
            except Exception as exc:
                status.append((source, False, "", 0, f"{type(exc).__name__}: {exc}"[:1000]))

    candidates = sorted(merged.values(), key=lambda x: (x["company"], x["job_key"]))
    seen = {}
    gaps = []
    detected = now_iso()

    for x in candidates:
        seen.setdefault(x["company"], []).append(x)
        matched = (x["job_id"] and x["job_id"] in known_ids) or x["detail_url"] in known_urls
        if not matched:
            gaps.append([run_id, detected, x["company"], "GITHUB_ONLY", x["job_key"], x["job_id"],
                         x["title"], x["detail_url"], x["source_type"],
                         "HIGH" if x["confidence"] >= .75 else "MEDIUM",
                         f"Candidate absent from JOBS_ALL by Job ID/canonical URL; confidence={x['confidence']}"])

    by_company_status = {}
    errors = 0
    for source, ok, parser, count, error in status:
        by_company_status.setdefault(source.company, []).append((ok, count))
        if not ok:
            errors += 1
            gaps.append([run_id, detected, source.company, "SOURCE_ERROR", "", "", "", source.url,
                         source.source_type, "HIGH", error])
        elif count == 0:
            gaps.append([run_id, detected, source.company, "SOURCE_EMPTY", "", "", "", source.url,
                         source.source_type, "LOW",
                         "Source loaded but no job-detail candidates were extracted. Not proof of zero openings."])

    for company in companies:
        current = seen.get(company, [])
        old_count = prev.get(company, 0)
        if old_count >= 5 and len(current) < max(2, old_count // 2):
            gaps.append([run_id, detected, company, "COUNT_DROP", "", "", "", "", "", "MEDIUM",
                         f"Candidate count dropped from {old_count} to {len(current)}"])
        current_ids = {x["job_id"] for x in current if x["job_id"]}
        current_urls = {x["detail_url"] for x in current}
        if any(ok and n > 0 for ok, n in by_company_status.get(company, [])):
            for r in active.get(company, []):
                jid = clean(r.get("Job ID"))
                url = canonical(clean(r.get("공식 URL")))
                if (jid and jid in current_ids) or (url and url in current_urls):
                    continue
                gaps.append([run_id, detected, company, "MASTER_ACTIVE_NOT_SEEN",
                             clean(r.get("Record ID")), jid, clean(r.get("공고명")), url, "", "LOW",
                             "Do not close automatically. Re-check official status in ChatGPT Job Tracker."])

    snapshot_rows = [[run_id, detected, x["company"], x["source_type"], x["source_url"], x["priority"],
                      x["job_key"], x["job_id"], x["title"], x["location"], x["detail_url"], x["parser"],
                      x["confidence"], x["raw_hash"]] for x in candidates]
    store.append(SNAPSHOT, snapshot_rows)
    store.append(GAPS, gaps)
    store.append(RUNLOG, [[run_id, start, now_iso(), len(companies), len(sources), len(candidates),
                           len(gaps), errors, os.environ.get("GITHUB_EVENT_NAME", "manual"),
                           os.environ.get("GITHUB_SHA", ""),
                           "Blind-spot detector only; master DB and Notion are never changed here."]])

    csv_out("snapshot.csv", candidates,
            ["company","source_type","source_url","priority","job_key","job_id","title","location",
             "detail_url","parser","confidence","raw_hash"])
    csv_out("gaps.csv", [dict(zip(["Run ID","Detected At","Company","Gap Type","Job Key","Job ID","Title",
                                   "Detail URL","Source Type","Severity","Detail"], r)) for r in gaps],
            ["Run ID","Detected At","Company","Gap Type","Job Key","Job ID","Title","Detail URL",
             "Source Type","Severity","Detail"])

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"## Job Tracker blind-spot v2\n\n- Companies: {len(companies)}\n- Sources: {len(sources)}\n"
                    f"- Candidates: {len(candidates)}\n- Gaps: {len(gaps)}\n- Source errors: {errors}\n")
    print(json.dumps({"run_id": run_id, "companies": len(companies), "sources": len(sources),
                      "candidates": len(candidates), "gaps": len(gaps), "errors": errors}, ensure_ascii=False))


if __name__ == "__main__":
    main()
