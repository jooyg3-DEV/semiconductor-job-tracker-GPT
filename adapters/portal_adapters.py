from __future__ import annotations

import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, collect_candidate_links, extract_job_id_from_url, parse_jobposting_json_ld, safe_goto
from core.models import JobRecord
from core.utils import clean_text, extract_education_and_experience, infer_job_function, is_phd_preferred


class GenericDetailPlaywrightAdapter(BaseAdapter):
    max_candidates = 40
    max_details = 20

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            candidates = collect_candidate_links(page, self.source_cfg.url, limit=self.max_candidates)
            browser.close()

        seen: set[str] = set()
        headers = {"User-Agent": USER_AGENT}
        for link_text, url in candidates[: self.max_details]:
            if url in seen:
                continue
            seen.add(url)
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            html = r.text
            soup = BeautifulSoup(html, "lxml")
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else link_text)
            raw = clean_text(soup.get_text(" ", strip=True))
            if not title and not raw:
                continue
            json_ld = parse_jobposting_json_ld(html)
            job_title = clean_text(json_ld.get("title") or title)
            location = clean_text((((json_ld.get("jobLocation") or {}).get("address") or {}).get("streetAddress")) if isinstance(json_ld.get("jobLocation"), dict) else "")
            employment_type = clean_text(json_ld.get("employmentType") or "")
            qualification = extract_education_and_experience(
                clean_text(json_ld.get("educationRequirements")) + " " + clean_text(json_ld.get("experienceRequirements")) + " " + raw
            )
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=job_title or link_text,
                url=url,
                raw_text=raw,
                deadline="없음",
                location=location,
                employment_type=employment_type,
                qualification=qualification,
                phd_preferred=is_phd_preferred(raw),
                job_id=extract_job_id_from_url(url),
            ))
        return records


class ApplyInAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 50
    max_details = 30

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(2500)
            candidates: list[tuple[str, str]] = []
            sels = ["a[href*='/jobs/']", "a[href*='jobview']", "a[href*='recruit/view']"]
            seen = set()
            for sel in sels:
                loc = page.locator(sel)
                for i in range(min(loc.count(), 100)):
                    a = loc.nth(i)
                    try:
                        href = a.get_attribute("href") or ""
                        text = clean_text(a.inner_text())
                    except Exception:
                        continue
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href in seen:
                        continue
                    seen.add(href)
                    candidates.append((text, href))
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        for text, url in candidates[: self.max_details]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else text)
            deadline = "없음"
            m = re.search(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2})", raw)
            if m:
                deadline = m.group(1).replace(".", "-").replace("/", "-")
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                deadline=deadline,
                job_id=extract_job_id_from_url(url),
            ))
        return records


class CareerLinkAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 50
    max_details = 30

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(2500)
            total_text = clean_text(page.locator("body").inner_text())
            if "총 0건" in total_text or "0건의 공고" in total_text:
                browser.close()
                return []
            candidates = collect_candidate_links(page, self.source_cfg.url, limit=self.max_candidates)
            browser.close()
        headers = {"User-Agent": USER_AGENT}
        for text, url in candidates[: self.max_details]:
            if "jobs" not in url and "RC" not in url:
                continue
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else text)
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                job_id=extract_job_id_from_url(url),
            ))
        return records


class WorkdayAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 80
    max_details = 30

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(4000)
            candidates = []
            seen = set()
            sels = [
                "a[href*='job/']",
                "a[data-automation-id='jobTitle']",
                "a[href*='job-details']",
                "a[href*='/External/job/']",
                "a[href*='/Search/job/']",
            ]
            for sel in sels:
                loc = page.locator(sel)
                for i in range(min(loc.count(), 120)):
                    node = loc.nth(i)
                    try:
                        href = node.get_attribute("href") or ""
                        text = clean_text(node.inner_text())
                    except Exception:
                        continue
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href in seen:
                        continue
                    seen.add(href)
                    candidates.append((text, href))
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        for text, url in candidates[: self.max_details]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            html = r.text
            soup = BeautifulSoup(html, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else text)
            json_ld = parse_jobposting_json_ld(html)
            location = clean_text((((json_ld.get("jobLocation") or {}).get("address") or {}).get("addressLocality")) if isinstance(json_ld.get("jobLocation"), dict) else "")
            employment_type = clean_text(json_ld.get("employmentType") or "")
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                location=location,
                employment_type=employment_type,
                job_id=extract_job_id_from_url(url),
            ))
        return records


class RecruiterAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 50
    max_details = 25


class ASMAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 80
    max_details = 40

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(3000)
            candidates = []
            seen = set()
            for sel in ["a[href*='vacancies']", "a[href*='job']", "a[href*='open-vacancies']"]:
                loc = page.locator(sel)
                for i in range(min(loc.count(), 120)):
                    node = loc.nth(i)
                    try:
                        href = node.get_attribute("href") or ""
                        text = clean_text(node.inner_text())
                    except Exception:
                        continue
                    if not href or len(text) < 3:
                        continue
                    if href.startswith("/"):
                        href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href in seen:
                        continue
                    seen.add(href)
                    candidates.append((text, href))
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        for text, url in candidates[: self.max_details]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            raw = clean_text(soup.get_text(" ", strip=True))
            title = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else text)
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get("source_label", self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                job_id=extract_job_id_from_url(url),
            ))
        return records
