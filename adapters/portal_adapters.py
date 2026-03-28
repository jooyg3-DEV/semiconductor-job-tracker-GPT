from __future__ import annotations

import re
from urllib.parse import urlparse
import re

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from adapters.base import BaseAdapter
from adapters.playwright_utils import USER_AGENT, build_record_from_detail, collect_candidate_links, extract_job_id_from_url, parse_jobposting_json_ld, safe_goto
from core.models import JobRecord
from core.utils import clean_text, extract_education_and_experience, infer_job_function


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
            qual_source = " ".join([clean_text(json_ld.get("educationRequirements")), clean_text(json_ld.get("experienceRequirements")), clean_text(json_ld.get("qualifications")), clean_text(json_ld.get("responsibilities"))])
            qualification = extract_education_and_experience(qual_source or raw[:1200])
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
            html = page.content()
            for m in re.finditer(r'https://[^"\']*applyin\.co\.kr/jobs/\d+', html):
                href = m.group(0)
                candidates.append(("", href))
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
    max_candidates = 120
    max_details = 40

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        candidates = []
        seen = set()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(4500)
            html = page.content()
            for m in re.finditer(r"https://[^\"']*myworkdayjobs\.com[^\"']*/job/[^\"']+", html):
                href = m.group(0)
                if href not in seen:
                    seen.add(href)
                    candidates.append((href.rsplit('/', 1)[-1].replace('_', ' '), href))
            sels = [
                "a[href*='/job/']",
                "a[data-automation-id='jobTitle']",
                "a[href*='/Search/job/']",
                "a[href*='/UR/job/']",
            ]
            for sel in sels:
                loc = page.locator(sel)
                for i in range(min(loc.count(), 160)):
                    node = loc.nth(i)
                    try:
                        href = node.get_attribute('href') or ''
                        txt = clean_text(node.inner_text())
                    except Exception:
                        continue
                    if href.startswith('/'):
                        href = f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href and href not in seen:
                        seen.add(href)
                        candidates.append((txt, href))
            browser.close()

        headers = {"User-Agent": USER_AGENT}
        for text0, url in candidates[: self.max_details]:
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            raw = clean_text(soup.get_text(' ', strip=True))
            title = clean_text(soup.find('h1').get_text(' ', strip=True) if soup.find('h1') else text0)
            json_ld = parse_jobposting_json_ld(html)
            location = clean_text((((json_ld.get('jobLocation') or {}).get('address') or {}).get('addressLocality')) if isinstance(json_ld.get('jobLocation'), dict) else '')
            employment_type = clean_text(json_ld.get('employmentType') or '')
            records.append(build_record_from_detail(
                company=self.company_cfg.name,
                region=self.source_cfg.region,
                source_label=self.source_cfg.meta.get('source_label', self.source_cfg.name),
                title=title,
                url=url,
                raw_text=raw,
                location=location,
                employment_type=employment_type,
                job_id=extract_job_id_from_url(url),
            ))
        return records


class RecruiterAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 80
    max_details = 40

    def fetch(self) -> list[JobRecord]:
        records: list[JobRecord] = []
        candidates: list[tuple[str, str]] = []
        seen = set()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(2500)
            html = page.content()
            for m in re.finditer(r"https://[^\"']*recruiter\.co\.kr/career/jobs/\d+", html):
                href=m.group(0)
                if href not in seen:
                    seen.add(href); candidates.append(("", href))
            for m in re.finditer(r'/career/jobs/\d+', html):
                href=f"https://{urlparse(self.source_cfg.url).netloc}{m.group(0)}"
                if href not in seen:
                    seen.add(href); candidates.append(("", href))
            loc = page.locator("a[href*='/career/jobs/']")
            for i in range(min(loc.count(),120)):
                node=loc.nth(i)
                try:
                    href=node.get_attribute('href') or ''
                    text=clean_text(node.inner_text())
                except Exception:
                    continue
                if href.startswith('/'):
                    href=f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                if href and href not in seen:
                    seen.add(href); candidates.append((text, href))
            browser.close()
        headers={"User-Agent": USER_AGENT}
        for text0, url in candidates[:self.max_details]:
            try:
                r=requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            soup=BeautifulSoup(r.text, 'lxml')
            raw=clean_text(soup.get_text(' ', strip=True))
            title=clean_text(soup.find('h1').get_text(' ', strip=True) if soup.find('h1') else text0)
            deadline='없음'
            m=re.search(r'(20\d{2}[./-]\d{1,2}[./-]\d{1,2}|채용시|채용 시 마감)', raw)
            if m:
                deadline=m.group(1).replace('.', '-').replace('/', '-')
            records.append(build_record_from_detail(company=self.company_cfg.name, region=self.source_cfg.region, source_label=self.source_cfg.meta.get('source_label', self.source_cfg.name), title=title, url=url, raw_text=raw, deadline=deadline, job_id=extract_job_id_from_url(url)))
        return records


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


class AMDAdapter(GenericDetailPlaywrightAdapter):
    max_candidates = 80
    max_details = 30

    def fetch(self) -> list[JobRecord]:
        records=[]
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True)
            page=browser.new_page(user_agent=USER_AGENT)
            safe_goto(page, self.source_cfg.url)
            page.wait_for_timeout(3500)
            candidates=[]
            seen=set()
            html=page.content()
            for m in re.finditer(r'https://careers\.amd\.com/careers-home/jobs/\d+', html):
                href=m.group(0)
                if href not in seen:
                    seen.add(href); candidates.append(("", href))
            for sel in ["a[href*='/careers-home/jobs/']", "a[href*='/jobs/']"]:
                loc=page.locator(sel)
                for i in range(min(loc.count(),120)):
                    node=loc.nth(i)
                    try:
                        href=node.get_attribute('href') or ''
                        txt=clean_text(node.inner_text())
                    except Exception:
                        continue
                    if href.startswith('/'):
                        href=f"https://{urlparse(self.source_cfg.url).netloc}{href}"
                    if href and href not in seen:
                        seen.add(href); candidates.append((txt, href))
            browser.close()
        headers={"User-Agent": USER_AGENT}
        for text0,url in candidates[:self.max_details]:
            try:
                r=requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
            except Exception:
                continue
            html=r.text
            soup=BeautifulSoup(html,'lxml')
            raw=clean_text(soup.get_text(' ', strip=True))
            title=clean_text(soup.find('h1').get_text(' ', strip=True) if soup.find('h1') else text0)
            if not title:
                continue
            json_ld=parse_jobposting_json_ld(html)
            location=clean_text((((json_ld.get('jobLocation') or {}).get('address') or {}).get('addressLocality')) if isinstance(json_ld.get('jobLocation'), dict) else '')
            employment_type=clean_text(json_ld.get('employmentType') or '')
            qualification=extract_education_and_experience(' '.join([clean_text(json_ld.get('educationRequirements')), clean_text(json_ld.get('experienceRequirements')), clean_text(json_ld.get('qualifications')), raw[:800]]))
            records.append(build_record_from_detail(company=self.company_cfg.name, region=self.source_cfg.region, source_label=self.source_cfg.meta.get('source_label', self.source_cfg.name), title=title, url=url, raw_text=raw, location=location, employment_type=employment_type, qualification=qualification, job_id=extract_job_id_from_url(url)))
        return records
