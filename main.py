from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import os

from adapters.platforms import DEBUG_DIR, DEBUG_FIELDS, SearchPlatformAdapter
from adapters.registry import build_adapter
from config.loader import load_config
from core.debug_csv import append_audit_event, append_audit_rows, append_summary_row, safe_token, write_csv_rows
from core.dedup import dedupe_records
from core.filtering import evaluate_record, filter_records
from core.models import CompanyConfig, JobRecord
from core.pipeline import reconcile_records
from sheets.google_sheets import GoogleSheetsClient
from state.state_manager import SheetStateManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sync", "init"], default="sync")
    parser.add_argument("--companies", default="", help="Comma-separated company names to process during sync. Empty means all companies.")
    parser.add_argument("--run-platforms", choices=["all", "none", "only", "others", "debug"], default="all")
    parser.add_argument("--debug-company", default="")
    parser.add_argument("--debug-platform", default="")
    return parser.parse_args()


FETCH_FIELDS = ["company", "source", "source_type", "title", "url", "canonical_url", "location", "employment_type", "recruitment_type", "search_keyword", "search_region_hint"]


def _write_fetch_csv(company_name: str, source_name: str, source_type: str, records: list[JobRecord]) -> None:
    rows = [{
        "company": company_name,
        "source": source_name,
        "source_type": source_type,
        "title": r.title,
        "url": r.url,
        "canonical_url": r.canonical_url,
        "location": r.location,
        "employment_type": r.employment_type,
        "recruitment_type": r.recruitment_type,
        "search_keyword": r.metadata.get("search_keyword", ""),
        "search_region_hint": r.metadata.get("search_region_hint", ""),
    } for r in records]
    write_csv_rows(f"fetch_{safe_token(company_name)}_{safe_token(source_name)}.csv", FETCH_FIELDS, rows, append=False)
    append_audit_rows("fetch", [{**row, "decision": "FETCH", "reason": "raw_fetch"} for row in rows])


def _append_filter_debug_rows(csv_path: Path, platform: str, company: str, records: list[JobRecord], filtered: list[JobRecord], config) -> None:
    if not records:
        return
    DEBUG_DIR.mkdir(exist_ok=True)
    filtered_keys = {r.unique_key for r in filtered}
    rows = []
    for record in records:
        _accepted, reason, include_matches, exclude_matches, hard_excludes, meta = evaluate_record(company, record, "platform", config.filters.include_keywords, config.filters.exclude_keywords)
        final = "PASS" if record.unique_key in filtered_keys else "REJECT"
        rows.append({
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "platform": platform,
            "company": company,
            "stage": "filter",
            "keyword": record.metadata.get("search_keyword", ""),
            "region_hint": record.metadata.get("search_region_hint", ""),
            "search_url": record.metadata.get("search_url", ""),
            "page_title": record.metadata.get("page_title", ""),
            "result_count_text": record.metadata.get("result_count_text", ""),
            "candidate_links_count": record.metadata.get("candidate_links_count", ""),
            "alias_candidates_count": record.metadata.get("alias_candidates_count", ""),
            "title": record.title,
            "url": record.url,
            "detail_title": record.title,
            "decision": final,
            "reason": reason,
            "include_matches": ", ".join(include_matches),
            "exclude_matches": ", ".join(exclude_matches),
            "hard_excludes": ", ".join(hard_excludes),
            "note": "score={} matched_keyword={} internship={}".format(
                meta["score"],
                meta["keyword"],
                "; ".join(f"{h['field']}:{h['term']}" for h in meta.get("internship_hits", [])) if meta.get("internship_hits") else "",
            ),
        })
    with csv_path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=DEBUG_FIELDS)
        if f.tell() == 0:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
    append_audit_rows("platform_filter", rows)


def _run_sources(company_cfg: CompanyConfig, config, grouped_records: dict[str, list[JobRecord]], grouped_source_scope: dict[str, set[str]]) -> None:
    for source_cfg in company_cfg.sources:
        adapter = build_adapter(company_cfg, source_cfg)
        source_label = source_cfg.meta.get("source_label", source_cfg.name)
        try:
            records = adapter.fetch()
            print(f"[INFO] fetched {len(records)} records from {company_cfg.name}/{source_cfg.name}")
        except Exception as exc:
            print(f"[WARN] {company_cfg.name}/{source_cfg.name}: {exc}")
            append_audit_event("source_error", company=company_cfg.name, source=source_cfg.name, source_type=source_cfg.source_type, decision="WARN", reason="fetch_failed", note=str(exc))
            records = []
        _write_fetch_csv(company_cfg.name, source_cfg.name, source_cfg.source_type, records)
        filtered = filter_records(company_cfg.name, source_cfg.name, source_cfg.source_type, records, config.filters.include_keywords, config.filters.exclude_keywords)
        append_audit_event("source_summary", company=company_cfg.name, source=source_cfg.name, source_type=source_cfg.source_type, decision="SUMMARY", reason="source_summary", note=f"fetched={len(records)} filtered={len(filtered)}")
        append_summary_row(kind="source", company=company_cfg.name, source=source_cfg.name, source_type=source_cfg.source_type, fetched=len(records), filtered=len(filtered), decision="SUMMARY", reason="source_summary")
        print(f"[INFO] source summary {company_cfg.name}/{source_cfg.name}: fetched={len(records)} filtered={len(filtered)}")
        grouped_records[company_cfg.name].extend(filtered)
        grouped_source_scope[company_cfg.name].add(source_label)


def _should_run_platform(source_name: str, run_mode: str, debug_platform: str) -> bool:
    if run_mode in {"only", "all"}:
        return True
    if run_mode == "others":
        return source_name in {"사람인", "잡코리아"}
    if run_mode == "debug":
        return source_name == debug_platform
    return False


def _run_platforms(config, selected_companies: set[str], grouped_records: dict[str, list[JobRecord]], grouped_source_scope: dict[str, set[str]], *, run_mode: str, debug_company: str, debug_platform: str) -> None:
    if not config.platform_sources:
        return
    if run_mode == "others" and os.getenv("ENABLE_PLATFORM_REGULAR_SYNC", "0") != "1":
        print("[INFO] platform regular sync is paused; set ENABLE_PLATFORM_REGULAR_SYNC=1 to run others mode")
        append_audit_event("platform_skip", company="ALL", source="platforms", decision="SKIP", reason="platform_regular_sync_paused")
        append_summary_row(kind="platform_skip", company="ALL", source="platforms", decision="SKIP", reason="platform_regular_sync_paused")
        return
    pseudo_company = CompanyConfig(name="채용플랫폼", sources=[])
    targets = {debug_company} if debug_company else set(selected_companies)
    for source_cfg in config.platform_sources:
        if not _should_run_platform(source_cfg.name, run_mode, debug_platform):
            continue
        source_cfg.meta = dict(source_cfg.meta or {})
        if targets:
            source_cfg.meta["target_companies"] = sorted(targets)
        source_cfg.meta["debug"] = (run_mode == "debug")
        source_cfg.meta["light_mode"] = (run_mode == "others")
        if run_mode == "others":
            source_cfg.meta["max_candidates"] = min(int(source_cfg.meta.get("max_candidates", 20)), 8)
            source_cfg.meta["max_details"] = min(int(source_cfg.meta.get("max_details", 10)), 4)
        adapter = SearchPlatformAdapter(pseudo_company, source_cfg)
        try:
            records = adapter.fetch()
            print(f"[INFO] fetched {len(records)} records from 채용플랫폼/{source_cfg.name}")
        except Exception as exc:
            print(f"[WARN] 채용플랫폼/{source_cfg.name}: {exc}")
            append_audit_event("platform_error", source=source_cfg.name, platform=source_cfg.name, decision="WARN", reason="fetch_failed", note=str(exc))
            records = []
        by_company: dict[str, list[JobRecord]] = defaultdict(list)
        for r in records:
            if targets and r.company not in targets:
                continue
            by_company[r.company].append(r)
        for company_name, recs in by_company.items():
            _write_fetch_csv(company_name, source_cfg.name, source_cfg.source_type, recs)
            filtered = filter_records(company_name, source_cfg.name, source_cfg.source_type, recs, config.filters.include_keywords, config.filters.exclude_keywords)
            append_audit_event("source_summary", company=company_name, source=source_cfg.name, source_type=source_cfg.source_type, platform=source_cfg.name, decision="SUMMARY", reason="source_summary", note=f"fetched={len(recs)} filtered={len(filtered)}")
            append_summary_row(kind="source", company=company_name, source=source_cfg.name, source_type=source_cfg.source_type, platform=source_cfg.name, fetched=len(recs), filtered=len(filtered), decision="SUMMARY", reason="source_summary")
            print(f"[INFO] source summary {company_name}/{source_cfg.name}: fetched={len(recs)} filtered={len(filtered)}")
            if run_mode == "debug":
                csv_path = DEBUG_DIR / f"debug_{company_name}_{source_cfg.name}.csv"
                _append_filter_debug_rows(csv_path, source_cfg.name, company_name, recs, filtered, config)
            grouped_records[company_name].extend(filtered)
            grouped_source_scope[company_name].add(source_cfg.meta.get("source_label", source_cfg.name))


def main() -> None:
    args = parse_args()
    config = load_config(Path("config/companies.yaml"))
    sheets = GoogleSheetsClient.from_env()

    real_company_names = [c.name for c in config.companies]
    if args.mode == "init":
        sheets.reset_and_initialize(real_company_names)
        append_audit_event("init", company="ALL", decision="INIT", reason="init_complete", note=f"initialized={len(real_company_names)}")
        print(f"[INFO] initialized {len(real_company_names)} company sheets + 종료공고 + _STATE")
        return

    selected_companies = {name.strip() for name in args.companies.split(",") if name.strip()}
    companies_to_process = config.companies
    if selected_companies:
        companies_to_process = [c for c in config.companies if c.name in selected_companies]
        print(f"[INFO] sync subset companies={sorted(selected_companies)}")

    state = SheetStateManager(sheets)
    grouped_records: dict[str, list[JobRecord]] = defaultdict(list)
    grouped_source_scope: dict[str, set[str]] = defaultdict(set)

    if args.run_platforms not in {"only", "others", "linkedin", "debug"}:
        for company_cfg in companies_to_process:
            _run_sources(company_cfg, config, grouped_records, grouped_source_scope)

    if args.run_platforms != "none":
        _run_platforms(config, selected_companies, grouped_records, grouped_source_scope, run_mode=args.run_platforms, debug_company=args.debug_company, debug_platform=args.debug_platform)

    if args.run_platforms == "debug":
        print("[INFO] debug mode is read-only; skipped sheet write/state flush")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")
    all_closed: list[JobRecord] = []
    targets = {args.debug_company} if args.debug_company else (selected_companies or set(real_company_names))
    append_only_history = os.getenv("APPEND_ONLY_HISTORY", "1") == "1"
    skip_sheet_write = os.getenv("SKIP_SHEET_WRITE", "0") == "1"
    for sheet_key in real_company_names:
        if sheet_key not in targets:
            continue
        source_scope = grouped_source_scope.get(sheet_key, set())
        incoming = grouped_records.get(sheet_key, [])
        if append_only_history:
            active = incoming
            closed = []
            append_audit_event("reconcile_summary", company=sheet_key, source=",".join(sorted(source_scope)), decision="SUMMARY", reason="append_only_history", note=f"incoming={len(incoming)} active={len(active)} closed=0")
            append_summary_row(kind="reconcile", company=sheet_key, source=",".join(sorted(source_scope)), written=len(active), closed=0, decision="SUMMARY", reason="append_only_history")
            print(f"[INFO] reconcile summary {sheet_key}: source_scope={sorted(source_scope)} deduped={len(incoming)} active={len(active)} closed=0")
            sheets.write_company_records(sheet_key, active, preserve_existing_order=False, append_only=True, write_to_sheet=not skip_sheet_write)
            print(f"[INFO] wrote {len(active)} active / 0 closed for {sheet_key}")
            continue
        deduped = dedupe_records(grouped_records.get(sheet_key, []), company_name=sheet_key)
        active, closed = reconcile_records(sheet_key, deduped, state, today_str=today_str, miss_threshold=config.runtime.miss_threshold, source_scope=source_scope)
        append_audit_event("reconcile_summary", company=sheet_key, source=",".join(sorted(source_scope)), decision="SUMMARY", reason="reconcile_summary", note=f"deduped={len(deduped)} active={len(active)} closed={len(closed)}")
        append_summary_row(kind="reconcile", company=sheet_key, source=",".join(sorted(source_scope)), written=len(active), closed=len(closed), decision="SUMMARY", reason="reconcile_summary")
        print(f"[INFO] reconcile summary {sheet_key}: source_scope={sorted(source_scope)} deduped={len(deduped)} active={len(active)} closed={len(closed)}")
        sheets.write_company_records(sheet_key, active, preserve_existing_order=True, append_only=False, write_to_sheet=not skip_sheet_write)
        all_closed.extend(closed)
        print(f"[INFO] wrote {len(active)} active / {len(closed)} closed for {sheet_key}")

    skip_state_flush = append_only_history or os.getenv("SKIP_STATE_FLUSH", "0") == "1"
    skip_closed_write = append_only_history or os.getenv("SKIP_CLOSED_WRITE", "0") == "1"

    if skip_state_flush:
        print("[INFO] skipped state flush")
    else:
        state.flush()

    if skip_closed_write:
        print("[INFO] skipped closed sheet write")
    else:
        sheets.write_closed_records(all_closed, write_to_sheet=not skip_sheet_write)
        append_audit_event("closed_write", company="종료공고", decision="SUMMARY", reason="closed_write", note=f"closed={len(all_closed)}")
        append_summary_row(kind="closed", company="종료공고", written=0, closed=len(all_closed), decision="SUMMARY", reason="closed_write")
        print(f"[INFO] wrote {len(all_closed)} total closed records to 종료공고")


if __name__ == "__main__":
    main()
