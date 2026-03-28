from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from adapters.platforms import SearchPlatformAdapter
from adapters.registry import build_adapter
from config.loader import load_config
from core.dedup import dedupe_records
from core.filtering import filter_records
from core.models import CompanyConfig, JobRecord, SourceConfig
from core.pipeline import reconcile_records
from sheets.google_sheets import GoogleSheetsClient
from state.state_manager import SheetStateManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sync", "init"], default="sync")
    parser.add_argument("--companies", default="", help="Comma-separated company names to process during sync. Empty means all companies.")
    parser.add_argument("--run-platforms", choices=["all", "none", "only", "linkedin", "others"], default="all")
    return parser.parse_args()


def _run_sources(company_cfg: CompanyConfig, config, grouped_records: dict[str, list[JobRecord]]) -> None:
    for source_cfg in company_cfg.sources:
        adapter = build_adapter(company_cfg, source_cfg)
        try:
            records = adapter.fetch()
            print(f"[INFO] fetched {len(records)} records from {company_cfg.name}/{source_cfg.name}")
        except Exception as exc:
            print(f"[WARN] {company_cfg.name}/{source_cfg.name}: {exc}")
            records = []
        filtered = filter_records(company_cfg.name, source_cfg.name, source_cfg.source_type, records, config.filters.include_keywords, config.filters.exclude_keywords)
        print(f"[INFO] kept {len(filtered)} records after filtering for {company_cfg.name}/{source_cfg.name}")
        grouped_records[company_cfg.name].extend(filtered)


def _run_platforms(config, selected_companies: set[str], grouped_records: dict[str, list[JobRecord]], mode: str = "all") -> None:
    if not config.platform_sources:
        return
    pseudo_company = CompanyConfig(name="채용플랫폼", sources=[])
    for source_cfg in config.platform_sources:
        src_name = source_cfg.meta.get("source_label", source_cfg.name)
        if mode == "linkedin" and src_name != "링크드인":
            continue
        if mode == "others" and src_name == "링크드인":
            continue
        source_cfg.meta = dict(source_cfg.meta or {})
        if selected_companies:
            source_cfg.meta["target_companies"] = sorted(selected_companies)
        adapter = SearchPlatformAdapter(pseudo_company, source_cfg)
        try:
            records = adapter.fetch()
            print(f"[INFO] fetched {len(records)} records from 채용플랫폼/{source_cfg.name}")
        except Exception as exc:
            print(f"[WARN] 채용플랫폼/{source_cfg.name}: {exc}")
            records = []
        # route by actual record.company
        by_company: dict[str, list[JobRecord]] = defaultdict(list)
        for r in records:
            if selected_companies and r.company not in selected_companies:
                continue
            by_company[r.company].append(r)
        for company_name, recs in by_company.items():
            filtered = filter_records(company_name, source_cfg.name, source_cfg.source_type, recs, config.filters.include_keywords, config.filters.exclude_keywords)
            print(f"[INFO] kept {len(filtered)} records after filtering for {company_name}/{source_cfg.name}")
            grouped_records[company_name].extend(filtered)


def main() -> None:
    args = parse_args()
    config = load_config(Path("config/companies.yaml"))
    sheets = GoogleSheetsClient.from_env()

    real_company_names = [c.name for c in config.companies]
    if args.mode == "init":
        sheets.reset_and_initialize(real_company_names)
        print(f"[INFO] initialized {len(real_company_names)} company sheets + 종료공고 + _STATE")
        return

    selected_companies = {name.strip() for name in args.companies.split(",") if name.strip()}
    companies_to_process = config.companies
    if selected_companies:
        companies_to_process = [c for c in config.companies if c.name in selected_companies]
        print(f"[INFO] sync subset companies={sorted(selected_companies)}")

    state = SheetStateManager(sheets)
    grouped_records: dict[str, list[JobRecord]] = defaultdict(list)

    if args.run_platforms != "only":
        for company_cfg in companies_to_process:
            _run_sources(company_cfg, config, grouped_records)

    if args.run_platforms != "none":
        plat_mode = "all" if args.run_platforms in ("all", "only") else args.run_platforms
        _run_platforms(config, selected_companies, grouped_records, mode=plat_mode)

    today_str = datetime.now().strftime("%Y-%m-%d")
    all_closed: list[JobRecord] = []
    targets = selected_companies or set(real_company_names)
    for sheet_key in real_company_names:
        if sheet_key not in targets:
            continue
        deduped = dedupe_records(grouped_records.get(sheet_key, []))
        active, closed = reconcile_records(sheet_key, deduped, state, today_str=today_str, miss_threshold=config.runtime.miss_threshold)
        sheets.write_company_records(sheet_key, active)
        all_closed.extend(closed)
        print(f"[INFO] wrote {len(active)} active / {len(closed)} closed for {sheet_key}")

    state.flush()
    sheets.write_closed_records(all_closed)
    print(f"[INFO] wrote {len(all_closed)} total closed records to 종료공고")


if __name__ == "__main__":
    main()
