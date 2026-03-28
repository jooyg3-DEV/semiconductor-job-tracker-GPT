from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from adapters.registry import build_adapter
from config.loader import load_config
from core.dedup import dedupe_records
from core.filtering import filter_records
from core.models import JobRecord
from core.pipeline import reconcile_records
from sheets.google_sheets import GoogleSheetsClient
from state.state_manager import SheetStateManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sync", "init"], default="sync")
    parser.add_argument(
        "--companies",
        default="",
        help="Comma-separated company names to process during sync. Empty means all companies.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(Path("config/companies.yaml"))
    sheets = GoogleSheetsClient.from_env()

    real_company_names = [c.name for c in config.companies if any(s.source_type == "official" for s in c.sources)]

    if args.mode == "init":
        sheets.reset_and_initialize(real_company_names)
        print(f"[INFO] initialized {len(real_company_names)} company sheets + 종료공고 + _STATE")
        return

    selected_companies = {name.strip() for name in args.companies.split(",") if name.strip()}
    if selected_companies:
        config.companies = [c for c in config.companies if c.name in selected_companies]
        real_company_names = [name for name in real_company_names if name in selected_companies]
        print(f"[INFO] sync subset companies={sorted(selected_companies)}")

    state = SheetStateManager(sheets)
    grouped_records: dict[str, list[JobRecord]] = defaultdict(list)

    for company_cfg in config.companies:
        for source_cfg in company_cfg.sources:
            adapter = build_adapter(company_cfg, source_cfg)
            try:
                records = adapter.fetch()
                print(f"[INFO] fetched {len(records)} records from {company_cfg.name}/{source_cfg.name}")
            except Exception as exc:
                print(f"[WARN] {company_cfg.name}/{source_cfg.name}: {exc}")
                records = []

            filtered = filter_records(
                company_name=company_cfg.name,
                source_name=source_cfg.name,
                records=records,
                include_keywords=config.filters.include_keywords,
                exclude_keywords=config.filters.exclude_keywords,
                education_rule=config.filters.education_rule,
            )
            print(f"[INFO] kept {len(filtered)} records after filtering for {company_cfg.name}/{source_cfg.name}")
            for record in filtered:
                grouped_records[record.sheet_key].append(record)

    today_str = datetime.now().strftime("%Y-%m-%d")
    all_closed: list[JobRecord] = []
    for company_name in real_company_names:
        deduped = dedupe_records(grouped_records.get(company_name, []))
        active, closed = reconcile_records(
            sheet_key=company_name,
            incoming_records=deduped,
            state_manager=state,
            today_str=today_str,
            miss_threshold=config.runtime.miss_threshold,
        )
        sheets.write_company_records(company_name, active)
        all_closed.extend(closed)
        print(f"[INFO] wrote {len(active)} active / {len(closed)} closed for {company_name}")

    sheets.write_closed_records(all_closed)
    print(f"[INFO] wrote {len(all_closed)} total closed records to 종료공고")
    state.flush()


if __name__ == "__main__":
    main()
