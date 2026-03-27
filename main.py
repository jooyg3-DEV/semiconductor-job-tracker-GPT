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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(Path("config/companies.yaml"))
    sheets = GoogleSheetsClient.from_env()

    sheet_keys = []
    for company_cfg in config.companies:
        for source_cfg in company_cfg.sources:
            sheet_keys.append(f"{company_cfg.name}-{source_cfg.region}")
    sheet_keys = sorted(set(sheet_keys))

    if args.mode == "init":
        sheets.initialize_structure(sheet_keys)
        print(f"[INFO] initialized {len(sheet_keys)} active sheets + closed sheets + _STATE")
        return

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
            sheet_key = f"{company_cfg.name}-{source_cfg.region}"
            grouped_records.setdefault(sheet_key, [])
            for record in filtered:
                grouped_records[record.sheet_key].append(record)

    today_str = datetime.now().strftime("%Y-%m-%d")
    for sheet_key, records in grouped_records.items():
        deduped = dedupe_records(records)
        active, closed = reconcile_records(
            sheet_key=sheet_key,
            incoming_records=deduped,
            state_manager=state,
            today_str=today_str,
            miss_threshold=config.runtime.miss_threshold,
        )
        # sync mode에서는 미리 만든 탭에만 기록한다.
        if sheets.worksheet_exists(sheet_key):
            sheets.write_active_records(sheet_key, active, create_if_missing=False)
        else:
            print(f"[WARN] missing active sheet in sync mode: {sheet_key}")

        closed_title = f"종료-{sheet_key}"
        if sheets.worksheet_exists(closed_title):
            sheets.write_closed_records(sheet_key, closed, create_if_missing=False)
        else:
            print(f"[WARN] missing closed sheet in sync mode: {closed_title}")
        print(f"[INFO] wrote {len(active)} active / {len(closed)} closed for {sheet_key}")

    state.flush()


if __name__ == "__main__":
    main()
