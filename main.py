
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from adapters.registry import build_adapter
from config.loader import load_config
from core.dedup import dedupe_records
from core.filtering import filter_records
from core.models import JobRecord
from core.pipeline import reconcile_records
from sheets.google_sheets import GoogleSheetsClient
from state.state_manager import SheetStateManager


def main() -> None:
    config = load_config(Path("config/companies.yaml"))
    sheets = GoogleSheetsClient.from_env()
    state = SheetStateManager(sheets)

    grouped_records: dict[str, list[JobRecord]] = defaultdict(list)

    for company_cfg in config.companies:
        for source_cfg in company_cfg.sources:
            adapter = build_adapter(company_cfg, source_cfg)
            try:
                records = adapter.fetch()
            except Exception as exc:
                print(f"[WARN] {company_cfg.name}/{source_cfg.name}: {exc}")
                continue

            filtered = filter_records(
                company_name=company_cfg.name,
                source_name=source_cfg.name,
                records=records,
                include_keywords=config.filters.include_keywords,
                exclude_keywords=config.filters.exclude_keywords,
                education_rule=config.filters.education_rule,
            )
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
        sheets.write_active_records(sheet_key, active)
        sheets.write_closed_records(sheet_key, closed)

    state.flush()


if __name__ == "__main__":
    main()
