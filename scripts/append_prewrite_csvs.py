from __future__ import annotations

import argparse
import csv
from pathlib import Path
from collections import defaultdict

from sheets.google_sheets import GoogleSheetsClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    root = Path(args.root)
    sheets = GoogleSheetsClient.from_env()
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for path in root.rglob('prewrite_*.csv'):
        with path.open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('is_valid_row') != 'Y':
                    continue
                company = (row.get('company') or '').strip()
                title = (row.get('title') or '').strip()
                url = (row.get('url') or '').strip()
                source = (row.get('source') or '').strip()
                if not (company and title and url and source):
                    continue
                grouped[company].append(row)
    for company, rows in grouped.items():
        sheets.append_company_csv_rows(company, rows)
        print(f'[INFO] appended {len(rows)} rows to {company}')


if __name__ == '__main__':
    main()
