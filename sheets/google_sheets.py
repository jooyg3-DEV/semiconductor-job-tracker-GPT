
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from core.models import JobRecord

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = ["검색일", "출처", "마감일", "회사", "공고명", "지원자격", "채용직무", "근무지", "채용형태", "박사우대여부", "링크"]
STATE_HEADERS = ["sheet_key", "unique_key", "payload_json"]


class GoogleSheetsClient:
    def __init__(self, spreadsheet_id: str, gc: gspread.Client, service) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.gc = gc
        self.service = service
        self.sh = gc.open_by_key(spreadsheet_id)

    @classmethod
    def from_env(cls) -> "GoogleSheetsClient":
        raw = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        data = json.loads(raw)
        creds = Credentials.from_service_account_info(data, scopes=SCOPES)
        gc = gspread.authorize(creds)
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return cls(os.environ["GOOGLE_SHEET_ID"], gc, service)

    def _ensure_worksheet(self, title: str):
        try:
            return self.sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(title=title, rows=100, cols=20)

    def write_active_records(self, sheet_key: str, records: list[JobRecord]) -> None:
        ws = self._ensure_worksheet(sheet_key)
        today_str = datetime.now().strftime("%Y-%m-%d")
        values = [HEADERS] + [r.to_row(today_str) for r in records]
        ws.clear()
        ws.update(values=values, range_name="A1")
        self._basic_format(ws)
        self._sort_active_sheet(ws)

    def write_closed_records(self, sheet_key: str, records: list[JobRecord]) -> None:
        title = f"종료-{sheet_key}"
        ws = self._ensure_worksheet(title)
        today_str = datetime.now().strftime("%Y-%m-%d")
        values = [HEADERS] + [r.to_row(today_str) for r in records]
        ws.clear()
        ws.update(values=values, range_name="A1")
        self._basic_format(ws)
        self._apply_strikethrough(ws, start_row=2, end_row=max(2, len(values)))

    def _basic_format(self, ws) -> None:
        ws.freeze(rows=1)
        ws.format("A1:K1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
        })

    def _sort_active_sheet(self, ws) -> None:
        row_count = max(2, ws.row_count)
        body = {
            "requests": [
                {
                    "sortRange": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,
                            "endRowIndex": row_count,
                            "startColumnIndex": 0,
                            "endColumnIndex": 11,
                        },
                        "sortSpecs": [
                            {"dimensionIndex": 2, "sortOrder": "ASCENDING"},
                        ],
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()

    def _apply_strikethrough(self, ws, start_row: int, end_row: int) -> None:
        body = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": start_row - 1,
                            "endRowIndex": end_row,
                            "startColumnIndex": 0,
                            "endColumnIndex": 11,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"strikethrough": True}
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.strikethrough",
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body=body
        ).execute()

    def read_state_rows(self) -> list[list[str]]:
        ws = self._ensure_worksheet("_STATE")
        rows = ws.get_all_values()
        if not rows:
            ws.update("A1:C1", [STATE_HEADERS])
            return []
        return rows[1:]

    def write_state_rows(self, rows: list[list[str]]) -> None:
        ws = self._ensure_worksheet("_STATE")
        values = [STATE_HEADERS] + rows
        ws.clear()
        ws.update(values=values, range_name="A1")
