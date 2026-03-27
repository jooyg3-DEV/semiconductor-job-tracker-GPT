from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Callable, TypeVar

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from core.models import JobRecord

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = ["검색일", "출처", "마감일", "회사", "공고명", "지원자격", "채용직무", "근무지", "채용형태", "박사우대여부", "링크"]
STATE_HEADERS = ["sheet_key", "unique_key", "payload_json"]
T = TypeVar("T")


class GoogleSheetsClient:
    def __init__(self, spreadsheet_id: str, gc: gspread.Client, service) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.gc = gc
        self.service = service
        self.sh = gc.open_by_key(spreadsheet_id)
        self._worksheet_cache: dict[str, gspread.Worksheet] = {}
        self._populate_cache()

    @classmethod
    def from_env(cls) -> "GoogleSheetsClient":
        raw = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        data = json.loads(raw)
        creds = Credentials.from_service_account_info(data, scopes=SCOPES)
        gc = gspread.authorize(creds)
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return cls(os.environ["GOOGLE_SHEET_ID"], gc, service)

    def _populate_cache(self) -> None:
        worksheets = self._with_retry(lambda: self.sh.worksheets())
        self._worksheet_cache = {ws.title: ws for ws in worksheets}

    def _with_retry(self, fn: Callable[[], T], *, retries: int = 6, base_sleep: float = 2.0) -> T:
        last_exc = None
        for attempt in range(retries):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                text = str(exc)
                is_retryable = False
                if isinstance(exc, gspread.exceptions.APIError):
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    is_retryable = status in (429, 500, 502, 503, 504)
                elif isinstance(exc, HttpError):
                    is_retryable = getattr(exc.resp, "status", None) in (429, 500, 502, 503, 504)
                elif "429" in text or "Quota exceeded" in text or "rateLimitExceeded" in text:
                    is_retryable = True

                if not is_retryable or attempt == retries - 1:
                    raise

                sleep_s = base_sleep * (2 ** attempt)
                print(f"[WARN] Google Sheets quota/backoff: retry in {sleep_s:.1f}s ({attempt + 1}/{retries})")
                time.sleep(sleep_s)
        raise last_exc  # pragma: no cover

    def _ensure_worksheet(self, title: str, *, create_if_missing: bool = True):
        ws = self._worksheet_cache.get(title)
        if ws is not None:
            return ws
        if not create_if_missing:
            return None
        ws = self._with_retry(lambda: self.sh.add_worksheet(title=title, rows=100, cols=20))
        self._worksheet_cache[title] = ws
        return ws

    def worksheet_exists(self, title: str) -> bool:
        return title in self._worksheet_cache

    def initialize_structure(self, sheet_keys: list[str]) -> None:
        ordered_titles: list[tuple[str, list[str]]] = []
        for sheet_key in sheet_keys:
            ordered_titles.append((sheet_key, HEADERS))
            ordered_titles.append((f"종료-{sheet_key}", HEADERS))
        ordered_titles.append(("_STATE", STATE_HEADERS))

        for title, headers in ordered_titles:
            if self.worksheet_exists(title):
                continue
            ws = self._ensure_worksheet(title, create_if_missing=True)
            self._replace_sheet_values(ws, [headers])
            print(f"[INFO] initialized worksheet {title}")
            time.sleep(1.0)

    def write_active_records(self, sheet_key: str, records: list[JobRecord], *, create_if_missing: bool = False) -> None:
        ws = self._ensure_worksheet(sheet_key, create_if_missing=create_if_missing)
        if ws is None:
            print(f"[WARN] active worksheet missing, skipped: {sheet_key}")
            return
        today_str = datetime.now().strftime("%Y-%m-%d")
        records = self._sorted_records(records)
        values = [HEADERS] + [r.to_row(today_str) for r in records]
        self._replace_sheet_values(ws, values)

    def write_closed_records(self, sheet_key: str, records: list[JobRecord], *, create_if_missing: bool = False) -> None:
        title = f"종료-{sheet_key}"
        ws = self._ensure_worksheet(title, create_if_missing=create_if_missing)
        if ws is None:
            print(f"[WARN] closed worksheet missing, skipped: {title}")
            return
        today_str = datetime.now().strftime("%Y-%m-%d")
        values = [HEADERS] + [r.to_row(today_str) for r in records]
        self._replace_sheet_values(ws, values)
        if records:
            self._apply_strikethrough(ws, start_row=2, end_row=len(values))

    def _replace_sheet_values(self, ws, values: list[list[str]]) -> None:
        end_col = self._col_letter(len(values[0]))
        end_row = max(len(values), 2)
        padded = values + ([[""] * len(values[0]) ] if len(values) == 1 else [])
        self._with_retry(lambda: ws.update(range_name=f"A1:{end_col}{end_row}", values=padded))

    @staticmethod
    def _col_letter(n: int) -> str:
        result = ""
        while n:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    @staticmethod
    def _sorted_records(records: list[JobRecord]) -> list[JobRecord]:
        def sort_key(r: JobRecord):
            # 마감일 없음 최상단, 그다음 가까운 마감일 순
            if not r.deadline or r.deadline == "없음":
                return (0, "")
            return (1, r.deadline)
        return sorted(records, key=sort_key)

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
        self._with_retry(lambda: self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute())

    def read_state_rows(self) -> list[list[str]]:
        ws = self._ensure_worksheet("_STATE", create_if_missing=False)
        if ws is None:
            return []
        rows = self._with_retry(lambda: ws.get_all_values())
        if not rows:
            return []
        return rows[1:]

    def write_state_rows(self, rows: list[list[str]], *, create_if_missing: bool = False) -> None:
        ws = self._ensure_worksheet("_STATE", create_if_missing=create_if_missing)
        if ws is None:
            print("[WARN] _STATE worksheet missing, skipped state flush")
            return
        values = [STATE_HEADERS] + rows
        self._replace_sheet_values(ws, values)
