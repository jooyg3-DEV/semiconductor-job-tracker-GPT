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

HEADERS = ["검색일", "출처", "마감일", "회사", "공고명", "지원자격", "채용직무", "근무지", "채용형태", "모집구분", "경력", "석사", "박사", "링크"]
STATE_HEADERS = ["sheet_key", "unique_key", "payload_json"]
CLOSED_SHEET_TITLE = "종료공고"
T = TypeVar("T")
RECRUITMENT_RANK = {"인재풀": 0, "채용시 마감": 1, "상시": 2, "일반": 3}


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
                elif any(token in text for token in ["429", "Quota exceeded", "rateLimitExceeded"]):
                    is_retryable = True
                if not is_retryable or attempt == retries - 1:
                    raise
                sleep_s = base_sleep * (2 ** attempt)
                print(f"[WARN] Google Sheets quota/backoff: retry in {sleep_s:.1f}s ({attempt + 1}/{retries})")
                time.sleep(sleep_s)
        raise last_exc

    def worksheet_exists(self, title: str) -> bool:
        return title in self._worksheet_cache

    def _ensure_worksheet(self, title: str, *, create_if_missing: bool = True):
        ws = self._worksheet_cache.get(title)
        if ws is not None:
            return ws
        if not create_if_missing:
            return None
        ws = self._with_retry(lambda: self.sh.add_worksheet(title=title, rows=100, cols=24))
        self._worksheet_cache[title] = ws
        return ws

    def reset_and_initialize(self, company_names: list[str]) -> None:
        self._populate_cache()
        keeper = next(iter(self._worksheet_cache.values()), None)
        if keeper is None:
            keeper = self._with_retry(lambda: self.sh.add_worksheet(title="__INIT__", rows=10, cols=24))
            self._worksheet_cache[keeper.title] = keeper
        for title, ws in list(self._worksheet_cache.items()):
            if ws.id == keeper.id:
                continue
            self._with_retry(lambda ws=ws: self.sh.del_worksheet(ws))
            self._worksheet_cache.pop(title, None)
            time.sleep(0.5)
        first_title = company_names[0] if company_names else "_STATE"
        self._with_retry(lambda: keeper.update_title(first_title))
        self._worksheet_cache = {first_title: keeper}
        self._replace_sheet_values(keeper, [HEADERS] if first_title != "_STATE" else [STATE_HEADERS], clear_first=True)
        print(f"[INFO] initialized worksheet {first_title}")
        for title in company_names[1:] + [CLOSED_SHEET_TITLE, "_STATE"]:
            ws = self._ensure_worksheet(title, create_if_missing=True)
            headers = STATE_HEADERS if title == "_STATE" else HEADERS
            self._replace_sheet_values(ws, [headers], clear_first=True)
            print(f"[INFO] initialized worksheet {title}")
            time.sleep(0.5)

    def write_company_records(self, sheet_title: str, records: list[JobRecord]) -> None:
        ws = self._ensure_worksheet(sheet_title, create_if_missing=False)
        if ws is None:
            print(f"[WARN] company worksheet missing, skipped: {sheet_title}")
            return
        today_str = datetime.now().strftime("%Y-%m-%d")
        records = self._sorted_records(records)
        values = [HEADERS] + [r.to_row(today_str) for r in records]
        self._replace_sheet_values(ws, values, clear_first=True)

    def write_closed_records(self, records: list[JobRecord]) -> None:
        ws = self._ensure_worksheet(CLOSED_SHEET_TITLE, create_if_missing=False)
        if ws is None:
            print(f"[WARN] closed worksheet missing, skipped: {CLOSED_SHEET_TITLE}")
            return
        today_str = datetime.now().strftime("%Y-%m-%d")
        records = self._sorted_closed_records(records)
        values = [HEADERS] + [r.to_row(today_str) for r in records]
        self._replace_sheet_values(ws, values, clear_first=True)
        if records:
            self._apply_strikethrough(ws, start_row=2, end_row=len(values))

    def _replace_sheet_values(self, ws, values: list[list[str]], *, clear_first: bool = False) -> None:
        if clear_first:
            self._with_retry(lambda: ws.clear())
        sanitized = [[self._sanitize_cell(v) for v in row] for row in values]
        end_col = self._col_letter(len(sanitized[0]))
        end_row = len(sanitized)
        self._with_retry(lambda: ws.update(range_name=f"A1:{end_col}{end_row}", values=sanitized))

    @staticmethod
    def _sanitize_cell(value):
        if value is None:
            return ""
        s = str(value)
        return s[:45000]

    @staticmethod
    def _col_letter(n: int) -> str:
        result = ""
        while n:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    @staticmethod
    def _sorted_records(records: list[JobRecord]) -> list[JobRecord]:
        def region_rank(r: JobRecord):
            return 0 if r.effective_region == "국내" else 1
        def recruitment_rank(r: JobRecord):
            return RECRUITMENT_RANK.get(r.recruitment_type or "일반", 9)
        def deadline_rank(r: JobRecord):
            return (0, "") if not r.deadline or r.deadline == "없음" else (1, r.deadline)
        return sorted(records, key=lambda r: (region_rank(r), recruitment_rank(r), deadline_rank(r), r.title))

    @staticmethod
    def _sorted_closed_records(records: list[JobRecord]) -> list[JobRecord]:
        def recruitment_rank(r: JobRecord):
            return RECRUITMENT_RANK.get(r.recruitment_type or "일반", 9)
        def deadline_rank(r: JobRecord):
            return (0, "") if not r.deadline or r.deadline == "없음" else (1, r.deadline)
        return sorted(records, key=lambda r: (r.company, recruitment_rank(r), deadline_rank(r), r.title))

    def _apply_strikethrough(self, ws, start_row: int, end_row: int) -> None:
        body = {"requests": [{"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": start_row - 1, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": len(HEADERS)}, "cell": {"userEnteredFormat": {"textFormat": {"strikethrough": True}}}, "fields": "userEnteredFormat.textFormat.strikethrough"}}]}
        self._with_retry(lambda: self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute())

    def read_state_rows(self) -> list[list[str]]:
        ws = self._ensure_worksheet("_STATE", create_if_missing=False)
        if ws is None:
            return []
        rows = self._with_retry(lambda: ws.get_all_values())
        return rows[1:] if rows else []

    def write_state_rows(self, rows: list[list[str]], *, create_if_missing: bool = False) -> None:
        ws = self._ensure_worksheet("_STATE", create_if_missing=create_if_missing)
        if ws is None:
            print("[WARN] _STATE worksheet missing, skipped state flush")
            return
        values = [STATE_HEADERS] + rows
        self._replace_sheet_values(ws, values, clear_first=True)
