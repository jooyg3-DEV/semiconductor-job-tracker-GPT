
from __future__ import annotations

import json
from collections import defaultdict


class SheetStateManager:
    def __init__(self, sheets_client):
        self.sheets = sheets_client
        self._state: dict[str, dict[str, dict]] = defaultdict(dict)
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        for row in self.sheets.read_state_rows():
            if len(row) < 3:
                continue
            sheet_key, unique_key, payload_json = row[0], row[1], row[2]
            try:
                payload = json.loads(payload_json)
            except Exception:
                payload = {}
            self._state[sheet_key][unique_key] = payload
        self._loaded = True

    def get_sheet_state(self, sheet_key: str) -> dict[str, dict]:
        self._load()
        return self._state[sheet_key]

    def set_sheet_state(self, sheet_key: str, state: dict[str, dict]) -> None:
        self._load()
        self._state[sheet_key] = state

    def flush(self) -> None:
        rows = []
        for sheet_key, bucket in self._state.items():
            for unique_key, payload in bucket.items():
                rows.append([sheet_key, unique_key, json.dumps(payload, ensure_ascii=False)])
        self.sheets.write_state_rows(rows)
