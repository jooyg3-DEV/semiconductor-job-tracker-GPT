
# Semiconductor Job Tracker

기업 공식 채용 페이지와 일부 채용 플랫폼에서 공고를 수집해 Google Sheets에 기록하는 프로젝트입니다.

## 저장 위치
아래 구조 그대로 GitHub 저장소 루트에 저장하세요.

```text
semiconductor-job-tracker/
├─ .github/workflows/job_tracker.yml
├─ adapters/
├─ config/
├─ core/
├─ sheets/
├─ state/
├─ main.py
├─ requirements.txt
└─ README.md
```

## GitHub Secrets
저장소 `Settings > Secrets and variables > Actions` 에 아래 2개를 넣으세요.

- `GOOGLE_SERVICE_ACCOUNT_JSON`: 서비스 계정 JSON 전체 문자열
- `GOOGLE_SHEET_ID`: Google Spreadsheet ID

## 로컬 실행
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
python main.py
```

## GitHub Actions 실행 시각
- KST 00:00
- KST 13:00

GitHub cron 은 UTC 기준이므로 워크플로에는 다음으로 들어갑니다.
- 15:00 UTC
- 04:00 UTC

## 현재 구현 범위
- 공통 프레임워크
- Google Sheets 쓰기/읽기
- 상태 추적(_STATE 탭)
- 종료 공고 탭 이동 및 취소선 적용
- 공식 사이트 우선 중복 제거
- 일부 공식 채용 어댑터 구현
  - 삼성전자DS
  - SK하이닉스
  - ASML 글로벌
  - TSMC
- 나머지 사이트는 기본 어댑터/스켈레톤 포함

## 주의
- 동적 사이트는 Playwright 기반입니다.
- 사이트 구조 변경 시 어댑터 수정이 필요합니다.


- 기업 탭 내 국내/글로벌 정렬은 source 구분이 아니라 **근무지(location) 기준**으로 판정합니다. location이 비어 있으면 source 기본 region을 fallback으로 사용합니다.


- LinkedIn 공개 채용 페이지는 로그인 배너가 있어도 상세 정보가 보이면 수집 대상으로 인정합니다.
- Catch/하이브레인넷처럼 잡음이 큰 플랫폼은 공고 상세 URL과 회사명 명시가 없으면 버리도록 강화했습니다.
