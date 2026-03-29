# Semiconductor Job Tracker

기업 공식 채용 페이지와 일부 채용 플랫폼에서 공고를 수집해 Google Sheets에 기록하는 프로젝트입니다.

## 현재 포함 플랫폼
자동화 플랫폼은 아래 3개만 사용합니다.
- 사람인
- 잡코리아
- 링커리어

LinkedIn은 당분간 **자동화 제외 / 디버그 전용**입니다.

## 시트 구조
기업별 탭 1개씩 사용합니다.
- 삼성전자DS
- SK하이닉스
- ASML
- Applied Materials
- KLA
- Lam Research
- TEL
- Micron
- ASM
- TSMC
- NVIDIA
- AMD
- 종료공고
- _STATE

헤더는 아래 순서입니다.
- 검색일
- 출처
- 마감일
- 회사
- 공고명
- 지원자격
- 채용직무
- 근무지
- 채용형태
- 모집구분
- 경력
- 석사
- 박사
- 링크

## 핵심 운영 원칙
- 공식 채용 페이지와 플랫폼은 별도 흐름으로 수집합니다.
- 직무 적합성으로 먼저 수집합니다.
- `경력`, `석사`, `박사`는 필터가 아니라 표시용 Y/N입니다.
- LinkedIn은 무겁기 때문에 자동화에서 제외하고 디버그 전용으로만 돌립니다.
- 플랫폼과 공식 페이지 모두 **긴 검색어 1회** 대신 **회사명 + 짧은 키워드 1개** 방식의 **순차 검색/순차 매칭**을 사용합니다.
- 제목/직무의 제외 키워드는 단순 substring이 아니라 **단어 경계 기반**으로 강하게 적용합니다.
- 공식/플랫폼 일부 실행이 기존 공식 공고를 지우지 않도록 **source-aware merge**를 사용합니다.
- 신규 공고는 가능한 한 **기존 행 순서를 유지한 채 하단 append**합니다.
- debug 모드는 **read-only**이며 시트와 `_STATE`를 갱신하지 않습니다.

## 순차 검색 키워드
기본 검색 순서는 아래와 같습니다.
- process engineer
- process support engineer
- field application engineer
- application engineer
- customer engineer
- metrology
- lithography
- deposition
- etch
- yield
- integration
- packaging
- manufacturing engineer
- production engineer
- module engineer
- quality engineer

회사별로 우선 키워드와 지역 힌트를 일부 다르게 둡니다.

## GitHub Secrets
저장소 `Settings > Secrets and variables > Actions`에 아래 2개를 등록합니다.
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`

## 워크플로
### 1. 초기화
- `job-tracker-init`
- 탭/헤더/_STATE를 새 구조로 다시 만듭니다.
- 헤더 구조나 상태 구조가 바뀐 패치 후에는 먼저 한 번 실행하는 것이 좋습니다.

### 2. 공식 채용 페이지 자동화
- `job-tracker-sync-group1`
- `job-tracker-sync-group2`
- `job-tracker-sync-group3`

이 세 워크플로는 **공식 채용 페이지만** 수집합니다.

### 3. 플랫폼 자동화
- `job-tracker-sync-group4-platforms`

이 워크플로는 **사람인 / 잡코리아 / 링커리어**만 자동으로 수집합니다.
플랫폼 공고는 기존 공식 공고를 지우지 않고 병합합니다.

### 4. 플랫폼 디버그
- `job-tracker-debug-platform`

입력값은 둘 다 **드롭다운(choice)** 입니다.
- `company`: 회사명 선택
- `platform`: 사람인 / 잡코리아 / 링커리어 / 링크드인

디버그 목적:
- 회사 1개 × 플랫폼 1개만 실행
- 어떤 단계에서 0건이 되는지 로그 확인
- `debug_outputs/*.csv` 아티팩트를 업로드해 검색 URL, 페이지 제목, 후보 링크 수, 제외 이유를 확인
- debug 모드는 **시트 write 없이** CSV artifact만 생성

## 로그 / CSV 항목
플랫폼 debug CSV에는 아래 단계 정보를 남깁니다.
- `search_meta`
- `candidate_raw`
- `candidate_pruned`
- `detail_parse`
- `filter`

주요 필드:
- `search_url`
- `page_title`
- `result_count_text`
- `candidate_links_count`
- `alias_candidates_count`
- `title`
- `url`
- `include_matches`
- `exclude_matches`
- `hard_excludes`
- `decision`
- `reason`

## 로컬 실행
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
python main.py --mode init
python main.py --mode sync --run-platforms none --companies 'ASML'
python main.py --mode sync --run-platforms others
python main.py --mode sync --run-platforms debug --debug-company 'Applied Materials' --debug-platform '링크드인'
```

## 주의
- 동적 사이트는 Playwright 기반입니다.
- 사이트 구조가 바뀌면 어댑터 수정이 필요합니다.
- GitHub Actions의 실행 시간 제한 때문에 플랫폼은 자동화와 디버그를 분리했습니다.
- partial sync와 platform-only sync도 `_STATE` 기준으로 기존 공고를 보존하지만, 최초 실행 직후에는 공식 sync를 한 번 먼저 돌리는 것이 안전합니다.
