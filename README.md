# Semiconductor Job Tracker

기업 공식 채용 페이지와 일부 채용 플랫폼에서 공고를 수집해 Google Sheets에 기록하는 프로젝트입니다.

## 현재 포함 플랫폼
플랫폼 자동화는 아래 3개만 사용합니다.
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

`모집구분` 값:
- 인재풀
- 채용시 마감
- 상시
- 일반

정렬 우선순위:
1. 국내
2. 모집구분(인재풀 우선)
3. 마감일 없음 우선
4. 마감일 가까운 순

## GitHub Secrets
저장소 `Settings > Secrets and variables > Actions`에 아래 2개를 등록합니다.
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`

## 워크플로
### 1. 초기화
- `job-tracker-init`
- 탭/헤더/_STATE를 새 구조로 다시 만듭니다.
- 헤더 구조를 바꾸는 패치 후에는 먼저 한 번 실행하는 것이 좋습니다.

### 2. 공식 채용 페이지 자동화
- `job-tracker-sync-group1`
- `job-tracker-sync-group2`
- `job-tracker-sync-group3`

이 세 워크플로는 **공식 채용 페이지만** 수집합니다.

### 3. 플랫폼 자동화
- `job-tracker-sync-group4-platforms`

이 워크플로는 **사람인 / 잡코리아 / 링커리어**만 자동으로 수집합니다.

### 4. 플랫폼 디버그
- `job-tracker-debug-platform`

입력값:
- `company`: 회사명
- `platform`: 사람인 / 잡코리아 / 링커리어 / 링크드인

예시:
- Applied Materials × 사람인
- TSMC × 링크드인
- TEL × 잡코리아

디버그 목적:
- 회사 1개 × 플랫폼 1개만 실행
- 어떤 단계에서 0건이 되는지 로그 확인

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

## 현재 운영 원칙
- 공식 채용 페이지와 플랫폼은 별도 흐름으로 수집합니다.
- 직무 적합성으로 먼저 수집합니다.
- `경력`, `석사`, `박사`는 필터가 아니라 표시용 Y/N입니다.
- LinkedIn은 무겁기 때문에 자동화에서 제외하고 디버그 전용으로만 돌립니다.
- 플랫폼은 회사별로 후보를 좁힌 뒤 상세를 확인합니다.

## 주의
- 동적 사이트는 Playwright 기반입니다.
- 사이트 구조가 바뀌면 어댑터 수정이 필요합니다.
- GitHub Actions의 실행 시간 제한 때문에 플랫폼은 자동화와 디버그를 분리했습니다.
