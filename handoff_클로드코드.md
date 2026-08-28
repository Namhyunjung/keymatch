# 키맞추기 (대장단지 따라가기) — 프로젝트 인수인계 문서

이 문서는 claude.ai에서 진행한 작업을 클로드 코드로 이어가기 위한 정리본이에요.
아래 파일들을 프로젝트 폴더에 넣고, 맨 아래 "클로드 코드 첫 프롬프트"를 그대로 붙여넣으면 돼요.

---

## 1. 프로젝트 컨셉 (한 줄 요약)

지역의 **대장단지**(가장 비싼 단지)와 여러 정책 국면(규제강화기/급등기/하락기/회복기)에서 **일관되게 같은 폭으로 움직인 저평가 단지**를 찾아주는 서비스. 국토교통부 실거래가 공공데이터를 씀.

---

## 2. 파일 구조 (다운로드한 그대로 폴더에 배치)

```
keymatch/
├── keymatch.html                  # 화면 목업 (대시보드/동조단지 리스트/비교) — 지금은 파이프라인이 계산한 실제 값이 박혀 있음
├── schema.sql                     # 운영용 DB 스키마 (Postgres)
├── 동조판정_알고리즘_설계.md         # 핵심 알고리즘 설계 문서 (α, n 파라미터 등)
├── backtest_plan.md               # α/n 파라미터 검증(walk-forward) 방법론
├── api_pipeline_design.md         # 국토부 API 연동 스펙 + 파이프라인 아키텍처 설계
├── scheduler_design.md            # cron/Airflow 스케줄러 설계
└── pipeline/
    ├── sync_engine.py             # 핵심 로직: 국면별 상승률 계산, 동조 판정, 대장단지 판정
    ├── apt_trade_connector.py     # 국토부 API 커넥터 (실제 서비스키만 넣으면 동작)
    ├── synthetic_source.py        # ⚠️ 임시 — 실제 API 대신 쓴 가짜 데이터 생성기 (샌드박스 네트워크 제약 때문)
    ├── batch_runner.py            # 배치 오케스트레이션 진입점 (extract→transform→load→recompute)
    ├── export_for_frontend.py     # DB → keymatch.html용 JSON 변환
    ├── sqlite_schema.sql          # 로컬 개발/테스트용 SQLite 스키마 (schema.sql의 축소판)
    └── keymatch.db                # 위 파이프라인을 synthetic 데이터로 돌린 결과가 든 SQLite 파일 (예시용)
```

---

## 3. 지금까지 진행 상황

| 단계 | 상태 | 비고 |
|---|---|---|
| 컨셉/네이밍 | ✅ 완료 | "키맞추기 — 대장단지 따라가기" |
| 화면 설계 & 목업 | ✅ 완료 | `keymatch.html`, 3화면 + 단지 선택 + 정책 설명 모달 |
| 핵심 알고리즘 설계 | ✅ 완료 | `동조판정_알고리즘_설계.md`, `sync_engine.py`로 구현·테스트까지 완료 |
| 백테스트 방법론 | ✅ 설계만 완료 | `backtest_plan.md` — 실제 실행은 실데이터 쌓인 뒤에 |
| DB 스키마 | ✅ 완료 | `schema.sql` (Postgres, 운영용) |
| API 연동 스펙 | ✅ 완료 | `api_pipeline_design.md` — 실제 국토부 API 문서 기반으로 검증됨 |
| 배치 파이프라인 코드 | ✅ 작동 확인 | SQLite로 end-to-end 실행 검증 완료 (synthetic 데이터로) |
| **실제 API 연동** | ❌ 미완료 | **여기부터 클로드 코드에서 이어가야 함** — 샌드박스가 네트워크 차단이라 여기선 불가능했음 |
| 스케줄러 배포 | ❌ 미완료 | 설계만 있음 (`scheduler_design.md`), 실서버에 cron 등록 필요 |

---

## 4. 지금 keymatch.html이 보여주는 데이터의 정체

**중요**: 화면의 숫자들(대치SK뷰 29.3억, 래미안 팰리스2차 20.5억 등)은 **진짜 실거래가가 아니에요.** `synthetic_source.py`가 만든 가짜 거래를 실제 파이프라인 코드(정제→집계→동조판정)에 통과시켜 나온 값이에요. **코드 경로는 진짜와 동일**하지만 **입력 데이터가 가짜**라는 뜻이에요. 실제 서비스키로 API를 붙이면 이 부분만 진짜 숫자로 바뀌어요.

---

## 5. 클로드 코드에서 해야 할 일 (우선순위 순)

1. **서비스키 발급 확인**: data.go.kr에서 "국토교통부_아파트매매 실거래자료(상세)" 활용신청 → Decoding 키 발급 (자동승인, 보통 몇 시간 내)
2. `apt_trade_connector.py`가 실제로 API를 호출하는지 검증 (지역 1개, 월 1개로 스모크 테스트)
3. `batch_runner.py`의 `extract()` 함수에서 `synthetic_source.generate_raw_transactions()` 호출을 `AptTradeConnector.fetch_bulk()` 호출로 교체
4. `sqlite_schema.sql` → 운영에서는 `schema.sql`(Postgres) 기준으로 전환할지, SQLite로 계속 갈지 결정 (초기엔 SQLite로도 충분함)
5. 법정동코드 마스터 시딩 (행정표준코드관리시스템에서 법정동코드 전체자료 다운로드 → `regions` 테이블)
6. 대치동(법정동코드 11680 강남구 기준 확인 필요) 실제 데이터로 첫 실행 → `keymatch.html` mock 데이터를 진짜로 교체
7. `scheduler_design.md` 기준으로 cron 등록

---

## 6. 클로드 코드 첫 프롬프트 (그대로 복사해서 붙여넣기)

```
이 프로젝트는 "키맞추기"라는 한국 부동산 분석 서비스야. 대장단지(지역 최고가 단지)와
정책 국면별로 동조해서 움직이는 저평가 단지를 찾아주는 서비스를 만들고 있어.

폴더 구조는 다음과 같아:
- keymatch.html: 화면 목업 (지금은 synthetic 가짜 데이터가 박혀 있음)
- schema.sql: 운영용 Postgres 스키마
- 동조판정_알고리즘_설계.md: 핵심 알고리즘 설계 (α=12%p 오차범위, n=6개월 윈도우)
- backtest_plan.md: 파라미터 검증 방법론
- api_pipeline_design.md: 국토부 API 연동 스펙 (실제 API 문서 기반 검증됨)
- scheduler_design.md: cron/Airflow 배치 스케줄러 설계
- pipeline/sync_engine.py: 핵심 동조판정 로직 (테스트 완료)
- pipeline/apt_trade_connector.py: 국토부 API 커넥터 (아직 실제 API 호출 테스트 안 됨)
- pipeline/synthetic_source.py: 임시 가짜 데이터 생성기 (실제 API로 교체해야 함)
- pipeline/batch_runner.py: 배치 오케스트레이션 (synthetic_source로 end-to-end 검증됨)
- pipeline/export_for_frontend.py: DB → keymatch.html용 JSON export

먼저 각 파일을 읽고 전체 구조를 파악해줘. 그 다음:

1. data.go.kr 서비스키를 .env로 안전하게 넣는 방법을 안내해줘
   (키는 내가 직접 발급받아서 넣을게)
2. apt_trade_connector.py로 실제 API를 스모크 테스트해줘
   (서울 강남구 법정동코드 11680, 최근 1개월치로 시작)
3. 문제없으면 batch_runner.py의 extract() 함수를 
   synthetic_source 대신 실제 apt_trade_connector 호출로 교체해줘
4. 실제 데이터로 파이프라인을 한 번 돌려서 export_for_frontend.py로
   keymatch.html의 mock 데이터를 진짜 값으로 교체해줘

각 단계마다 실행 결과를 보여주고 다음 단계로 넘어가기 전에 확인받아줘.
```

---

## 7. 참고: 서비스키 안전하게 관리하기

클로드 코드에서 작업할 때 서비스키를 코드에 직접 쓰지 말고 `.env` 파일로 분리하는 걸 권장해요.

```bash
# .env (git에 커밋하지 말 것 — .gitignore에 추가)
APT_API_KEY=발급받은_디코딩_키
```

```python
# 코드에서는 이렇게
import os
from dotenv import load_dotenv
load_dotenv()
service_key = os.environ["APT_API_KEY"]
```
