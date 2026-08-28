-- ============================================================
-- 키맞추기 DB 스키마
-- 국토부 실거래가 API(data.go.kr) 연동 + 동조판정 결과 저장
-- ============================================================

-- 1. 지역 마스터 (법정동코드 기준 — 국토부 API 조회 파라미터)
CREATE TABLE regions (
  region_code   CHAR(10)     PRIMARY KEY,   -- 법정동코드 10자리
  sido          VARCHAR(20)  NOT NULL,       -- 시/도
  sigungu       VARCHAR(20)  NOT NULL,       -- 시/군/구
  eupmyeondong  VARCHAR(20),                 -- 읍/면/동
  updated_at    TIMESTAMP    DEFAULT now()
);

-- 2. 단지 마스터
-- 국토부 실거래가 API는 단지 고유ID를 주지 않음 → 단지명+법정동코드+좌표로
-- 내부 마스터를 만들고, 공동주택 단지코드 API(data.go.kr)와 매칭해서 보강함
CREATE TABLE complexes (
  complex_id     BIGSERIAL    PRIMARY KEY,
  complex_name   VARCHAR(100) NOT NULL,
  region_code    CHAR(10)     NOT NULL REFERENCES regions(region_code),
  apt_seq        VARCHAR(20)  UNIQUE,          -- 국토부 API 자체 단지식별자 (예: '11110-2417') — 1차 매칭키
  road_address   VARCHAR(200),
  lat            DECIMAL(9,6),
  lng            DECIMAL(9,6),
  households     INT,                        -- 세대수
  built_year     SMALLINT,                    -- 준공년도
  danji_code     VARCHAR(20),                 -- 공동주택관리정보시스템 단지코드 (있으면)
  match_confidence DECIMAL(3,2),              -- 이름/주소 매칭 신뢰도 (fuzzy match score)
  created_at     TIMESTAMP    DEFAULT now(),
  UNIQUE (complex_name, region_code)
);

-- 3. 평형 그룹 (전용면적 구간 — 84㎡ 국평 기준 ±3㎡ 등으로 그룹핑)
CREATE TABLE pyeong_groups (
  pyeong_group_id SMALLSERIAL PRIMARY KEY,
  label           VARCHAR(20) NOT NULL,       -- '59㎡', '84㎡', '114㎡' 등
  area_min        DECIMAL(5,2) NOT NULL,
  area_max        DECIMAL(5,2) NOT NULL
);

-- 4. 원본 실거래 (국토부 API 적재 원천 테이블)
CREATE TABLE transactions (
  txn_id          BIGSERIAL   PRIMARY KEY,
  complex_id      BIGINT      NOT NULL REFERENCES complexes(complex_id),
  pyeong_group_id SMALLINT    NOT NULL REFERENCES pyeong_groups(pyeong_group_id),
  area_m2         DECIMAL(6,2) NOT NULL,
  floor           SMALLINT,
  txn_ym          CHAR(6)     NOT NULL,        -- 'YYYYMM'
  txn_day         SMALLINT,
  price_10k       INT         NOT NULL,        -- 만원 단위 (국토부 API 원 단위)
  is_direct_txn   BOOLEAN     DEFAULT false,    -- 직거래 여부(공공데이터 플래그)
  is_cancelled    BOOLEAN     DEFAULT false,    -- 계약 해제 여부 (API cdealType='O')
  cancelled_date  DATE,                          -- 해제사유발생일 (API cdealDay)
  is_outlier      BOOLEAN     DEFAULT false,    -- 전처리 단계에서 트리밍된 이상치
  raw_source_id   VARCHAR(50),                  -- API 응답 원본 식별자(중복적재 방지용)
  created_at      TIMESTAMP   DEFAULT now(),
  UNIQUE (raw_source_id)
);
CREATE INDEX idx_txn_complex_ym ON transactions(complex_id, pyeong_group_id, txn_ym);

-- 5. 월별 대표가 (파생 테이블 — 배치로 재계산)
CREATE TABLE monthly_price (
  complex_id      BIGINT   NOT NULL REFERENCES complexes(complex_id),
  pyeong_group_id SMALLINT NOT NULL REFERENCES pyeong_groups(pyeong_group_id),
  ym              CHAR(6)  NOT NULL,
  rep_price_10k   INT      NOT NULL,      -- 트리밍 후 대표가(최고가 or 중위값)
  txn_count       SMALLINT NOT NULL,
  PRIMARY KEY (complex_id, pyeong_group_id, ym)
);

-- 6. 정책 국면 마스터 (하드코딩 대신 테이블로 관리 → 구간 조정이 쉬움)
CREATE TABLE regimes (
  regime_id     SMALLSERIAL PRIMARY KEY,
  label         VARCHAR(30) NOT NULL,     -- '규제강화기' 등
  start_ym      CHAR(6)     NOT NULL,
  end_ym        CHAR(6)     NOT NULL,
  description   TEXT,
  display_order SMALLINT    NOT NULL
);

-- 7. 대장단지 판정 결과 (지역×평형별로 배치 계산 후 캐시)
CREATE TABLE leader_complexes (
  region_code     CHAR(10)  NOT NULL REFERENCES regions(region_code),
  pyeong_group_id SMALLINT  NOT NULL REFERENCES pyeong_groups(pyeong_group_id),
  complex_id      BIGINT    NOT NULL REFERENCES complexes(complex_id),
  price_rank_pct  DECIMAL(4,3),           -- 지역 내 평당가 순위 백분위
  stable_regimes  SMALLINT,               -- 상위 10% 유지한 국면 수
  as_of           DATE      NOT NULL,
  PRIMARY KEY (region_code, pyeong_group_id, as_of)
);

-- 8. 국면별 상승률 계산 결과
CREATE TABLE regime_returns (
  complex_id       BIGINT   NOT NULL REFERENCES complexes(complex_id),
  pyeong_group_id  SMALLINT NOT NULL REFERENCES pyeong_groups(pyeong_group_id),
  regime_id        SMALLINT NOT NULL REFERENCES regimes(regime_id),
  start_price_n    INT,                  -- 시작시점 n개월 최고가
  end_price_n      INT,                  -- 종료시점 n개월 최고가
  return_rate      DECIMAL(6,4),         -- (end/start)-1
  txn_count_regime SMALLINT,             -- 해당 국면 내 거래건수
  is_judgable      BOOLEAN,              -- 유동성 컷 통과 여부
  PRIMARY KEY (complex_id, pyeong_group_id, regime_id)
);

-- 9. 동조판정 결과 (대장단지 대비 후보단지, 국면별)
CREATE TABLE sync_results (
  region_code           CHAR(10) NOT NULL,
  pyeong_group_id       SMALLINT NOT NULL,
  leader_complex_id     BIGINT   NOT NULL REFERENCES complexes(complex_id),
  candidate_complex_id  BIGINT   NOT NULL REFERENCES complexes(complex_id),
  regime_id             SMALLINT NOT NULL REFERENCES regimes(regime_id),
  is_synced             BOOLEAN,          -- NULL = 판정불가(유동성 컷)
  gap_pp                DECIMAL(6,4),     -- |후보 상승률 - 대장 상승률|
  PRIMARY KEY (leader_complex_id, candidate_complex_id, regime_id)
);

-- 10. 종합 동조지수 캐시 (프론트 리스트 화면이 바로 조회하는 테이블)
CREATE TABLE sync_summary (
  region_code           CHAR(10) NOT NULL,
  pyeong_group_id       SMALLINT NOT NULL,
  leader_complex_id     BIGINT   NOT NULL REFERENCES complexes(complex_id),
  candidate_complex_id  BIGINT   NOT NULL REFERENCES complexes(complex_id),
  sync_count            SMALLINT NOT NULL,
  judgable_count         SMALLINT NOT NULL,
  sync_index            DECIMAL(4,3) NOT NULL,   -- sync_count / judgable_count
  current_price_gap_10k INT,                      -- 대장단지 대비 현재가 갭
  updated_at            TIMESTAMP DEFAULT now(),
  PRIMARY KEY (leader_complex_id, candidate_complex_id)
);
CREATE INDEX idx_sync_summary_region ON sync_summary(region_code, pyeong_group_id, sync_index DESC);

-- 11. 배치 실행 이력 (batch_runner.py가 매 실행마다 기록)
CREATE TABLE pipeline_runs (
  run_id        BIGSERIAL   PRIMARY KEY,
  started_at    TIMESTAMP   NOT NULL,
  finished_at   TIMESTAMP,
  status        VARCHAR(10),  -- 'running' | 'success' | 'failed'
  rows_ingested INT,
  error_msg     TEXT
);

-- 12. 알고리즘 파라미터 (사용자가 설정화면에서 조정 → 버전 관리)
CREATE TABLE algo_params (
  param_set_id   SERIAL PRIMARY KEY,
  n_months       SMALLINT DEFAULT 6,        -- 최고가 산출 윈도우
  alpha_pp       DECIMAL(5,2) DEFAULT 12.0, -- 오차범위(%p)
  min_txn_regime SMALLINT DEFAULT 3,        -- 국면당 최소 거래건수
  min_judgable   SMALLINT DEFAULT 3,        -- 최소 판정가능 국면 수
  min_sync_index DECIMAL(4,3) DEFAULT 0.75, -- 최소 동조지수
  leader_top_pct DECIMAL(4,3) DEFAULT 0.10, -- 대장단지 평당가 상위 %
  is_active      BOOLEAN DEFAULT true,
  created_at     TIMESTAMP DEFAULT now()
);
