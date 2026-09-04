-- ============================================================
-- SQLite 버전 스키마 (로컬 배치 데모/개발용)
-- 운영 환경은 schema.sql(Postgres)을 그대로 사용하고,
-- 이 파일은 batch_runner.py가 로컬에서 전체 흐름을 검증할 때 씀.
-- 테이블/컬럼명은 schema.sql과 1:1 대응시켜서 나중에 Postgres로
-- 옮길 때 쿼리를 거의 그대로 재사용할 수 있게 함.
-- ============================================================

CREATE TABLE regions (
  region_code   TEXT PRIMARY KEY,
  sido          TEXT NOT NULL,
  sigungu       TEXT NOT NULL,
  eupmyeondong  TEXT
);

CREATE TABLE complexes (
  complex_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  complex_name   TEXT NOT NULL,
  region_code    TEXT NOT NULL REFERENCES regions(region_code),
  apt_seq        TEXT UNIQUE,
  households     INTEGER,
  built_year     INTEGER,
  is_leader_cand INTEGER DEFAULT 0
);

CREATE TABLE pyeong_groups (
  pyeong_group_id INTEGER PRIMARY KEY AUTOINCREMENT,
  label           TEXT NOT NULL,
  area_min        REAL NOT NULL,
  area_max        REAL NOT NULL
);

CREATE TABLE transactions (
  txn_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  complex_id      INTEGER NOT NULL REFERENCES complexes(complex_id),
  pyeong_group_id INTEGER NOT NULL REFERENCES pyeong_groups(pyeong_group_id),
  area_m2         REAL NOT NULL,
  floor           INTEGER,
  txn_ym          TEXT NOT NULL,
  txn_day         INTEGER,
  price_10k       INTEGER NOT NULL,
  is_direct_txn   INTEGER DEFAULT 0,
  is_cancelled    INTEGER DEFAULT 0,
  raw_source_id   TEXT UNIQUE
);
CREATE INDEX idx_txn_complex_ym ON transactions(complex_id, pyeong_group_id, txn_ym);

CREATE TABLE monthly_price (
  complex_id      INTEGER NOT NULL,
  pyeong_group_id INTEGER NOT NULL,
  ym              TEXT NOT NULL,
  rep_price_10k   INTEGER NOT NULL,
  txn_count       INTEGER NOT NULL,
  PRIMARY KEY (complex_id, pyeong_group_id, ym)
);

CREATE TABLE regimes (
  regime_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  label         TEXT NOT NULL,
  start_ym      TEXT NOT NULL,
  end_ym        TEXT NOT NULL,
  display_order INTEGER NOT NULL
);

CREATE TABLE regime_returns (
  complex_id       INTEGER NOT NULL,
  pyeong_group_id  INTEGER NOT NULL,
  regime_id        INTEGER NOT NULL,
  return_rate      REAL,
  txn_count_regime INTEGER,
  is_judgable      INTEGER,
  PRIMARY KEY (complex_id, pyeong_group_id, regime_id)
);

CREATE TABLE leader_complexes (
  region_code     TEXT NOT NULL,
  pyeong_group_id INTEGER NOT NULL,
  complex_id      INTEGER NOT NULL,
  as_of           TEXT NOT NULL,
  PRIMARY KEY (region_code, pyeong_group_id, as_of)
);

CREATE TABLE sync_summary (
  region_code           TEXT NOT NULL,
  pyeong_group_id       INTEGER NOT NULL,
  leader_complex_id     INTEGER NOT NULL,
  candidate_complex_id  INTEGER NOT NULL,
  sync_count            INTEGER NOT NULL,
  judgable_count        INTEGER NOT NULL,
  sync_index            REAL NOT NULL,
  current_price_gap_10k INTEGER,
  updated_at            TEXT,
  PRIMARY KEY (leader_complex_id, candidate_complex_id, pyeong_group_id)
);

CREATE TABLE pipeline_runs (
  run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at   TEXT NOT NULL,
  finished_at  TEXT,
  status       TEXT,       -- 'success' | 'failed'
  rows_ingested INTEGER,
  error_msg    TEXT
);
