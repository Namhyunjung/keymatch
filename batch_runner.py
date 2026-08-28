"""
batch_runner.py
cron이든 Airflow든, 이 스크립트 하나를 실행하면 전체 파이프라인이 돈다.
    python batch_runner.py --mode full        # 전체 재계산 (신규 지역 백필용, 201705~ 전체월 API 호출)
    python batch_runner.py --mode incremental # 최근 3개월만 (매일 새벽 배치)

EXTRACT는 AptTradeConnector로 실 API 호출. .env의 APT_API_KEY 필요.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from apt_trade_connector import AptTradeConnector, clean_transactions, build_monthly_price
from sync_engine import AlgoParams, Regime, evaluate_candidate, compute_regime_return, pick_leader
import synthetic_source

load_dotenv()

LAWD_CD_LIST = ['11680']  # 강남구. regions 시딩과 맞춰서 확장.

# 법정동코드 (행정표준코드관리시스템 code.go.kr 조회, 2026-08 기준 현존 15건: 강남구 자체 + 하위 14개 동)
# API 응답의 umdNm(동명)으로 단지를 정확한 동에 매핑하는 데 씀.
GANGNAM_DONG_CODES = {
    '역삼동': '1168010100', '개포동': '1168010300', '청담동': '1168010400',
    '삼성동': '1168010500', '대치동': '1168010600', '신사동': '1168010700',
    '논현동': '1168010800', '압구정동': '1168011000', '세곡동': '1168011100',
    '자곡동': '1168011200', '율현동': '1168011300', '일원동': '1168011400',
    '수서동': '1168011500', '도곡동': '1168011800',
}
GANGNAM_REGION_CODE = '1168000000'  # 강남구 자체 (구 단위 집계용, leader_complexes/sync_summary에서 씀)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger('batch_runner')

DB_PATH = Path(__file__).parent / 'keymatch.db'
SCHEMA_PATH = Path(__file__).parent / 'sqlite_schema.sql'

REGIMES = [
    Regime(1, '규제강화기', '201705', '201912'),
    Regime(2, '급등기', '202001', '202204'),
    Regime(3, '하락기', '202205', '202312'),
    Regime(4, '회복기', '202401', '202608'),
]


# ------------------------------------------------------------
# DB 초기화
# ------------------------------------------------------------
def init_db(conn: sqlite3.Connection):
    if not DB_PATH.exists() or _tables_missing(conn):
        log.info("스키마 초기화")
        conn.executescript(SCHEMA_PATH.read_text(encoding='utf-8'))
        _seed_static(conn)


def _tables_missing(conn) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return len(cur.fetchall()) == 0


def _seed_static(conn):
    conn.execute("INSERT INTO regions VALUES (?,?,?,?)", (GANGNAM_REGION_CODE, '서울', '강남구', None))
    for dong_name, code in GANGNAM_DONG_CODES.items():
        conn.execute("INSERT INTO regions VALUES (?,?,?,?)", (code, '서울', '강남구', dong_name))
    conn.execute("INSERT INTO pyeong_groups (label, area_min, area_max) VALUES ('84㎡', 81, 88)")
    for r in REGIMES:
        conn.execute(
            "INSERT INTO regimes (label, start_ym, end_ym, display_order) VALUES (?,?,?,?)",
            (r.label, r.start_ym, r.end_ym, r.regime_id)
        )
    conn.commit()


# ------------------------------------------------------------
# EXTRACT
# ------------------------------------------------------------
def _ym_range(start_ym: str, end_ym: str) -> list[str]:
    start = datetime.strptime(start_ym, '%Y%m')
    end = datetime.strptime(end_ym, '%Y%m')
    out = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def extract(mode: str) -> pd.DataFrame:
    """
    incremental 모드는 최근 3개월, full은 REGIMES 전체 기간(201705~) 수집.
    """
    log.info(f"EXTRACT 시작 (mode={mode})")

    if mode == 'incremental':
        now = datetime.now(timezone.utc)
        total_months = now.year * 12 + (now.month - 1) - 2  # 3개월 전
        start_y, start_m = divmod(total_months, 12)
        yms = _ym_range(f"{start_y}{start_m + 1:02d}", f"{now.year}{now.month:02d}")
    else:
        yms = _ym_range(REGIMES[0].start_ym, REGIMES[-1].end_ym)

    connector = AptTradeConnector(service_key=os.environ["APT_API_KEY"])
    raw = connector.fetch_bulk(lawd_cd_list=LAWD_CD_LIST, deal_ymd_list=yms)
    log.info(f"EXTRACT 완료 — {len(raw)}행")
    return raw


# ------------------------------------------------------------
# TRANSFORM + LOAD
# ------------------------------------------------------------
def transform_and_load(conn: sqlite3.Connection, raw: pd.DataFrame) -> int:
    log.info("TRANSFORM 시작")
    cleaned = clean_transactions(raw)

    # 동조판정은 같은 평형끼리만 비교 가능 — pyeong_groups 범위로 거래 필터링.
    # (전체 평형 섞어서 대표가를 뽑으면 대장단지가 엉뚱한 초대형 평형으로 튐)
    pg_id, area_min, area_max = conn.execute(
        "SELECT pyeong_group_id, area_min, area_max FROM pyeong_groups LIMIT 1"
    ).fetchone()
    cleaned = cleaned[cleaned['area_m2'].between(area_min, area_max)].copy()
    log.info(f"평형 필터 적용 ({area_min}~{area_max}㎡) — {len(cleaned)}행 남음")

    # 단지 마스터 upsert (apt_seq 기준)
    # 세대수는 국토부 실거래가 API에 없는 필드 — 공동주택관리정보시스템(k-apt) 등
    # 별도 데이터셋과 조인해서 채워야 함. 데모에서는 synthetic_source의 마스터로 대체.
    master = synthetic_source.complex_master().set_index('apt_seq')
    for apt_seq, group in cleaned.groupby('apt_seq'):
        name = group['complex_name'].iloc[0]
        by = group['built_year'].dropna()
        built_year = int(by.iloc[0]) if not by.empty else None
        households = int(master.loc[apt_seq, 'households']) if apt_seq in master.index else None
        dong_name = group['dong_name'].iloc[0]
        region_code = GANGNAM_DONG_CODES.get(dong_name)
        if region_code is None:
            log.warning(f"미등록 동명 '{dong_name}' (apt_seq={apt_seq}) — 강남구로 대체")
            region_code = GANGNAM_REGION_CODE
        conn.execute(
            """INSERT INTO complexes (complex_name, region_code, apt_seq, built_year, households)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(apt_seq) DO UPDATE SET complex_name=excluded.complex_name, region_code=excluded.region_code""",
            (name, region_code, apt_seq, built_year, households)
        )
    conn.commit()

    complex_id_map = {
        row[1]: row[0] for row in conn.execute("SELECT complex_id, apt_seq FROM complexes")
    }

    # 원본 거래 적재 (raw_source_id로 중복방지)
    rows_ingested = 0
    for _, row in cleaned.iterrows():
        raw_source_id = f"{row['apt_seq']}_{row['txn_ym']}_{row['txn_day']}_{row['price_10k']}_{row['floor']}"
        try:
            conn.execute(
                """INSERT OR IGNORE INTO transactions
                   (complex_id, pyeong_group_id, area_m2, floor, txn_ym, txn_day,
                    price_10k, is_direct_txn, is_cancelled, raw_source_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (complex_id_map[row['apt_seq']], pg_id, row['area_m2'], row['floor'],
                 row['txn_ym'], row['txn_day'], row['price_10k'],
                 int(row['is_direct_txn']), int(row['is_cancelled']), raw_source_id)
            )
            rows_ingested += 1
        except Exception as e:
            log.warning(f"적재 실패, 건너뜀: {raw_source_id} — {e}")
    conn.commit()

    # 월별 대표가 재계산 — 이번에 수집된 (단지×월) 건만 갱신.
    # 과거 이력을 지우지 않도록 전체 DELETE 대신 PK(complex_id, ym) 단위로 REPLACE
    monthly = build_monthly_price(cleaned)
    for apt_seq, group in monthly.groupby('apt_seq'):
        cid = complex_id_map[apt_seq]
        for _, r in group.iterrows():
            conn.execute(
                """INSERT OR REPLACE INTO monthly_price
                   (complex_id, pyeong_group_id, ym, rep_price_10k, txn_count)
                   VALUES (?,?,?,?,?)""",
                (cid, pg_id, r['ym'], int(r['rep_price_10k']), int(r['txn_count']))
            )
    conn.commit()
    log.info(f"LOAD 완료 — 거래 {rows_ingested}건, 월별대표가 {len(monthly)}행")
    return rows_ingested


# ------------------------------------------------------------
# RECOMPUTE: 대장단지 판정 + 국면별 상승률 + 동조지수
# ------------------------------------------------------------
def recompute(conn: sqlite3.Connection):
    log.info("RECOMPUTE 시작")
    params = AlgoParams()
    pyeong_group_id = conn.execute("SELECT pyeong_group_id FROM pyeong_groups LIMIT 1").fetchone()[0]

    complexes = conn.execute("SELECT complex_id, apt_seq, complex_name FROM complexes").fetchall()

    def load_monthly(complex_id: int) -> pd.DataFrame:
        rows = conn.execute(
            "SELECT ym, rep_price_10k, txn_count FROM monthly_price WHERE complex_id=? ORDER BY ym",
            (complex_id,)
        ).fetchall()
        return pd.DataFrame(rows, columns=['ym', 'rep_price_10k', 'txn_count'])

    latest_prices = []
    for cid, apt_seq, name in complexes:
        m = load_monthly(cid)
        if m.empty:
            continue
        latest_prices.append((cid, apt_seq, name, m.iloc[-1]['rep_price_10k']))

    # 대장단지 판정: 국면 종료시점마다 상위 10% 안에 몇 번 들었는지(순위안정성) — sync_engine.pick_leader()
    price_rows = conn.execute(
        "SELECT complex_id, ym, rep_price_10k FROM monthly_price WHERE pyeong_group_id=?",
        (pyeong_group_id,)
    ).fetchall()
    price_df = pd.DataFrame(price_rows, columns=['complex_id', 'ym', 'price_per_pyeong'])
    ranking = pick_leader(price_df, REGIMES, params)
    if ranking.empty:
        raise RuntimeError("대장단지 판정 불가 — 국면 종료시점에 거래 데이터 없음")
    leader_id = int(ranking.iloc[0]['complex_id'])
    leader_apt_seq, leader_name = next((a, n) for cid, a, n in complexes if cid == leader_id)
    log.info(
        f"대장단지 순위안정성: stable_regimes={int(ranking.iloc[0]['stable_regimes'])}/{len(REGIMES)} "
        f"(상위 {int(params.leader_top_pct*100)}% 기준)"
    )
    log.info(f"대장단지 판정: {leader_name} ({leader_apt_seq})")

    conn.execute("DELETE FROM leader_complexes")
    conn.execute(
        "INSERT INTO leader_complexes VALUES ('1168000000', ?, ?, ?)",
        (pyeong_group_id, leader_id, datetime.now(timezone.utc).isoformat())
    )

    leader_monthly = load_monthly(leader_id)
    conn.execute("DELETE FROM regime_returns")
    conn.execute("DELETE FROM sync_summary")

    for regime in REGIMES:
        stat = compute_regime_return(leader_monthly, regime, params)
        rr = stat['return_rate']
        conn.execute(
            """INSERT OR REPLACE INTO regime_returns
               (complex_id, pyeong_group_id, regime_id, return_rate, txn_count_regime, is_judgable)
               VALUES (?,?,?,?,?,?)""",
            (leader_id, pyeong_group_id, regime.regime_id,
             float(rr) if rr is not None else None,
             int(stat['txn_count_regime']), int(stat['is_judgable']))
        )

    for cid, apt_seq, name, _ in latest_prices:
        if cid == leader_id:
            continue
        cand_monthly = load_monthly(cid)
        result = evaluate_candidate(leader_monthly, cand_monthly, REGIMES, params)

        for r in result['per_regime']:
            cr = r['candidate_return']
            conn.execute(
                """INSERT OR REPLACE INTO regime_returns
                   (complex_id, pyeong_group_id, regime_id, return_rate, txn_count_regime, is_judgable)
                   VALUES (?,?,?,?,?,?)""",
                (cid, pyeong_group_id, r['regime_id'],
                 float(cr) if cr is not None else None, None, int(r['judgable']))
            )

        leader_price = int(leader_monthly.iloc[-1]['rep_price_10k'])
        cand_price = int(cand_monthly.iloc[-1]['rep_price_10k']) if not cand_monthly.empty else None
        gap = (cand_price - leader_price) if cand_price is not None else None

        conn.execute(
            """INSERT OR REPLACE INTO sync_summary
               (region_code, pyeong_group_id, leader_complex_id, candidate_complex_id,
                sync_count, judgable_count, sync_index, current_price_gap_10k, updated_at)
               VALUES ('1168000000', ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pyeong_group_id, leader_id, cid, int(result['sync_count']), int(result['judgable_count']),
             float(result['sync_index']), int(gap) if gap is not None else None,
             datetime.now(timezone.utc).isoformat())
        )
    conn.commit()
    log.info("RECOMPUTE 완료")


# ------------------------------------------------------------
# 메인 엔트리포인트 (cron/Airflow가 호출하는 지점)
# ------------------------------------------------------------
def run(mode: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF")  # SQLite 데모 편의상 완화 (운영 Postgres는 FK 유지)

    run_id = None
    try:
        init_db(conn)
        conn.execute(
            "INSERT INTO pipeline_runs (started_at, status) VALUES (?, 'running')",
            (started.isoformat(),)
        )
        run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()

        raw = extract(mode)
        rows_ingested = transform_and_load(conn, raw)
        recompute(conn)

        conn.execute(
            "UPDATE pipeline_runs SET finished_at=?, status='success', rows_ingested=? WHERE run_id=?",
            (datetime.now(timezone.utc).isoformat(), rows_ingested, run_id)
        )
        conn.commit()
        log.info("파이프라인 성공")
        return {'status': 'success', 'run_id': run_id}

    except Exception as e:
        log.exception("파이프라인 실패")
        if run_id is not None:
            conn.execute(
                "UPDATE pipeline_runs SET finished_at=?, status='failed', error_msg=? WHERE run_id=?",
                (datetime.now(timezone.utc).isoformat(), str(e), run_id)
            )
            conn.commit()
        return {'status': 'failed', 'error': str(e)}
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['full', 'incremental'], default='incremental')
    args = parser.parse_args()

    result = run(args.mode)
    sys.exit(0 if result['status'] == 'success' else 1)
