"""
batch_runner.py
cron이든 Airflow든, 이 스크립트 하나를 실행하면 전체 파이프라인이 돈다.
    python batch_runner.py --mode full        # 전체 재계산 (신규 지역 백필용, 201705~ 전체월 API 호출)
    python batch_runner.py --mode incremental # 최근 3개월만 (매일 새벽 배치)

EXTRACT는 AptTradeConnector로 실 API 호출. .env의 APT_API_KEY 필요.
DB는 Supabase(Postgres). .env의 DATABASE_URL 필요 (db.py 참고).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

import db
from apt_trade_connector import AptTradeConnector, clean_transactions, build_monthly_price
from kapt_connector import HouseholdsResolver
from sync_engine import AlgoParams, Regime, evaluate_candidate, compute_regime_return, pick_leader

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

SCHEMA_PATH = Path(__file__).parent / 'schema.sql'

REGIMES = [
    Regime(1, '규제강화기', '201705', '201912'),
    Regime(2, '급등기', '202001', '202204'),
    Regime(3, '하락기', '202205', '202312'),
    Regime(4, '회복기', '202401', '202608'),
]


# ------------------------------------------------------------
# DB 초기화
# ------------------------------------------------------------
def init_db(conn):
    exists = conn.execute("SELECT to_regclass('public.regions')").fetchone()[0]
    if exists is None:
        log.info("스키마 초기화")
        conn.execute(SCHEMA_PATH.read_text(encoding='utf-8'))
        conn.commit()
    if conn.execute("SELECT COUNT(*) FROM regions").fetchone()[0] == 0:
        log.info("정적 시드 데이터 없음 — 시딩")
        _seed_static(conn)


def _seed_static(conn):
    conn.execute(
        "INSERT INTO regions (region_code, sido, sigungu, eupmyeondong) VALUES (%s,%s,%s,%s)",
        (GANGNAM_REGION_CODE, '서울', '강남구', None)
    )
    for dong_name, code in GANGNAM_DONG_CODES.items():
        conn.execute(
            "INSERT INTO regions (region_code, sido, sigungu, eupmyeondong) VALUES (%s,%s,%s,%s)",
            (code, '서울', '강남구', dong_name)
        )
    conn.execute("INSERT INTO pyeong_groups (label, area_min, area_max) VALUES ('84㎡', 81, 88)")
    for r in REGIMES:
        conn.execute(
            "INSERT INTO regimes (label, start_ym, end_ym, display_order) VALUES (%s,%s,%s,%s)",
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
def transform_and_load(conn, raw: pd.DataFrame) -> int:
    log.info("TRANSFORM 시작")
    cleaned = clean_transactions(raw)

    # 동조판정은 같은 평형끼리만 비교 가능 — pyeong_groups 범위로 거래 필터링.
    # (전체 평형 섞어서 대표가를 뽑으면 대장단지가 엉뚱한 초대형 평형으로 튐)
    pg_id, area_min, area_max = conn.execute(
        "SELECT pyeong_group_id, area_min, area_max FROM pyeong_groups LIMIT 1"
    ).fetchone()
    area_min, area_max = float(area_min), float(area_max)
    cleaned = cleaned[cleaned['area_m2'].between(area_min, area_max)].copy()
    log.info(f"평형 필터 적용 ({area_min}~{area_max}㎡) — {len(cleaned)}행 남음")

    # 단지 마스터 upsert (apt_seq 기준)
    # 세대수는 국토부 실거래가 API에 없는 필드 — 공동주택 단지목록/기본정보 API(kapt_connector)에서
    # 단지명+준공년도(실거래가 API에서 온 진짜 값)로 매칭해서 채움. 이미 매칭된 건 재조회 안 함
    # (API 왕복비용 있고 세대수는 거의 안 바뀌는 값이라 danji_code가 있으면 재사용).
    already_matched = {
        row[0]: row[1] for row in conn.execute(
            "SELECT apt_seq, households FROM complexes WHERE households IS NOT NULL"
        )
    }
    resolver = HouseholdsResolver(os.environ["APT_API_KEY"])
    resolved_count = 0

    for apt_seq, group in cleaned.groupby('apt_seq'):
        name = group['complex_name'].iloc[0]
        by = group['built_year'].dropna()
        built_year = int(by.iloc[0]) if not by.empty else None
        dong_name = group['dong_name'].iloc[0]
        region_code = GANGNAM_DONG_CODES.get(dong_name)
        if region_code is None:
            log.warning(f"미등록 동명 '{dong_name}' (apt_seq={apt_seq}) — 강남구로 대체")
            region_code = GANGNAM_REGION_CODE

        if apt_seq in already_matched:
            households, danji_code, match_confidence = already_matched[apt_seq], None, None
        else:
            households, danji_code, match_confidence = resolver.resolve(region_code, name, built_year)
            if households is not None:
                resolved_count += 1

        conn.execute(
            """INSERT INTO complexes (complex_name, region_code, apt_seq, built_year, households, danji_code, match_confidence)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (apt_seq) DO UPDATE SET
                 complex_name=EXCLUDED.complex_name, region_code=EXCLUDED.region_code,
                 households=COALESCE(complexes.households, EXCLUDED.households),
                 danji_code=COALESCE(complexes.danji_code, EXCLUDED.danji_code),
                 match_confidence=COALESCE(complexes.match_confidence, EXCLUDED.match_confidence)""",
            (name, region_code, apt_seq, built_year, households, danji_code, match_confidence)
        )
    conn.commit()
    log.info(f"세대수 매칭 — 이번에 새로 매칭 {resolved_count}건, 기존 캐시 {len(already_matched)}건")

    complex_id_map = {
        row[1]: row[0] for row in conn.execute("SELECT complex_id, apt_seq FROM complexes")
    }

    # 원본 거래 적재 (raw_source_id로 중복방지) — 원격 DB라 묶어서 insert
    txn_rows = []
    for _, row in cleaned.iterrows():
        raw_source_id = f"{row['apt_seq']}_{row['txn_ym']}_{row['txn_day']}_{row['price_10k']}_{row['floor']}"
        txn_rows.append((
            complex_id_map[row['apt_seq']], pg_id, row['area_m2'], row['floor'],
            row['txn_ym'], row['txn_day'], row['price_10k'],
            bool(row['is_direct_txn']), bool(row['is_cancelled']), raw_source_id
        ))
    db.insert_many(
        conn, 'transactions',
        ['complex_id', 'pyeong_group_id', 'area_m2', 'floor', 'txn_ym', 'txn_day',
         'price_10k', 'is_direct_txn', 'is_cancelled', 'raw_source_id'],
        txn_rows,
        on_conflict='ON CONFLICT (raw_source_id) DO NOTHING',
    )
    conn.commit()
    rows_ingested = len(txn_rows)

    # 월별 대표가 재계산 — 이번에 수집된 (단지×월) 건만 갱신.
    monthly = build_monthly_price(cleaned)
    monthly_rows = [
        (complex_id_map[r['apt_seq']], pg_id, r['ym'], int(r['rep_price_10k']), int(r['txn_count']))
        for _, r in monthly.iterrows()
    ]
    db.insert_many(
        conn, 'monthly_price',
        ['complex_id', 'pyeong_group_id', 'ym', 'rep_price_10k', 'txn_count'],
        monthly_rows,
        on_conflict='''ON CONFLICT (complex_id, pyeong_group_id, ym)
                        DO UPDATE SET rep_price_10k=EXCLUDED.rep_price_10k, txn_count=EXCLUDED.txn_count''',
    )
    conn.commit()
    log.info(f"LOAD 완료 — 거래 {rows_ingested}건, 월별대표가 {len(monthly)}행")
    return rows_ingested


# ------------------------------------------------------------
# RECOMPUTE: 대장단지 판정 + 국면별 상승률 + 동조지수
# ------------------------------------------------------------
def recompute(conn):
    log.info("RECOMPUTE 시작")
    params = AlgoParams()
    pyeong_group_id = conn.execute("SELECT pyeong_group_id FROM pyeong_groups LIMIT 1").fetchone()[0]

    complexes = conn.execute("SELECT complex_id, apt_seq, complex_name FROM complexes").fetchall()

    # 단지마다 매번 SELECT 왕복하면 원격 DB에서 느림 — 이 평형의 월별가격 전체를 한 번에 긁어서
    # 메모리에서 단지별로 나눔 (pick_leader용 price_df도 같은 조회 재사용)
    all_monthly = pd.DataFrame(
        conn.execute(
            "SELECT complex_id, ym, rep_price_10k, txn_count FROM monthly_price WHERE pyeong_group_id=%s ORDER BY ym",
            (pyeong_group_id,)
        ).fetchall(),
        columns=['complex_id', 'ym', 'rep_price_10k', 'txn_count']
    )
    monthly_by_complex = {
        cid: g[['ym', 'rep_price_10k', 'txn_count']].reset_index(drop=True)
        for cid, g in all_monthly.groupby('complex_id')
    }
    empty_monthly = pd.DataFrame(columns=['ym', 'rep_price_10k', 'txn_count'])

    def load_monthly(complex_id: int) -> pd.DataFrame:
        return monthly_by_complex.get(complex_id, empty_monthly)

    latest_prices = []
    for cid, apt_seq, name in complexes:
        m = load_monthly(cid)
        if m.empty:
            continue
        latest_prices.append((cid, apt_seq, name, m.iloc[-1]['rep_price_10k']))

    # 대장단지 판정: 국면 종료시점마다 상위 10% 안에 몇 번 들었는지(순위안정성) — sync_engine.pick_leader()
    price_df = all_monthly[['complex_id', 'ym', 'rep_price_10k']].rename(columns={'rep_price_10k': 'price_per_pyeong'})
    ranking = pick_leader(price_df, REGIMES, params)
    if ranking.empty:
        raise RuntimeError("대장단지 판정 불가 — 국면 종료시점에 거래 데이터 없음")
    leader_id = int(ranking.iloc[0]['complex_id'])
    stable_regimes = int(ranking.iloc[0]['stable_regimes'])
    leader_apt_seq, leader_name = next((a, n) for cid, a, n in complexes if cid == leader_id)
    log.info(
        f"대장단지 순위안정성: stable_regimes={stable_regimes}/{len(REGIMES)} "
        f"(상위 {int(params.leader_top_pct*100)}% 기준)"
    )
    log.info(f"대장단지 판정: {leader_name} ({leader_apt_seq})")

    conn.execute("DELETE FROM leader_complexes")
    conn.execute(
        """INSERT INTO leader_complexes (region_code, pyeong_group_id, complex_id, stable_regimes, as_of)
           VALUES (%s, %s, %s, %s, %s)""",
        (GANGNAM_REGION_CODE, pyeong_group_id, leader_id, stable_regimes, datetime.now(timezone.utc).date())
    )

    leader_monthly = load_monthly(leader_id)
    conn.execute("DELETE FROM regime_returns")
    conn.execute("DELETE FROM sync_summary")

    # 원격 DB라 후보 단지 수만큼 row-by-row insert하면 왕복비용이 큼 — 모아서 한번에 넣음
    regime_return_rows = []
    sync_summary_rows = []

    for regime in REGIMES:
        stat = compute_regime_return(leader_monthly, regime, params)
        rr = stat['return_rate']
        regime_return_rows.append((
            leader_id, pyeong_group_id, regime.regime_id,
            float(rr) if rr is not None else None,
            int(stat['txn_count_regime']), bool(stat['is_judgable'])
        ))

    for cid, apt_seq, name, _ in latest_prices:
        if cid == leader_id:
            continue
        cand_monthly = load_monthly(cid)
        result = evaluate_candidate(leader_monthly, cand_monthly, REGIMES, params)

        for r in result['per_regime']:
            cr = r['candidate_return']
            regime_return_rows.append((
                cid, pyeong_group_id, r['regime_id'],
                float(cr) if cr is not None else None, None, bool(r['judgable'])
            ))

        leader_price = int(leader_monthly.iloc[-1]['rep_price_10k'])
        cand_price = int(cand_monthly.iloc[-1]['rep_price_10k']) if not cand_monthly.empty else None
        gap = (cand_price - leader_price) if cand_price is not None else None

        sync_summary_rows.append((
            GANGNAM_REGION_CODE, pyeong_group_id, leader_id, cid, int(result['sync_count']), int(result['judgable_count']),
            float(result['sync_index']), int(gap) if gap is not None else None,
            datetime.now(timezone.utc)
        ))

    db.insert_many(
        conn, 'regime_returns',
        ['complex_id', 'pyeong_group_id', 'regime_id', 'return_rate', 'txn_count_regime', 'is_judgable'],
        regime_return_rows,
    )
    db.insert_many(
        conn, 'sync_summary',
        ['region_code', 'pyeong_group_id', 'leader_complex_id', 'candidate_complex_id',
         'sync_count', 'judgable_count', 'sync_index', 'current_price_gap_10k', 'updated_at'],
        sync_summary_rows,
    )
    conn.commit()
    log.info("RECOMPUTE 완료")


# ------------------------------------------------------------
# 메인 엔트리포인트 (cron/Airflow가 호출하는 지점)
# ------------------------------------------------------------
def run(mode: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = db.get_conn()

    run_id = None
    try:
        init_db(conn)
        run_id = conn.execute(
            "INSERT INTO pipeline_runs (started_at, status) VALUES (%s, 'running') RETURNING run_id",
            (started,)
        ).fetchone()[0]
        conn.commit()

        raw = extract(mode)
        rows_ingested = transform_and_load(conn, raw)
        recompute(conn)

        conn.execute(
            "UPDATE pipeline_runs SET finished_at=%s, status='success', rows_ingested=%s WHERE run_id=%s",
            (datetime.now(timezone.utc), rows_ingested, run_id)
        )
        conn.commit()
        log.info("파이프라인 성공")
        return {'status': 'success', 'run_id': run_id}

    except Exception as e:
        log.exception("파이프라인 실패")
        if run_id is not None:
            conn.execute(
                "UPDATE pipeline_runs SET finished_at=%s, status='failed', error_msg=%s WHERE run_id=%s",
                (datetime.now(timezone.utc), str(e), run_id)
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
