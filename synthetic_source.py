"""
synthetic_source.py
이 샌드박스는 외부 네트워크가 막혀 있어 data.go.kr에 실제로 접속할 수 없음.
그래서 apt_trade_connector.fetch_bulk()가 반환하는 것과 '동일한 컬럼 shape'의
데이터를 생성해서, 그 이후 파이프라인(clean_transactions → build_monthly_price →
sync_engine)이 실제 API 응답을 받았을 때와 완전히 동일한 코드 경로로 동작하는지
검증한다. 실제 서비스키가 생기면 이 파일만 apt_trade_connector.fetch_bulk() 호출로
교체하면 됨 (batch_runner.py의 EXTRACT 단계 한 줄만 바뀜).
"""
from __future__ import annotations

import random
import numpy as np
import pandas as pd


# 4개 국면 경계 (t=0~1 정규화 좌표, sync_engine.py Regime과 동일 구간)
_REGIME_ANCHORS_YM = ['201705', '201912', '202204', '202312', '202608']

# 단지별 지수(index, 2017.05=100) 경로 — 화면 목업/설계 문서에서 쓴 값과 동일
_COMPLEX_DEFS = [
    dict(apt_seq='11680-0001', name='대치 SK뷰', base_price_10k=139000, households=1631, built_year=2016,
         index_anchors=[100, 118, 210, 165, 205], role='leader'),
    dict(apt_seq='11680-0002', name='래미안 대치팰리스2차', base_price_10k=102000, households=1608, built_year=2015,
         index_anchors=[100, 115, 198, 158, 194], role='candidate'),
    dict(apt_seq='11680-0003', name='대치 삼성1차', base_price_10k=105000, households=1489, built_year=2000,
         index_anchors=[100, 117, 202, 162, 199], role='candidate'),
    dict(apt_seq='11680-0004', name='대치 현대', base_price_10k=138900, households=630, built_year=1999,
         index_anchors=[100, 112, 190, 148, 126], role='candidate'),
    dict(apt_seq='11680-0005', name='대치 롯데캐슬리베', base_price_10k=102600, households=497, built_year=2014,
         index_anchors=[100, 118, 208, 203, 235], role='candidate'),
]


def _month_range(start_ym: str, end_ym: str) -> list[str]:
    return pd.period_range(start_ym, end_ym, freq='M').strftime('%Y%m').tolist()


def _interp_index_series(anchors: list[float], months: list[str]) -> dict:
    """앵커(국면 경계) 지수를 전체 월별로 선형보간 + 약간의 노이즈"""
    anchor_positions = np.linspace(0, len(months) - 1, len(anchors))
    all_positions = np.arange(len(months))
    base = np.interp(all_positions, anchor_positions, anchors)
    noise = np.random.normal(0, 1.2, size=len(months))
    return dict(zip(months, base + noise))


def generate_raw_transactions(seed: int = 42) -> pd.DataFrame:
    """
    apt_trade_connector.clean_transactions()가 기대하는 것과 동일한
    raw 컬럼 shape으로 월별 2~6건씩 거래를 생성.
    """
    random.seed(seed)
    np.random.seed(seed)
    months = _month_range(_REGIME_ANCHORS_YM[0], _REGIME_ANCHORS_YM[-1])

    rows = []
    for cdef in _COMPLEX_DEFS:
        index_by_month = _interp_index_series(cdef['index_anchors'], months)
        for ym in months:
            # 월별 거래건수: 유동성 컷(3건) 검증 의미가 있게 가끔 0~2건도 나오게 함
            n_txn = np.random.choice([0, 1, 2, 3, 4, 5, 6], p=[0.08, 0.1, 0.15, 0.22, 0.2, 0.15, 0.1])
            month_price = cdef['base_price_10k'] * index_by_month[ym] / 100
            for _ in range(n_txn):
                price = int(month_price * np.random.normal(1.0, 0.03))
                rows.append({
                    'apt_seq': cdef['apt_seq'],
                    'complex_name': cdef['name'],
                    'dong_name': '대치동',
                    'sgg_cd': '11680',
                    'lawd_cd': '11680',
                    'area_m2': '84.95',
                    'price_10k_raw': f"{price:,}",
                    'deal_year': ym[:4],
                    'deal_month': str(int(ym[4:])),
                    'deal_day': str(np.random.randint(1, 28)),
                    'floor': str(np.random.randint(3, 25)),
                    'built_year': str(cdef['built_year']),
                    'dealing_gbn': np.random.choice(['중개거래', '직거래'], p=[0.92, 0.08]),
                    # 약 1.5% 확률로 해제 거래 시뮬레이션 (실제 데이터의 특성 반영)
                    'cdeal_type': 'O' if np.random.random() < 0.015 else None,
                    'cdeal_day': f"{ym[2:4]}.{ym[4:]}.15" if np.random.random() < 0.015 else None,
                    'jibun': '999',
                    'road_name': '삼성로',
                })

    df = pd.DataFrame(rows)
    # cdeal_type이 'O'인 행에서만 cdeal_day를 채우고 나머지는 None으로 정리
    df.loc[df['cdeal_type'] != 'O', 'cdeal_day'] = None
    return df


def complex_master() -> pd.DataFrame:
    return pd.DataFrame([
        {'apt_seq': c['apt_seq'], 'complex_name': c['name'],
         'households': c['households'], 'built_year': c['built_year'], 'role': c['role']}
        for c in _COMPLEX_DEFS
    ])
