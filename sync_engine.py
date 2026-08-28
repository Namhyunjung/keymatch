"""
키맞추기 — 동조 판정 알고리즘 구현
schema.sql의 monthly_price / regimes 테이블을 pandas DataFrame으로 다룬다고 가정.
실제 서비스에서는 DB 쿼리 결과를 그대로 아래 함수들에 넣으면 됨.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import pandas as pd
import numpy as np


# ------------------------------------------------------------------
# 0. 파라미터
# ------------------------------------------------------------------
@dataclass
class AlgoParams:
    n_months: int = 6          # 최고가 산출 윈도우
    alpha_pp: float = 0.12     # 오차범위 (12%p = 0.12)
    min_txn_regime: int = 3    # 국면당 최소 거래건수
    min_judgable: int = 3      # 최소 판정가능 국면 수
    min_sync_index: float = 0.75  # 최소 동조지수
    leader_top_pct: float = 0.10  # 대장단지 평당가 상위 %


@dataclass
class Regime:
    regime_id: int
    label: str
    start_ym: str  # 'YYYYMM'
    end_ym: str


# ------------------------------------------------------------------
# 1. n개월 최고가 산출
# ------------------------------------------------------------------
def max_price_in_window(monthly: pd.DataFrame, center_ym: str, n_months: int) -> Optional[float]:
    """
    monthly: columns = ['ym', 'rep_price_10k', 'txn_count'], ym 오름차순 정렬 가정
    center_ym 기준 앞뒤 n_months/2 범위가 아니라, '해당 시점 direction'으로 n개월을 봄
    (여기서는 center_ym을 포함해 최근 n개월 — 국면 시작점은 시작 이후 n개월, 종료점은 종료 이전 n개월)
    """
    window = monthly[monthly['ym'].between(*_ym_range(center_ym, n_months))]
    if window.empty:
        return None
    return window['rep_price_10k'].max()


def _ym_range(center_ym: str, n_months: int, forward: bool = True):
    """center_ym으로부터 n_months 범위의 (start, end) YYYYMM 문자열 반환"""
    y, m = int(center_ym[:4]), int(center_ym[4:])
    idx = y * 12 + (m - 1)
    if forward:
        end_idx = idx + n_months - 1
        start_idx, end_idx = idx, end_idx
    else:
        start_idx = idx - n_months + 1
        end_idx = idx
    def to_ym(i):
        yy, mm = divmod(i, 12)
        return f"{yy:04d}{mm+1:02d}"
    return to_ym(start_idx), to_ym(end_idx)


# ------------------------------------------------------------------
# 2. 국면별 상승률 계산
# ------------------------------------------------------------------
def compute_regime_return(monthly: pd.DataFrame, regime: Regime, params: AlgoParams) -> dict:
    """
    monthly: 특정 단지×평형의 월별 대표가 (ym, rep_price_10k, txn_count)
    반환: return_rate, txn_count_regime, is_judgable
    """
    start_lo, start_hi = _ym_range(regime.start_ym, params.n_months, forward=True)
    end_lo, end_hi = _ym_range(regime.end_ym, params.n_months, forward=False)

    start_window = monthly[monthly['ym'].between(start_lo, start_hi)]
    end_window = monthly[monthly['ym'].between(end_lo, end_hi)]

    regime_txns = monthly[monthly['ym'].between(regime.start_ym, regime.end_ym)]
    txn_count_regime = int(regime_txns['txn_count'].sum())

    if start_window.empty or end_window.empty:
        return dict(return_rate=None, txn_count_regime=txn_count_regime, is_judgable=False)

    start_price = start_window['rep_price_10k'].max()
    end_price = end_window['rep_price_10k'].max()
    return_rate = end_price / start_price - 1

    is_judgable = txn_count_regime >= params.min_txn_regime
    return dict(return_rate=return_rate, txn_count_regime=txn_count_regime, is_judgable=is_judgable)


# ------------------------------------------------------------------
# 3. 동조 판정 (국면 1개)
# ------------------------------------------------------------------
def is_synced(leader_return: float, candidate_return: float, alpha_pp: float) -> bool:
    """
    ① 방향 일치: 부호가 다르면 무조건 비동조
    ② 폭 일치: |후보 - 대장| <= alpha_pp
    """
    if leader_return == 0:
        # 대장단지가 보합인 국면 — 후보도 |return| <= alpha_pp면 동조로 봄
        return abs(candidate_return) <= alpha_pp

    same_direction = (leader_return > 0 and candidate_return > 0) or \
                      (leader_return < 0 and candidate_return < 0)
    if not same_direction:
        return False

    return abs(candidate_return - leader_return) <= alpha_pp


# ------------------------------------------------------------------
# 4. 후보 단지 하나에 대해 전체 국면 동조 판정 → 동조지수 산출
# ------------------------------------------------------------------
def evaluate_candidate(
    leader_monthly: pd.DataFrame,
    candidate_monthly: pd.DataFrame,
    regimes: list[Regime],
    params: AlgoParams,
) -> dict:
    per_regime = []
    for regime in regimes:
        leader_stat = compute_regime_return(leader_monthly, regime, params)
        cand_stat = compute_regime_return(candidate_monthly, regime, params)

        judgable = (
            leader_stat['is_judgable'] and cand_stat['is_judgable']
            and leader_stat['return_rate'] is not None
            and cand_stat['return_rate'] is not None
        )

        synced = None
        if judgable:
            synced = is_synced(leader_stat['return_rate'], cand_stat['return_rate'], params.alpha_pp)

        per_regime.append({
            'regime_id': regime.regime_id,
            'label': regime.label,
            'leader_return': leader_stat['return_rate'],
            'candidate_return': cand_stat['return_rate'],
            'judgable': judgable,
            'synced': synced,
        })

    judgable_regimes = [r for r in per_regime if r['judgable']]
    sync_count = sum(1 for r in judgable_regimes if r['synced'])
    judgable_count = len(judgable_regimes)
    sync_index = sync_count / judgable_count if judgable_count > 0 else 0.0

    passes_filter = (
        judgable_count >= params.min_judgable
        and sync_index >= params.min_sync_index
    )

    return {
        'per_regime': per_regime,
        'sync_count': sync_count,
        'judgable_count': judgable_count,
        'sync_index': round(sync_index, 3),
        'passes_filter': passes_filter,
    }


# ------------------------------------------------------------------
# 5. 대장단지 판정
# ------------------------------------------------------------------
def _quarterly_yms(start_ym: str, end_ym: str) -> list[str]:
    """국면 구간을 분기(3개월) 간격으로 샘플링. 종료시점은 항상 포함."""
    idx = int(start_ym[:4]) * 12 + (int(start_ym[4:]) - 1)
    end_idx = int(end_ym[:4]) * 12 + (int(end_ym[4:]) - 1)
    out = []
    while idx <= end_idx:
        yy, mm = divmod(idx, 12)
        out.append(f"{yy:04d}{mm + 1:02d}")
        idx += 3
    if out[-1] != end_ym:
        out.append(end_ym)
    return out


def pick_leader(
    price_per_pyeong_by_complex: pd.DataFrame,  # columns: complex_id, ym, price_per_pyeong
    regimes: list[Regime],
    params: AlgoParams,
) -> pd.DataFrame:
    """
    국면 종료 시점 스냅샷 하나로만 순위를 매기면, 그 한 시점이 이상치 거래로
    왜곡됐을 때 대장단지 판정 전체가 흔들린다. 그래서 국면 구간을 분기별로
    나눠 각 시점의 평당가 순위를 구하고, 국면 내 평균 순위로 상위권 여부를 본다
    (특정 시점 하나의 왜곡이 다른 시점들에 희석됨).

    상위 leader_top_pct 안에 몇 개 국면 들었는지로 순위 안정성을 계산.
    반환: complex_id별 stable_regimes, is_leader_candidate
    """
    records = []
    for regime in regimes:
        sample_yms = _quarterly_yms(regime.start_ym, regime.end_ym)
        rank_series = []
        for ym in sample_yms:
            snap = price_per_pyeong_by_complex[price_per_pyeong_by_complex['ym'] == ym]
            if snap.empty:
                continue
            rank_series.append(
                snap.set_index('complex_id')['price_per_pyeong'].rank(pct=True, ascending=True)
            )
        if not rank_series:
            continue
        avg_rank = pd.concat(rank_series, axis=1).mean(axis=1)  # 결측 시점은 제외하고 평균
        top_tier = avg_rank >= (1 - params.leader_top_pct)
        records.append(pd.DataFrame({
            'complex_id': avg_rank.index,
            'regime_id': regime.regime_id,
            'top_tier': top_tier.values,
        }))

    if not records:
        return pd.DataFrame(columns=['complex_id', 'stable_regimes', 'is_leader_candidate'])

    all_snaps = pd.concat(records)
    summary = all_snaps.groupby('complex_id')['top_tier'].sum().reset_index()
    summary.columns = ['complex_id', 'stable_regimes']
    summary['is_leader_candidate'] = summary['stable_regimes'] >= (len(regimes) - 1)
    return summary.sort_values('stable_regimes', ascending=False)


# ------------------------------------------------------------------
# 예시 실행 (mock 데이터 — 화면 목업에서 쓴 값과 동일한 형태)
# ------------------------------------------------------------------
if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # Windows cp949 콘솔 대응

    regimes = [
        Regime(1, '규제강화기', '201705', '201912'),
        Regime(2, '급등기', '202001', '202204'),
        Regime(3, '하락기', '202205', '202312'),
        Regime(4, '회복기', '202401', '202608'),
    ]
    params = AlgoParams()

    def make_monthly(prices_by_regime_end: dict[str, float], base=13.9) -> pd.DataFrame:
        """간단 mock: 국면 경계 지점만 값 부여, 나머지는 선형보간 (테스트용)"""
        ym_list = pd.period_range('2017-05', '2026-08', freq='M').strftime('%Y%m').tolist()
        rows = []
        for ym in ym_list:
            rows.append({'ym': ym, 'rep_price_10k': np.nan, 'txn_count': 4})
        df = pd.DataFrame(rows)
        df['rep_price_10k'] = df['rep_price_10k'].astype(float)
        for ym, price in prices_by_regime_end.items():
            df.loc[df['ym'] == ym, 'rep_price_10k'] = price * 10000
        df['rep_price_10k'] = df['rep_price_10k'].interpolate().bfill().ffill()
        return df

    leader_monthly = make_monthly({
        '201705': 13.9, '201912': 16.4, '202204': 29.2, '202312': 22.9, '202608': 28.5
    })
    raemian_monthly = make_monthly({
        '201705': 10.2, '201912': 11.7, '202204': 20.2, '202312': 16.1, '202608': 19.8
    })
    hyundai_monthly = make_monthly({
        # 회복기(4번 국면)에 대장과 다르게 하락 → 비동조 기대
        '201705': 13.89, '201912': 15.5, '202204': 26.4, '202312': 20.5, '202608': 17.5
    })

    for name, monthly in [('래미안 대치팰리스2차', raemian_monthly), ('대치 현대', hyundai_monthly)]:
        result = evaluate_candidate(leader_monthly, monthly, regimes, params)
        print(f"\n[{name}] 동조지수: {result['sync_index']} "
              f"({result['sync_count']}/{result['judgable_count']}) "
              f"필터통과: {result['passes_filter']}")
        for r in result['per_regime']:
            lr = f"{r['leader_return']:.1%}" if r['leader_return'] is not None else '-'
            cr = f"{r['candidate_return']:.1%}" if r['candidate_return'] is not None else '-'
            print(f"  {r['label']:8s} 대장 {lr:>7s}  후보 {cr:>7s}  동조: {r['synced']}")

    # ---- pick_leader() 자가검증: 분기 샘플링 + 여러시점평균 순위 ----
    assert _quarterly_yms('201705', '201912') == [
        '201705', '201708', '201711', '201802', '201805', '201808', '201811',
        '201902', '201905', '201908', '201911', '201912'
    ], "분기 샘플링이 3개월 간격으로 안 나옴"
    assert _quarterly_yms('202401', '202401') == ['202401'], "1개월짜리 국면도 종료시점은 포함해야 함"

    # A=꾸준히 2등이지만 안 흔들림, B=한 시점(202001)만 반짝 1등이고 다음엔 최하위, C=꾸준히 낮음
    # → 국면 종료시점 스냅샷 하나만 봤으면 B가 대장으로 뽑혔을 상황. 여러시점평균이면 A가 이겨야 함.
    mock_price = pd.DataFrame([
        {'complex_id': 'A', 'ym': '202001', 'price_per_pyeong': 100},
        {'complex_id': 'B', 'ym': '202001', 'price_per_pyeong': 110},  # 반짝 1등
        {'complex_id': 'C', 'ym': '202001', 'price_per_pyeong': 50},
        {'complex_id': 'A', 'ym': '202004', 'price_per_pyeong': 100},
        {'complex_id': 'B', 'ym': '202004', 'price_per_pyeong': 40},   # 다음 시점엔 최하위로 추락
        {'complex_id': 'C', 'ym': '202004', 'price_per_pyeong': 50},
    ])
    test_params = AlgoParams(leader_top_pct=0.2)  # 평균순위 상위 20%만 통과하게 좁혀서 A만 걸리게
    ranking = pick_leader(mock_price, [Regime(2, '급등기', '202001', '202004')], test_params)
    top = ranking.iloc[0]
    assert top['complex_id'] == 'A', "여러 시점 평균이면 꾸준한 2등이 반짝 1등을 이겨야 함"
    print(f"\npick_leader 자가검증 통과 — 여러시점평균 1위: complex_id={top['complex_id']} (반짝 1등 B에 안 흔들림)")
