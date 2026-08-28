"""
pyeong_correlation_check.py
"84㎡ 순위가 높으면 59㎡ 순위도 높다고 볼 수 있는가?"를 실제 API 데이터로 검증.

독립 실행 스크립트임 — DB/스키마는 안 건드림. 84㎡/59㎡ 둘 다 실 API에서 새로
받아서 메모리에서만 계산 (지금 파이프라인 DB엔 84㎡ 한 평형만 있어서, 이 검증
하나 때문에 전체 파이프라인을 다평형 지원으로 확장하는 건 배보다 배꼽).

사용법:
    python pyeong_correlation_check.py --lawd_cd 11680
"""
from __future__ import annotations

import argparse
import os

import pandas as pd
from dotenv import load_dotenv
from scipy.stats import spearmanr

from apt_trade_connector import AptTradeConnector, clean_transactions, build_monthly_price

PYEONG_BANDS = {
    '84㎡': (81.0, 88.0),
    '59㎡': (56.0, 63.0),
}

# 전체기간(REGIMES 범위)과 맞춤 — batch_runner.py의 REGIMES 참고
YM_START, YM_END = '201705', '202608'


def _ym_range(start_ym: str, end_ym: str) -> list[str]:
    y, m = int(start_ym[:4]), int(start_ym[4:])
    end_y, end_m = int(end_ym[:4]), int(end_ym[4:])
    out = []
    while (y, m) <= (end_y, end_m):
        out.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def build_price_per_pyeong(cleaned: pd.DataFrame, band_label: str, area_min: float, area_max: float) -> pd.DataFrame:
    """한 평형 밴드로 필터링해서 apt_seq×ym별 평당가(3.3㎡=1평 환산) 산출."""
    band = cleaned[cleaned['area_m2'].between(area_min, area_max)].copy()
    if band.empty:
        return pd.DataFrame(columns=['apt_seq', 'ym', 'price_per_pyeong'])
    monthly = build_monthly_price(band)
    mid_area = (area_min + area_max) / 2.0
    monthly['price_per_pyeong'] = monthly['rep_price_10k'] / (mid_area / 3.3058)
    monthly['pyeong_label'] = band_label
    return monthly[['apt_seq', 'ym', 'price_per_pyeong', 'pyeong_label']]


def compute_rank_correlation(df_a: pd.DataFrame, df_b: pd.DataFrame, label_a: str, label_b: str):
    merged = df_a.merge(df_b, on=['apt_seq', 'ym'], suffixes=('_a', '_b'))
    if merged.empty:
        print(f"[안내] {label_a}와 {label_b}가 동시에 존재하는 (단지, 월) 조합이 없습니다.")
        return None

    per_month = []
    for ym, group in merged.groupby('ym'):
        if len(group) < 4:  # 상관계수 계산에 최소 표본 필요
            continue
        rho, pval = spearmanr(group['price_per_pyeong_a'], group['price_per_pyeong_b'])
        per_month.append({'ym': ym, 'n_complexes': len(group), 'spearman_rho': rho, 'p_value': pval})

    if not per_month:
        print("[안내] 월별 표본이 4개 미만이라 상관계수를 계산할 시점이 없습니다.")
        return None

    result_df = pd.DataFrame(per_month)
    avg_rho = result_df['spearman_rho'].mean()

    print(f"\n=== {label_a} vs {label_b} 평당가 순위 상관관계 ({len(result_df)}개 시점) ===")
    print(result_df.to_string(index=False))
    print(f"\n전체 평균 스피어만 상관계수: {avg_rho:.3f}")
    print(interpret(avg_rho))
    return {'per_month': result_df, 'avg_rho': avg_rho}


def interpret(rho: float) -> str:
    if rho >= 0.7:
        return "-> 강한 양의 상관관계. '84 순위 = 59 순위' 참고 가능."
    elif rho >= 0.4:
        return "-> 중간 정도 상관관계. 방향성은 있지만 예외가 꽤 있음. 약한 참고 신호 정도로만 쓰세요."
    else:
        return "-> 약한 상관관계. 84㎡ 순위로 59㎡ 순위를 추정하면 안 됨. 평형별로 완전 독립 판정 필요."


if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # Windows cp949 콘솔 대응

    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument('--lawd_cd', default='11680')
    args = parser.parse_args()

    connector = AptTradeConnector(service_key=os.environ["APT_API_KEY"])
    yms = _ym_range(YM_START, YM_END)
    print(f"실 API 수집 중 — {args.lawd_cd} / {yms[0]}~{yms[-1]} ({len(yms)}개월)...")
    raw = connector.fetch_bulk(lawd_cd_list=[args.lawd_cd], deal_ymd_list=yms)
    if raw.empty:
        raise SystemExit("EXTRACT 결과 0행 — API 호출 실패")
    cleaned = clean_transactions(raw)
    print(f"수집 완료 — 원본 {len(raw)}행")

    price_84 = build_price_per_pyeong(cleaned, '84㎡', *PYEONG_BANDS['84㎡'])
    price_59 = build_price_per_pyeong(cleaned, '59㎡', *PYEONG_BANDS['59㎡'])
    print(f"84㎡: {price_84['apt_seq'].nunique()}개 단지 / {len(price_84)}행")
    print(f"59㎡: {price_59['apt_seq'].nunique()}개 단지 / {len(price_59)}행")

    compute_rank_correlation(price_84, price_59, '84㎡', '59㎡')
