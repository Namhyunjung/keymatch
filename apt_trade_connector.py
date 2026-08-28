"""
국토부 아파트매매 실거래가 상세 API 커넥터
엔드포인트: RTMSDataSvcAptTradeDev (data.go.kr)

사용법:
    connector = AptTradeConnector(service_key="발급받은 Decoding key")
    df = connector.fetch_region_month(lawd_cd="11680", deal_ymd="202608")
    # df를 여러 월 모아서 build_monthly_price()에 넣으면
    # sync_engine.py가 바로 쓸 수 있는 monthly_price 형태가 됨
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional
import requests
import xml.etree.ElementTree as ET
import pandas as pd


BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

# API 응답 필드 → 내부 컬럼명 매핑 (schema.sql 컬럼명과 정렬)
FIELD_MAP = {
    'aptSeq': 'apt_seq',
    'aptNm': 'complex_name',
    'umdNm': 'dong_name',
    'sggCd': 'sgg_cd',
    'excluUseAr': 'area_m2',
    'dealAmount': 'price_10k_raw',
    'dealYear': 'deal_year',
    'dealMonth': 'deal_month',
    'dealDay': 'deal_day',
    'floor': 'floor',
    'buildYear': 'built_year',
    'dealingGbn': 'dealing_gbn',
    'cdealType': 'cdeal_type',
    'cdealDay': 'cdeal_day',
    'jibun': 'jibun',
    'roadNm': 'road_name',
}


class AptTradeAPIError(Exception):
    pass


@dataclass
class FetchStats:
    lawd_cd: str
    deal_ymd: str
    total_count: int
    pages_fetched: int
    rows_returned: int


class AptTradeConnector:
    def __init__(self, service_key: str, timeout: int = 10, max_retries: int = 3, page_size: int = 100):
        self.service_key = service_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.page_size = page_size

    # ------------------------------------------------------------
    # 1. 단일 (지역, 월) 페이지네이션 전체 수집
    # ------------------------------------------------------------
    def fetch_region_month(self, lawd_cd: str, deal_ymd: str) -> pd.DataFrame:
        all_rows = []
        page_no = 1
        total_count = None

        while True:
            xml_root = self._request_with_retry(lawd_cd, deal_ymd, page_no)
            result_code = xml_root.findtext('.//resultCode')
            if result_code != '000':
                result_msg = xml_root.findtext('.//resultMsg')
                raise AptTradeAPIError(f"{lawd_cd}/{deal_ymd} 응답 오류: {result_code} {result_msg}")

            if total_count is None:
                total_count = int(xml_root.findtext('.//totalCount') or 0)

            items = xml_root.findall('.//item')
            for item in items:
                row = {}
                for api_field, col in FIELD_MAP.items():
                    el = item.find(api_field)
                    row[col] = el.text.strip() if (el is not None and el.text) else None
                row['lawd_cd'] = lawd_cd
                row['deal_ymd'] = deal_ymd
                all_rows.append(row)

            if total_count == 0 or page_no * self.page_size >= total_count:
                break
            page_no += 1
            time.sleep(0.05)  # 과도한 연속호출 방지

        return pd.DataFrame(all_rows)

    # ------------------------------------------------------------
    # 2. 재시도 포함 단건 요청
    # ------------------------------------------------------------
    def _request_with_retry(self, lawd_cd: str, deal_ymd: str, page_no: int) -> ET.Element:
        params = {
            'serviceKey': self.service_key,
            'LAWD_CD': lawd_cd,
            'DEAL_YMD': deal_ymd,
            'pageNo': page_no,
            'numOfRows': self.page_size,
        }
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(BASE_URL, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return ET.fromstring(resp.content)
            except Exception as e:  # 네트워크 오류, 파싱 오류 등
                last_err = e
                backoff = 2 ** attempt
                time.sleep(backoff)
        raise AptTradeAPIError(
            f"{lawd_cd}/{deal_ymd} page={page_no} {self.max_retries}회 재시도 실패: {last_err}"
        )

    # ------------------------------------------------------------
    # 3. 여러 지역 × 여러 월 일괄 수집
    # ------------------------------------------------------------
    def fetch_bulk(self, lawd_cd_list: list[str], deal_ymd_list: list[str]) -> pd.DataFrame:
        frames = []
        for lawd_cd in lawd_cd_list:
            for deal_ymd in deal_ymd_list:
                try:
                    df = self.fetch_region_month(lawd_cd, deal_ymd)
                    frames.append(df)
                except AptTradeAPIError as e:
                    print(f"[WARN] 수집 실패, 건너뜀: {e}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ------------------------------------------------------------------
# 4. 전처리: raw → schema.sql transactions 형태로 정제
# ------------------------------------------------------------------
def clean_transactions(raw: pd.DataFrame) -> pd.DataFrame:
    """
    fetch_bulk() 결과를 받아서:
    - 거래금액 콤마 제거 → int
    - txn_ym, txn_day 조합
    - 해제거래(is_cancelled) 플래그
    - 직거래 플래그
    """
    df = raw.copy()
    df['price_10k'] = (
        df['price_10k_raw'].str.replace(',', '', regex=False).astype(int)
    )
    df['txn_ym'] = df['deal_year'].str.zfill(4) + df['deal_month'].str.zfill(2)
    df['txn_day'] = df['deal_day'].astype('Int64')
    df['area_m2'] = df['area_m2'].astype(float)
    df['built_year'] = df['built_year'].astype('Int64')

    df['is_cancelled'] = df['cdeal_type'].fillna('').str.strip() == 'O'
    df['is_direct_txn'] = df['dealing_gbn'].fillna('').str.contains('직거래')

    keep_cols = [
        'apt_seq', 'complex_name', 'dong_name', 'sgg_cd', 'lawd_cd',
        'area_m2', 'floor', 'txn_ym', 'txn_day', 'price_10k',
        'built_year', 'is_direct_txn', 'is_cancelled', 'jibun', 'road_name',
    ]
    return df[keep_cols]


# ------------------------------------------------------------------
# 5. 이상치/특수관계 저가거래 방어 + 월별 대표가 산출
# ------------------------------------------------------------------
def build_monthly_price(
    clean_df: pd.DataFrame,
    trim_pct: float = 0.05,
    rep_method: str = 'max',             # 'max' | 'median'
    direct_deal_low_pct: float = 0.15,   # 직거래 + 기준가 대비 -15% 이상 낮으면 배제
    extreme_low_pct: float = 0.25,       # 거래유형 무관, 기준가 대비 -25% 이상 낮으면 배제
    ref_window_months: int = 5,          # 기준가(ref) 산출용 롤링 윈도우(개월)
) -> pd.DataFrame:
    """
    clean_transactions() 결과를 받아서 apt_seq × txn_ym 별 월별 대표가를 산출.

    거래건수(n>=5) 트리밍만으로는 '그 달에 거래가 1~2건뿐인데 그중 하나가
    증여성 저가 직거래'인 경우를 못 거른다 (트리밍 조건 자체가 안 걸림).
    그래서 아래처럼 2단계로 처리한다:

    1단계 — 단지별 롤링 기준가(ref) 산출
        월별 raw median(㎡당가)을 구하고, 인접 개월로 스무딩해서 '이 시점 대략
        이 정도가 시세다'라는 기준선을 만듦. 이 기준선 자체엔 아직 필터를 안 걸어서
        저가거래 1건이 그 달 유일한 거래여도 기준선은 앞뒤 달의 정상거래로 보정됨.

    2단계 — 기준가 대비 이탈 거래 배제
        - 직거래 + 기준가 대비 direct_deal_low_pct 이상 낮음 → 배제
        - 거래유형 무관 + 기준가 대비 extreme_low_pct 이상 낮음 → 배제 (통계적 극단치)
        남은 거래만으로 기존 상하위 5% 트리밍(n>=5일 때만) + 대표가(최고가) 산출.
        한 달에 유효 거래가 0건이 되면 그 달은 결측으로 남김
        (compute_regime_return의 n개월 윈도우 탐색이 인접 개월에서 값을 찾음).

    반환 컬럼: apt_seq, ym, rep_price_10k, txn_count, excluded_outlier_count
    → sync_engine.py의 monthly 인자에 apt_seq별로 groupby해서 그대로 넣을 수 있음
    """
    active = clean_df[~clean_df['is_cancelled']].copy()
    active['price_per_m2'] = active['price_10k'] / active['area_m2']

    # ---- 1단계: 단지별 롤링 기준가 ----
    monthly_raw_median = (
        active.groupby(['apt_seq', 'txn_ym'])['price_per_m2']
        .median()
        .reset_index()
        .sort_values(['apt_seq', 'txn_ym'])
    )
    monthly_raw_median['ref_price_per_m2'] = (
        monthly_raw_median.groupby('apt_seq')['price_per_m2']
        .transform(lambda s: s.rolling(ref_window_months, center=True, min_periods=1).median())
    )
    ref_map = monthly_raw_median.set_index(['apt_seq', 'txn_ym'])['ref_price_per_m2']

    active['ref_price_per_m2'] = active.set_index(['apt_seq', 'txn_ym']).index.map(ref_map)
    active['deviation'] = (active['price_per_m2'] - active['ref_price_per_m2']) / active['ref_price_per_m2']

    # ---- 2단계: 이탈 거래 배제 ----
    is_direct_low = active['is_direct_txn'] & (active['deviation'] <= -direct_deal_low_pct)
    is_extreme_low = active['deviation'] <= -extreme_low_pct
    active['is_suspected_outlier'] = is_direct_low | is_extreme_low

    def summarize(group: pd.DataFrame) -> pd.Series:
        valid = group[~group['is_suspected_outlier']]
        excluded = len(group) - len(valid)
        if valid.empty:
            return pd.Series({'rep_price_10k': None, 'txn_count': len(group), 'excluded_outlier_count': excluded})
        prices = valid['price_10k'].sort_values()
        n = len(prices)
        if n >= 5:
            cut = max(1, int(n * trim_pct))
            prices = prices.iloc[cut:n - cut]
        rep = prices.max() if rep_method == 'max' else prices.median()
        return pd.Series({'rep_price_10k': rep, 'txn_count': len(group), 'excluded_outlier_count': excluded})

    result = (
        active.groupby(['apt_seq', 'txn_ym'])
        .apply(summarize)
        .reset_index()
        .rename(columns={'txn_ym': 'ym'})
    )
    result = result.dropna(subset=['rep_price_10k'])
    result['rep_price_10k'] = result['rep_price_10k'].astype(int)
    return result.sort_values(['apt_seq', 'ym']).reset_index(drop=True)


# ------------------------------------------------------------------
# 예시 실행
# ------------------------------------------------------------------
if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # Windows cp949 콘솔 대응

    # 실제 서비스키 없이 구조만 확인하고 싶다면 raw 샘플로 clean/build 단계를 테스트
    sample_raw = pd.DataFrame([
        {'apt_seq': '11680-101', 'complex_name': '대치 SK뷰', 'dong_name': '대치동', 'sgg_cd': '11680',
         'lawd_cd': '11680', 'area_m2': '84.95', 'price_10k_raw': '285,000', 'deal_year': '2026',
         'deal_month': '7', 'deal_day': '12', 'floor': '10', 'built_year': '2016',
         'dealing_gbn': '중개거래', 'cdeal_type': None, 'cdeal_day': None, 'jibun': '123', 'road_name': '삼성로'},
        {'apt_seq': '11680-101', 'complex_name': '대치 SK뷰', 'dong_name': '대치동', 'sgg_cd': '11680',
         'lawd_cd': '11680', 'area_m2': '84.95', 'price_10k_raw': '283,000', 'deal_year': '2026',
         'deal_month': '7', 'deal_day': '20', 'floor': '5', 'built_year': '2016',
         'dealing_gbn': '중개거래', 'cdeal_type': None, 'cdeal_day': None, 'jibun': '123', 'road_name': '삼성로'},
        {'apt_seq': '11680-101', 'complex_name': '대치 SK뷰', 'dong_name': '대치동', 'sgg_cd': '11680',
         'lawd_cd': '11680', 'area_m2': '84.95', 'price_10k_raw': '310,000', 'deal_year': '2026',
         'deal_month': '7', 'deal_day': '22', 'floor': '18', 'built_year': '2016',
         'dealing_gbn': '직거래', 'cdeal_type': 'O', 'cdeal_day': '26.08.01', 'jibun': '123', 'road_name': '삼성로'},
    ])
    cleaned = clean_transactions(sample_raw)
    print(cleaned[['apt_seq', 'txn_ym', 'price_10k', 'is_cancelled', 'is_direct_txn']])

    monthly = build_monthly_price(cleaned)
    print("\n월별 대표가:")
    print(monthly)

    # ---- 이상치 배제 로직 자가검증: 정상 시세 다수 + 증여성 저가 직거래 1건 ----
    def _row(day, price, dealing_gbn='중개거래'):
        return {
            'apt_seq': '11680-999', 'complex_name': '테스트단지', 'dong_name': '대치동', 'sgg_cd': '11680',
            'lawd_cd': '11680', 'area_m2': '84.00', 'price_10k_raw': price, 'deal_year': '2026',
            'deal_month': '7', 'deal_day': str(day), 'floor': '10', 'built_year': '2016',
            'dealing_gbn': dealing_gbn, 'cdeal_type': None, 'cdeal_day': None, 'jibun': '1', 'road_name': '삼성로',
        }

    outlier_sample = pd.DataFrame([
        _row(1, '280,000'), _row(5, '282,000'), _row(10, '279,000'), _row(15, '281,000'),
        _row(20, '230,000', dealing_gbn='직거래'),  # 기준가(약28억) 대비 -18% — 증여성 의심으로 배제돼야 함
    ])
    outlier_monthly = build_monthly_price(clean_transactions(outlier_sample))
    excluded = int(outlier_monthly.iloc[0]['excluded_outlier_count'])
    assert excluded == 1, f"기준가 대비 -18% 직거래는 배제돼야 하는데 배제 건수={excluded}"
    assert outlier_monthly.iloc[0]['rep_price_10k'] == 282000, "배제 후 대표가(최고가)는 28.2억이어야 함"
    print(f"\n이상치 배제 자가검증 통과 — 5건 중 {excluded}건 배제, 대표가 {outlier_monthly.iloc[0]['rep_price_10k']}만원")
