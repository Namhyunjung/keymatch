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
# 5. 이상치 트리밍 + 월별 대표가 산출 (sync_engine.py 입력 포맷으로)
# ------------------------------------------------------------------
def build_monthly_price(
    clean_df: pd.DataFrame,
    trim_pct: float = 0.05,
    rep_method: str = 'max',  # 'max' | 'median'
) -> pd.DataFrame:
    """
    clean_transactions() 결과를 받아서 apt_seq × txn_ym 별 월별 대표가를 산출.
    반환 컬럼: apt_seq, ym, rep_price_10k, txn_count
    → sync_engine.py의 monthly 인자에 apt_seq별로 groupby해서 그대로 넣을 수 있음
    """
    active = clean_df[~clean_df['is_cancelled']].copy()

    def trim_and_summarize(group: pd.DataFrame) -> pd.Series:
        prices = group['price_10k'].sort_values()
        n = len(prices)
        if n >= 5:
            cut = max(1, int(n * trim_pct))
            prices = prices.iloc[cut:n - cut]
        rep = prices.max() if rep_method == 'max' else prices.median()
        return pd.Series({'rep_price_10k': rep, 'txn_count': n})

    result = (
        active.groupby(['apt_seq', 'txn_ym'])
        .apply(trim_and_summarize)
        .reset_index()
        .rename(columns={'txn_ym': 'ym'})
    )
    return result.sort_values(['apt_seq', 'ym']).reset_index(drop=True)


# ------------------------------------------------------------------
# 예시 실행
# ------------------------------------------------------------------
if __name__ == '__main__':
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
