"""
공동주택 단지 목록/기본정보 API 커넥터 (data.go.kr, 국토교통부).

apt_trade_connector.py(실거래가)의 apt_seq와 이 API의 kaptCode는 서로 다른 코드체계라
직접 조인이 안 됨 — 법정동코드로 후보를 좁히고 단지명 유사도 + 준공년도로 매칭해서
세대수(households)를 채우는 데 씀. built_year는 이미 실거래가 API에서 온 진짜 데이터라
매칭 검증(사용승인년도 비교)에만 쓰고 덮어쓰지 않음.
"""
from __future__ import annotations

import difflib
from typing import Optional

import requests

LIST_URL = "https://apis.data.go.kr/1613000/AptListService4/getLegaldongAptList4"
BASIS_URL = "https://apis.data.go.kr/1613000/AptBasisInfoServiceV5/getAphusBassInfoV5"


class KaptAPIError(Exception):
    pass


def fetch_legaldong_apt_list(service_key: str, bjd_code: str, timeout: int = 10) -> list[dict]:
    """법정동코드(10자리)로 그 동 안의 단지 목록(kaptCode, kaptName) 전부 가져옴."""
    all_items = []
    page_no = 1
    page_size = 50
    while True:
        params = {'serviceKey': service_key, 'bjdCode': bjd_code, 'pageNo': page_no, 'numOfRows': page_size}
        resp = requests.get(LIST_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        header = data['response']['header']
        if header['resultCode'] != '00':
            raise KaptAPIError(f"{bjd_code}: {header['resultCode']} {header['resultMsg']}")
        body = data['response']['body']
        items = body.get('items') or []
        all_items.extend(items)
        total = int(body.get('totalCount', 0))
        if page_no * page_size >= total or not items:
            break
        page_no += 1
    return all_items


def fetch_apt_basis(service_key: str, kapt_code: str, timeout: int = 10) -> Optional[dict]:
    """단지코드로 기본정보(세대수/사용승인일 등) 조회. 없으면 None."""
    params = {'serviceKey': service_key, 'kaptCode': kapt_code}
    resp = requests.get(BASIS_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    header = data['response']['header']
    if header['resultCode'] != '00':
        return None
    return data['response']['body'].get('item')


class HouseholdsResolver:
    """법정동코드×단지명×준공년도로 세대수를 찾아주는 캐시된 매칭기.
    (단지목록/기본정보 API는 왕복비용이 있어서 동/단지코드별로 한 번만 호출)"""

    def __init__(self, service_key: str, name_cutoff: float = 0.35):
        self.service_key = service_key
        self.name_cutoff = name_cutoff
        self._list_cache: dict[str, list[dict]] = {}
        self._basis_cache: dict[str, Optional[dict]] = {}

    def _candidates(self, bjd_code: str) -> list[dict]:
        if bjd_code not in self._list_cache:
            try:
                self._list_cache[bjd_code] = fetch_legaldong_apt_list(self.service_key, bjd_code)
            except KaptAPIError:
                self._list_cache[bjd_code] = []
        return self._list_cache[bjd_code]

    def _basis(self, kapt_code: str) -> Optional[dict]:
        if kapt_code not in self._basis_cache:
            self._basis_cache[kapt_code] = fetch_apt_basis(self.service_key, kapt_code)
        return self._basis_cache[kapt_code]

    def resolve(self, bjd_code: str, complex_name: str, built_year: Optional[int]) -> tuple:
        """반환: (households, kapt_code, match_confidence) — 매칭 실패시 (None, None, None)."""
        candidates = self._candidates(bjd_code)
        if not candidates:
            return None, None, None

        names = [c['kaptName'] for c in candidates]
        # 이름 유사도 상위 몇 개를 준공년도로 재검증 (동명이인/유사명 단지 오매칭 방지)
        close = difflib.get_close_matches(complex_name, names, n=3, cutoff=self.name_cutoff)
        for name in close:
            item = next(c for c in candidates if c['kaptName'] == name)
            basis = self._basis(item['kaptCode'])
            if basis is None or not basis.get('kaptUsedate') or not basis.get('kaptdaCnt'):
                continue
            api_built_year = int(str(basis['kaptUsedate'])[:4])
            if built_year is not None and abs(api_built_year - built_year) > 1:
                continue  # 준공년도 안 맞음 — 다른 단지로 봄
            score = difflib.SequenceMatcher(None, complex_name, name).ratio()
            return int(float(basis['kaptdaCnt'])), item['kaptCode'], round(score, 2)
        return None, None, None
