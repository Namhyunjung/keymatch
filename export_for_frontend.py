"""
export_for_frontend.py
batch_runner.py가 채운 Supabase(Postgres)를 읽어서, keymatch.html의 지역선택 UI가
바로 소비할 수 있는 멀티 지역 JSON을 만든다.

출력 shape:
{
  "regions": [{"code": "1168010600", "label": "서울 강남구 대치동", "sido":..., "sigungu":..., "dong":...}, ...],
  "pyeongGroups": [{"id": 2, "label": "59㎡"}, ...],  # area_min 오름차순
  "defaultRegion": "1168010600",
  "defaultPyeongGroup": 3,
  "data": { "<region_code>": { "<pyeong_group_id>": {"leader": {...}, "units": [...]}, ... }, ... }
}
지역×평형 조합 중 동조단지가 하나라도 있는 조합만 담김 — 프론트는 지역 고른 다음
data[region_code]의 키(있는 평형)만 선택지로 보여주면 됨.
"""
from __future__ import annotations

import json
from pathlib import Path

from dotenv import load_dotenv

import db
from batch_runner import REGIMES  # 타임라인 시작/끝을 REGIMES와 한 군데서만 정의 — 따로 하드코딩하면
                                   # 국면 갱신할 때마다 여기도 같이 고쳐야 하는 걸 잊기 쉬움
                                   # (마지막 국면 종료월이 202608로 하드코딩돼 있었던 사례, 2026-09-01).

load_dotenv()

DEFAULT_REGION_CODE = '1168010600'  # 대치동 (batch_runner.REGIONS['11680']['dongs']와 동일한 값)
DEFAULT_PYEONG_LABEL = '84㎡'        # 국민평형 기준값 — pyeong_group_id는 DB SERIAL이라 고정 안 됨


def month_index(ym: str) -> float:
    """ym을 전체 타임라인(REGIMES 첫 국면 시작~마지막 국면 종료) 기준 0~1 t값으로 변환"""
    y, m = int(ym[:4]), int(ym[4:])
    start_y, start_m = int(REGIMES[0].start_ym[:4]), int(REGIMES[0].start_ym[4:])
    end_y, end_m = int(REGIMES[-1].end_ym[:4]), int(REGIMES[-1].end_ym[4:])
    idx = (y - start_y) * 12 + (m - start_m)
    total = (end_y - start_y) * 12 + (end_m - start_m)
    return round(idx / total, 4)


def series_to_pts(rows: list[tuple], base_price: float) -> list[list[float]]:
    """(ym, rep_price_10k) 목록을 index(base=100) 시계열 [[t, index], ...]로 변환"""
    return [[month_index(ym), round(price / base_price * 100, 1)] for ym, price in rows]


def _build_region_data(
    leader_id: int, summaries: list[tuple], pyeong_label: str,
    complex_info: dict, monthly_by_complex: dict, regime_returns: dict,
) -> dict:
    """leader_complex_id + sync_summary 행들 -> keymatch.html이 먹는 {leader, units} 하나 (평형 하나 기준)."""
    leader_name, leader_built, leader_house = complex_info[leader_id]
    leader_rows = monthly_by_complex[leader_id]
    leader_base = leader_rows[0][1]
    leader_pts = series_to_pts(leader_rows, leader_base)
    leader_current = leader_rows[-1][1] / 10000  # 억 단위

    units = []
    for cid, sync_count, judgable_count, sync_index, gap in summaries:
        name, built_year, households = complex_info[cid]
        rows = monthly_by_complex.get(cid, [])
        if not rows:
            continue
        base = rows[0][1]
        pts = series_to_pts(rows, base)
        current_price = rows[-1][1] / 10000

        regime_flags = []
        for regime in REGIMES:
            regime_id = regime.regime_id
            entry = regime_returns.get((cid, regime_id))
            if entry is None or not entry[0]:
                regime_flags.append(None)  # 판정불가
                continue
            leader_entry = regime_returns.get((leader_id, regime_id))
            same_dir = (leader_entry[1] > 0) == (entry[1] > 0)
            synced = same_dir and abs(entry[1] - leader_entry[1]) <= 0.12
            regime_flags.append(bool(synced))

        units.append({
            'id': f'complex-{cid}',
            'name': name,
            'meta': f"{households:,}세대 · {built_year or '-'}년 준공" if households else f"- · {built_year or '-'}년 준공",
            'gap': f"{gap/10000:.1f}억".replace('-', '−'),
            'syncFlags': regime_flags,
            'pts': pts,
            'currentPrice': round(current_price, 1),
            'syncIndex': float(sync_index),
            # 완전동조(전체 판정국면 동조) 외에도 부분동조(2국면↑ 동조)까지 노출 —
            # 예전엔 sync_index>=0.75 게이트라 2국면 동조는 프론트에 아예 안 왔음.
            'passesFilter': judgable_count >= 3 and sync_count >= 2,
            'info': {
                'households': f"{households:,}세대" if households else '-',
                'year': f"{built_year or '-'}년",
                'size': f'전용 {pyeong_label}',
                'recent': f"{rows[-1][0][2:4]}.{rows[-1][0][4:]} · {current_price:.1f}억",
            }
        })

    passing_units = [u for u in units if u['passesFilter']]
    return {
        'leader': {
            'name': leader_name,
            'meta': f"{leader_house:,}세대 · {leader_built or '-'}년 준공 · 전용 {pyeong_label} 기준" if leader_house else f"- · {leader_built or '-'}년 준공 · 전용 {pyeong_label} 기준",
            'currentPrice': round(leader_current, 1),
            'pts': leader_pts,
        },
        'units': passing_units,
        'excluded_count': len(units) - len(passing_units),
    }


def export_all(default_region_code: str = DEFAULT_REGION_CODE) -> dict:
    conn = db.get_conn()

    # 단지마다 왕복 쿼리하면 원격 DB에서 느림 — 필요한 테이블 전체를 한 번씩만 긁어서
    # 메모리(dict)에서 조합함. 지역/평형이 늘어나도 이 부분은 그대로 재사용됨.
    complex_info = {
        cid: (name, built_year, households)
        for cid, name, built_year, households in conn.execute(
            "SELECT complex_id, complex_name, built_year, households FROM complexes"
        )
    }

    # 평형은 서로 다른 단지 모수라 완전히 독립 — (complex_id, pyeong_group_id)로 키를 나눔.
    # 안 그러면 59㎡ 시계열이랑 84㎡ 시계열이 같은 단지 밑에 섞여서 그래프가 뒤죽박죽 됨.
    monthly_by_complex: dict[tuple[int, int], list[tuple]] = {}
    for cid, pg_id, ym, price in conn.execute(
        "SELECT complex_id, pyeong_group_id, ym, rep_price_10k FROM monthly_price ORDER BY complex_id, ym"
    ):
        monthly_by_complex.setdefault((cid, pg_id), []).append((ym, price))

    regime_returns = {}  # (complex_id, pyeong_group_id, regime_id) -> (is_judgable, return_rate)
    for cid, pg_id, regime_id, is_judgable, return_rate in conn.execute(
        "SELECT complex_id, pyeong_group_id, regime_id, is_judgable, return_rate FROM regime_returns"
    ):
        regime_returns[(cid, pg_id, regime_id)] = (is_judgable, return_rate)

    region_parts = {}  # region_code -> (sido, sigungu, dong) — 프론트 시/도>시/군/구>읍면동 계층선택용
    for region_code, sido, sigungu, eupmyeondong in conn.execute(
        "SELECT region_code, sido, sigungu, eupmyeondong FROM regions"
    ):
        region_parts[region_code] = (sido, sigungu, eupmyeondong)

    pyeong_group_rows = conn.execute(
        "SELECT pyeong_group_id, label FROM pyeong_groups ORDER BY area_min ASC"
    ).fetchall()
    pyeong_labels = {pg_id: label for pg_id, label in pyeong_group_rows}
    default_pg_id = next((pg_id for pg_id, label in pyeong_group_rows if label == DEFAULT_PYEONG_LABEL), None)

    leaders = conn.execute(
        """SELECT DISTINCT ON (region_code, pyeong_group_id) region_code, pyeong_group_id, complex_id
           FROM leader_complexes ORDER BY region_code, pyeong_group_id, as_of DESC"""
    ).fetchall()

    data: dict[str, dict] = {}
    region_codes_used = set()
    pg_ids_used = set()
    for region_code, pg_id, leader_id in leaders:
        key = (leader_id, pg_id)
        if leader_id not in complex_info or key not in monthly_by_complex:
            continue
        summaries = conn.execute(
            """SELECT candidate_complex_id, sync_count, judgable_count, sync_index, current_price_gap_10k
               FROM sync_summary WHERE leader_complex_id=%s AND pyeong_group_id=%s
               ORDER BY sync_index DESC, current_price_gap_10k ASC""",
            (leader_id, pg_id)
        ).fetchall()
        pg_monthly = {cid: rows for (cid, p), rows in monthly_by_complex.items() if p == pg_id}
        pg_regime_returns = {(cid, rid): v for (cid, p, rid), v in regime_returns.items() if p == pg_id}
        region_data = _build_region_data(
            leader_id, summaries, pyeong_labels.get(pg_id, '?'),
            complex_info, pg_monthly, pg_regime_returns
        )
        if not region_data['units']:
            continue  # 동조 필터 통과한 단지가 하나도 없으면 프론트에 노출할 실익 없음
        data.setdefault(region_code, {})[str(pg_id)] = region_data
        region_codes_used.add(region_code)
        pg_ids_used.add(pg_id)

    conn.close()

    if not data:
        raise RuntimeError("대장단지 판정 결과가 하나도 없음 — recompute() 먼저 실행 필요")

    regions = []
    for region_code in region_codes_used:
        sido, sigungu, dong = region_parts.get(region_code, (None, None, None))
        label = ' '.join(p for p in (sido, sigungu, dong) if p) or region_code
        regions.append({'code': region_code, 'label': label, 'sido': sido, 'sigungu': sigungu, 'dong': dong})
    regions.sort(key=lambda r: r['label'])

    pyeong_groups = [{'id': pg_id, 'label': pyeong_labels[pg_id]}
                      for pg_id, _ in pyeong_group_rows if pg_id in pg_ids_used]

    default_region = default_region_code if default_region_code in data else regions[0]['code']
    region_pg_ids = set(data[default_region].keys())
    default_pyeong = (
        default_pg_id if default_pg_id is not None and str(default_pg_id) in region_pg_ids
        else int(next(iter(region_pg_ids)))
    )

    return {
        'regions': regions, 'pyeongGroups': pyeong_groups,
        'defaultRegion': default_region, 'defaultPyeongGroup': default_pyeong,
        'data': data,
    }


if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # Windows cp949 콘솔 대응

    result = export_all()
    out_path = Path(__file__).parent / 'frontend_data.json'
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    combo_count = sum(len(pgs) for pgs in result['data'].values())
    print(f"내보내기 완료: {out_path} — {len(result['regions'])}개 지역 × {len(result['pyeongGroups'])}개 평형 "
          f"(조합 {combo_count}개)")
    for r in result['regions']:
        for pg_id_str, d in result['data'][r['code']].items():
            label = next(p['label'] for p in result['pyeongGroups'] if str(p['id']) == pg_id_str)
            print(f"  [{r['label']} · {label}] 대장단지: {d['leader']['name']} "
                  f"({d['leader']['currentPrice']}억) · 동조단지 {len(d['units'])}개")
