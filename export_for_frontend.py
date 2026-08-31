"""
export_for_frontend.py
batch_runner.py가 채운 Supabase(Postgres)를 읽어서, keymatch.html의 지역선택 UI가
바로 소비할 수 있는 멀티 지역 JSON을 만든다.

출력 shape:
{
  "regions": [{"code": "1168010600", "label": "서울 강남구 대치동"}, ...],  # 대장단지 판정된 지역만, label순
  "defaultRegion": "1168010600",
  "data": { "<region_code>": {"leader": {...}, "units": [...]}, ... }
}
"""
from __future__ import annotations

import json
from pathlib import Path

from dotenv import load_dotenv

import db

load_dotenv()

DEFAULT_REGION_CODE = '1168010600'  # 대치동 (batch_runner.REGIONS['11680']['dongs']와 동일한 값)


def month_index(ym: str) -> float:
    """ym을 전체 타임라인(2017-05~2026-08) 기준 0~1 t값으로 변환"""
    y, m = int(ym[:4]), int(ym[4:])
    start_y, start_m = 2017, 5
    end_y, end_m = 2026, 8
    idx = (y - start_y) * 12 + (m - start_m)
    total = (end_y - start_y) * 12 + (end_m - start_m)
    return round(idx / total, 4)


def series_to_pts(rows: list[tuple], base_price: float) -> list[list[float]]:
    """(ym, rep_price_10k) 목록을 index(base=100) 시계열 [[t, index], ...]로 변환"""
    return [[month_index(ym), round(price / base_price * 100, 1)] for ym, price in rows]


def _build_region_data(
    leader_id: int, summaries: list[tuple],
    complex_info: dict, monthly_by_complex: dict, regime_returns: dict,
) -> dict:
    """leader_complex_id + sync_summary 행들 -> keymatch.html이 먹는 {leader, units} 하나."""
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
        for regime_id in range(1, 5):
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
            'passesFilter': judgable_count >= 3 and sync_index >= 0.75,
            'info': {
                'households': f"{households:,}세대" if households else '-',
                'year': f"{built_year or '-'}년",
                'size': '전용 84㎡',
                'recent': f"{rows[-1][0][2:4]}.{rows[-1][0][4:]} · {current_price:.1f}억",
            }
        })

    passing_units = [u for u in units if u['passesFilter']]
    return {
        'leader': {
            'name': leader_name,
            'meta': f"{leader_house:,}세대 · {leader_built or '-'}년 준공 · 전용 84㎡ 기준" if leader_house else f"- · {leader_built or '-'}년 준공 · 전용 84㎡ 기준",
            'currentPrice': round(leader_current, 1),
            'pts': leader_pts,
        },
        'units': passing_units,
        'excluded_count': len(units) - len(passing_units),
    }


def export_all(default_region_code: str = DEFAULT_REGION_CODE) -> dict:
    conn = db.get_conn()

    # 단지마다 왕복 쿼리하면 원격 DB에서 느림 — 필요한 테이블 전체를 한 번씩만 긁어서
    # 메모리(dict)에서 조합함. 지역이 여러 개로 늘어나도 이 부분은 그대로 재사용됨.
    complex_info = {
        cid: (name, built_year, households)
        for cid, name, built_year, households in conn.execute(
            "SELECT complex_id, complex_name, built_year, households FROM complexes"
        )
    }

    monthly_by_complex: dict[int, list[tuple]] = {}
    for cid, ym, price in conn.execute(
        "SELECT complex_id, ym, rep_price_10k FROM monthly_price ORDER BY complex_id, ym"
    ):
        monthly_by_complex.setdefault(cid, []).append((ym, price))

    regime_returns = {}  # (complex_id, regime_id) -> (is_judgable, return_rate)
    for cid, regime_id, is_judgable, return_rate in conn.execute(
        "SELECT complex_id, regime_id, is_judgable, return_rate FROM regime_returns"
    ):
        regime_returns[(cid, regime_id)] = (is_judgable, return_rate)

    region_labels = {}  # region_code -> "시도 시군구 동" 표시용 라벨
    for region_code, sido, sigungu, eupmyeondong in conn.execute(
        "SELECT region_code, sido, sigungu, eupmyeondong FROM regions"
    ):
        region_labels[region_code] = ' '.join(p for p in (sido, sigungu, eupmyeondong) if p)

    leaders = conn.execute(
        """SELECT DISTINCT ON (region_code) region_code, complex_id
           FROM leader_complexes ORDER BY region_code, as_of DESC"""
    ).fetchall()

    data = {}
    regions = []
    for region_code, leader_id in leaders:
        if leader_id not in complex_info or leader_id not in monthly_by_complex:
            continue
        summaries = conn.execute(
            """SELECT candidate_complex_id, sync_count, judgable_count, sync_index, current_price_gap_10k
               FROM sync_summary WHERE leader_complex_id=%s ORDER BY sync_index DESC, current_price_gap_10k ASC""",
            (leader_id,)
        ).fetchall()
        region_data = _build_region_data(leader_id, summaries, complex_info, monthly_by_complex, regime_returns)
        if not region_data['units']:
            continue  # 동조 필터 통과한 단지가 하나도 없으면 프론트에 노출할 실익 없음
        data[region_code] = region_data
        regions.append({'code': region_code, 'label': region_labels.get(region_code, region_code)})

    conn.close()

    if not data:
        raise RuntimeError("대장단지 판정 결과가 하나도 없음 — recompute() 먼저 실행 필요")

    regions.sort(key=lambda r: r['label'])
    default_region = default_region_code if default_region_code in data else regions[0]['code']

    return {'regions': regions, 'defaultRegion': default_region, 'data': data}


if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # Windows cp949 콘솔 대응

    result = export_all()
    out_path = Path(__file__).parent / 'frontend_data.json'
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"내보내기 완료: {out_path} — {len(result['regions'])}개 지역")
    for r in result['regions']:
        d = result['data'][r['code']]
        print(f"  [{r['label']}] 대장단지: {d['leader']['name']} ({d['leader']['currentPrice']}억) · 동조단지 {len(d['units'])}개")
