"""
export_for_frontend.py
batch_runner.py가 채운 keymatch.db를 읽어서, keymatch.html의 하드코딩된
mock 배열(leaderPts, units)과 동일한 shape의 JSON을 만든다.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / 'keymatch.db'
REGIME_BOUNDS_YM = ['201705', '201912', '202204', '202312', '202608']


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


def export() -> dict:
    conn = sqlite3.connect(DB_PATH)

    leader_id = conn.execute(
        "SELECT complex_id FROM leader_complexes ORDER BY as_of DESC LIMIT 1"
    ).fetchone()[0]
    leader_name, leader_built, leader_house = conn.execute(
        "SELECT complex_name, built_year, households FROM complexes WHERE complex_id=?", (leader_id,)
    ).fetchone()

    leader_rows = conn.execute(
        "SELECT ym, rep_price_10k FROM monthly_price WHERE complex_id=? ORDER BY ym", (leader_id,)
    ).fetchall()
    leader_base = leader_rows[0][1]
    leader_pts = series_to_pts(leader_rows, leader_base)
    leader_current = leader_rows[-1][1] / 10000  # 억 단위

    units = []
    summaries = conn.execute(
        """SELECT candidate_complex_id, sync_count, judgable_count, sync_index, current_price_gap_10k
           FROM sync_summary WHERE leader_complex_id=? ORDER BY sync_index DESC, current_price_gap_10k ASC""",
        (leader_id,)
    ).fetchall()

    for cid, sync_count, judgable_count, sync_index, gap in summaries:
        name, built_year, households = conn.execute(
            "SELECT complex_name, built_year, households FROM complexes WHERE complex_id=?", (cid,)
        ).fetchone()
        rows = conn.execute(
            "SELECT ym, rep_price_10k FROM monthly_price WHERE complex_id=? ORDER BY ym", (cid,)
        ).fetchall()
        if not rows:
            continue
        base = rows[0][1]
        pts = series_to_pts(rows, base)
        current_price = rows[-1][1] / 10000

        regime_flags = []
        for regime_id in range(1, 5):
            row = conn.execute(
                "SELECT is_judgable, return_rate FROM regime_returns WHERE complex_id=? AND regime_id=?",
                (cid, regime_id)
            ).fetchone()
            if row is None or not row[0]:
                regime_flags.append(None)  # 판정불가
                continue
            leader_row = conn.execute(
                "SELECT return_rate FROM regime_returns WHERE complex_id=? AND regime_id=?",
                (leader_id, regime_id)
            ).fetchone()
            same_dir = (leader_row[0] > 0) == (row[1] > 0)
            synced = same_dir and abs(row[1] - leader_row[0]) <= 0.12
            regime_flags.append(bool(synced))

        units.append({
            'id': f'complex-{cid}',
            'name': name,
            'meta': f"{households:,}세대 · {built_year or '-'}년 준공" if households else f"- · {built_year or '-'}년 준공",
            'gap': f"{gap/10000:.1f}억".replace('-', '\u2212'),
            'syncFlags': regime_flags,
            'pts': pts,
            'currentPrice': round(current_price, 1),
            'syncIndex': sync_index,
            'passesFilter': judgable_count >= 3 and sync_index >= 0.75,
            'info': {
                'households': f"{households:,}세대" if households else '-',
                'year': f"{built_year or '-'}년",
                'size': '전용 84㎡',
                'recent': f"{rows[-1][0][2:4]}.{rows[-1][0][4:]} · {current_price:.1f}억",
            }
        })

    conn.close()
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


if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')  # Windows cp949 콘솔 대응

    data = export()
    out_path = Path(__file__).parent / 'frontend_data.json'
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"내보내기 완료: {out_path}")
    print(f"대장단지: {data['leader']['name']} ({data['leader']['currentPrice']}억)")
    for u in data['units']:
        flag_str = ''.join('✓' if f else ('·' if f is None else '✕') for f in u['syncFlags'])
        passes = 'O' if u['passesFilter'] else 'X'
        print(f"  {u['name']:16s} {u['currentPrice']:5.1f}억  {u['gap']:>8s}  {flag_str}  필터통과:{passes}")
