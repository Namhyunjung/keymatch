"""
bake_default_snapshot.py
export_for_frontend.py가 만든 data/index.json + data/r/*을 읽어서, index.html
대시보드 화면(1화면)의 자리표시 값(대장단지 이름/가격/세대수 등)을 실제 기본 지역
스냅샷으로 바꿔 박아넣는다.

왜 필요한가: 대시보드 콘텐츠는 전부 JS가 fetch 후 채움(index.html:1136-1150) — 그런데
JS 안 돌리고 raw HTML만 보는 크롤러(AdSense 심사 등)한테는 예시로 박아둔 옛날 값
("대치 SK뷰 28.5억")이 그대로 보임. 페이지 자체가 비어있진 않지만 값이 실제와 다르므로
빌드/배포 때마다 이 스크립트로 최신 기본 지역 값으로 갈아끼운다.

리스트/비교 화면(2,3번 스크린)은 기본 숨김 화면이라 스코프 밖 — 손 안 댐.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).parent
INDEX_HTML = ROOT / 'index.html'
DATA_DIR = ROOT / 'data'


def fmt_eok(v: float) -> str:
    return f"{v:.1f}억"


def naver_map_url(query: str) -> str:
    from urllib.parse import quote
    return 'https://map.naver.com/p/search/' + quote(query.strip())


def sub_marker(html: str, name: str, new_inner: str) -> str:
    # 중첩 태그가 들어가는 자리라 id 정규식 대신 <!--bake:X-->...<!--/bake:X--> 주석 마커로 갈아끼움
    pattern = re.compile(rf'(<!--bake:{name}-->).*?(<!--/bake:{name}-->)', re.DOTALL)
    new_html, n = pattern.subn(lambda m: m.group(1) + new_inner + m.group(2), html, count=1)
    if n != 1:
        raise RuntimeError(f'bake:{name} 마커 못 찾음 — index.html 구조가 바뀌었을 수 있음')
    return new_html


def snapshot_html(leader: dict, units: list, region_label: str, pg_label: str, delta_text: str) -> str:
    from html import escape
    rows = []
    for u in sorted(units, key=lambda u: u['currentPrice'], reverse=True):
        judgable = sum(f is not None for f in u['syncFlags'])
        synced = sum(f is True for f in u['syncFlags'])
        rows.append(f"<tr><td>{escape(u['name'])}</td><td>{fmt_eok(u['currentPrice'])}</td>"
                    f"<td>{escape(u['gap'])}</td><td>{synced}/{judgable}</td></tr>")
    intro = (f"<p>{escape(region_label)} {pg_label}의 대장단지는 <b>{escape(leader['name'])}</b>예요. "
             f"최근 대표가는 {fmt_eok(leader['currentPrice'])}이고, 2017년 5월 대비 {delta_text} 움직였어요. "
             f"이 대장단지와 국면별로 같이 움직인 동조단지는 {len(units)}곳이에요.</p>")
    if not rows:
        return intro
    table = ("<table><thead><tr><th>단지</th><th>최근가</th><th>대장 대비 갭</th><th>동조 국면</th></tr></thead>"
             f"<tbody>{''.join(rows)}</tbody></table>")
    note = ("<p>'동조 국면'은 거래가 충분해 판정 가능했던 국면 중 대장단지와 같이 움직인 국면 수예요. "
            "갭이 큰데 동조 국면이 많은 단지일수록 대장단지를 아직 덜 따라간 단지일 수 있어요.</p>")
    return intro + table + note


def recent_posts_html(limit: int = 5) -> str:
    # insights.html의 정적 목록(손으로 관리)을 그대로 읽어 위에서 limit개 복사 — 목록 관리처는 한 곳만 유지
    src = (ROOT / 'insights.html').read_text(encoding='utf-8')
    items = re.findall(r'<a class="report post-item" href="([^"]+)">.*?<h3>(.*?)</h3><p class="post-summary">(.*?)</p>', src)
    if not items:
        raise RuntimeError('insights.html 글 카드 못 찾음 — 카드 형식이 바뀌었으면 이 정규식도 고칠 것')
    return ''.join(f'<li><a href="{href}">{title}</a>{summary}</li>' for href, title, summary in items[:limit])


def bake() -> None:
    index = json.loads((DATA_DIR / 'index.json').read_text(encoding='utf-8'))
    region_code = index['defaultRegion']
    pg_id = index['defaultPyeongGroup']
    region_label = next(r['label'] for r in index['regions'] if r['code'] == region_code)

    payload = json.loads((DATA_DIR / 'r' / region_code / f'{pg_id}.json').read_text(encoding='utf-8'))
    leader = payload['leader']
    units_count = len(payload['units'])

    start_idx, end_idx = leader['pts'][0][1], leader['pts'][-1][1]
    total_return = (end_idx / start_idx - 1) * 100
    delta_text = f"{'+' if total_return >= 0 else ''}{total_return:.1f}%"

    map_url = naver_map_url(f"{region_label} {leader['name']}")

    html = INDEX_HTML.read_text(encoding='utf-8')

    def sub_tag(html: str, elem_id: str, new_inner: str) -> str:
        pattern = re.compile(rf'(id="{elem_id}"[^>]*>)(.*?)(</)', re.DOTALL)
        new_html, n = pattern.subn(lambda m: m.group(1) + new_inner + m.group(3), html, count=1)
        if n != 1:
            raise RuntimeError(f'id="{elem_id}" 자리 못 찾음 — index.html 구조가 바뀌었을 수 있음')
        return new_html

    html = sub_tag(html, 'locLabel', region_label)
    html = sub_tag(html, 'leaderName', leader['name'])
    html = sub_tag(html, 'leaderMeta', leader['meta'])
    html = sub_tag(html, 'leaderPrice', fmt_eok(leader['currentPrice']))
    html = sub_tag(html, 'leaderDelta', delta_text)
    html = sub_tag(html, 'leaderLegendName', leader['name'])
    html = sub_tag(html, 'ctaCount', f"{units_count}개")
    html = re.sub(r'(id="mapLinkDash"[^>]*href=")[^"]*(")', rf'\g<1>{map_url}\g<2>', html, count=1)

    # 하단 정적 섹션(.static-intro) — 크롤러가 JS 없이 읽는 실데이터 요약 + 최근 글 목록
    pg_label = next(g['label'] for g in index['pyeongGroups'] if g['id'] == pg_id)
    # 기본 지역이 고정이라 숫자는 새 실거래가 반영될 때만 바뀜 — "오늘의"가 아니라 최근 거래월을 기준으로 표기
    latest_ym = max((u['info']['recent'].split(' ')[0] for u in payload['units'] if u['info'].get('recent')), default='')
    basis = f" · 최근 거래 20{latest_ym} 기준" if latest_ym else ''
    html = sub_tag(html, 'staticSnapshotTitle', f"실거래 스냅샷: {region_label} {pg_label}")
    html = sub_tag(html, 'staticSnapshotBasis', f"매일 자동 갱신{basis}")
    html = sub_marker(html, 'snapshot', snapshot_html(leader, payload['units'], region_label, pg_label, delta_text))
    html = sub_marker(html, 'posts', recent_posts_html())

    INDEX_HTML.write_text(html, encoding='utf-8')
    print(f"index.html 대시보드 스냅샷 갱신: {region_label} · {leader['name']} ({fmt_eok(leader['currentPrice'])}, {delta_text}) · 동조단지 {units_count}개")


if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    bake()
