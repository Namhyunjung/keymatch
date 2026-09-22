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

    INDEX_HTML.write_text(html, encoding='utf-8')
    print(f"index.html 대시보드 스냅샷 갱신: {region_label} · {leader['name']} ({fmt_eok(leader['currentPrice'])}, {delta_text}) · 동조단지 {units_count}개")


if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    bake()
