"""
publish_drafts.py
drafts/queue.json에 예약된 글 중 발행일(KST)이 된 것을 insights/로 옮겨 발행한다.
GitHub Actions(.github/workflows/publish.yml)가 매일 돌림 — 사람이 접속 안 해도 정해진 날짜에 글이 올라감.

발행 = 3가지를 한 번에: drafts/<slug>.html → insights/<slug>.html (날짜 채우고 noindex 제거),
insights.html 목록 맨 위에 <li> 추가, sitemap.xml에 URL 추가. 메인 '최근 분석 글'은 뒤이어
bake_default_snapshot.py가 insights.html을 읽어 채움.

초안 작성 규칙: drafts/<slug>.html 안에 날짜 자리는 {{DATE}}, <meta name="robots" content="noindex,nofollow"> 포함
(drafts/는 .vercelignore로 배포에서 빠지지만, 혹시 새어나가도 색인 안 되게 이중 안전장치).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path

ROOT = Path(__file__).parent
DRAFTS = ROOT / 'drafts'
QUEUE = DRAFTS / 'queue.json'
SITE = 'https://keymatch-steel.vercel.app'
NOINDEX = '<meta name="robots" content="noindex,nofollow">\n'


def today_kst() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=9)).strftime('%Y-%m-%d')


def publish_one(entry: dict, root: Path = ROOT) -> None:
    slug, date = entry['slug'], entry['date']
    html = (root / 'drafts' / f'{slug}.html').read_text(encoding='utf-8')
    html = html.replace('{{DATE}}', date).replace(NOINDEX, '')
    (root / 'insights' / f'{slug}.html').write_text(html, encoding='utf-8')
    (root / 'drafts' / f'{slug}.html').unlink()

    li = (f'    <li class="post-item">\n'
          f'      <a href="insights/{slug}.html">{escape(entry["title"])}</a>\n'
          f'      <div class="post-date">{date}</div>\n'
          f'      <div class="post-summary">{escape(entry["summary"])}</div>\n'
          f'    </li>\n')
    anchor = '<ul class="post-list" id="postList">\n'
    idx = root / 'insights.html'
    s = idx.read_text(encoding='utf-8')
    if anchor not in s:
        raise RuntimeError('insights.html 목록 자리 못 찾음')
    idx.write_text(s.replace(anchor, anchor + li, 1), encoding='utf-8')

    sm = root / 'sitemap.xml'
    s = sm.read_text(encoding='utf-8')
    url = (f'  <url>\n    <loc>{SITE}/insights/{slug}.html</loc>\n'
           f'    <lastmod>{date}</lastmod>\n    <changefreq>monthly</changefreq>\n    <priority>0.5</priority>\n  </url>\n')
    sm.write_text(s.replace('</urlset>', url + '</urlset>', 1), encoding='utf-8')


def run(today: str, root: Path = ROOT) -> list[str]:
    queue_path = root / 'drafts' / 'queue.json'
    queue = json.loads(queue_path.read_text(encoding='utf-8'))
    due = [e for e in queue if e['date'] <= today]
    for e in sorted(due, key=lambda e: e['date']):
        publish_one(e, root)
    queue_path.write_text(json.dumps([e for e in queue if e['date'] > today], ensure_ascii=False, indent=2) + '\n',
                          encoding='utf-8')
    return [e['slug'] for e in due]


def _selfcheck() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        r = Path(d)
        (r / 'drafts').mkdir(); (r / 'insights').mkdir()
        (r / 'drafts' / 'a.html').write_text(f'<head>{NOINDEX}</head><div class="meta">{{{{DATE}}}}</div>', encoding='utf-8')
        (r / 'drafts' / 'b.html').write_text('x', encoding='utf-8')
        (r / 'drafts' / 'queue.json').write_text(json.dumps([
            {'slug': 'a', 'date': '2026-01-02', 'title': 'A & B', 'summary': 's'},
            {'slug': 'b', 'date': '2026-01-05', 'title': 'B', 'summary': 's'}]), encoding='utf-8')
        (r / 'insights.html').write_text('<ul class="post-list" id="postList">\n</ul>', encoding='utf-8')
        (r / 'sitemap.xml').write_text('<urlset>\n</urlset>', encoding='utf-8')
        assert run('2026-01-03', r) == ['a']
        out = (r / 'insights' / 'a.html').read_text(encoding='utf-8')
        assert '2026-01-02' in out and 'noindex' not in out
        assert 'A &amp; B' in (r / 'insights.html').read_text(encoding='utf-8')
        assert 'insights/a.html' in (r / 'sitemap.xml').read_text(encoding='utf-8')
        assert (r / 'drafts' / 'b.html').exists() and not (r / 'drafts' / 'a.html').exists()
        assert run('2026-01-03', r) == []  # 같은 날 재실행해도 중복 발행 없음
    print('selfcheck ok')


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if '--selfcheck' in sys.argv:
        _selfcheck()
    else:
        published = run(today_kst())
        print('발행:', ', '.join(published) if published else '없음')
