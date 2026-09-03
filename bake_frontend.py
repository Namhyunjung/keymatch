"""
frontend_data.json을 index.html(구 keymatch.html)의 <script id="pipelineData"> 안에 구워넣는다.
export_for_frontend.py 실행 직후에 씀 (batch_runner.py 실행 -> export_for_frontend.py -> bake_frontend.py 순).
"""
import re
from pathlib import Path

html_path = Path(__file__).parent / 'index.html'
data_path = Path(__file__).parent / 'frontend_data.json'

html = html_path.read_text(encoding='utf-8')
data = data_path.read_text(encoding='utf-8')
pattern = re.compile(r'(<script type="application/json" id="pipelineData">).*?(</script>)', re.S)
if not pattern.search(html):
    raise SystemExit("치환 실패 — pipelineData 스크립트 태그를 못 찾음")
new_html = pattern.sub(lambda m: m.group(1) + data + m.group(2), html, count=1)
html_path.write_text(new_html, encoding='utf-8')
print("index.html에 반영 완료")
