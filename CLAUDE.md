# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

This repository currently contains only `handoff_클로드코드.md` — a handoff document from prior work done in claude.ai. None of the files it describes (`keymatch.html`, `schema.sql`, `pipeline/*.py`, design docs) exist in this repo yet. Read that handoff doc in full before starting; it is the source of truth for what to build and in what order.

## Project concept

"키맞추기" (Keymatch) — a Korean real-estate analysis service. It finds undervalued apartment complexes that move in sync with their region's "대장단지" (the priciest, trend-leading complex in an area) across multiple policy regimes (규제강화기/급등기/하락기/회복기 — tightening/surge/decline/recovery phases), using MOLIT (국토교통부) real-transaction public data.

## Planned architecture (per handoff doc)

- `keymatch.html` — frontend mockup (dashboard, synced-complex list, comparison view)
- `schema.sql` — production Postgres schema
- `동조판정_알고리즘_설계.md` — core sync-detection algorithm design (key params: α=12%p tolerance band, n=6-month window)
- `backtest_plan.md` — walk-forward validation methodology for α/n params
- `api_pipeline_design.md` — MOLIT API integration spec
- `scheduler_design.md` — cron/Airflow batch scheduler design
- `pipeline/sync_engine.py` — core sync-detection logic (phase-by-phase price-change calc, sync judgment, 대장단지 判定)
- `pipeline/apt_trade_connector.py` — MOLIT API connector (needs a real service key to test)
- `pipeline/synthetic_source.py` — **temporary** fake-data generator standing in for the real API (built because the original sandbox had no network access)
- `pipeline/batch_runner.py` — batch orchestration entrypoint (extract → transform → load → recompute)
- `pipeline/export_for_frontend.py` — DB → JSON export for `keymatch.html`
- `pipeline/sqlite_schema.sql` — local dev/test schema (subset of `schema.sql`)
- `pipeline/keymatch.db` — SQLite output from an end-to-end run using synthetic data

Important: numbers currently baked into `keymatch.html` mockups are **not real transactions** — they came from `synthetic_source.py` fake data run through the real pipeline code path. Only the input data is fake; the transform/aggregation/sync-detection logic is the real thing.

## Next steps (priority order, per handoff doc §5)

1. Confirm MOLIT service key issuance (data.go.kr → "국토교통부_아파트매매 실거래자료(상세)" → apply for Decoding key)
2. Verify `apt_trade_connector.py` actually calls the real API (smoke test: 1 region, 1 month)
3. Swap `synthetic_source.generate_raw_transactions()` for `AptTradeConnector.fetch_bulk()` inside `batch_runner.py`'s `extract()`
4. Decide whether production stays on SQLite or moves to Postgres (`schema.sql`) — SQLite is fine to start
5. Seed the 법정동코드 (legal-dong code) master table from 행정표준코드관리시스템 into the `regions` table
6. First real run against 대치동 (법정동코드 11680, Gangnam-gu — verify this code) to replace `keymatch.html` mock data with real values
7. Register the batch job via cron per `scheduler_design.md`

## Secrets handling

The MOLIT API key must go in a `.env` file (git-ignored), never hardcoded:

```bash
# .env
APT_API_KEY=issued_decoding_key
```

```python
import os
from dotenv import load_dotenv
load_dotenv()
service_key = os.environ["APT_API_KEY"]
```
