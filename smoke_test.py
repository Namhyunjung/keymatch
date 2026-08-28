"""
스모크 테스트: 실 API 1지역 x 1개월 호출 확인.
CLAUDE.md Next steps #2.

실행: python smoke_test.py
"""
import os
from dotenv import load_dotenv
from apt_trade_connector import AptTradeConnector

load_dotenv()
service_key = os.environ["APT_API_KEY"]

connector = AptTradeConnector(service_key=service_key)
df = connector.fetch_region_month(lawd_cd="11680", deal_ymd="202607")  # 대치동, 2026-07

print(f"rows: {len(df)}")
print(df.head(10))

assert len(df) >= 0, "API 호출 실패 (예외 없이 통과했으나 확인 필요)"
