"""
Supabase(Postgres) 연결 헬퍼.

pg8000 + Supabase Supavisor 풀러 조합에서 SCRAM 채널바인딩(tls-server-end-point)이
프록시의 TLS 종단 때문에 실제 백엔드 인증서와 안 맞아서, 맞는 비밀번호로도
"password authentication failed"가 나는 문제가 있음 (Node.js pg 드라이버로 교차검증해서 확인함).
채널바인딩만 끄면 우회됨 — TLS 암호화 자체는 그대로 유지됨.
"""
import os
from urllib.parse import urlparse

import scramp
import pg8000.core as _pg8000_core
import pg8000.dbapi as pg8000

scramp.make_channel_binding = lambda *a, **k: None
_pg8000_core.scramp.make_channel_binding = lambda *a, **k: None


class _ConnWrapper:
    """pg8000 Connection엔 sqlite3.Connection의 .execute() 편의 메서드가 없어서
    (cursor()만 있음) 얇게 흉내냄 — 호출부를 sqlite3 코드와 최대한 비슷하게 유지하기 위함."""

    def __init__(self, raw_conn):
        self._conn = raw_conn

    def execute(self, sql, params=()):
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def get_conn():
    url = urlparse(os.environ["DATABASE_URL"])
    raw = pg8000.connect(
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        ssl_context=True,
    )
    return _ConnWrapper(raw)


def insert_many(conn, table, columns, rows, on_conflict="", chunk_size=500):
    """여러 행을 하나의 INSERT문으로 묶어서 실행 (원격 DB라 행 단위 execute는 왕복비용 큼).

    rows: 각 row가 columns 순서와 맞는 tuple의 iterable.
    on_conflict: 'ON CONFLICT (...) DO NOTHING' 같은 절 (없으면 생략).
    """
    rows = list(rows)
    if not rows:
        return
    cur = conn.cursor()
    col_list = ", ".join(columns)
    ncols = len(columns)
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        placeholders = ", ".join(
            "(" + ", ".join(["%s"] * ncols) + ")" for _ in chunk
        )
        flat_params = [v for row in chunk for v in row]
        sql = f"INSERT INTO {table} ({col_list}) VALUES {placeholders} {on_conflict}"
        cur.execute(sql, flat_params)
