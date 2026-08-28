# 배치 스케줄러 설계: cron vs Airflow

## 결론부터: 지금 단계는 cron으로 시작하세요

혼자 개발하는 MVP 단계에서 Airflow는 과한 인프라예요. 서버 하나 띄우고, Airflow 자체를 운영(웹서버+스케줄러+메타DB)하는 비용이 지금 파이프라인(지역 1~2개, 하루 배치 1번) 대비 너무 커요. **cron + `batch_runner.py`로 시작하고, 지역이 많아지고 배치 의존성이 복잡해질 때 Airflow로 넘어가는 경로**를 권장해요.

전환 신호(이때 Airflow로 옮기세요):
- 배치 단계가 늘어나서 "이 작업이 실패하면 다음 걸 건너뛰고 알림만" 같은 조건부 흐름이 필요할 때
- 여러 지역을 병렬로 수집해야 해서 동시성 제어가 필요할 때
- "실패한 것만 재실행" 같은 부분 재시도가 cron 스크립트 안에서 처리하기 번거로워질 때
- 운영 인원이 늘어나서 실행 이력을 웹 UI로 봐야 할 때

---

## 1. cron 기반 설계 (지금 단계)

### crontab

```cron
# 매일 새벽 3시: 최근 3개월 증분 수집 (신고지연·계약해제 반영)
0 3 * * * /home/deploy/keymatch/run_batch.sh incremental >> /var/log/keymatch/batch.log 2>&1

# 매주 일요일 새벽 4시: 전체 재계산 (파라미터 드리프트 점검용, 가벼우면 매일도 가능)
0 4 * * 0 /home/deploy/keymatch/run_batch.sh full >> /var/log/keymatch/batch_full.log 2>&1
```

### 래퍼 스크립트 (`run_batch.sh`)

cron은 실행 실패를 스스로 감지하지 못하니, 래퍼에서 종료코드를 보고 알림을 보내요.

```bash
#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-incremental}"
cd /home/deploy/keymatch/pipeline
source venv/bin/activate

if python batch_runner.py --mode "$MODE"; then
    echo "[$(date)] $MODE 배치 성공"
else
    echo "[$(date)] $MODE 배치 실패 — 알림 전송"
    curl -s -X POST "$SLACK_WEBHOOK_URL" \
         -d "{\"text\": \"⚠️ 키맞추기 배치 실패 (mode=$MODE) — 로그 확인 필요\"}"
    exit 1
fi
```

### 중복 실행 방지 (락 파일)

배치가 예상보다 오래 걸려서 다음 스케줄과 겹치는 걸 막아요.

```bash
#!/usr/bin/env bash
LOCKFILE=/tmp/keymatch_batch.lock
if [ -e "$LOCKFILE" ]; then
    echo "이미 실행 중 — 종료"
    exit 1
fi
trap "rm -f $LOCKFILE" EXIT
touch "$LOCKFILE"

# ... 기존 배치 로직
```

### 모니터링 (cron 단계에서 최소한으로)

- `pipeline_runs` 테이블(schema.sql에 이미 있음)에 매 실행 기록 → 간단한 대시보드 쿼리로 최근 실패율 확인
  ```sql
  SELECT status, COUNT(*) FROM pipeline_runs
  WHERE started_at > now() - interval '7 days' GROUP BY status;
  ```
- 실패 시 Slack 웹훅 알림 (위 래퍼 스크립트에 이미 포함)
- 이 정도면 지역 몇 개짜리 MVP에는 충분해요

---

## 2. Airflow로 넘어갈 때 (성장 이후)

같은 `batch_runner.py`의 함수들(`extract`, `transform_and_load`, `recompute`)을 그대로 Task로 감싸면 돼요. 로직을 다시 짤 필요 없이 오케스트레이션 계층만 바뀌는 구조예요.

```python
# dags/keymatch_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

import sys
sys.path.append('/opt/keymatch/pipeline')
from batch_runner import extract, transform_and_load, recompute, init_db
import sqlite3  # 운영에서는 psycopg2 등 Postgres 드라이버로 교체

default_args = {
    'owner': 'keymatch',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_notification(
        slack_conn_id='slack_default',
        text='⚠️ 키맞추기 파이프라인 실패: {{ task_instance.task_id }}',
    ),
}

with DAG(
    dag_id='keymatch_daily_pipeline',
    default_args=default_args,
    schedule='0 3 * * *',      # 매일 새벽 3시
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # cron의 락파일 역할을 Airflow가 대신함
) as dag:

    def _extract(**context):
        raw = extract(mode='incremental')
        context['ti'].xcom_push(key='raw_count', value=len(raw))
        raw.to_parquet('/tmp/keymatch_raw.parquet')  # Task 간 데이터 전달

    def _transform_load(**context):
        import pandas as pd
        raw = pd.read_parquet('/tmp/keymatch_raw.parquet')
        conn = sqlite3.connect('/opt/keymatch/keymatch.db')
        init_db(conn)
        rows = transform_and_load(conn, raw)
        conn.close()
        context['ti'].xcom_push(key='rows_ingested', value=rows)

    def _recompute(**context):
        conn = sqlite3.connect('/opt/keymatch/keymatch.db')
        recompute(conn)
        conn.close()

    extract_task = PythonOperator(task_id='extract', python_callable=_extract)
    transform_load_task = PythonOperator(task_id='transform_load', python_callable=_transform_load)
    recompute_task = PythonOperator(task_id='recompute', python_callable=_recompute)

    extract_task >> transform_load_task >> recompute_task
```

**Airflow로 넘어가면 얻는 것**:
- Task별 독립 재시도 (extract만 실패했으면 extract만 재실행, 이미 성공한 transform은 다시 안 돌림)
- 지역별 병렬 실행 (`extract.expand(lawd_cd=region_list)` 형태의 Dynamic Task Mapping)
- Gantt/Graph 뷰로 어느 단계가 오래 걸리는지 한눈에 확인
- 백필(과거 특정 날짜 재실행)이 UI 클릭 몇 번으로 가능

---

## 3. 지금 산출물이 두 경로 모두에서 재사용되는 이유

`batch_runner.py`를 cron이 직접 실행하든 Airflow가 Task로 감싸든, **핵심 로직(extract/transform_and_load/recompute)은 그대로**예요. 오케스트레이션 계층만 바뀌기 때문에, 지금 cron으로 시작해도 나중에 Airflow 전환 비용이 크지 않아요. 이게 "작게 시작하되 나중에 갈아엎지 않아도 되는" 구조를 짜는 핵심이에요.
