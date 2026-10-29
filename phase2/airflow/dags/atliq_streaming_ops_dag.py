"""
AtliQ Phase 2 - Streaming Ops DAG
=================================
The stream never stops - but the SCHEDULED work around it is our job:
data-quality gate, table maintenance, daily rollup. Hourly DAG:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml / .env:  _PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://adb-<workspace-id>.<n>.azuredatabricks.net | Password: <PAT token>
3. Paste your SQL warehouse HTTP path below.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/046073d5006b9a58"   # <-- paste YOUR path

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # TODO 1 - Data-quality gate.
    # retries=0 so a real failure shows red immediately (needed for the screenshot).
    # For the red screenshot: temporarily change "2 HOURS" to "5 MINUTES", then set back.
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        retries=0,
        sql="""
            SELECT assert_true(
                COUNT(*) > 0,
                'DQ FAIL: no events landed in silver_order_events in the last 2 hours'
            )
            FROM atliq.streaming.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL 2 HOURS
        """,
    )

    # TODO 2 - Table maintenance.
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            "OPTIMIZE atliq.streaming.silver_order_events",
            "OPTIMIZE atliq.streaming.gold_revenue_5min",
        ],
    )

    # TODO 3 - Daily rollup (match the 3 event_type names to your real values).
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            CREATE OR REPLACE TABLE atliq.streaming.gold_daily_summary AS
            SELECT
                CAST(event_ts AS DATE) AS event_date,
                COUNT(DISTINCT CASE WHEN event_type = 'order_placed'     THEN order_id END) AS orders_placed,
                COUNT(DISTINCT CASE WHEN event_type = 'payment_received' THEN order_id END) AS orders_paid,
                COUNT(DISTINCT CASE WHEN event_type = 'order_cancelled'  THEN order_id END) AS orders_cancelled,
                SUM(CASE WHEN event_type = 'payment_received' THEN order_amount END)        AS revenue
            FROM atliq.streaming.silver_order_events
            GROUP BY CAST(event_ts AS DATE)
        """,
    )

    # TODO 4 - Chain them in order.
    check_fresh_events >> optimize_tables >> refresh_daily_summary