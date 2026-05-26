"""
NYC Taxi ETL DAG — 轻量版
演示:DAG 依赖 / 失败重试 / 幂等性(TRUNCATE+INSERT)/ 数据质量检查
从 ads.trip_search(230k 行明细)聚合刷新 ads.borough_summary
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

PG = dict(host="postgres", dbname="nyc_taxi", user="taxi_user", password="taxi_pass123")
 
def _conn():
    return psycopg2.connect(**PG)

# ── Task 1: 检查源表 ──
def check_source(**ctx):
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ads.trip_search")
    n = cur.fetchone()[0]; conn.close()
    print(f"✅ 源表 ads.trip_search 行数: {n:,}")
    if n == 0:
        raise ValueError("源表为空,中止 ETL!")

# ── Task 2: 刷新 borough 汇总(幂等:TRUNCATE + INSERT)──
def refresh_borough_summary(**ctx):
    conn = _conn(); cur = conn.cursor()
    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS ads.borough_summary (
            pickup_borough TEXT, time_bucket TEXT,
            total_trips BIGINT, total_revenue NUMERIC(14,2)
        );
        TRUNCATE ads.borough_summary;
        INSERT INTO ads.borough_summary
        SELECT pickup_borough, time_bucket, SUM(trips), SUM(revenue)
        FROM ads.trip_search
        GROUP BY pickup_borough, time_bucket;
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM ads.borough_summary")
    print(f"✅ borough_summary 刷新完成,{cur.fetchone()[0]} 行")
    conn.close()

# ── Task 3: 数据质量检查 ──
def quality_check(**ctx):
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*), SUM(total_trips) FROM ads.borough_summary")
    rows, trips = cur.fetchone(); conn.close()
    print(f"✅ 质量检查: {rows} 行, 总订单 {trips:,}")
    if rows == 0 or trips == 0:
        raise ValueError("结果表异常!")

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="nyc_taxi_etl",
    default_args=default_args,
    description="NYC Taxi ADS 层刷新",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["nyc-taxi"],
) as dag:
    t1 = PythonOperator(task_id="check_source", python_callable=check_source)
    t2 = PythonOperator(task_id="refresh_borough_summary", python_callable=refresh_borough_summary)
    t3 = PythonOperator(task_id="quality_check", python_callable=quality_check)

    t1 >> t2 >> t3   # 依赖:检查源 → 刷新 → 质量检查
