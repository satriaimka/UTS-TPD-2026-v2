"""
Apache Airflow DAG — TikTok ETL Pipeline
Schedule: Setiap hari pukul 06.00 WIB (0 6 * * *)
Tasks: extract → transform → load → report
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# Path ke project root
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["datateam@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_setup_sources(**kwargs):
    from scripts.setup_source_data import setup_postgres_source, setup_mongodb_source
    setup_postgres_source()
    setup_mongodb_source()


def task_extract(**kwargs):
    from scripts.extract import run_extract
    spark, df_tiktok, df_influencer, df_comments, df_taxonomy = run_extract()
    spark.stop()


def task_transform(**kwargs):
    from scripts.extract import create_spark_session
    from scripts.transform import run_transform
    from config.settings import STAGING_DIR
    spark = create_spark_session()
    df_tiktok = spark.read.parquet(os.path.join(STAGING_DIR, "tiktok_videos"))
    df_influencer = spark.read.parquet(os.path.join(STAGING_DIR, "influencer_profiles"))
    df_comments = spark.read.parquet(os.path.join(STAGING_DIR, "raw_comments"))
    df_taxonomy = spark.read.parquet(os.path.join(STAGING_DIR, "hashtag_taxonomy"))
    run_transform(spark, df_tiktok, df_influencer, df_comments, df_taxonomy)
    spark.stop()


def task_load(**kwargs):
    from scripts.extract import create_spark_session
    from scripts.load import run_load
    from config.settings import STAGING_DIR
    spark = create_spark_session()
    df_final = spark.read.parquet(os.path.join(STAGING_DIR, "transformed_final"))
    df_influencer = spark.read.parquet(os.path.join(STAGING_DIR, "influencer_profiles"))
    df_taxonomy = spark.read.parquet(os.path.join(STAGING_DIR, "hashtag_taxonomy"))
    run_load(spark, df_final, df_influencer, df_taxonomy)
    spark.stop()


def task_generate_report(**kwargs):
    print("Generating daily report...")
    # Jalankan OLAP queries jika diperlukan
    from scripts.olap_queries import run_all_olap
    run_all_olap()
    print("Report generated.")


with DAG(
    dag_id="tiktok_etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline TikTok — extract, transform, load ke DW",
    schedule="0 6 * * *",  # Setiap hari pukul 06.00
    catchup=False,
    max_active_runs=1,
    tags=["tiktok", "etl", "datawarehouse"],
) as dag:

    t1 = PythonOperator(task_id="setup_sources", python_callable=task_setup_sources)
    t2 = PythonOperator(task_id="extract_data", python_callable=task_extract)
    t3 = PythonOperator(task_id="transform_data", python_callable=task_transform)
    t4 = PythonOperator(task_id="load_to_warehouse", python_callable=task_load)
    t5 = PythonOperator(task_id="generate_report", python_callable=task_generate_report)

    # Dependency graph
    t1 >> t2 >> t3 >> t4 >> t5
