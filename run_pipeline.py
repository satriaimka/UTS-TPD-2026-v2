"""
Runner Utama — Menjalankan seluruh ETL Pipeline secara lokal (tanpa Airflow)
Urutan: Setup Source → Extract → Transform → Load
"""
import os
import sys
import time

# Set HADOOP_HOME untuk Spark di Windows
os.environ["HADOOP_HOME"] = "C:/hadoop-3.0.0"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    start_time = time.time()

    print("╔" + "═" * 58 + "╗")
    print("║     TikTok ETL Pipeline — Data Warehouse & BI           ║")
    print("║     Tanggal: 1 Mei 2026                                 ║")
    print("╚" + "═" * 58 + "╝")

    # ---- Step 1: Setup Source Data ----
    print("\n>>> STEP 1: Setup Source Data (PostgreSQL + MongoDB)")
    from scripts.setup_source_data import setup_postgres_source, setup_mongodb_source
    setup_postgres_source()
    setup_mongodb_source()

    # ---- Step 2: Extract ----
    print("\n>>> STEP 2: Extract dari 4 Sumber Data")
    from scripts.extract import run_extract
    spark, df_tiktok, df_influencer, df_comments, df_taxonomy = run_extract()

    # ---- Step 3: Transform ----
    print("\n>>> STEP 3: Transform (Cleaning, Feature Engineering, Join, Agregasi)")
    from scripts.transform import run_transform
    df_final, df_inf_clean, df_tax_clean = run_transform(
        spark, df_tiktok, df_influencer, df_comments, df_taxonomy
    )

    # ---- Step 4: Load ----
    print("\n>>> STEP 4: Load ke Data Warehouse PostgreSQL")
    from scripts.load import run_load
    run_load(spark, df_final, df_inf_clean, df_tax_clean)

    # ---- Selesai ----
    elapsed = time.time() - start_time
    spark.stop()

    print("\n" + "╔" + "═" * 58 + "╗")
    print(f"║  Pipeline selesai dalam {elapsed:.1f} detik                      ║")
    print("║  Data Warehouse: tiktok_warehouse (PostgreSQL)          ║")
    print("╚" + "═" * 58 + "╝")


if __name__ == "__main__":
    main()
