"""
EXTRACT LAYER — Pengambilan Data dari 4 Sumber
Sumber 1: TikTok Hashtag Scraper (JSON file)
Sumber 2: Influencer Profiles (CSV file)
Sumber 3: Raw Video Comments (MongoDB NoSQL)
Sumber 4: Hashtag Category Taxonomy (PostgreSQL RDBMS)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract


def create_spark_session():
    """Buat SparkSession dengan MongoDB dan PostgreSQL connector."""
    # Set Hadoop home untuk Windows (winutils.exe)
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["HADOOP_HOME"] = r"C:\hadoop-3.0.0"
    os.environ["PATH"] = os.environ["PATH"] + r";C:\hadoop-3.0.0\bin"

    spark = SparkSession.builder \
        .appName("TikTok_ETL") \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def flatten_columns(df):
    """Rename kolom yang mengandung titik agar mudah diakses."""
    for c in df.columns:
        if '.' in c:
            df = df.withColumnRenamed(c, c.replace('.', '_'))
    return df


# ============================================================
# SUMBER 1: TikTok Hashtag Scraper (JSON)
# ============================================================
def extract_tiktok_json(spark):
    """Baca data video TikTok dari JSON file hasil scraping Apify."""
    print("\n[EXTRACT 1/4] TikTok Hashtag Scraper (JSON)...")
    df = spark.read.option("multiLine", True).json(JSON_PATH)
    df = flatten_columns(df)

    print(f"  Records loaded: {df.count()}")
    df.printSchema()
    return df


# ============================================================
# SUMBER 2: Influencer Profiles (CSV)
# ============================================================
def extract_influencer_csv(spark):
    """Baca data profil kreator TikTok dari CSV file."""
    print("\n[EXTRACT 2/4] Influencer Profiles (CSV)...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)

    print(f"  Influencer records: {df.count()}")
    df.printSchema()
    return df


# ============================================================
# SUMBER 3: Raw Video Comments (MongoDB / NoSQL)
# ============================================================
def extract_comments(spark):
    """Baca komentar video dari MongoDB. Fallback ke JSON file jika gagal."""
    print("\n[EXTRACT 3/4] Raw Video Comments (MongoDB)...")
    try:
        df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", MONGO_URI) \
            .option("spark.mongodb.read.database", MONGO_DB) \
            .option("spark.mongodb.read.collection", MONGO_COLLECTION) \
            .load()
        print(f"  Comment records (dari MongoDB): {df.count()}")
    except Exception as e:
        print(f"  MongoDB connector gagal ({e}), fallback ke JSON file...")
        df = spark.read.option("multiLine", True).json(NOSQL_PATH)
        print(f"  Comment records (dari JSON file): {df.count()}")

    df.printSchema()
    return df


# ============================================================
# SUMBER 4: Hashtag Category Taxonomy (PostgreSQL)
# ============================================================
def extract_taxonomy(spark):
    """Baca hashtag taxonomy dari PostgreSQL. Fallback ke CSV file jika gagal."""
    print("\n[EXTRACT 4/4] Hashtag Category Taxonomy (PostgreSQL)...")
    try:
        df = spark.read.format("jdbc") \
            .option("url", PG_RAW_URL) \
            .option("dbtable", "hashtag_category_taxonomy") \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        print(f"  Taxonomy records (dari PostgreSQL): {df.count()}")
    except Exception as e:
        print(f"  JDBC gagal ({e}), fallback ke CSV file...")
        df = spark.read.option("header", True).option("inferSchema", True).csv(POSTGRES_CSV_PATH)
        print(f"  Taxonomy records (dari CSV file): {df.count()}")

    df.printSchema()
    return df


# ============================================================
# MAIN — Jalankan semua extract dan simpan ke staging (Parquet)
# ============================================================
def run_extract(spark=None):
    """Jalankan seluruh proses extract dan return DataFrames."""
    if spark is None:
        spark = create_spark_session()

    print("=" * 60)
    print("  PHASE 1: EXTRACT")
    print("=" * 60)

    df_tiktok = extract_tiktok_json(spark)
    df_influencer = extract_influencer_csv(spark)
    df_comments = extract_comments(spark)
    df_taxonomy = extract_taxonomy(spark)

    # Simpan ke staging area (Parquet) untuk checkpoint
    os.makedirs(STAGING_DIR, exist_ok=True)
    df_tiktok.write.mode("overwrite").parquet(os.path.join(STAGING_DIR, "tiktok_videos"))
    df_influencer.write.mode("overwrite").parquet(os.path.join(STAGING_DIR, "influencer_profiles"))
    df_comments.write.mode("overwrite").parquet(os.path.join(STAGING_DIR, "raw_comments"))
    df_taxonomy.write.mode("overwrite").parquet(os.path.join(STAGING_DIR, "hashtag_taxonomy"))

    print("\n  Semua data berhasil di-extract dan disimpan ke staging area.")
    return spark, df_tiktok, df_influencer, df_comments, df_taxonomy


if __name__ == "__main__":
    spark, *_ = run_extract()
    spark.stop()
    print("Extract selesai.")
