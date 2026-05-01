"""
LOAD LAYER — Penyimpanan ke Data Warehouse PostgreSQL
Star Schema: fact_video_performance + dim_creator (SCD2) + dim_date + dim_hashtag + dim_music
Database DWH otomatis dibuat saat proses load.
"""
import os
import sys
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import *

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyspark.sql.functions import (
    col, lit, current_date, monotonically_increasing_id,
    year, month, dayofweek, quarter, weekofyear, date_format,
    when, abs as spark_abs, row_number, coalesce,
    round as spark_round
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd


# ============================================================
# HELPER: PostgreSQL Connection
# ============================================================
def get_pg_connection(dbname=PG_DW_DB):
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=dbname)


# ============================================================
# SETUP DATA WAREHOUSE (otomatis saat load)
# ============================================================
def setup_warehouse():
    """Buat database tiktok_warehouse dan semua tabel star schema."""
    print("\n[LOAD 0/5] Setup Data Warehouse...")

    # Buat database
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PG_DW_DB}'")
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{PG_DW_DB}"')
        print(f"  Database '{PG_DW_DB}' dibuat.")
    else:
        print(f"  Database '{PG_DW_DB}' sudah ada.")
    cur.close()
    conn.close()

    # Buat tabel-tabel
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY, full_date DATE NOT NULL,
        year SMALLINT, quarter SMALLINT, month SMALLINT, month_name VARCHAR(15),
        week_of_year SMALLINT, day_of_week SMALLINT, day_name VARCHAR(10),
        is_weekend BOOLEAN, is_holiday BOOLEAN DEFAULT FALSE, holiday_name VARCHAR(100)
    );""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_creator (
        creator_sk SERIAL PRIMARY KEY, creator_nk VARCHAR(50) NOT NULL,
        display_name VARCHAR(100), follower_count BIGINT, following_count INTEGER,
        total_videos INTEGER, country VARCHAR(5), niche_category VARCHAR(50),
        is_verified BOOLEAN DEFAULT FALSE, account_type VARCHAR(20),
        valid_from DATE NOT NULL, valid_to DATE, is_current BOOLEAN DEFAULT TRUE
    );""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_hashtag (
        hashtag_id VARCHAR(10) PRIMARY KEY, hashtag VARCHAR(100) NOT NULL,
        category VARCHAR(50), subcategory VARCHAR(60),
        is_brand_safe BOOLEAN DEFAULT TRUE, search_volume_tier VARCHAR(10)
    );""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_music (
        music_sk SERIAL PRIMARY KEY, music_name VARCHAR(200),
        music_author VARCHAR(150), is_original BOOLEAN DEFAULT FALSE
    );""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_video_performance (
        video_sk SERIAL PRIMARY KEY, video_id VARCHAR(50) NOT NULL,
        creator_sk INTEGER REFERENCES dim_creator(creator_sk),
        date_id INTEGER REFERENCES dim_date(date_id),
        music_sk INTEGER REFERENCES dim_music(music_sk),
        play_count BIGINT DEFAULT 0, like_count BIGINT DEFAULT 0,
        comment_count INTEGER DEFAULT 0, share_count INTEGER DEFAULT 0,
        video_duration_sec INTEGER, engagement_rate DECIMAL(8,6),
        is_viral BOOLEAN DEFAULT FALSE,
        total_comments_scraped INTEGER DEFAULT 0,
        positive_comment_pct DECIMAL(5,2), negative_comment_pct DECIMAL(5,2),
        neutral_comment_pct DECIMAL(5,2), avg_sentiment_score DECIMAL(5,4),
        primary_category VARCHAR(50), hashtag_count SMALLINT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, pipeline_run_id VARCHAR(50)
    );""")

    # Indexes
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_dim_creator_nk ON dim_creator(creator_nk);",
        "CREATE INDEX IF NOT EXISTS idx_dim_creator_current ON dim_creator(is_current);",
        "CREATE INDEX IF NOT EXISTS idx_fact_video_id ON fact_video_performance(video_id);",
        "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_video_performance(date_id);",
        "CREATE INDEX IF NOT EXISTS idx_fact_creator ON fact_video_performance(creator_sk);",
        "CREATE INDEX IF NOT EXISTS idx_fact_viral ON fact_video_performance(is_viral);"
    ]:
        cur.execute(idx_sql)

    conn.commit()
    cur.close()
    conn.close()
    print("  Star schema tables berhasil dibuat.")


# ============================================================
# LOAD dim_date
# ============================================================
def load_dim_date(spark):
    """Generate dan load tabel dimensi tanggal."""
    print("\n[LOAD 1/5] Loading dim_date...")
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_date")
    if cur.fetchone()[0] > 0:
        print("  dim_date sudah terisi, skip.")
        cur.close(); conn.close()
        return

    start = date(2019, 1, 1)
    end = date(2027, 12, 31)
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_names = ["", "January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]

    rows = []
    d = start
    while d <= end:
        date_id = int(d.strftime("%Y%m%d"))
        wd = d.weekday()
        rows.append((
            date_id, d, d.year, (d.month - 1) // 3 + 1, d.month, month_names[d.month],
            d.isocalendar()[1], wd + 1, day_names[wd],
            wd >= 5, False, None
        ))
        d += timedelta(days=1)

    cur.executemany("""
        INSERT INTO dim_date (date_id, full_date, year, quarter, month, month_name,
            week_of_year, day_of_week, day_name, is_weekend, is_holiday, holiday_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (date_id) DO NOTHING
    """, rows)
    conn.commit()
    cur.close(); conn.close()
    print(f"  dim_date: {len(rows)} tanggal di-load.")


# ============================================================
# LOAD dim_music
# ============================================================
def load_dim_music(spark, df_tiktok):
    """Extract dan load dimensi musik dari data TikTok."""
    print("\n[LOAD 2/5] Loading dim_music...")
    df_music = df_tiktok.select(
        col("musicMeta_musicName").alias("music_name"),
        col("musicMeta_musicAuthor").alias("music_author"),
        col("musicMeta_musicOriginal").alias("is_original")
    ).dropDuplicates(["music_name", "music_author"])

    # Convert ke Pandas dan insert via psycopg2
    pdf = df_music.toPandas()
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dim_music RESTART IDENTITY CASCADE")
    for _, row in pdf.iterrows():
        cur.execute("""
            INSERT INTO dim_music (music_name, music_author, is_original)
            VALUES (%s, %s, %s)
        """, (row["music_name"], row["music_author"], bool(row["is_original"])))
    conn.commit()
    cur.close(); conn.close()
    print(f"  dim_music: {len(pdf)} musik unik di-load.")


# ============================================================
# LOAD dim_hashtag
# ============================================================
def load_dim_hashtag(spark, df_taxonomy):
    """Load dimensi hashtag dari taxonomy."""
    print("\n[LOAD 3/5] Loading dim_hashtag...")
    df_dim = df_taxonomy.select(
        col("hashtag_id"), col("hashtag"), col("category"), col("subcategory"),
        when(col("is_brand_safe") == 1, True).otherwise(False).alias("is_brand_safe"),
        col("search_volume_tier")
    )

    pdf = df_dim.toPandas()
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dim_hashtag CASCADE")
    for _, row in pdf.iterrows():
        cur.execute("""
            INSERT INTO dim_hashtag (hashtag_id, hashtag, category, subcategory, is_brand_safe, search_volume_tier)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (hashtag_id) DO NOTHING
        """, (row["hashtag_id"], row["hashtag"], row["category"],
              row["subcategory"], bool(row["is_brand_safe"]), row["search_volume_tier"]))
    conn.commit()
    cur.close(); conn.close()
    print(f"  dim_hashtag: {len(pdf)} hashtag di-load.")


# ============================================================
# LOAD dim_creator (SCD Type 2)
# ============================================================
def load_dim_creator_scd2(spark, df_influencer):
    """Load dimensi kreator dengan SCD Type 2."""
    print("\n[LOAD 4/5] Loading dim_creator (SCD Type 2)...")
    conn = get_pg_connection()
    cur = conn.cursor()

    # Baca data existing
    cur.execute("SELECT creator_nk, follower_count, creator_sk FROM dim_creator WHERE is_current = TRUE")
    existing = {row[0]: {"follower_count": row[1], "creator_sk": row[2]} for row in cur.fetchall()}

    pdf = df_influencer.toPandas()
    new_count = 0
    changed_count = 0
    today = date.today()

    for _, row in pdf.iterrows():
        username = str(row["username"]).strip().lower()
        fc_new = int(row["follower_count"]) if pd.notna(row["follower_count"]) else 0

        if username in existing:
            fc_old = existing[username]["follower_count"] or 0
            # Deteksi perubahan follower count (threshold 5%)
            if fc_old > 0 and abs(fc_new - fc_old) / fc_old > SCD2_CHANGE_THRESHOLD:
                # Tutup record lama
                cur.execute("""
                    UPDATE dim_creator SET valid_to = %s, is_current = FALSE
                    WHERE creator_nk = %s AND is_current = TRUE
                """, (today, username))
                # Insert record baru
                cur.execute("""
                    INSERT INTO dim_creator (creator_nk, display_name, follower_count, following_count,
                        total_videos, country, niche_category, is_verified, account_type, valid_from, is_current)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE)
                """, (username, row.get("display_name"), fc_new,
                      int(row.get("following_count", 0)),
                      int(row.get("total_videos", 0)),
                      row.get("country"), row.get("niche_category"),
                      bool(row.get("is_verified", False)),
                      row.get("account_type"), today))
                changed_count += 1
        else:
            # Kreator baru
            cur.execute("""
                INSERT INTO dim_creator (creator_nk, display_name, follower_count, following_count,
                    total_videos, country, niche_category, is_verified, account_type, valid_from, is_current)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE)
            """, (username, row.get("display_name"), fc_new,
                  int(row.get("following_count", 0)),
                  int(row.get("total_videos", 0)),
                  row.get("country"), row.get("niche_category"),
                  bool(row.get("is_verified", False)),
                  row.get("account_type"), today))
            new_count += 1

    conn.commit()
    cur.close(); conn.close()
    print(f"  dim_creator: {new_count} baru, {changed_count} diperbarui (SCD Type 2)")


# ============================================================
# LOAD fact_video_performance
# ============================================================
def load_fact_table(spark, df_final):
    """Load fact table dengan FK lookup ke dimensi."""
    print("\n[LOAD 5/5] Loading fact_video_performance...")
    pipeline_run_id = datetime.now().strftime("RUN_%Y%m%d_%H%M%S")
    conn = get_pg_connection()
    cur = conn.cursor()

    # Ambil mapping creator_sk
    cur.execute("SELECT creator_nk, creator_sk FROM dim_creator WHERE is_current = TRUE")
    creator_map = {row[0]: row[1] for row in cur.fetchall()}

    # Ambil mapping music_sk
    cur.execute("SELECT music_sk, music_name, music_author FROM dim_music")
    music_map = {}
    for row in cur.fetchall():
        key = (str(row[1] or ""), str(row[2] or ""))
        music_map[key] = row[0]

    pdf = df_final.toPandas()
    inserted = 0

    for _, r in pdf.iterrows():
        # Resolve FK: creator_sk
        author = str(r.get("authorMeta_name", "")).strip().lower()
        creator_sk = creator_map.get(author)

        # Resolve FK: date_id (YYYYMMDD)
        ts = r.get("createTimeISO")
        date_id = None
        if pd.notna(ts):
            if isinstance(ts, str):
                ts = pd.to_datetime(ts)
            date_id = int(ts.strftime("%Y%m%d"))

        # Resolve FK: music_sk
        m_name = str(r.get("musicMeta_musicName", "") or "")
        m_author = str(r.get("musicMeta_musicAuthor", "") or "")
        music_sk = music_map.get((m_name, m_author))

        cur.execute("""
            INSERT INTO fact_video_performance (
                video_id, creator_sk, date_id, music_sk,
                play_count, like_count, comment_count, share_count,
                video_duration_sec, engagement_rate, is_viral,
                total_comments_scraped, positive_comment_pct, negative_comment_pct,
                neutral_comment_pct, avg_sentiment_score,
                primary_category, hashtag_count, pipeline_run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            r.get("video_id"), creator_sk, date_id, music_sk,
            _safe_int(r, "playCount"), _safe_int(r, "diggCount"),
            _safe_int(r, "commentCount"), _safe_int(r, "shareCount"),
            _safe_int(r, "videoMeta_duration"),
            _safe_float(r, "engagement_rate"),
            bool(r.get("is_viral", False)),
            _safe_int(r, "total_comments_scraped"),
            _safe_float(r, "positive_pct"),
            _safe_float(r, "negative_pct"),
            _safe_float(r, "neutral_pct"),
            _safe_float(r, "avg_sentiment_score"),
            r.get("primary_category", "Entertainment"),
            _safe_int(r, "hashtag_count"),
            pipeline_run_id
        ))
        inserted += 1

    conn.commit()
    cur.close(); conn.close()
    print(f"  fact_video_performance: {inserted} records di-load.")
    print(f"  Pipeline Run ID: {pipeline_run_id}")


def _safe_int(row, key):
    v = row.get(key)
    if pd.isna(v):
        return 0
    return int(v)

def _safe_float(row, key):
    v = row.get(key)
    if pd.isna(v):
        return None
    return float(v)


# ============================================================
# MAIN
# ============================================================
def run_load(spark, df_final, df_influencer, df_taxonomy):
    """Jalankan seluruh proses load."""
    print("\n" + "=" * 60)
    print("  PHASE 3: LOAD")
    print("=" * 60)

    # Baca df_tiktok dari staging untuk dim_music
    df_tiktok = spark.read.parquet(os.path.join(STAGING_DIR, "tiktok_videos"))

    setup_warehouse()
    load_dim_date(spark)
    load_dim_music(spark, df_tiktok)
    load_dim_hashtag(spark, df_taxonomy)
    load_dim_creator_scd2(spark, df_influencer)
    load_fact_table(spark, df_final)

    print("\n  Load selesai. Data Warehouse terisi.")


if __name__ == "__main__":
    from extract import create_spark_session
    spark = create_spark_session()
    df_final = spark.read.parquet(os.path.join(STAGING_DIR, "transformed_final"))
    df_influencer = spark.read.parquet(os.path.join(STAGING_DIR, "influencer_profiles"))
    df_taxonomy = spark.read.parquet(os.path.join(STAGING_DIR, "hashtag_taxonomy"))
    run_load(spark, df_final, df_influencer, df_taxonomy)
    spark.stop()
    print("Load selesai.")
