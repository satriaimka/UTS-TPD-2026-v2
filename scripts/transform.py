"""
TRANSFORM LAYER — Pengolahan Data
Tahap: Cleaning → Feature Engineering → Join → Agregasi → Validasi
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import *

from pyspark.sql.functions import (
    col, when, trim, lower, regexp_replace, to_timestamp, coalesce, lit,
    hour, dayofweek, month, year, size, split, explode,
    round as spark_round, udf, expr,
    avg as spark_avg, count as spark_count, sum as spark_sum, max as spark_max,
    row_number
)
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window


# ============================================================
# 5.1 DATA CLEANING
# ============================================================
def clean_data(df_tiktok, df_influencer, df_comments, df_taxonomy):
    """Membersihkan keempat sumber data."""
    print("\n[TRANSFORM 1/5] Data Cleaning...")

    # --- Cleaning: Sumber 1 (TikTok JSON) ---
    df_clean = df_tiktok \
        .dropDuplicates(["webVideoUrl"]) \
        .filter(col("playCount") > 0) \
        .withColumn("createTimeISO", to_timestamp("createTimeISO")) \
        .withColumn("commentCount", coalesce(col("commentCount"), lit(0))) \
        .withColumn("shareCount", coalesce(col("shareCount"), lit(0))) \
        .withColumn("authorMeta_name", trim(lower(col("authorMeta_name"))))

    # --- Cleaning: Sumber 2 (CSV) ---
    df_inf_clean = df_influencer \
        .dropDuplicates(["username"]) \
        .withColumn("username", trim(lower(col("username")))) \
        .filter(col("follower_count") > 0)

    # --- Cleaning: Sumber 3 (MongoDB) ---
    df_com_clean = df_comments \
        .filter(col("sentiment_label").isin("positive", "negative", "neutral")) \
        .filter(col("video_id").isNotNull()) \
        .withColumn("comment_text", regexp_replace("comment_text", r"[^\w\s.,!?]", ""))

    # --- Cleaning: Sumber 4 (PostgreSQL) ---
    df_tax_clean = df_taxonomy \
        .withColumn("hashtag", trim(lower(col("hashtag")))) \
        .dropDuplicates(["hashtag"])

    counts = (df_clean.count(), df_inf_clean.count(), df_com_clean.count(), df_tax_clean.count())
    print(f"  Cleaned: videos={counts[0]}, influencers={counts[1]}, comments={counts[2]}, hashtags={counts[3]}")
    return df_clean, df_inf_clean, df_com_clean, df_tax_clean


# ============================================================
# 5.2 FEATURE ENGINEERING
# ============================================================
def feature_engineering(df_clean):
    """Membuat fitur baru dari data mentah."""
    print("\n[TRANSFORM 2/5] Feature Engineering...")

    # Engagement rate = (like + comment + share) / play
    df_feat = df_clean.withColumn(
        "engagement_rate",
        spark_round(
            (col("diggCount") + col("commentCount") + col("shareCount")) /
            col("playCount"), 6
        )
    )

    # Flag viral: play count >= 1 juta
    df_feat = df_feat.withColumn(
        "is_viral",
        when(col("playCount") >= VIRAL_THRESHOLD, True).otherwise(False)
    )

    # Ekstrak komponen waktu
    df_feat = df_feat \
        .withColumn("post_hour", hour("createTimeISO")) \
        .withColumn("post_day_of_week", dayofweek("createTimeISO")) \
        .withColumn("post_month", month("createTimeISO")) \
        .withColumn("post_year", year("createTimeISO"))

    # Ekstrak daftar hashtag dari kolom text
    @udf(returnType=ArrayType(StringType()))
    def extract_hashtags(text):
        if not text:
            return []
        return [w.lstrip("#").lower().strip(".,!?;:")
                for w in text.split()
                if w.startswith("#") and len(w) > 1]

    df_feat = df_feat.withColumn("hashtag_list", extract_hashtags("text"))
    df_feat = df_feat.withColumn("hashtag_count", size("hashtag_list"))

    # Durasi video dalam kategori bucket
    df_feat = df_feat.withColumn(
        "duration_bucket",
        when(col("videoMeta_duration") <= 15, "short_15s")
        .when(col("videoMeta_duration") <= 60, "medium_60s")
        .when(col("videoMeta_duration") <= 180, "long_3m")
        .otherwise("extended")
    )

    viral_count = df_feat.filter(col("is_viral")).count()
    print(f"  Features created. Viral videos: {viral_count}")
    return df_feat


# ============================================================
# 5.3 JOIN ANTAR DATASET
# ============================================================
def join_datasets(df_feat, df_inf_clean, df_com_clean, df_tax_clean):
    """Menggabungkan keempat sumber data."""
    print("\n[TRANSFORM 3/5] Join Antar Dataset...")

    # Join 1: TikTok Video + Influencer Profile
    df_j1 = df_feat.join(
        df_inf_clean.select(
            "username", "follower_count", "following_count",
            "country", "niche_category", "is_verified", "account_type"
        ),
        df_feat["authorMeta_name"] == df_inf_clean["username"],
        how="left"
    )
    print(f"  Join 1 (Video + Influencer): {df_j1.count()} records")

    # Join 2: Agregasi sentimen komentar per video
    df_sentiment_agg = df_com_clean.groupBy("video_id").agg(
        spark_count("*").alias("total_comments_scraped"),
        spark_round(
            spark_sum(when(col("sentiment_label") == "positive", 1).otherwise(0)) /
            spark_count("*") * 100, 2
        ).alias("positive_pct"),
        spark_round(
            spark_sum(when(col("sentiment_label") == "negative", 1).otherwise(0)) /
            spark_count("*") * 100, 2
        ).alias("negative_pct"),
        spark_round(
            spark_sum(when(col("sentiment_label") == "neutral", 1).otherwise(0)) /
            spark_count("*") * 100, 2
        ).alias("neutral_pct"),
        spark_round(spark_avg("sentiment_score"), 4).alias("avg_sentiment_score")
    )

    df_j2 = df_j1.join(df_sentiment_agg, on="video_id", how="left")
    print(f"  Join 2 (+ Sentimen): {df_j2.count()} records")

    # Join 3: Hashtag → Kategori konten
    df_exploded = df_j2.withColumn("hashtag_single", explode("hashtag_list"))

    df_with_cat = df_exploded.join(
        df_tax_clean.select("hashtag", "category", "is_brand_safe"),
        df_exploded["hashtag_single"] == df_tax_clean["hashtag"],
        how="left"
    )

    # Ambil kategori pertama yang cocok per video (primary category)
    window_spec = Window.partitionBy("video_id").orderBy(col("is_brand_safe").desc())
    df_primary_cat = df_with_cat \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .select("video_id", col("category").alias("primary_category"),
                col("is_brand_safe").alias("primary_brand_safe"))

    df_final = df_j2.join(df_primary_cat, on="video_id", how="left")
    df_final = df_final.fillna({"primary_category": "Entertainment", "primary_brand_safe": 1})
    print(f"  Join 3 (+ Kategori): {df_final.count()} records")
    return df_final


# ============================================================
# 5.4 AGREGASI STATISTIK
# ============================================================
def aggregate_stats(df_final):
    """Menghasilkan tabel ringkasan untuk pelaporan."""
    print("\n[TRANSFORM 4/5] Agregasi Statistik...")

    # Agregasi per kreator
    df_creator_stats = df_final.groupBy("authorMeta_name").agg(
        spark_count("video_id").alias("total_videos_in_period"),
        spark_round(spark_avg("playCount"), 0).alias("avg_play_count"),
        spark_round(spark_avg("engagement_rate"), 6).alias("avg_engagement_rate"),
        spark_sum(when(col("is_viral"), 1).otherwise(0)).alias("viral_video_count"),
        spark_max("playCount").alias("max_play_count")
    )
    print(f"  Creator stats: {df_creator_stats.count()} kreator")

    # Agregasi per kategori konten
    df_category_stats = df_final.groupBy("primary_category").agg(
        spark_count("video_id").alias("total_videos"),
        spark_round(spark_avg("playCount"), 0).alias("avg_play_count"),
        spark_round(spark_avg("engagement_rate"), 6).alias("avg_engagement_rate"),
        spark_round(spark_avg("positive_pct"), 2).alias("avg_positive_sentiment_pct")
    )
    print(f"  Category stats: {df_category_stats.count()} kategori")
    df_category_stats.show(truncate=False)

    # Agregasi per hari dan jam posting
    df_time_stats = df_final.groupBy("post_day_of_week", "post_hour").agg(
        spark_count("video_id").alias("video_count"),
        spark_round(spark_avg("playCount"), 0).alias("avg_play_count"),
        spark_round(spark_avg("engagement_rate"), 6).alias("avg_engagement_rate")
    )
    print(f"  Time stats: {df_time_stats.count()} kombinasi hari-jam")

    return df_creator_stats, df_category_stats, df_time_stats


# ============================================================
# 5.5 VALIDASI DATA
# ============================================================
def validate_dataframe(df, name):
    """Memastikan integritas data sebelum dimuat ke Data Warehouse."""
    print(f"\n[TRANSFORM 5/5] Validasi: {name}")
    total = df.count()
    null_video_id = df.filter(col("video_id").isNull()).count()
    null_play = df.filter(col("playCount").isNull() | (col("playCount") <= 0)).count()
    neg_eng_rate = df.filter(col("engagement_rate") < 0).count()

    print(f"  Total records     : {total:,}")
    print(f"  Null video_id     : {null_video_id}")
    print(f"  Invalid playCount : {null_play}")
    print(f"  Negative eng_rate : {neg_eng_rate}")
    if null_video_id == 0 and null_play == 0 and neg_eng_rate == 0:
        print("  STATUS: PASSED ✓")
        return True
    else:
        print("  STATUS: WARNING — cek data sebelum load")
        return False


# ============================================================
# MAIN — Jalankan seluruh transform
# ============================================================
def run_transform(spark, df_tiktok, df_influencer, df_comments, df_taxonomy):
    """Jalankan seluruh proses transform dan return DataFrames."""
    print("\n" + "=" * 60)
    print("  PHASE 2: TRANSFORM")
    print("=" * 60)

    # Cleaning
    df_clean, df_inf_clean, df_com_clean, df_tax_clean = clean_data(
        df_tiktok, df_influencer, df_comments, df_taxonomy
    )

    # Feature Engineering
    df_feat = feature_engineering(df_clean)

    # Join
    df_final = join_datasets(df_feat, df_inf_clean, df_com_clean, df_tax_clean)

    # Agregasi
    df_creator_stats, df_category_stats, df_time_stats = aggregate_stats(df_final)

    # Validasi
    validate_dataframe(df_final, "df_final")

    # Simpan hasil transform ke staging
    df_final.write.mode("overwrite").parquet(os.path.join(STAGING_DIR, "transformed_final"))
    print("\n  Transform selesai. Data disimpan ke staging.")

    return df_final, df_inf_clean, df_tax_clean


if __name__ == "__main__":
    from extract import create_spark_session, run_extract
    spark, df_tiktok, df_influencer, df_comments, df_taxonomy = run_extract()
    run_transform(spark, df_tiktok, df_influencer, df_comments, df_taxonomy)
    spark.stop()
    print("Transform selesai.")
