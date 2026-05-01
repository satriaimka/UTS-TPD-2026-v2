# Laporan ETL Pipeline — Studi Kasus TikTok Analytics

**Mata Kuliah:** Data Warehouse & Business Intelligence  
**Topik:** Perancangan dan Implementasi ETL Pipeline Berbasis Multi-Source Data  
**Platform Studi Kasus:** TikTok  
**Tanggal:** 1 Mei 2026

---

## Daftar Isi

1. [Latar Belakang](#1-latar-belakang)
2. [Tujuan Proyek](#2-tujuan-proyek)
3. [Arsitektur Pipeline ETL](#3-arsitektur-pipeline-etl)
4. [Extract — Pengambilan Data](#4-extract--pengambilan-data)
5. [Transform — Pengolahan Data](#5-transform--pengolahan-data)
6. [Load — Penyimpanan Data](#6-load--penyimpanan-data)
7. [Rancangan Data Warehouse](#7-rancangan-data-warehouse)
8. [Otomatisasi dan Penjadwalan](#8-otomatisasi-dan-penjadwalan)
9. [OLAP — Analisis Multidimensi](#9-olap--analisis-multidimensi)
10. [Pelaporan dan Visualisasi](#10-pelaporan-dan-visualisasi)
11. [Kesimpulan](#11-kesimpulan)

---

## 1. Latar Belakang

TikTok adalah salah satu platform media sosial dengan pertumbuhan paling pesat di dunia, dengan lebih dari 1 miliar pengguna aktif per bulan. Di balik setiap video yang viral, terdapat pola tersembunyi yang dapat diungkap melalui analisis data — mulai dari waktu posting yang optimal, kategori konten yang mendominasi, hingga korelasi antara sentimen komentar dan tingkat engagement.

Namun, data TikTok bersifat heterogen dan tersebar. Data performa video tersimpan dalam format JSON hasil scraping, profil kreator dapat dikumpulkan dalam format tabular (CSV), komentar pengguna bersifat tidak terstruktur dan cocok disimpan di NoSQL, sementara metadata klasifikasi konten lebih tepat dikelola dalam database relasional. Kondisi ini menciptakan tantangan nyata yang hanya dapat diatasi dengan pipeline ETL (Extract, Transform, Load) yang dirancang secara sistematis.

Proyek ini membangun sebuah ETL pipeline end-to-end yang mengintegrasikan empat sumber data berbeda, melakukan transformasi menggunakan PySpark, menyimpan hasil ke dalam Data Warehouse berstruktur star schema, serta menghasilkan laporan analitik berbasis OLAP. Seluruh pipeline diorkestrasi menggunakan Apache Airflow untuk menjamin otomatisasi dan keandalan proses.

---

## 2. Tujuan Proyek

1. Membangun pipeline ETL yang mengintegrasikan minimal dua sumber data dengan format yang beragam.
2. Merancang proses transformasi yang mencakup data cleaning, join antar dataset, agregasi statistik, dan feature engineering.
3. Merancang Data Warehouse dengan skema yang optimal untuk kebutuhan pelaporan (star schema + SCD Type 2).
4. Mengimplementasikan otomatisasi pipeline menggunakan Apache Airflow dengan penjadwalan harian.
5. Menghasilkan laporan analitik berbasis OLAP untuk mengungkap insight dari data TikTok.

---

## 3. Arsitektur Pipeline ETL

Pipeline ini dirancang dalam tiga lapisan utama yang berjalan secara sekuensial dan dikelola oleh Apache Airflow sebagai orchestrator.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EXTRACT LAYER                                │
│                                                                     │
│  [JSON Scraper]  [CSV File]  [MongoDB (NoSQL)]  [PostgreSQL RDBMS]  │
│   TikTok Data    Influencer    Raw Comments      Hashtag Taxonomy   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                    [Staging Area]
                    Raw Ingestion
                           │
┌──────────────────────────▼──────────────────────────────────────────┐
│                       TRANSFORM LAYER                               │
│                                                                     │
│              PySpark Jobs (via Apache Spark)                        │
│   Cleaning → Join → Agregasi → Feature Engineering → Validasi      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────┐
│                         LOAD LAYER                                  │
│                                                                     │
│            Data Warehouse — Star Schema (PostgreSQL)                │
│        fact_video_performance + dim_creator (SCD Type 2)           │
│              + dim_date + dim_hashtag + dim_music                   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                  [Reporting & OLAP]
               Dashboard + Query Analitik
```

**Orkestrator:** Apache Airflow (schedule: `0 6 * * *` — setiap hari pukul 06.00)

---

## 4. Extract — Pengambilan Data

### 4.1 Ringkasan Sumber Data

| # | Sumber | Format | Teknologi | Jumlah Record |
|---|--------|--------|-----------|--------------|
| 1 | TikTok Hashtag Scraper | JSON | Web Scraping (Apify) | 1.000 video |
| 2 | Influencer Profiles | CSV | File flat / Excel | 221 kreator |
| 3 | Raw Video Comments | JSON Document | MongoDB (NoSQL) | 6.034 komentar |
| 4 | Hashtag Category Taxonomy | Tabular | PostgreSQL (RDBMS) | 1.927 hashtag |

### 4.2 Sumber 1: TikTok Hashtag Scraper (JSON)

**Metode pengambilan:** Web scraping menggunakan Apify TikTok Hashtag Scraper yang menghasilkan file JSON flat berisi data performa video.

**Lokasi file:** `data/raw/json/tiktok_hashtag_scraper.json`

**Statistik data:**

| Atribut | Nilai |
|---------|-------|
| Total video | 1.000 |
| Rentang tanggal | 16 November 2019 – 1 Mei 2026 |
| Rata-rata play count | 6.493.109 |
| Maksimum play count | 212.100.000 |
| Rata-rata durasi video | 65 detik |
| Durasi terpanjang | 1.724 detik (~28 menit) |
| Video dengan musik original | 776 dari 1.000 (77,6%) |
| Jumlah kreator unik | 871 |

**Skema field:**

```json
{
  "authorMeta.avatar"       : "URL foto profil kreator",
  "authorMeta.name"         : "Username kreator (join key)",
  "text"                    : "Caption video + hashtag",
  "diggCount"               : "Jumlah like",
  "shareCount"              : "Jumlah share",
  "playCount"               : "Jumlah tayangan",
  "commentCount"            : "Jumlah komentar",
  "videoMeta.duration"      : "Durasi video dalam detik",
  "musicMeta.musicName"     : "Nama musik/sound",
  "musicMeta.musicAuthor"   : "Nama kreator musik",
  "musicMeta.musicOriginal" : "Boolean: musik original atau bukan",
  "createTimeISO"           : "Timestamp upload (ISO 8601)",
  "webVideoUrl"             : "URL video (sumber video_id)"
}
```

**Kode ekstraksi (PySpark):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TikTok_ETL").getOrCreate()

df_tiktok = spark.read.option("multiLine", True).json(
    "data/raw/json/tiktok_hashtag_scraper.json"
)

# Ekstrak video_id dari webVideoUrl
from pyspark.sql.functions import regexp_extract
df_tiktok = df_tiktok.withColumn(
    "video_id",
    regexp_extract("webVideoUrl", r"/video/(\d+)", 1)
)

print(f"Records loaded: {df_tiktok.count()}")
df_tiktok.printSchema()
```

---

### 4.3 Sumber 2: Influencer Profiles (CSV)

**Metode pengambilan:** File CSV yang berisi profil lengkap kreator TikTok, termasuk jumlah follower, kategori konten (niche), negara asal, dan status verifikasi.

**Lokasi file:** `data/raw/csv/influencer_profiles.csv`

**Statistik data:**

| Atribut | Nilai |
|---------|-------|
| Total kreator | 221 |
| Kreator terverifikasi | 37 (16,7%) |
| Niche terbanyak | Technology (20), Lifestyle (19), Sports (17) |
| Negara terbanyak | US, ID, PH, IN, UK |

**Skema field:**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `influencer_id` | VARCHAR | ID unik kreator (INFxxxxx) |
| `username` | VARCHAR | Username TikTok (**join key** ke Sumber 1) |
| `display_name` | VARCHAR | Nama tampilan |
| `follower_count` | BIGINT | Jumlah follower |
| `following_count` | INTEGER | Jumlah following |
| `total_videos` | INTEGER | Total video yang pernah diupload |
| `avg_views_per_video` | BIGINT | Rata-rata views per video |
| `country` | CHAR(2) | Kode negara (ISO 3166-1) |
| `niche_category` | VARCHAR | Kategori konten utama |
| `is_verified` | TINYINT | 1 = terverifikasi, 0 = tidak |
| `account_type` | VARCHAR | Personal / Creator / Business |
| `joined_date` | DATE | Tanggal bergabung ke TikTok |
| `bio_length_chars` | INTEGER | Panjang bio dalam karakter |
| `has_external_link` | TINYINT | 1 = punya link di bio, 0 = tidak |

**Kode ekstraksi (PySpark):**

```python
df_influencer = spark.read.option("header", True).option("inferSchema", True).csv(
    "data/raw/csv/influencer_profiles.csv"
)

print(f"Influencer records: {df_influencer.count()}")
df_influencer.printSchema()
```

---

### 4.4 Sumber 3: Raw Video Comments (MongoDB / NoSQL)

**Metode pengambilan:** Dokumen JSON disimpan ke dalam koleksi MongoDB (`tiktok_raw.tiktok_comments`). Setiap dokumen merepresentasikan satu komentar pada video TikTok, dilengkapi label sentimen.

**Lokasi file:** `data/raw/nosql/raw_comments.json`  
**Collection:** `tiktok_raw.tiktok_comments`

**Statistik data:**

| Atribut | Nilai |
|---------|-------|
| Total komentar | 6.034 |
| Komentar positif | 3.291 (54,5%) |
| Komentar negatif | 1.255 (20,8%) |
| Komentar netral | 1.488 (24,7%) |
| Rata-rata komentar per video | ~6 |

**Skema dokumen:**

```json
{
  "_id"                : "CMT00000001",
  "video_id"           : "7634762696440761613",
  "video_url"          : "https://www.tiktok.com/@.../video/...",
  "creator_username"   : "nspacenews.nsn",
  "comment_text"       : "This is pure gold 💯",
  "sentiment_label"    : "positive",
  "sentiment_score"    : 0.7283,
  "like_count"         : 1464,
  "is_reply"           : false,
  "parent_comment_id"  : null,
  "comment_timestamp"  : "2026-05-01T17:09:00.000Z",
  "user_follower_tier" : "macro",
  "language_detected"  : "en"
}
```

**Kode import MongoDB:**

```bash
mongoimport \
  --uri "mongodb://localhost:27017" \
  --db tiktok_raw \
  --collection tiktok_comments \
  --file data/raw/nosql/raw_comments.json \
  --jsonArray --drop
```

**Kode ekstraksi ke PySpark (via MongoDB Spark Connector):**

```python
df_comments = spark.read.format("mongodb") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "tiktok_raw") \
    .option("collection", "tiktok_comments") \
    .load()

print(f"Comment records: {df_comments.count()}")
```

---

### 4.5 Sumber 4: Hashtag Category Taxonomy (PostgreSQL)

**Metode pengambilan:** Tabel relasional di PostgreSQL yang memetakan setiap hashtag ke kategori dan subkategori konten, lengkap dengan flag brand-safety dan volume pencarian.

**Lokasi file:** `data/raw/postgres/hashtag_category_taxonomy.csv`  
**Tabel:** `tiktok_raw.hashtag_category_taxonomy`

**Statistik data:**

| Atribut | Nilai |
|---------|-------|
| Total hashtag | 1.927 |
| Kategori terbanyak | Entertainment (1.837), Science & Education (21), Music (13) |
| Hashtag brand-safe | 1.917 (99,5%) |

**Skema tabel:**

```sql
CREATE TABLE hashtag_category_taxonomy (
    hashtag_id         VARCHAR(10) PRIMARY KEY,
    hashtag            VARCHAR(100) NOT NULL UNIQUE,
    category           VARCHAR(50)  NOT NULL,
    subcategory        VARCHAR(60),
    is_brand_safe      SMALLINT DEFAULT 1,
    content_rating     VARCHAR(10),
    search_volume_tier VARCHAR(10),   -- low / medium / high / viral
    language_primary   VARCHAR(5)  DEFAULT 'en',
    created_at         DATE,
    last_updated       DATE
);
```

**Kode ekstraksi ke PySpark (via JDBC):**

```python
df_taxonomy = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/tiktok_raw") \
    .option("dbtable", "hashtag_category_taxonomy") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Taxonomy records: {df_taxonomy.count()}")
```

---

## 5. Transform — Pengolahan Data

Seluruh transformasi dilakukan menggunakan **Apache PySpark** untuk memanfaatkan kemampuan pemrosesan terdistribusi. Proses transformasi terdiri dari lima tahap utama.

### 5.1 Data Cleaning

Tahap ini memastikan kualitas data sebelum proses join dan agregasi.

```python
from pyspark.sql.functions import (
    col, when, trim, lower, regexp_replace,
    to_timestamp, coalesce, lit
)

# --- Cleaning: Sumber 1 (TikTok JSON) ---
df_clean = df_tiktok \
    .dropDuplicates(["webVideoUrl"]) \
    .filter(col("playCount") > 0) \
    .withColumn("createTimeISO", to_timestamp("createTimeISO")) \
    .withColumn("commentCount", coalesce(col("commentCount"), lit(0))) \
    .withColumn("shareCount",   coalesce(col("shareCount"),   lit(0))) \
    .withColumn("authorMeta.name", trim(lower(col("authorMeta.name"))))

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
```

### 5.2 Feature Engineering

Membuat fitur baru yang bernilai analitik dari data mentah.

```python
from pyspark.sql.functions import (
    hour, dayofweek, month, year, size, split,
    array_distinct, explode, round as spark_round,
    udf, expr
)
from pyspark.sql.types import ArrayType, StringType

# Engagement rate = (like + comment + share) / play
df_feat = df_clean.withColumn(
    "engagement_rate",
    spark_round(
        (col("diggCount") + col("commentCount") + col("shareCount")) /
        col("playCount"), 6
    )
)

# Flag viral: play count > 1 juta
df_feat = df_feat.withColumn(
    "is_viral",
    when(col("playCount") >= 1_000_000, True).otherwise(False)
)

# Ekstrak komponen waktu
df_feat = df_feat \
    .withColumn("post_hour",        hour("createTimeISO")) \
    .withColumn("post_day_of_week", dayofweek("createTimeISO")) \
    .withColumn("post_month",       month("createTimeISO")) \
    .withColumn("post_year",        year("createTimeISO"))

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
    when(col("videoMeta.duration") <= 15, "short_15s")
    .when(col("videoMeta.duration") <= 60, "medium_60s")
    .when(col("videoMeta.duration") <= 180, "long_3m")
    .otherwise("extended")
)
```

### 5.3 Join Antar Dataset

Menggabungkan keempat sumber data berdasarkan kunci yang telah didefinisikan.

```python
from pyspark.sql.functions import avg as spark_avg, count as spark_count, \
                                   sum as spark_sum, max as spark_max

# Join 1: TikTok Video + Influencer Profile
# Key: authorMeta.name = username
df_j1 = df_feat.join(
    df_inf_clean.select(
        "username", "follower_count", "following_count",
        "country", "niche_category", "is_verified", "account_type"
    ),
    df_feat["authorMeta.name"] == df_inf_clean["username"],
    how="left"
)

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

# Join 3: Hashtag → Kategori konten (explode, join, ambil kategori primer)
df_exploded = df_j2.withColumn("hashtag_single", explode("hashtag_list"))

df_with_cat = df_exploded.join(
    df_tax_clean.select("hashtag", "category", "is_brand_safe"),
    df_exploded["hashtag_single"] == df_tax_clean["hashtag"],
    how="left"
)

# Ambil kategori pertama yang cocok per video (primary category)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("video_id").orderBy(col("is_brand_safe").desc())
df_primary_cat = df_with_cat \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select("video_id", col("category").alias("primary_category"),
            col("is_brand_safe").alias("primary_brand_safe"))

df_final = df_j2.join(df_primary_cat, on="video_id", how="left")
df_final = df_final.fillna({"primary_category": "Entertainment",
                             "primary_brand_safe": 1})
```

### 5.4 Agregasi Statistik

Menghasilkan tabel-tabel ringkasan untuk kebutuhan pelaporan.

```python
# Agregasi per kreator
df_creator_stats = df_final.groupBy("authorMeta.name").agg(
    spark_count("video_id").alias("total_videos_in_period"),
    spark_round(spark_avg("playCount"), 0).alias("avg_play_count"),
    spark_round(spark_avg("engagement_rate"), 6).alias("avg_engagement_rate"),
    spark_sum(when(col("is_viral"), 1).otherwise(0)).alias("viral_video_count"),
    spark_max("playCount").alias("max_play_count")
)

# Agregasi per kategori konten
df_category_stats = df_final.groupBy("primary_category").agg(
    spark_count("video_id").alias("total_videos"),
    spark_round(spark_avg("playCount"), 0).alias("avg_play_count"),
    spark_round(spark_avg("engagement_rate"), 6).alias("avg_engagement_rate"),
    spark_round(spark_avg("positive_pct"), 2).alias("avg_positive_sentiment_pct")
)

# Agregasi per hari dan jam posting
df_time_stats = df_final.groupBy("post_day_of_week", "post_hour").agg(
    spark_count("video_id").alias("video_count"),
    spark_round(spark_avg("playCount"), 0).alias("avg_play_count"),
    spark_round(spark_avg("engagement_rate"), 6).alias("avg_engagement_rate")
)
```

### 5.5 Validasi Data

Memastikan integritas data sebelum dimuat ke Data Warehouse.

```python
def validate_dataframe(df, name):
    total = df.count()
    null_video_id = df.filter(col("video_id").isNull()).count()
    null_play    = df.filter(col("playCount").isNull() | (col("playCount") <= 0)).count()
    neg_eng_rate = df.filter(col("engagement_rate") < 0).count()

    print(f"\n=== Validasi: {name} ===")
    print(f"  Total records     : {total:,}")
    print(f"  Null video_id     : {null_video_id}")
    print(f"  Invalid playCount : {null_play}")
    print(f"  Negative eng_rate : {neg_eng_rate}")
    if null_video_id == 0 and null_play == 0 and neg_eng_rate == 0:
        print("  STATUS: PASSED ✓")
    else:
        print("  STATUS: WARNING — cek data sebelum load")

validate_dataframe(df_final, "df_final")
```

---

## 6. Load — Penyimpanan Data

Data hasil transformasi dimuat ke dalam Data Warehouse PostgreSQL menggunakan mode `overwrite` untuk dimensi statis dan `append` untuk fact table. Tabel dimensi `dim_creator` menggunakan logika SCD Type 2.

```python
dw_url = "jdbc:postgresql://localhost:5432/tiktok_warehouse"
dw_props = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Load dim_date (generate dengan Python, load sekali)
df_dim_date.write.jdbc(dw_url, "dim_date", mode="overwrite", properties=dw_props)

# Load dim_hashtag
df_tax_clean.write.jdbc(dw_url, "dim_hashtag", mode="overwrite", properties=dw_props)

# Load dim_music
df_dim_music.write.jdbc(dw_url, "dim_music", mode="overwrite", properties=dw_props)

# Load dim_creator dengan SCD Type 2
# (lihat logika SCD Type 2 di bagian 7.3)
load_scd2_creator(df_inf_clean, dw_url, dw_props)

# Load fact table
df_fact.write.jdbc(dw_url, "fact_video_performance", mode="append", properties=dw_props)

print("Load selesai.")
```

---

## 7. Rancangan Data Warehouse

### 7.1 Star Schema

Data Warehouse menggunakan **Star Schema** dengan satu fact table sentral yang dikelilingi oleh empat dimensi.

```
                    ┌───────────────┐
                    │   dim_date    │
                    │  (date_id PK) │
                    └──────┬────────┘
                           │ FK
          ┌────────────────┼────────────────┐
          │                │                │
  ┌───────┴──────┐  ┌──────▼──────────────────────┐  ┌───────────────┐
  │ dim_creator  │  │   fact_video_performance     │  │  dim_hashtag  │
  │(creator_sk)  ├──┤  (video_sk PK)               ├──┤(hashtag_id PK)│
  │  SCD Type 2  │  │  creator_sk FK               │  └───────────────┘
  └──────────────┘  │  date_id    FK               │
                    │  music_sk   FK               │  ┌───────────────┐
                    │  hashtag_id FK               │  │   dim_music   │
                    │  play_count                  ├──┤ (music_sk PK) │
                    │  like_count                  │  └───────────────┘
                    │  comment_count               │
                    │  share_count                 │
                    │  engagement_rate             │
                    │  is_viral                    │
                    │  positive_comment_pct        │
                    │  negative_comment_pct        │
                    │  neutral_comment_pct         │
                    │  avg_sentiment_score         │
                    │  primary_category            │
                    │  hashtag_count               │
                    └─────────────────────────────┘
```

### 7.2 Definisi Tabel

**Fact Table: `fact_video_performance`**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `video_sk` | SERIAL PK | Surrogate key |
| `video_id` | VARCHAR | ID video TikTok (natural key) |
| `creator_sk` | INTEGER FK | Referensi ke `dim_creator` |
| `date_id` | INTEGER FK | Referensi ke `dim_date` (YYYYMMDD) |
| `music_sk` | INTEGER FK | Referensi ke `dim_music` |
| `play_count` | BIGINT | Total tayangan |
| `like_count` | BIGINT | Total like |
| `comment_count` | INTEGER | Total komentar |
| `share_count` | INTEGER | Total share |
| `video_duration_sec` | INTEGER | Durasi video (detik) |
| `engagement_rate` | DECIMAL(8,6) | (like+comment+share)/play |
| `is_viral` | BOOLEAN | play_count >= 1.000.000 |
| `total_comments_scraped` | INTEGER | Komentar dari MongoDB |
| `positive_comment_pct` | DECIMAL(5,2) | % komentar positif |
| `negative_comment_pct` | DECIMAL(5,2) | % komentar negatif |
| `neutral_comment_pct` | DECIMAL(5,2) | % komentar netral |
| `avg_sentiment_score` | DECIMAL(5,4) | Rata-rata skor sentimen |
| `primary_category` | VARCHAR | Kategori konten utama |
| `hashtag_count` | SMALLINT | Jumlah hashtag dipakai |
| `loaded_at` | TIMESTAMP | Waktu data dimuat |
| `pipeline_run_id` | VARCHAR | ID eksekusi Airflow |

**Dimension: `dim_creator` (SCD Type 2)**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `creator_sk` | SERIAL PK | Surrogate key |
| `creator_nk` | VARCHAR | Natural key (username) |
| `display_name` | VARCHAR | Nama tampilan |
| `follower_count` | BIGINT | Jumlah follower |
| `following_count` | INTEGER | Jumlah following |
| `country` | VARCHAR | Kode negara |
| `niche_category` | VARCHAR | Kategori niche |
| `is_verified` | BOOLEAN | Status verifikasi |
| `account_type` | VARCHAR | Personal/Creator/Business |
| `valid_from` | DATE | Tanggal record mulai berlaku |
| `valid_to` | DATE | Tanggal record tidak berlaku (NULL = aktif) |
| `is_current` | BOOLEAN | TRUE jika record terkini |

**Dimension: `dim_date`**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `date_id` | INTEGER PK | Format YYYYMMDD |
| `full_date` | DATE | Tanggal lengkap |
| `year` | SMALLINT | Tahun |
| `quarter` | SMALLINT | Kuartal (1–4) |
| `month` | SMALLINT | Bulan (1–12) |
| `month_name` | VARCHAR | Nama bulan |
| `week_of_year` | SMALLINT | Minggu ke- dalam tahun |
| `day_of_week` | SMALLINT | Hari (1=Minggu, 7=Sabtu) |
| `day_name` | VARCHAR | Nama hari |
| `is_weekend` | BOOLEAN | TRUE jika Sabtu/Minggu |
| `is_holiday` | BOOLEAN | TRUE jika hari libur nasional |
| `holiday_name` | VARCHAR | Nama hari libur (jika ada) |

**Dimension: `dim_hashtag`**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `hashtag_id` | VARCHAR PK | ID unik (HTxxxxxx) |
| `hashtag` | VARCHAR | Teks hashtag (tanpa #) |
| `category` | VARCHAR | Kategori konten |
| `subcategory` | VARCHAR | Subkategori |
| `is_brand_safe` | BOOLEAN | Aman untuk brand atau tidak |
| `search_volume_tier` | VARCHAR | low/medium/high/viral |

**Dimension: `dim_music`**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `music_sk` | SERIAL PK | Surrogate key |
| `music_name` | VARCHAR | Nama musik/sound |
| `music_author` | VARCHAR | Kreator musik |
| `is_original` | BOOLEAN | Musik original atau tidak |

### 7.3 SCD Type 2 pada `dim_creator`

Slowly Changing Dimension Type 2 diimplementasikan pada `dim_creator` untuk melacak perubahan follower count kreator dari waktu ke waktu. Ketika follower count berubah lebih dari 5%, pipeline akan menutup record lama dan membuat record baru.

```python
from pyspark.sql.functions import current_date

def load_scd2_creator(df_new, url, props):
    # Baca data existing dari DW
    df_existing = spark.read.jdbc(url, "dim_creator", properties=props) \
                       .filter(col("is_current") == True)

    # Deteksi perubahan follower count (threshold 5%)
    df_joined = df_new.alias("new").join(
        df_existing.alias("old"),
        col("new.username") == col("old.creator_nk"),
        how="left"
    )

    df_changed = df_joined.filter(
        col("old.creator_nk").isNotNull() &
        (abs(col("new.follower_count") - col("old.follower_count")) /
         col("old.follower_count") > 0.05)
    )

    # Tutup record lama (set valid_to dan is_current=False)
    if df_changed.count() > 0:
        close_sql = """
        UPDATE dim_creator
        SET valid_to = CURRENT_DATE, is_current = FALSE
        WHERE creator_nk IN ({nks}) AND is_current = TRUE
        """.format(nks=",".join(
            [f"'{r.username}'" for r in df_changed.select("username").collect()]
        ))
        # Eksekusi via psycopg2 atau JDBC

    # Insert record baru (kreator baru + kreator yang berubah)
    df_new_records = df_new.withColumn("valid_from", current_date()) \
                           .withColumn("valid_to", lit(None).cast("date")) \
                           .withColumn("is_current", lit(True))

    df_new_records.write.jdbc(url, "dim_creator", mode="append", properties=props)
    print(f"SCD Type 2: {df_changed.count()} record diperbarui")
```

---

## 8. Otomatisasi dan Penjadwalan

Pipeline dikelola sepenuhnya oleh **Apache Airflow**. DAG (Directed Acyclic Graph) berjalan setiap hari pukul 06.00 WIB dan terdiri dari 9 task yang berjalan secara sekuensial.

### 8.1 Struktur DAG

```
extract_json
     │
     ├── extract_csv ──────────────────────────┐
     │                                         │
     ├── extract_mongodb ──────────────────────┤
     │                                         ▼
     └── extract_postgres ──────────── staging_area
                                               │
                                    pyspark_transform
                                               │
                                         load_dw
                                               │
                                     generate_report
```

### 8.2 Kode DAG Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    "owner"           : "data-engineer",
    "depends_on_past" : False,
    "start_date"      : days_ago(1),
    "email"           : ["datateam@example.com"],
    "email_on_failure": True,
    "email_on_retry"  : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
}

with DAG(
    dag_id="tiktok_etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline TikTok — extract, transform, load ke DW",
    schedule_interval="0 6 * * *",   # Setiap hari pukul 06.00
    catchup=False,
    tags=["tiktok", "etl", "datawarehouse"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_json",
        python_callable=extract_tiktok_json,
    )

    t2 = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_influencer_csv,
    )

    t3 = PythonOperator(
        task_id="extract_mongodb",
        python_callable=extract_mongo_comments,
    )

    t4 = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_pg_taxonomy,
    )

    t5 = PythonOperator(
        task_id="staging_area",
        python_callable=load_to_staging,
    )

    t6 = BashOperator(
        task_id="pyspark_transform",
        bash_command="spark-submit --master local[*] scripts/transform.py",
    )

    t7 = PythonOperator(
        task_id="load_dw",
        python_callable=load_to_warehouse,
    )

    t8 = PythonOperator(
        task_id="generate_report",
        python_callable=generate_daily_report,
    )

    # Dependency graph
    [t1, t2, t3, t4] >> t5 >> t6 >> t7 >> t8
```

### 8.3 Monitoring dan Alerting

| Mekanisme | Keterangan |
|-----------|------------|
| `on_failure_callback` | Email otomatis ketika task gagal |
| `retries: 2` | Retry otomatis 2 kali dengan jeda 5 menit |
| `catchup: False` | Tidak menjalankan backfill eksekusi yang terlewat |
| Airflow Web UI | Monitoring status DAG dan log eksekusi secara real-time |
| `pipeline_run_id` | Setiap eksekusi dicatat ID-nya di fact table untuk audit trail |

---

## 9. OLAP — Analisis Multidimensi

Berikut adalah 8 analisis OLAP yang dapat dilakukan menggunakan Data Warehouse yang telah dibangun. Setiap analisis menggunakan minimal dua dimensi dan memanfaatkan data dari lebih dari satu sumber.

---

### OLAP 1: Engagement Performance Analysis (Drill-Down)

**Pertanyaan bisnis:** Bagaimana performa engagement video TikTok secara keseluruhan, dan bagaimana trennya jika di-drill down dari tahun → bulan → minggu?

**Dimensi yang digunakan:** `dim_date`, `dim_creator`  
**Sumber data:** JSON Scraper + CSV Influencer

```sql
-- Drill-down: Tahun → Bulan
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(f.video_sk)                        AS total_videos,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    ROUND(AVG(f.like_count), 0)              AS avg_like_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)  AS avg_engagement_rate_pct,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
FROM fact_video_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY ROLLUP(d.year, d.month, d.month_name)
ORDER BY d.year, d.month;
```

**Insight yang diharapkan:** Bulan atau periode mana yang menghasilkan engagement tertinggi secara rata-rata, dan apakah ada tren seasonal.

---

### OLAP 2: Viral Content Pattern Analysis

**Pertanyaan bisnis:** Apa karakteristik umum video yang viral — dari sisi durasi, kategori konten, jumlah hashtag, dan waktu posting?

**Dimensi yang digunakan:** `dim_date`, `dim_hashtag`  
**Sumber data:** JSON Scraper + PostgreSQL Taxonomy

```sql
SELECT
    f.is_viral,
    f.primary_category,
    d.day_name,
    CASE
        WHEN f.video_duration_sec <= 15  THEN 'Short (≤15s)'
        WHEN f.video_duration_sec <= 60  THEN 'Medium (≤60s)'
        WHEN f.video_duration_sec <= 180 THEN 'Long (≤3m)'
        ELSE 'Extended (>3m)'
    END                                          AS duration_bucket,
    COUNT(*)                                     AS video_count,
    ROUND(AVG(f.play_count), 0)                  AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)       AS avg_engagement_pct,
    ROUND(AVG(f.hashtag_count), 1)               AS avg_hashtag_count
FROM fact_video_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY f.is_viral, f.primary_category, d.day_name, duration_bucket
ORDER BY f.is_viral DESC, avg_play_count DESC;
```

**Insight yang diharapkan:** Video pendek di kategori apa yang paling sering viral, dan pada hari apa biasanya diposting.

---

### OLAP 3: Creator Performance Ranking (dengan Tier Influencer)

**Pertanyaan bisnis:** Siapa kreator dengan performa terbaik? Apakah kreator dengan follower besar selalu mengalahkan kreator kecil dalam hal engagement rate?

**Dimensi yang digunakan:** `dim_creator`  
**Sumber data:** JSON Scraper + CSV Influencer

```sql
SELECT
    c.creator_nk                             AS username,
    c.niche_category,
    c.country,
    c.is_verified,
    CASE
        WHEN c.follower_count < 10000     THEN 'Nano (<10K)'
        WHEN c.follower_count < 100000    THEN 'Micro (10K–100K)'
        WHEN c.follower_count < 1000000   THEN 'Mid (100K–1M)'
        WHEN c.follower_count < 10000000  THEN 'Macro (1M–10M)'
        ELSE 'Mega (>10M)'
    END                                      AS influencer_tier,
    COUNT(f.video_sk)                        AS total_videos,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)  AS avg_engagement_rate_pct,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_videos,
    MAX(f.play_count)                        AS best_video_play_count
FROM fact_video_performance f
JOIN dim_creator c ON f.creator_sk = c.creator_sk AND c.is_current = TRUE
GROUP BY c.creator_nk, c.niche_category, c.country, c.is_verified, influencer_tier
ORDER BY avg_engagement_rate_pct DESC
LIMIT 50;
```

**Insight yang diharapkan:** Kreator nano/micro sering memiliki engagement rate lebih tinggi dari kreator mega — fenomena yang umum di TikTok karena algoritmanya berbasis konten, bukan follower.

---

### OLAP 4: Hashtag & Category Trend Analysis (Slice)

**Pertanyaan bisnis:** Kategori konten mana yang paling mendominasi, dan apakah konten brand-safe punya performa lebih baik dibanding yang tidak?

**Dimensi yang digunakan:** `dim_hashtag`, `dim_date`  
**Sumber data:** JSON Scraper + PostgreSQL Taxonomy

```sql
-- Slice: hanya video dari 2025-2026
SELECT
    f.primary_category,
    h.is_brand_safe,
    COUNT(f.video_sk)                        AS total_videos,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)  AS avg_engagement_pct,
    ROUND(AVG(f.hashtag_count), 1)           AS avg_hashtags_used,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
FROM fact_video_performance f
JOIN dim_hashtag h ON f.primary_category = h.category
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year >= 2025
GROUP BY f.primary_category, h.is_brand_safe
ORDER BY avg_play_count DESC;
```

**Insight yang diharapkan:** Kategori Entertainment dan Comedy mendominasi volume, namun Science & Education mungkin memiliki engagement rate yang kompetitif.

---

### OLAP 5: Sentiment vs Engagement Correlation

**Pertanyaan bisnis:** Apakah video dengan proporsi komentar positif yang tinggi juga menghasilkan engagement rate yang lebih tinggi? Bagaimana sentimen berhubungan dengan viralitas?

**Dimensi yang digunakan:** `dim_creator`, `dim_date`  
**Sumber data:** JSON Scraper + MongoDB Comments + CSV Influencer

```sql
SELECT
    CASE
        WHEN f.positive_comment_pct >= 70 THEN 'Highly Positive (≥70%)'
        WHEN f.positive_comment_pct >= 50 THEN 'Mostly Positive (50–69%)'
        WHEN f.positive_comment_pct >= 30 THEN 'Mixed (30–49%)'
        ELSE 'Mostly Negative (<30%)'
    END                                          AS sentiment_bucket,
    COUNT(f.video_sk)                            AS video_count,
    ROUND(AVG(f.play_count), 0)                  AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)       AS avg_engagement_pct,
    ROUND(AVG(f.avg_sentiment_score), 4)         AS avg_sentiment_score,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END)  AS viral_count,
    ROUND(
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2
    )                                            AS viral_rate_pct
FROM fact_video_performance f
WHERE f.total_comments_scraped > 0
GROUP BY sentiment_bucket
ORDER BY avg_engagement_pct DESC;
```

**Insight yang diharapkan:** Video dengan sentimen positif tinggi cenderung memiliki viral rate lebih tinggi, membuktikan bahwa komentar positif merupakan sinyal kuat bagi algoritma TikTok.

---

### OLAP 6: Time-based Posting Pattern Analysis

**Pertanyaan bisnis:** Jam berapa dan hari apa yang optimal untuk posting di TikTok? Apakah posting di hari libur memberikan keuntungan engagement?

**Dimensi yang digunakan:** `dim_date`  
**Sumber data:** JSON Scraper + Public Holiday API (via dim_date)

```sql
-- Pola per hari dan jam
SELECT
    d.day_name,
    d.is_weekend,
    d.is_holiday,
    EXTRACT(HOUR FROM f.loaded_at)           AS post_hour,
    COUNT(f.video_sk)                        AS video_count,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)  AS avg_engagement_pct,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
FROM fact_video_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.day_name, d.is_weekend, d.is_holiday, post_hour
ORDER BY avg_play_count DESC;

-- Perbandingan: Hari libur vs hari biasa
SELECT
    CASE WHEN d.is_holiday THEN 'Holiday' ELSE 'Regular Day' END AS day_type,
    COUNT(f.video_sk)                        AS total_videos,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)  AS avg_engagement_pct
FROM fact_video_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY day_type;
```

**Insight yang diharapkan:** Jam prime time (18.00–22.00) dan akhir pekan cenderung menghasilkan engagement lebih tinggi, sementara hari libur nasional dapat memberi boost tambahan.

---

### OLAP 7: Music & Sound Performance Analysis

**Pertanyaan bisnis:** Apakah video yang menggunakan musik original memiliki performa lebih baik dibanding yang menggunakan musik trending? Musik siapa yang paling sering muncul di video viral?

**Dimensi yang digunakan:** `dim_music`, `dim_date`  
**Sumber data:** JSON Scraper

```sql
-- Musik original vs non-original
SELECT
    m.is_original,
    COUNT(f.video_sk)                        AS total_videos,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)  AS avg_engagement_pct,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
FROM fact_video_performance f
JOIN dim_music m ON f.music_sk = m.music_sk
GROUP BY m.is_original;

-- Top 10 musik paling sering digunakan di video viral
SELECT
    m.music_name,
    m.music_author,
    m.is_original,
    COUNT(f.video_sk)                        AS usage_count,
    ROUND(AVG(f.play_count), 0)              AS avg_play_count,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_videos
FROM fact_video_performance f
JOIN dim_music m ON f.music_sk = m.music_sk
WHERE f.is_viral = TRUE
GROUP BY m.music_name, m.music_author, m.is_original
ORDER BY usage_count DESC
LIMIT 10;
```

**Insight yang diharapkan:** Musik non-original (trending sounds) cenderung membantu video muncul di FYP karena TikTok mempromosikan audio yang sedang trending.

---

### OLAP 8: Influencer Tier vs Engagement Rate Paradox

**Pertanyaan bisnis:** Apakah ada paradoks engagement di TikTok — di mana kreator dengan follower lebih sedikit justru memiliki engagement rate lebih tinggi? Bagaimana perbandingannya per niche?

**Dimensi yang digunakan:** `dim_creator`, `dim_hashtag`  
**Sumber data:** JSON Scraper + CSV Influencer + PostgreSQL Taxonomy

```sql
SELECT
    CASE
        WHEN c.follower_count < 10000     THEN '1. Nano (<10K)'
        WHEN c.follower_count < 100000    THEN '2. Micro (10K–100K)'
        WHEN c.follower_count < 1000000   THEN '3. Mid (100K–1M)'
        WHEN c.follower_count < 10000000  THEN '4. Macro (1M–10M)'
        ELSE '5. Mega (>10M)'
    END                                          AS influencer_tier,
    c.niche_category,
    COUNT(DISTINCT c.creator_nk)                 AS creator_count,
    COUNT(f.video_sk)                            AS total_videos,
    ROUND(AVG(f.play_count), 0)                  AS avg_play_count,
    ROUND(AVG(f.engagement_rate) * 100, 4)       AS avg_engagement_rate_pct,
    ROUND(AVG(f.positive_comment_pct), 2)        AS avg_positive_sentiment,
    SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END)  AS total_viral_videos,
    ROUND(
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2
    )                                            AS viral_rate_pct
FROM fact_video_performance f
JOIN dim_creator c ON f.creator_sk = c.creator_sk AND c.is_current = TRUE
GROUP BY influencer_tier, c.niche_category
ORDER BY influencer_tier, avg_engagement_rate_pct DESC;
```

**Insight yang diharapkan:** Kreator nano dan micro di kategori Science & Education atau Comedy sering memiliki engagement rate jauh melampaui kreator mega, membuktikan bahwa kualitas konten dan relevansi lebih penting daripada ukuran audiens di TikTok.

---

## 10. Pelaporan dan Visualisasi

Berikut adalah daftar visualisasi yang direkomendasikan berdasarkan hasil OLAP di atas.

| Visualisasi | Tipe Chart | OLAP | Tools |
|-------------|------------|------|-------|
| Tren engagement bulanan | Line Chart | OLAP 1 | Matplotlib / Plotly |
| Karakteristik video viral | Grouped Bar Chart | OLAP 2 | Seaborn |
| Top 20 kreator by engagement rate | Horizontal Bar | OLAP 3 | Plotly |
| Distribusi kategori konten | Pie + Bar | OLAP 4 | Matplotlib |
| Heatmap korelasi sentimen-viral | Heatmap | OLAP 5 | Seaborn |
| Heatmap waktu posting optimal | Calendar Heatmap | OLAP 6 | Plotly |
| Musik original vs non-original | Stacked Bar | OLAP 7 | Matplotlib |
| Engagement rate per tier influencer | Box Plot | OLAP 8 | Seaborn |

**Contoh kode visualisasi (OLAP 6 — Heatmap Waktu Posting):**

```python
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# df_time = hasil query OLAP 6
pivot = df_time.pivot_table(
    index="day_name", columns="post_hour",
    values="avg_play_count", aggfunc="mean"
)

plt.figure(figsize=(16, 6))
sns.heatmap(pivot, cmap="YlOrRd", fmt=".0f", annot=False,
            linewidths=0.5, cbar_kws={"label": "Avg Play Count"})
plt.title("Heatmap: Rata-rata Play Count berdasarkan Hari dan Jam Posting", fontsize=14)
plt.xlabel("Jam Posting (0–23)")
plt.ylabel("Hari")
plt.tight_layout()
plt.savefig("reports/heatmap_posting_time.png", dpi=150)
plt.show()
```

---

## 11. Kesimpulan

Proyek ini berhasil membangun sebuah ETL pipeline end-to-end yang mengintegrasikan **4 sumber data berbeda** (JSON scraping, CSV flat file, MongoDB NoSQL, dan PostgreSQL RDBMS) untuk menganalisis performa konten TikTok secara komprehensif.

**Pencapaian teknis:**

| Aspek | Implementasi |
|-------|-------------|
| Variasi sumber data | 4 sumber: JSON, CSV, NoSQL, RDBMS |
| Volume data | 1.000 video + 221 kreator + 6.034 komentar + 1.927 hashtag |
| Framework transformasi | Apache PySpark (distributed processing) |
| Desain Data Warehouse | Star Schema + SCD Type 2 pada dim_creator |
| Otomatisasi | Apache Airflow, schedule harian `0 6 * * *` |
| Jumlah OLAP | 8 analisis multidimensi (drill-down, slice, ranking, korelasi) |
| Monitoring | Email alerting + retry otomatis + audit trail via pipeline_run_id |

**Nilai tambah proyek:**

Penggabungan data sentimen komentar dari MongoDB dengan data performa video dari scraper TikTok menghasilkan dimensi analisis baru yang tidak bisa dilakukan dengan satu sumber data saja. Ini membuktikan bahwa kompleksitas pipeline bukan sekadar tentang jumlah data, melainkan tentang kekayaan insight yang dapat dihasilkan dari integrasi data multi-sumber yang terstruktur.

---

*Dokumen ini dihasilkan sebagai bagian dari tugas mata kuliah Data Warehouse & Business Intelligence.*