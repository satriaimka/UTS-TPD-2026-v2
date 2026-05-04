"""
Setup Source Data — Import data ke PostgreSQL (tiktok_raw) dan MongoDB
Jalankan script ini SEKALI sebelum menjalankan ETL pipeline.
"""
import os
import sys
import json
import csv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import *

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database_if_not_exists(db_name):
    """Buat database PostgreSQL jika belum ada."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{db_name}"')
        print(f"  Database '{db_name}' berhasil dibuat.")
    else:
        print(f"  Database '{db_name}' sudah ada.")
    cur.close()
    conn.close()


def setup_postgres_source():
    """Setup tabel hashtag_category_taxonomy di database tiktok_raw."""
    print("\n[1/2] Setup PostgreSQL source (tiktok_raw)...")
    create_database_if_not_exists(PG_RAW_DB)

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_RAW_DB
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS brand_safety_guideline (
            safety_status SMALLINT PRIMARY KEY,
            description VARCHAR(100),
            monetization_status VARCHAR(50)
        );
    """)

    # Insert data statis untuk tabel brand_safety_guideline
    cur.execute("""
        INSERT INTO brand_safety_guideline (safety_status, description, monetization_status)
        VALUES 
        (1, 'Safe for all advertisers and general audience', 'Eligible'),
        (0, 'Sensitive content, restricted audience', 'Not Eligible')
        ON CONFLICT DO NOTHING;
    """)

    # Buat tabel
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_category_taxonomy (
            hashtag_id         VARCHAR(10) PRIMARY KEY,
            hashtag            VARCHAR(100) NOT NULL UNIQUE,
            category           VARCHAR(50)  NOT NULL,
            subcategory        VARCHAR(60),
            is_brand_safe      SMALLINT DEFAULT 1 CHECK (is_brand_safe IN (0, 1)),
            content_rating     VARCHAR(10),
            search_volume_tier VARCHAR(10) CHECK (search_volume_tier IN ('low','medium','high','viral')),
            language_primary   VARCHAR(5) DEFAULT 'en',
            created_at         DATE DEFAULT CURRENT_DATE,
            last_updated       DATE DEFAULT CURRENT_DATE,
            FOREIGN KEY (is_brand_safe) REFERENCES brand_safety_guideline(safety_status)
        );
    """)

    # Cek apakah sudah ada data
    cur.execute("SELECT COUNT(*) FROM hashtag_category_taxonomy")
    count = cur.fetchone()[0]
    if count > 0:
        print(f"  Tabel sudah berisi {count} record, skip import.")
    else:
        # Import dari CSV
        with open(POSTGRES_CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip header
            for row in reader:
                cur.execute("""
                    INSERT INTO hashtag_category_taxonomy
                    (hashtag_id, hashtag, category, subcategory, is_brand_safe,
                     content_rating, search_volume_tier, language_primary, created_at, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hashtag_id) DO NOTHING
                """, row)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM hashtag_category_taxonomy")
        count = cur.fetchone()[0]
        print(f"  Berhasil import {count} hashtag ke PostgreSQL.")

    # Buat index
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hashtag_category ON hashtag_category_taxonomy(category);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hashtag_text ON hashtag_category_taxonomy(hashtag);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_brand_safe ON hashtag_category_taxonomy(is_brand_safe);")
    conn.commit()
    cur.close()
    conn.close()
    print("  PostgreSQL source setup selesai.")


def setup_mongodb_source():
    """Import raw_comments.json ke MongoDB collection tiktok_comments."""
    print("\n[2/2] Setup MongoDB source (tiktok_raw.tiktok_comments)...")
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        # Cek apakah sudah ada data
        count = collection.count_documents({})
        if count > 0:
            print(f"  Collection sudah berisi {count} dokumen, skip import.")
        else:
            with open(NOSQL_PATH, 'r', encoding='utf-8') as f:
                comments = json.load(f)
            collection.insert_many(comments)
            print(f"  Berhasil import {len(comments)} komentar ke MongoDB.")

        # Buat index
        collection.create_index("video_id")
        collection.create_index("sentiment_label")
        collection.create_index("creator_username")
        collection.create_index("comment_timestamp")
        print("  MongoDB source setup selesai.")
        client.close()
    except Exception as e:
        print(f"  WARNING: MongoDB setup gagal: {e}")
        print("  ETL pipeline akan menggunakan fallback (baca langsung dari JSON file).")


if __name__ == "__main__":
    print("=" * 60)
    print("  Setup Source Data untuk TikTok ETL Pipeline")
    print("=" * 60)
    setup_postgres_source()
    setup_mongodb_source()
    print("\n" + "=" * 60)
    print("  Setup selesai! Jalankan ETL pipeline selanjutnya.")
    print("=" * 60)
