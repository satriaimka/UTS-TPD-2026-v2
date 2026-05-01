"""
Konfigurasi untuk TikTok ETL Pipeline
"""
import os

# ============================================================
# PATH CONFIGURATION
# ============================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STAGING_DIR = os.path.join(DATA_DIR, "staging")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

# Raw data paths
JSON_PATH = os.path.join(RAW_DIR, "json", "tiktok_hashtag_scraper.json")
CSV_PATH = os.path.join(RAW_DIR, "csv", "influencer_profiles.csv")
NOSQL_PATH = os.path.join(RAW_DIR, "nosql", "raw_comments.json")
POSTGRES_CSV_PATH = os.path.join(RAW_DIR, "postgres", "hashtag_category_taxonomy.csv")

# ============================================================
# POSTGRESQL CONFIGURATION
# ============================================================
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "Satriaimka123"

# Source database (untuk hashtag taxonomy)
PG_RAW_DB = "tiktok_raw"
PG_RAW_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_RAW_DB}"

# Data Warehouse database
PG_DW_DB = "tiktok_warehouse"
PG_DW_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DW_DB}"

PG_JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ============================================================
# MONGODB CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "tiktok_raw"
MONGO_COLLECTION = "tiktok_comments"

# ============================================================
# ETL PARAMETERS
# ============================================================
VIRAL_THRESHOLD = 1_000_000          # Play count >= 1M = viral
SCD2_CHANGE_THRESHOLD = 0.05        # 5% change in follower count triggers new record
DATE_RANGE_START = "2019-01-01"     # dim_date range start
DATE_RANGE_END = "2027-12-31"       # dim_date range end

# Spark packages (Maven coordinates)
SPARK_PACKAGES = ",".join([
    "org.postgresql:postgresql:42.7.3",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1"
])
