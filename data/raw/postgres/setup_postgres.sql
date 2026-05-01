-- ============================================================
-- PostgreSQL Setup Script
-- Database: tiktok_raw
-- Table: hashtag_category_taxonomy
-- ============================================================

-- Buat database (jalankan sebagai superuser)
-- CREATE DATABASE tiktok_raw;

-- Buat tabel
CREATE TABLE IF NOT EXISTS hashtag_category_taxonomy (
    hashtag_id      VARCHAR(10) PRIMARY KEY,
    hashtag         VARCHAR(100) NOT NULL UNIQUE,
    category        VARCHAR(50) NOT NULL,
    subcategory     VARCHAR(60),
    is_brand_safe   SMALLINT DEFAULT 1 CHECK (is_brand_safe IN (0, 1)),
    content_rating  VARCHAR(10),
    search_volume_tier VARCHAR(10) CHECK (search_volume_tier IN ('low', 'medium', 'high', 'viral')),
    language_primary VARCHAR(5) DEFAULT 'en',
    created_at      DATE DEFAULT CURRENT_DATE,
    last_updated    DATE DEFAULT CURRENT_DATE
);

-- Index untuk join yang cepat
CREATE INDEX IF NOT EXISTS idx_hashtag_category ON hashtag_category_taxonomy(category);
CREATE INDEX IF NOT EXISTS idx_hashtag_text      ON hashtag_category_taxonomy(hashtag);
CREATE INDEX IF NOT EXISTS idx_brand_safe        ON hashtag_category_taxonomy(is_brand_safe);

-- Import dari CSV (sesuaikan path)
-- \COPY hashtag_category_taxonomy FROM '/path/to/hashtag_category_taxonomy.csv' CSV HEADER;

-- Verifikasi
-- SELECT category, COUNT(*) as total FROM hashtag_category_taxonomy GROUP BY category ORDER BY total DESC;

-- ============================================================
-- Data Warehouse Schema (Star Schema)
-- Database: tiktok_warehouse
-- ============================================================

-- CREATE DATABASE tiktok_warehouse;

-- Dimension: Creator (dengan SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_creator (
    creator_sk          SERIAL PRIMARY KEY,          -- Surrogate key
    creator_nk          VARCHAR(50) NOT NULL,        -- Natural key (username)
    display_name        VARCHAR(100),
    follower_count      BIGINT,
    following_count     INTEGER,
    total_videos        INTEGER,
    country             VARCHAR(5),
    niche_category      VARCHAR(50),
    is_verified         BOOLEAN DEFAULT FALSE,
    account_type        VARCHAR(20),
    -- SCD Type 2 fields
    valid_from          DATE NOT NULL,
    valid_to            DATE,
    is_current          BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_creator_nk      ON dim_creator(creator_nk);
CREATE INDEX IF NOT EXISTS idx_dim_creator_current ON dim_creator(is_current);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INTEGER PRIMARY KEY,   -- Format: YYYYMMDD
    full_date       DATE NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(15),
    week_of_year    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE,
    holiday_name    VARCHAR(100)
);

-- Dimension: Hashtag
CREATE TABLE IF NOT EXISTS dim_hashtag (
    hashtag_id      VARCHAR(10) PRIMARY KEY,
    hashtag         VARCHAR(100) NOT NULL,
    category        VARCHAR(50),
    subcategory     VARCHAR(60),
    is_brand_safe   BOOLEAN DEFAULT TRUE,
    search_volume_tier VARCHAR(10)
);

-- Dimension: Music
CREATE TABLE IF NOT EXISTS dim_music (
    music_sk        SERIAL PRIMARY KEY,
    music_name      VARCHAR(200),
    music_author    VARCHAR(150),
    is_original     BOOLEAN DEFAULT FALSE
);

-- Fact Table: Video Performance
CREATE TABLE IF NOT EXISTS fact_video_performance (
    video_sk            SERIAL PRIMARY KEY,
    video_id            VARCHAR(50) NOT NULL,
    creator_sk          INTEGER REFERENCES dim_creator(creator_sk),
    date_id             INTEGER REFERENCES dim_date(date_id),
    music_sk            INTEGER REFERENCES dim_music(music_sk),
    -- Metrics
    play_count          BIGINT DEFAULT 0,
    like_count          BIGINT DEFAULT 0,
    comment_count       INTEGER DEFAULT 0,
    share_count         INTEGER DEFAULT 0,
    video_duration_sec  INTEGER,
    engagement_rate     DECIMAL(8,6),
    is_viral            BOOLEAN DEFAULT FALSE,
    -- Sentiment aggregates (dari MongoDB)
    total_comments_scraped  INTEGER DEFAULT 0,
    positive_comment_pct    DECIMAL(5,2),
    negative_comment_pct    DECIMAL(5,2),
    neutral_comment_pct     DECIMAL(5,2),
    avg_sentiment_score     DECIMAL(5,4),
    -- Hashtag (primary category dari taxonomy)
    primary_category        VARCHAR(50),
    hashtag_count           SMALLINT,
    -- Load metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id     VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_fact_video_id   ON fact_video_performance(video_id);
CREATE INDEX IF NOT EXISTS idx_fact_date       ON fact_video_performance(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_creator    ON fact_video_performance(creator_sk);
CREATE INDEX IF NOT EXISTS idx_fact_viral      ON fact_video_performance(is_viral);
