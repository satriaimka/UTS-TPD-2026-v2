"""
OLAP — 8 Analisis Multidimensi + Visualisasi
Menggunakan Data Warehouse PostgreSQL (tiktok_warehouse)
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import *

import psycopg2
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

os.makedirs(REPORTS_DIR, exist_ok=True)
sns.set_theme(style="darkgrid")


def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DW_DB)


def query_to_df(sql):
    conn = get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# ============================================================
# OLAP 1: Engagement Performance Analysis (Drill-Down)
# ============================================================
def olap1_engagement_drilldown():
    print("\n[OLAP 1] Engagement Performance Drill-Down...")
    sql = """
    SELECT d.year, d.month, d.month_name,
        COUNT(f.video_sk) AS total_videos,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.like_count), 0) AS avg_like_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_rate_pct,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
    FROM fact_video_performance f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
    """
    df = query_to_df(sql)
    print(df.to_string(index=False))

    if len(df) > 0:
        fig, ax = plt.subplots(figsize=(14, 5))
        df['period'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
        ax.plot(df['period'], df['avg_engagement_rate_pct'], marker='o', color='#FF6B6B', linewidth=2)
        ax.set_title('Tren Engagement Rate Bulanan', fontsize=14, fontweight='bold')
        ax.set_xlabel('Periode')
        ax.set_ylabel('Avg Engagement Rate (%)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap1_engagement_trend.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 2: Viral Content Pattern Analysis
# ============================================================
def olap2_viral_patterns():
    print("\n[OLAP 2] Viral Content Pattern Analysis...")
    sql = """
    SELECT f.is_viral, f.primary_category, d.day_name,
        CASE
            WHEN f.video_duration_sec <= 15  THEN 'Short (<=15s)'
            WHEN f.video_duration_sec <= 60  THEN 'Medium (<=60s)'
            WHEN f.video_duration_sec <= 180 THEN 'Long (<=3m)'
            ELSE 'Extended (>3m)'
        END AS duration_bucket,
        COUNT(*) AS video_count,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_pct,
        ROUND(AVG(f.hashtag_count)::numeric, 1) AS avg_hashtag_count
    FROM fact_video_performance f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY f.is_viral, f.primary_category, d.day_name, duration_bucket
    ORDER BY f.is_viral DESC, avg_play_count DESC
    """
    df = query_to_df(sql)
    print(df.head(20).to_string(index=False))

    if len(df) > 0:
        viral_df = df[df['is_viral'] == True].groupby('duration_bucket')['video_count'].sum().reset_index()
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(viral_df['duration_bucket'], viral_df['video_count'], color=['#4ECDC4','#45B7D1','#96CEB4','#FFEAA7'])
        ax.set_title('Distribusi Video Viral per Durasi', fontsize=14, fontweight='bold')
        ax.set_ylabel('Jumlah Video Viral')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap2_viral_duration.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 3: Creator Performance Ranking
# ============================================================
def olap3_creator_ranking():
    print("\n[OLAP 3] Creator Performance Ranking...")
    sql = """
    SELECT c.creator_nk AS username, c.niche_category, c.country, c.is_verified,
        CASE
            WHEN c.follower_count < 10000    THEN 'Nano (<10K)'
            WHEN c.follower_count < 100000   THEN 'Micro (10K-100K)'
            WHEN c.follower_count < 1000000  THEN 'Mid (100K-1M)'
            WHEN c.follower_count < 10000000 THEN 'Macro (1M-10M)'
            ELSE 'Mega (>10M)'
        END AS influencer_tier,
        COUNT(f.video_sk) AS total_videos,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_rate_pct,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_videos,
        MAX(f.play_count) AS best_video_play_count
    FROM fact_video_performance f
    JOIN dim_creator c ON f.creator_sk = c.creator_sk AND c.is_current = TRUE
    GROUP BY c.creator_nk, c.niche_category, c.country, c.is_verified, influencer_tier
    ORDER BY avg_engagement_rate_pct DESC
    LIMIT 50
    """
    df = query_to_df(sql)
    print(df.head(20).to_string(index=False))

    if len(df) > 0:
        top20 = df.head(20)
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.barh(top20['username'], top20['avg_engagement_rate_pct'], color='#6C5CE7')
        ax.set_title('Top 20 Kreator by Engagement Rate', fontsize=14, fontweight='bold')
        ax.set_xlabel('Avg Engagement Rate (%)')
        ax.invert_yaxis()
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap3_creator_ranking.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 4: Hashtag & Category Trend Analysis (Slice)
# ============================================================
def olap4_category_trend():
    print("\n[OLAP 4] Hashtag & Category Trend (Slice 2025-2026)...")
    sql = """
    SELECT f.primary_category,
        COUNT(f.video_sk) AS total_videos,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_pct,
        ROUND(AVG(f.hashtag_count)::numeric, 1) AS avg_hashtags_used,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
    FROM fact_video_performance f
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year >= 2025
    GROUP BY f.primary_category
    ORDER BY avg_play_count DESC
    """
    df = query_to_df(sql)
    print(df.to_string(index=False))

    if len(df) > 0:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        ax1.pie(df['total_videos'], labels=df['primary_category'], autopct='%1.1f%%',
                colors=sns.color_palette("pastel"))
        ax1.set_title('Distribusi Video per Kategori')
        ax2.bar(df['primary_category'], df['avg_engagement_pct'], color='#00B894')
        ax2.set_title('Avg Engagement Rate per Kategori')
        ax2.set_ylabel('%')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap4_category_trend.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 5: Sentiment vs Engagement Correlation
# ============================================================
def olap5_sentiment_engagement():
    print("\n[OLAP 5] Sentiment vs Engagement Correlation...")
    sql = """
    SELECT
        CASE
            WHEN f.positive_comment_pct >= 70 THEN 'Highly Positive (>=70%)'
            WHEN f.positive_comment_pct >= 50 THEN 'Mostly Positive (50-69%)'
            WHEN f.positive_comment_pct >= 30 THEN 'Mixed (30-49%)'
            ELSE 'Mostly Negative (<30%)'
        END AS sentiment_bucket,
        COUNT(f.video_sk) AS video_count,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_pct,
        ROUND(AVG(f.avg_sentiment_score)::numeric, 4) AS avg_sentiment_score,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count,
        ROUND(SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2) AS viral_rate_pct
    FROM fact_video_performance f
    WHERE f.total_comments_scraped > 0
    GROUP BY sentiment_bucket
    ORDER BY avg_engagement_pct DESC
    """
    df = query_to_df(sql)
    print(df.to_string(index=False))

    if len(df) > 0:
        fig, ax = plt.subplots(figsize=(10, 5))
        colors = ['#2ECC71', '#F1C40F', '#E67E22', '#E74C3C']
        ax.bar(df['sentiment_bucket'], df['avg_engagement_pct'], color=colors[:len(df)])
        ax.set_title('Engagement Rate by Sentiment Bucket', fontsize=14, fontweight='bold')
        ax.set_ylabel('Avg Engagement Rate (%)')
        plt.xticks(rotation=15)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap5_sentiment_engagement.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 6: Time-based Posting Pattern Analysis
# ============================================================
def olap6_posting_pattern():
    print("\n[OLAP 6] Time-based Posting Pattern...")
    sql = """
    SELECT d.day_name, d.day_of_week, d.is_weekend,
        COUNT(f.video_sk) AS video_count,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_pct,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
    FROM fact_video_performance f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.day_name, d.day_of_week, d.is_weekend
    ORDER BY d.day_of_week
    """
    df = query_to_df(sql)
    print(df.to_string(index=False))

    if len(df) > 0:
        fig, ax = plt.subplots(figsize=(10, 5))
        colors = ['#3498DB' if not w else '#E74C3C' for w in df['is_weekend']]
        ax.bar(df['day_name'], df['avg_play_count'], color=colors)
        ax.set_title('Avg Play Count per Hari', fontsize=14, fontweight='bold')
        ax.set_ylabel('Avg Play Count')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap6_posting_pattern.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 7: Music & Sound Performance Analysis
# ============================================================
def olap7_music_performance():
    print("\n[OLAP 7] Music & Sound Performance...")
    sql = """
    SELECT m.is_original,
        COUNT(f.video_sk) AS total_videos,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_pct,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS viral_count
    FROM fact_video_performance f
    JOIN dim_music m ON f.music_sk = m.music_sk
    GROUP BY m.is_original
    """
    df = query_to_df(sql)
    print(df.to_string(index=False))

    # Top 10 musik di video viral
    sql2 = """
    SELECT m.music_name, m.music_author, m.is_original,
        COUNT(f.video_sk) AS usage_count,
        ROUND(AVG(f.play_count), 0) AS avg_play_count
    FROM fact_video_performance f
    JOIN dim_music m ON f.music_sk = m.music_sk
    WHERE f.is_viral = TRUE
    GROUP BY m.music_name, m.music_author, m.is_original
    ORDER BY usage_count DESC
    LIMIT 10
    """
    df2 = query_to_df(sql2)
    print("\nTop 10 Musik di Video Viral:")
    print(df2.to_string(index=False))

    if len(df) > 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        labels = ['Original' if o else 'Non-Original' for o in df['is_original']]
        ax.bar(labels, df['total_videos'], color=['#6C5CE7', '#00CEC9'])
        ax2 = ax.twinx()
        ax2.plot(labels, df['avg_engagement_pct'], 'ro-', markersize=10, linewidth=2)
        ax2.set_ylabel('Avg Engagement Rate (%)', color='red')
        ax.set_title('Musik Original vs Non-Original', fontsize=14, fontweight='bold')
        ax.set_ylabel('Total Videos')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap7_music_performance.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# OLAP 8: Influencer Tier vs Engagement Rate Paradox
# ============================================================
def olap8_tier_paradox():
    print("\n[OLAP 8] Influencer Tier vs Engagement Paradox...")
    sql = """
    SELECT
        CASE
            WHEN c.follower_count < 10000    THEN '1. Nano (<10K)'
            WHEN c.follower_count < 100000   THEN '2. Micro (10K-100K)'
            WHEN c.follower_count < 1000000  THEN '3. Mid (100K-1M)'
            WHEN c.follower_count < 10000000 THEN '4. Macro (1M-10M)'
            ELSE '5. Mega (>10M)'
        END AS influencer_tier,
        COUNT(DISTINCT c.creator_nk) AS creator_count,
        COUNT(f.video_sk) AS total_videos,
        ROUND(AVG(f.play_count), 0) AS avg_play_count,
        ROUND(AVG(f.engagement_rate) * 100, 4) AS avg_engagement_rate_pct,
        ROUND(AVG(f.positive_comment_pct)::numeric, 2) AS avg_positive_sentiment,
        SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END) AS total_viral_videos,
        ROUND(SUM(CASE WHEN f.is_viral THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*),0) * 100, 2) AS viral_rate_pct
    FROM fact_video_performance f
    JOIN dim_creator c ON f.creator_sk = c.creator_sk AND c.is_current = TRUE
    GROUP BY influencer_tier
    ORDER BY influencer_tier
    """
    df = query_to_df(sql)
    print(df.to_string(index=False))

    if len(df) > 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#DFE6E9']
        ax.bar(df['influencer_tier'], df['avg_engagement_rate_pct'], color=colors[:len(df)])
        ax.set_title('Engagement Rate Paradox: Tier Influencer', fontsize=14, fontweight='bold')
        ax.set_ylabel('Avg Engagement Rate (%)')
        ax.set_xlabel('Influencer Tier')
        plt.xticks(rotation=15)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, 'olap8_tier_paradox.png'), dpi=150)
        plt.close()
    return df


# ============================================================
# MAIN
# ============================================================
def run_all_olap():
    print("=" * 60)
    print("  OLAP — 8 Analisis Multidimensi")
    print("=" * 60)
    olap1_engagement_drilldown()
    olap2_viral_patterns()
    olap3_creator_ranking()
    olap4_category_trend()
    olap5_sentiment_engagement()
    olap6_posting_pattern()
    olap7_music_performance()
    olap8_tier_paradox()
    print(f"\n  Visualisasi disimpan di: {REPORTS_DIR}")


if __name__ == "__main__":
    run_all_olap()
