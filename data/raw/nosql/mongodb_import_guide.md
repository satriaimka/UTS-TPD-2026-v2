# MongoDB Import Guide — raw_comments.json

## Collection: `tiktok_comments`
## Database: `tiktok_raw`

### Import Command
```bash
mongoimport \
  --uri "mongodb://localhost:27017" \
  --db tiktok_raw \
  --collection tiktok_comments \
  --file raw_comments.json \
  --jsonArray \
  --drop
```

### Schema per Document
| Field | Type | Keterangan |
|---|---|---|
| `_id` | String | ID unik komentar (CMTxxxxxxxx) |
| `video_id` | String | ID video TikTok (join key dengan data scraper) |
| `video_url` | String | URL lengkap video |
| `creator_username` | String | Username kreator video |
| `comment_text` | String | Teks komentar |
| `sentiment_label` | String | positive / negative / neutral |
| `sentiment_score` | Float | Confidence score (0.0 - 1.0) |
| `like_count` | Integer | Jumlah like pada komentar |
| `is_reply` | Boolean | Apakah ini reply dari komentar lain |
| `parent_comment_id` | String / null | ID komentar induk (jika reply) |
| `comment_timestamp` | String (ISO8601) | Waktu komentar dibuat |
| `user_follower_tier` | String | nano / micro / mid / macro / mega |
| `language_detected` | String | Kode bahasa (en, id, es, dst.) |

### Indexing Recommendations
```javascript
db.tiktok_comments.createIndex({ "video_id": 1 })
db.tiktok_comments.createIndex({ "sentiment_label": 1 })
db.tiktok_comments.createIndex({ "creator_username": 1 })
db.tiktok_comments.createIndex({ "comment_timestamp": 1 })
```

### Stats
- Total documents: 6.034
- Mencakup 1.000 video unik dari scraper TikTok
- Rata-rata ~6 komentar per video
