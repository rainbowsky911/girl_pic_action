-- sync_images_test for D1 (SQLite)
-- 测试用: 新建 girls_pic_test (结构与 girls_pic 完全一致), 从 girls_pic_haixiu 同步已检查图片
-- 旧 sync_images.sql / sync-images.yml 不动, 此文件仅供新定时任务使用

CREATE TABLE IF NOT EXISTS girls_pic_test (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  title TEXT NOT NULL,
  tag TEXT,
  url TEXT NOT NULL,
  image_list TEXT,
  start INTEGER NOT NULL DEFAULT 0,
  score REAL NOT NULL DEFAULT 0,
  origin_score REAL,
  url_location TEXT,
  tencent_url_location TEXT,
  remark TEXT,
  label TEXT,
  image_check_result TEXT,
  is_safe TEXT,
  file_name TEXT,
  md5_hash TEXT UNIQUE,
  img_link TEXT,
  is_third_party_image TEXT,
  acess_status TEXT,
  thumbnail_location TEXT,
  category TEXT,
  keywords TEXT,
  description TEXT,
  suggested_filename TEXT
);
CREATE INDEX IF NOT EXISTS idx_girls_pic_test_label ON girls_pic_test(label);
CREATE INDEX IF NOT EXISTS idx_girls_pic_test_score ON girls_pic_test(score DESC);

INSERT OR IGNORE INTO girls_pic_test (
    created_at, title, tag, url, image_list, start,
    score, url_location, remark, label,
    image_check_result, is_safe, file_name, md5_hash
)
SELECT
    created_at, title, tag, url, image_list, start,
    score, url_location, remark, label,
    image_check_result, is_safe, file_name, md5_hash
FROM girls_pic_haixiu
WHERE md5_hash IS NOT NULL
  AND image_check_result IS NOT NULL;
