-- sync_images for D1 (SQLite)
-- 从 girls_pic_haixiu 复制已检查的图片到 girls_pic
INSERT OR IGNORE INTO girls_pic (
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
