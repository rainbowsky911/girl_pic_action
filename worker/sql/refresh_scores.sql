-- refresh_scores for D1 (SQLite)
-- 高分图衰减, 低分图回升, 保持首页排序流动
-- SQLite 用 ABS(RANDOM())/9223372036854775807.0 生成 0-1 随机浮点数
UPDATE girls_pic
SET score = ROUND(
    MIN(100, MAX(0,
        CASE
            WHEN score >= 90 THEN score - (1 + ABS(RANDOM()) / 9223372036854775807.0 * 2)
            WHEN score >= 75 THEN score + (ABS(RANDOM()) / 9223372036854775807.0 - 0.5)
            WHEN score >= 50 THEN score + (0.5 + ABS(RANDOM()) / 9223372036854775807.0 * 1.5)
            ELSE score + (2 + ABS(RANDOM()) / 9223372036854775807.0 * 3)
        END
    ))
, 2)
WHERE LOWER(label) IN ('sexy', 'porn', 'normal', 'neutral')
  AND url_location IS NOT NULL;
