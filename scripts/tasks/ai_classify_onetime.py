#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ai_classify 一次性执行脚本 (D1 版)
调用智谱 GLM-4V-Flash 识别图片, 更新 D1
"""
import asyncio
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from d1_client import d1_query, d1_execute

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ai_classify")

ZHIPU_API_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
ZHIPU_API_KEY = os.getenv("ZHIPU_API_KEY", "")
IMAGE_BASE_URL = os.getenv("IMAGE_BASE_URL", "https://img.dawei666.xyz")
MAX_IMAGES = int(sys.argv[1]) if len(sys.argv) > 1 else 200
SLEEP = 5

PROMPT = """请分析这张女性图片，严格按以下要求输出 JSON：
1. 分类(单选): 面部特写/半身人像/全身人像/性感穿搭/内衣泳装/裙装礼服/角色扮演/多人合照/其他
2. 关键词(2-3个中文词)
3. 文件名: {风格}_{服装} 或 {服装}_{姿势}
4. 描述(1-2句话)
输出纯JSON: {"category":"分类","keywords":["词1","词2"],"description":"描述","filename":"中文文件名"}"""


async def main():
    logger.info(f"=== ai_classify D1 === max={MAX_IMAGES}")

    images = d1_query(
        "SELECT id, url_location FROM girls_pic "
        "WHERE (suggested_filename IS NULL OR suggested_filename = '') "
        "AND url_location IS NOT NULL AND url_location != '' "
        "ORDER BY id DESC LIMIT ?",
        [MAX_IMAGES]
    )
    if not images:
        logger.info("no unclassified images")
        print("ai_classify: no images")
        return

    logger.info(f"found {len(images)} images")

    success = fail = 0
    async with httpx.AsyncClient() as client:
        for idx, row in enumerate(images, 1):
            rid = row["id"]
            ul = row["url_location"]
            img_url = ul if ul.startswith("http") else f"{IMAGE_BASE_URL}/{ul}"

            logger.info(f"[{idx}/{len(images)}] id={rid}")
            try:
                resp = await client.post(
                    ZHIPU_API_URL,
                    headers={"Authorization": f"Bearer {ZHIPU_API_KEY}", "Content-Type": "application/json"},
                    json={
                        "model": "glm-4v-flash",
                        "messages": [{"role": "user", "content": [
                            {"type": "text", "text": PROMPT},
                            {"type": "image_url", "image_url": {"url": img_url}},
                        ]}],
                        "temperature": 0.1, "max_tokens": 500
                    },
                    timeout=60
                )
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                if "```json" in content: content = content.replace("```json", "").replace("```", "").strip()
                elif "```" in content: content = content.replace("```", "").strip()
                result = json.loads(content)

                category = (result.get("category") or "")[:255]
                kw = result.get("keywords", [])
                keywords = ",".join([str(x) for x in kw if x])[:255] if isinstance(kw, list) else str(kw)[:255]
                description = (result.get("description") or "")[:500]
                filename = (result.get("filename") or "")[:255]

                d1_execute(
                    "UPDATE girls_pic SET category=?, keywords=?, description=?, suggested_filename=? WHERE id=?",
                    [category, keywords, description, filename, rid]
                )
                success += 1
                logger.info(f"  -> {category} / {filename}")
            except Exception as e:
                fail += 1
                logger.error(f"  -> {e}")

            if idx < len(images):
                await asyncio.sleep(SLEEP)

    print(f"ai_classify: success={success}, fail={fail}, total={len(images)}")


if __name__ == "__main__":
    asyncio.run(main())
