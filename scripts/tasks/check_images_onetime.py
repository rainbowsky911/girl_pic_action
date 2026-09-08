#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_images 一次性执行脚本 (D1 版)
查询 D1 中未检查的记录, 调用安全检查 API, 更新 D1
"""
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from d1_client import d1_query, d1_execute
import os

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("check_images")

BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 300
CHECK_API_URL = os.getenv("CHECK_API_URL", "")
API_TOKEN = os.getenv("CHECK_API_TOKEN", "")
IMAGE_BASE_URL = os.getenv("IMAGE_BASE_URL", "https://img.dawei666.xyz")

import random

def get_score(probabilities):
    porn = probabilities.get("porn", 0)
    sexy = probabilities.get("sexy", 0)
    raw = (porn + sexy) * 100
    if raw > 95: reduction = random.uniform(2, 5)
    elif raw > 90: reduction = random.uniform(1, 3)
    elif raw > 85: reduction = random.uniform(0, 1)
    else: reduction = 0
    return round(max(0, raw - reduction), 2)


async def main():
    logger.info(f"=== check_images D1 === batch={BATCH_SIZE}")

    records = d1_query(
        "SELECT id, url_location FROM girls_pic_haixiu "
        "WHERE (image_check_result IS NULL OR image_check_result = '') "
        "AND url_location IS NOT NULL LIMIT ?",
        [BATCH_SIZE]
    )
    if not records:
        logger.info("no unchecked records")
        print("check_images: no unchecked records")
        return

    logger.info(f"found {len(records)} records")

    success = fail = 0
    async with httpx.AsyncClient() as client:
        for rec in records:
            rid = rec["id"]
            url_loc = rec["url_location"]
            if url_loc.startswith("http"):
                img_url = url_loc
            else:
                img_url = f"{IMAGE_BASE_URL}/{url_loc}"

            try:
                resp = await client.post(
                    CHECK_API_URL,
                    headers={"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"},
                    json={"url": img_url},
                    timeout=30
                )
                result = resp.json()
                label = result.get("label", "unknown")
                is_safe = "true" if result.get("is_safe") else "false"
                score = get_score(result.get("probabilities", {}))
                check_json = json.dumps(result, ensure_ascii=False)

                d1_execute(
                    "UPDATE girls_pic_haixiu SET image_check_result=?, is_safe=?, score=?, label=?, updated_at=datetime('now') WHERE id=?",
                    [check_json, is_safe, score, label, rid]
                )
                success += 1
                logger.info(f"[{success+fail}/{len(records)}] id={rid} label={label} score={score}")
            except Exception as e:
                fail += 1
                logger.error(f"id={rid}: {e}")

            await asyncio.sleep(0.5)

    print(f"check_images: success={success}, fail={fail}")


if __name__ == "__main__":
    asyncio.run(main())
