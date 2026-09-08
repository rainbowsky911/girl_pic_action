#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫一次性执行脚本 (GitHub Actions / D1 版)
用 playwright 爬取图片, 上传 R2, 写入 D1
替代 app 内的 crawler 定时任务
"""
import os
import io
import json
import uuid
import random
import hashlib
import asyncio
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

# 添加脚本目录到 path (导入 d1_client)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import aiohttp
import boto3
from botocore.config import Config as BotoConfig
from playwright.async_api import async_playwright

from d1_client import d1_query, d1_execute

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("crawler")

# ===== 配置 =====
MAX_CONCURRENT_PAGES = 5
MAX_CONCURRENT_IMAGES = 15
RANDOM_DELAY_MIN = 1.0
RANDOM_DELAY_MAX = 3.0
PAGE_TIMEOUT = 30000
CRAWL_PAGES = 20  # 默认爬取最新20页

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "image")
R2_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID", "")
R2_DOMAIN = os.getenv("IMAGE_BASE_URL", "https://img.dawei666.xyz")

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

# ===== R2 上传 =====
_r2_client = None

def get_r2_client():
    global _r2_client
    if _r2_client is None:
        _r2_client = boto3.client(
            's3',
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name='auto',
            config=BotoConfig(proxies={'http': None, 'https': None})
        )
    return _r2_client


def upload_bytes_to_r2(data, r2_key):
    try:
        client = get_r2_client()
        client.put_object(Bucket=R2_BUCKET_NAME, Key=r2_key, Body=io.BytesIO(data))
        return True
    except Exception as e:
        logger.error(f"r2 upload failed: {r2_key}: {e}")
        return False


# ===== D1 判重+写入 =====
def is_md5_exists(md5_hash):
    try:
        result = d1_query("SELECT id FROM girls_pic_haixiu WHERE md5_hash = ?", [md5_hash])
        return len(result) > 0
    except Exception as e:
        logger.error(f"md5 check failed: {e}")
        return False


def save_to_d1(title, url, r2_key, md5_hash):
    try:
        d1_execute(
            "INSERT INTO girls_pic_haixiu (title, url, start, score, url_location, tag, md5_hash, updated_at, remark) "
            "VALUES (?, ?, 0, 0, ?, 'qingbuyaohaixiu', ?, datetime('now'), 'github-actions')",
            [title, url, r2_key, md5_hash]
        )
        logger.info(f"saved: {title}")
        return True
    except Exception as e:
        logger.error(f"d1 write failed: {e}")
        return False


# ===== 图片下载+处理 =====
_image_sem = None

async def process_image(session, title, img_url):
    global _image_sem
    if _image_sem is None:
        _image_sem = asyncio.Semaphore(MAX_CONCURRENT_IMAGES)

    async with _image_sem:
        headers = {
            'accept': 'image/*,*/*;q=0.8',
            'referer': 'https://qingbuyaohaixiu.com/',
            'user-agent': random.choice(UA_LIST)
        }
        try:
            async with session.get(img_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200 or 'image' not in resp.headers.get('content-type', ''):
                    logger.warning(f"not image: {img_url}")
                    return
                data = await resp.read()
        except Exception as e:
            logger.warning(f"download failed: {img_url}: {e}")
            return

        md5_hash = hashlib.md5(data).hexdigest()

        if is_md5_exists(md5_hash):
            logger.debug(f"skip (md5 exists): {title}")
            return

        file_name = os.path.basename(urlparse(img_url).path) or f"{uuid.uuid4()}.jpg"
        r2_key = f"girl/{file_name}"

        if upload_bytes_to_r2(data, r2_key):
            logger.info(f"uploaded: {title} -> {r2_key}")
            save_to_d1(title, img_url, r2_key, md5_hash)
        else:
            logger.error(f"upload failed: {title}")


# ===== 页面爬取 =====
async def crawl_page(context, session, page_num, sem):
    delay = random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX)
    await asyncio.sleep(delay)

    async with sem:
        url = f"https://qingbuyaohaixiu.com/?page={page_num}"
        logger.info(f"crawling page {page_num}")

        page = await context.new_page()
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            await page.wait_for_load_state('networkidle', timeout=PAGE_TIMEOUT)

            cards = await page.query_selector_all('.post-card')
            if not cards:
                logger.warning(f"page {page_num}: no cards")
                return

            tasks = []
            for card in cards:
                img = await card.query_selector('.post-image')
                title_elem = await card.query_selector('.card-title a')
                if not img or not title_elem:
                    continue
                img_url = await img.get_attribute('src')
                title = (await title_elem.text_content()).strip()
                if img_url:
                    tasks.append(asyncio.create_task(process_image(session, title, img_url)))
            if tasks:
                await asyncio.gather(*tasks)

            logger.info(f"page {page_num} done: {len(tasks)} images")
        except Exception as e:
            logger.error(f"page {page_num} failed: {e}")
        finally:
            await page.close()


# ===== 主入口 =====
async def main():
    pages = int(sys.argv[1]) if len(sys.argv) > 1 else CRAWL_PAGES
    logger.info(f"=== crawler start: {pages} pages ===")

    sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage']
        )
        context = await browser.new_context(user_agent=random.choice(UA_LIST))

        logger.info(f"plan: page 1 to {pages}, concurrent={MAX_CONCURRENT_PAGES}")

        async with aiohttp.ClientSession() as session:
            tasks = [crawl_page(context, session, i, sem) for i in range(1, pages + 1)]
            await asyncio.gather(*tasks)

        await browser.close()

    logger.info(f"=== crawler done: {datetime.now()} ===")


if __name__ == "__main__":
    asyncio.run(main())
