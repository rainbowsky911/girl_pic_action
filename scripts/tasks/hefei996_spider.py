#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
合肥百诺汇问答爬虫 (D1 版)
数据来源: http://hefei.bainaohui.cn/community/question/p/list
存储: Cloudflare D1 (通过 REST API, 替代原 MySQL/pymysql)
"""
import os
import sys
import json
from pathlib import Path
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ---- 加载 Cloudflare 凭据 (env 优先, 回退到 secrets_template.json) ----
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_secrets_path = PROJECT_ROOT / 'secrets_template.json'
if _secrets_path.exists():
    with open(_secrets_path, 'r', encoding='utf-8') as f:
        _secrets = json.load(f)
    os.environ.setdefault('CF_API_TOKEN', _secrets.get('cf_api_token', ''))
    os.environ.setdefault('CF_ACCOUNT_ID', _secrets.get('cf_account_id', ''))
    os.environ.setdefault('CF_DB_ID', _secrets.get('cf_db_id', ''))

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts' / 'tasks'))
from d1_client import d1_query, d1_execute

# ====== 请求配置 ======
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'http://hefei.bainaohui.cn',
    'Referer': 'http://hefei.bainaohui.cn/index.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}
cookies = {
    'JSESSIONID': '4d3ee96c-c941-4bd3-8ad9-3a2d018e8e1d'
}

# 复用连接 (keep-alive) 提速; requests.Session 线程安全, 可并发调用
session = requests.Session()
session.headers.update(headers)
session.cookies.update(cookies)

# 评论详情页并发数 (防限流, 留余量)
MAX_WORKERS = 5


# ====== 获取评论详情 ======
def fetch_comments_from_html(question_id: int):
    """抓取并解析问题详情页评论, 返回 [(question_id, content, commenter, comment_time), ...]"""
    url = f'http://hefei.bainaohui.cn/community/question/p/detail/{question_id}'
    try:
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, 'html.parser')

        comment_blocks = soup.select('.form-group')
        if not comment_blocks:
            print(f"[√] 无评论: question_id={question_id}")
            return []

        rows = []
        for block in comment_blocks:
            content_tag = block.select_one('.detail-body.jieda-body.photos')
            content = content_tag.get_text(strip=True) if content_tag else ''

            user_tag = block.select_one('.fly-detail-user cite')
            nickname = user_tag.get_text(strip=True) if user_tag else '匿名'

            time_tag = block.select_one('.detail-hits span')
            comment_time = time_tag.get_text(strip=True) if time_tag else None

            rows.append((question_id, content, nickname, comment_time))

        print(f"[√] 成功解析评论: question_id={question_id}, 数量={len(comment_blocks)}")
        return rows

    except Exception as e:
        print(f"[×] 解析失败: question_id={question_id}, 错误={e}")
        return []


def batch_insert_comments(all_rows):
    """跨问题批量写评论 (D1 限 100 变量, 评论 4 列 -> 每块 20 条)"""
    CHUNK = 20
    for i in range(0, len(all_rows), CHUNK):
        chunk = all_rows[i:i + CHUNK]
        placeholders = ",".join(["(?,?,?,?)"] * len(chunk))
        sql = (
            "INSERT INTO comments (question_id, content, commenter, comment_time) "
            f"VALUES {placeholders}"
        )
        d1_execute(sql, [v for row in chunk for v in row])


# ====== 爬取问题列表 ======
def fetch_questions():
    # 启动时一次性加载已有 question_id 和已有评论的 question_id, 避免逐条查询 D1
    existing_qids = {r['id'] for r in d1_query("SELECT id FROM questions")}
    qids_with_comments = {r['question_id'] for r in d1_query("SELECT DISTINCT question_id FROM comments")}
    print(f"[INIT] 已有 questions: {len(existing_qids)}, 已有评论的 question_id: {len(qids_with_comments)}")

    page_num = 1
    while True:
        print(f"===> 爬取第 {page_num} 页")
        data = {
            'pageSize': 10,
            'pageNum': page_num,
            'isAsc': 'asc',
            'title': ''
        }

        try:
            resp = session.post(
                'http://hefei.bainaohui.cn/community/question/p/list',
                data=data,
                timeout=10
            )
            json_data = resp.json()
            rows = json_data.get('rows', [])
            if not rows:
                break

            # 批量插入本页新问题 (INSERT OR IGNORE 自动跳过已存在; D1 限 100 变量, 12 列 -> 每批 8 条)
            new_items = [it for it in rows if it["id"] not in existing_qids]
            if new_items:
                QCHUNK = 8
                for i in range(0, len(new_items), QCHUNK):
                    chunk = new_items[i:i + QCHUNK]
                    placeholders = ",".join(["(?,?,?,?,?,?,?,?,?,?,?,?)"] * len(chunk))
                    sql = (
                        "INSERT OR IGNORE INTO questions ("
                        "id,title,description,creator,comment_count,view_count,like_count,"
                        "status,question_type_id,create_time,update_time,latest_comment_time"
                        f") VALUES {placeholders}"
                    )
                    params = []
                    for it in chunk:
                        params.extend([
                            it["id"], it["title"], it["description"], it["creator"],
                            it["commentCount"], it["viewCount"], it["likeCount"],
                            it["status"], it["questionTypeId"], it["createTime"],
                            it["updateTime"], it["latestCommentTime"]
                        ])
                    d1_execute(sql, params)
                for it in new_items:
                    existing_qids.add(it["id"])
                print(f"[√] 本页插入 {len(new_items)} 条新问题")

            # 补抓缺失的评论 (已有评论的跳过, 缺失的并发抓取后批量写 D1)
            missing = [item["id"] for item in rows if item["id"] not in qids_with_comments]
            if missing:
                all_rows = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    futures = {ex.submit(fetch_comments_from_html, qid): qid for qid in missing}
                    for fut in as_completed(futures):
                        all_rows.extend(fut.result())
                        qids_with_comments.add(futures[fut])
                batch_insert_comments(all_rows)

            page_num += 1
            sleep(1)

        except Exception as e:
            print(f"请求失败 (跳过本页继续): {e}")
            page_num += 1
            sleep(1)
            continue


# ====== 执行 ======
if __name__ == "__main__":
    fetch_questions()
    # D1 REST 无需关闭连接
