#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
D1 REST API 客户端
GitHub Actions 里用 CF_API_TOKEN 连接 Cloudflare D1 数据库
替代 pymysql 直连 MySQL
"""
import os
import httpx

CF_API_TOKEN = os.getenv("CF_API_TOKEN", "")
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID", "")
CF_DB_ID = os.getenv("CF_DB_ID", "")

D1_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DB_ID}/query"


def d1_query(sql, params=None):
    """Execute D1 SELECT and return rows as list of dicts"""
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    body = {"sql": sql}
    if params:
        body["params"] = [str(p) if not isinstance(p, (int, float, type(None))) else p for p in params]
    resp = httpx.post(D1_URL, headers=headers, json=body, timeout=30)
    data = resp.json()
    if not data.get("success"):
        errors = data.get("errors", [])
        raise Exception(f"D1 query failed: {errors}")
    return data["result"][0].get("results", [])


def d1_execute(sql, params=None):
    """Execute D1 INSERT/UPDATE/DELETE"""
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    body = {"sql": sql}
    if params:
        body["params"] = [str(p) if not isinstance(p, (int, float, type(None))) else p for p in params]
    resp = httpx.post(D1_URL, headers=headers, json=body, timeout=30)
    data = resp.json()
    if not data.get("success"):
        errors = data.get("errors", [])
        raise Exception(f"D1 execute failed: {errors}")
    return data["result"][0].get("meta", {})
