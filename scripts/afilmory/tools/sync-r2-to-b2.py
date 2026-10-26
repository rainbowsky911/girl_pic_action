#!/usr/bin/env python3
"""
将 Cloudflare R2 的对象增量备份到 Backblaze B2。

源：R2 桶 `image`，前缀 `life/`（图片为主）
目标：B2 桶 `zdw-images`，前缀 `life/`（key 与 R2 完全一致，结构镜像）

增量原理（状态文件驱动，避免每次全量扫描/全量复制）：
  - 状态 JSON 持久化在 B2 自身：`_syncstate/life.json`（放在 life/ 数据前缀之外，
    不参与数据扫描，CI 临时 runner 之间也能延续状态）
  - 状态记录每个已备份对象的 {size, etag}；每次运行只对比 R2 与状态：
      · R2 有、状态无            → 复制
      · size 变化                → 复制（覆盖，处理被替换的文件）
      · 简单 MD5 etag 变化       → 复制（多分片 etag 含 '-'，仅按 size 判断）
      · 其余                     → 跳过
  - 状态不存在（首次运行）时，先扫一遍 B2 已有对象建立基线，避免重复复制
  - 每复制 10 个或运行结束都把状态写回 B2，中断后下次自动续传

用法：
  python scripts/tools/sync-r2-to-b2.py                # 实际同步
  python scripts/tools/sync-r2-to-b2.py --dry-run      # 只列待同步文件，不传输
  python scripts/tools/sync-r2-to-b2.py --reset-state  # 丢弃状态、按 B2 现状重建基线
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

# 导入统一配置（两个 client 都内置系统代理绕过）
from config import (
    R2_CONFIG, B2_CONFIG,
    validate_config, get_r2_client,
    validate_b2_config, get_b2_client,
)

# ============================================================================
# 配置区域 - 请根据需要修改
# ============================================================================
R2_SOURCE_PREFIX = 'life/'                 # R2 源路径前缀（要备份的目录，末尾带 /）
B2_TARGET_PREFIX = 'life/'                 # B2 目标路径前缀（与 R2 一致，保持同 key 镜像结构）
B2_STATE_KEY     = '_syncstate/life.json'  # 状态文件在 B2 的 key（放在 life/ 之外避免与数据扫描冲突）
MAX_WORKERS      = 4                       # 并发数（每个 worker 会同时占用 R2 读 + B2 写连接，不宜过高）
INCLUDE_VIDEOS   = False                   # 是否包含视频；False = 只备份图片
DRY_RUN          = False                   # 命令行 --dry-run 会覆盖此项
# 小于该阈值读入内存再上传（可被 boto3 自动重试）；大于则直接流式传输
INMEMORY_THRESHOLD = 50 * 1024 * 1024      # 50 MB
# 每复制多少个对象就把状态写回一次 B2（兼顾中断持久化与上传开销）
STATE_SAVE_EVERY = 10
# RequestTimeTooSkewed 重试：S3 兼容端/runner 时钟偶发偏差，
# boto3 默认不重试此 4xx；这里显式重试，短暂等待让时钟自愈后重传。
# 小文件内存数据可直接重试 put；大文件需重新 get_object 拉流（流不可回放）。
CLOCK_SKEW_RETRIES = 3                      # 最多重试次数（不含首次）
CLOCK_SKEW_BACKOFF = 5                       # 每次重试前等待秒数（线性递增：5/10/15s）
# ============================================================================

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.heic', '.heif'}
VIDEO_EXTS = {'.mp4', '.mov', '.m4v'}

STATE_VERSION = 1

# 验证两端配置
try:
    validate_config()
    validate_b2_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

R2_BUCKET = R2_CONFIG['bucket_name']
B2_BUCKET = B2_CONFIG['bucket_name']

# 每端独立 client（线程间共享 client 是安全的，连接池内部管理）
r2_client = get_r2_client()
b2_client = get_b2_client()


def normalize_prefix(p):
    """确保前缀末尾带 /（除非是空前缀即整桶）"""
    if not p:
        return ''
    return p if p.endswith('/') else p + '/'


def allowed_ext(key):
    """根据 INCLUDE_VIDEOS 判断该 key 是否在备份范围内"""
    ext = os.path.splitext(key)[1].lower()
    if ext in IMAGE_EXTS:
        return True
    if INCLUDE_VIDEOS and ext in VIDEO_EXTS:
        return True
    return False


def now_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def list_objects_meta(client, bucket, prefix):
    """列出某桶指定前缀下所有对象，返回 {key: {'size': int, 'etag': str}}"""
    result = {}
    if prefix:
        paginator = client.get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=prefix)
    else:
        paginator = client.get_paginator('list_objects_v2').paginate(Bucket=bucket)
    for page in paginator:
        for obj in page.get('Contents', []):
            result[obj['Key']] = {
                'size': int(obj.get('Size', 0)),
                'etag': (obj.get('ETag') or '').strip('"'),
            }
    return result


def needs_sync(state_entry, r2_meta):
    """R2 对象是否需要复制：不在状态里 / size 变了 / 简单 MD5 etag 变了"""
    if not state_entry:
        return True
    if state_entry.get('size') != r2_meta['size']:
        return True
    se = state_entry.get('etag', '')
    me = r2_meta.get('etag', '')
    # 多分片 etag 含 '-'，无法跨存储直接比较，仅靠 size 判断
    if se and me and '-' not in se and '-' not in me and se != me:
        return True
    return False


def load_state():
    """从 B2 下载状态 JSON；不存在/损坏返回空状态（按首次运行处理）"""
    empty = {'version': STATE_VERSION, 'updated_at': None,
             'synced': {}, 'total_synced': 0}
    try:
        resp = b2_client.get_object(Bucket=B2_BUCKET, Key=B2_STATE_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
    except Exception as e:
        code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if code in ('NoSuchKey', '404', 'NoSuchBucket'):
            return empty
        print(f"⚠ 读取状态失败（按首次运行处理）: {e}")
        return empty
    if not isinstance(data, dict) or not isinstance(data.get('synced'), dict):
        print("⚠ 状态文件格式异常，按首次运行处理")
        return empty
    data.setdefault('version', STATE_VERSION)
    data.setdefault('updated_at', None)
    data.setdefault('total_synced', len(data['synced']))
    return data


def save_state(state):
    """把状态 JSON 写回 B2（频繁更新，禁缓存）"""
    state['updated_at'] = now_iso()
    state['total_synced'] = len(state['synced'])
    data = json.dumps(state, ensure_ascii=False, indent=2).encode('utf-8')
    b2_client.put_object(
        Bucket=B2_BUCKET, Key=B2_STATE_KEY, Body=data,
        ContentType='application/json', CacheControl='no-cache',
    )


def delete_state():
    """删除 B2 上的状态文件（--reset-state 用）"""
    try:
        b2_client.delete_object(Bucket=B2_BUCKET, Key=B2_STATE_KEY)
    except Exception as e:
        code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if code not in ('NoSuchKey', '404'):
            print(f"⚠ 删除状态失败（忽略）: {e}")


def seed_state_from_b2():
    """首次运行：扫描 B2 已有对象建立状态基线，避免重复复制"""
    b2_meta = list_objects_meta(b2_client, B2_BUCKET, normalize_prefix(B2_TARGET_PREFIX))
    synced = {}
    for k, meta in b2_meta.items():
        if k == B2_STATE_KEY:
            continue
        synced[k] = meta
    return {'version': STATE_VERSION, 'updated_at': now_iso(),
            'synced': synced, 'total_synced': len(synced)}


def _is_clock_skewed(exc):
    """判断异常是否为 RequestTimeTooSkewed（请求时钟与服务端偏差过大）"""
    if isinstance(exc, ClientError):
        return exc.response.get('Error', {}).get('Code', '') == 'RequestTimeTooSkewed'
    return False


def copy_one(key, size):
    """从 R2 流式复制单个对象到 B2，返回字节数。

    对 RequestTimeTooSkewed 自动重试（S3 兼容端/runner 时钟偶发偏差；
    boto3 默认不重试此 4xx）。小文件内存数据直接重试 put；
    大文件重新 get_object 拉流（流不可回放）。
    """
    for attempt in range(CLOCK_SKEW_RETRIES + 1):
        try:
            resp = r2_client.get_object(Bucket=R2_BUCKET, Key=key)
            content_type = resp.get('ContentType') or 'application/octet-stream'
            cache_control = resp.get('CacheControl') or 'public, max-age=31536000'

            if size < INMEMORY_THRESHOLD:
                # 小文件：读入内存，put_object 失败可被 boto3 自动重试
                data = resp['Body'].read()
                b2_client.put_object(
                    Bucket=B2_BUCKET, Key=key, Body=data,
                    ContentType=content_type, CacheControl=cache_control,
                )
            else:
                # 大文件：直接把 R2 流喂给 B2，省内存；失败重跑即可
                body = resp['Body']
                try:
                    b2_client.put_object(
                        Bucket=B2_BUCKET, Key=key, Body=body,
                        ContentType=content_type, CacheControl=cache_control,
                    )
                finally:
                    body.close()
            return size
        except ClientError as e:
            if _is_clock_skewed(e) and attempt < CLOCK_SKEW_RETRIES:
                wait = CLOCK_SKEW_BACKOFF * (attempt + 1)
                print(f"  ⏳ {key}: RequestTimeTooSkewed，{wait}s 后重试 "
                      f"({attempt + 1}/{CLOCK_SKEW_RETRIES})...")
                time.sleep(wait)
                continue
            raise


def get_size_str(n):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if n < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} TB"


def main():
    ap = argparse.ArgumentParser(description='R2 -> B2 增量备份（状态文件驱动）')
    ap.add_argument('--dry-run', action='store_true', default=DRY_RUN,
                    help='只列出待同步文件，不实际传输')
    ap.add_argument('--reset-state', action='store_true',
                    help='丢弃 B2 上的状态文件，按 B2 现状重建基线后重新同步')
    args = ap.parse_args()
    dry_run = args.dry_run

    r2_prefix = normalize_prefix(R2_SOURCE_PREFIX)
    b2_prefix = normalize_prefix(B2_TARGET_PREFIX)

    print("=" * 80)
    print("R2 -> B2 增量备份")
    print("=" * 80)
    print(f"R2 桶: {R2_BUCKET}   前缀: {r2_prefix}")
    print(f"B2 桶: {B2_BUCKET}   前缀: {b2_prefix}   (endpoint: {B2_CONFIG['endpoint']})")
    print(f"状态文件: b2://{B2_BUCKET}/{B2_STATE_KEY}")
    print(f"并发: {MAX_WORKERS}   包含视频: {INCLUDE_VIDEOS}   dry-run: {dry_run}")

    # 1. 加载/建立状态
    if args.reset_state:
        print("\n[1/4] --reset-state：删除旧状态并按 B2 现状重建基线 ...")
        delete_state()
        state = seed_state_from_b2()
        save_state(state)
        print(f"      已重建基线：B2 现有 {len(state['synced'])} 个对象")
    else:
        print("\n[1/4] 加载状态文件 ...")
        state = load_state()
        if not state['synced']:
            print("      状态为空（首次运行），扫描 B2 已有对象建立基线 ...")
            state = seed_state_from_b2()
            if not dry_run:
                save_state(state)
            print(f"      基线：B2 现有 {len(state['synced'])} 个对象")
        else:
            print(f"      状态已记录 {len(state['synced'])} 个已备份对象"
                  f"（updated: {state.get('updated_at')}）")

    # 2. 列出 R2 源对象
    print(f"[2/4] 列出 R2 源对象 ...")
    r2_objects = list_objects_meta(r2_client, R2_BUCKET, r2_prefix)
    # 只保留允许扩展名的对象；同时按 key 过滤掉非目标前缀下意外命中的项
    r2_objects = {k: v for k, v in r2_objects.items()
                  if k.startswith(r2_prefix) and allowed_ext(k)}
    print(f"      R2 待考虑对象: {len(r2_objects)} 个，"
          f"合计 {get_size_str(sum(m['size'] for m in r2_objects.values()))}")

    # 3. 计算待同步清单
    print(f"[3/4] 对比状态计算增量 ...")
    todo = []
    skipped = 0
    for key, meta in r2_objects.items():
        if needs_sync(state['synced'].get(key), meta):
            todo.append((key, meta))
        else:
            skipped += 1
    print(f"      待同步: {len(todo)} 个；跳过(状态匹配): {skipped} 个")

    if not todo:
        print("\n没有需要同步的文件，全部已是最新。")
        if not dry_run:
            save_state(state)  # 刷新 updated_at
        return

    total_todo_size = sum(m['size'] for _, m in todo)
    print(f"待同步大小: {get_size_str(total_todo_size)}")

    # 显示前几个示例
    print("\n待同步示例（前 5 个）:")
    for i, (key, meta) in enumerate(todo[:5], 1):
        print(f"  {i}. {key}  ({get_size_str(meta['size'])})")
    if len(todo) > 5:
        print(f"  ... 还有 {len(todo) - 5} 个")

    if dry_run:
        print("\n[dry-run] 不实际传输，也不写状态。去掉 --dry-run 执行真实同步。")
        return

    # 4. 复制
    print(f"\n[4/4] 开始同步（并发 {MAX_WORKERS}）...")
    print("=" * 80)

    start = datetime.now()
    success = 0
    failed = 0
    copied = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(copy_one, key, meta['size']): (key, meta)
                   for key, meta in todo}
        total = len(futures)
        for idx, fut in enumerate(as_completed(futures), 1):
            key, meta = futures[fut]
            try:
                n = fut.result()
                print(f"[{idx}/{total}] ✓ {key} ({get_size_str(n)})")
                success += 1
                copied += n
                # 成功即写入状态（内存），周期性落盘到 B2
                state['synced'][key] = meta
                if success % STATE_SAVE_EVERY == 0:
                    save_state(state)
            except Exception as e:
                print(f"[{idx}/{total}] ✗ {key} - {e}")
                failed += 1

    save_state(state)
    dur = (datetime.now() - start).total_seconds()

    print("\n" + "=" * 80)
    print("同步完成!")
    print(f"成功: {success}   失败: {failed}   跳过: {skipped}")
    print(f"复制大小: {get_size_str(copied)}")
    print(f"状态已备份对象总数: {len(state['synced'])}")
    print(f"耗时: {dur:.2f} 秒" + (f"  速度: {get_size_str(copied / dur)}/秒" if dur > 0 else ""))
    print(f"B2 路径: {b2_prefix}  (访问: {B2_CONFIG['custom_domain']}/{b2_prefix}...)")
    print(f"状态文件: b2://{B2_BUCKET}/{B2_STATE_KEY}")
    if failed:
        print(f"\n有 {failed} 个文件失败，重新运行本脚本即可自动续传（成功的不会重传）。")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断。已复制部分的状态已写回 B2，重新运行即可续传。")
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
