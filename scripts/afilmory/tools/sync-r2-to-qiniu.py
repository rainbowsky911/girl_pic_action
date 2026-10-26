#!/usr/bin/env python3
"""
将 Cloudflare R2 的对象镜像同步到七牛云 Kodo（S3 兼容 API）。

源：R2 桶 `image`（整桶全部对象，含图片/视频/manifest 元数据）
目标：七牛空间（key 与 R2 完全一致，整桶镜像）

镜像同步（最终一致，与 sync-r2-to-b2.py 的"只增不删增量备份"关键区别）：
  - R2 有、七牛无              → 上传
  - R2 size / 简单 etag 变化   → 覆盖上传
  - R2 无、七牛有              → 删除（保证七牛 = R2 的精确镜像）

不依赖状态文件：七牛当前对象列表即 source of truth。每次 list 一次七牛
（数据量 ≤ 10GB、几千对象以内，list 几秒；GET 请求 100 万次/月免费，无压力），
相比 B2 脚本的状态文件方案更简单且无漂移风险。

零成本前提（七牛免费额度内）：
  - 存储总量 ≤ 10GB/月（标准存储）
  - 上传流量 免费无上限（R2 → 七牛 写入零成本）
  - PUT/DELETE ≤ 10 万次/月（删除 + 上传合并计）
  - GET ≤ 100 万次/月（list + 读校验）
  超出则开始计费，详见 https://www.qiniu.com/prices/kodo

用法：
  python scripts/tools/sync-r2-to-qiniu.py                 # 实际镜像同步
  python scripts/tools/sync-r2-to-qiniu.py --dry-run       # 只列待上传/删除清单
  python scripts/tools/sync-r2-to-qiniu.py --no-delete     # 只上传不删除（保守模式）
"""

import os
import sys
import time
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

# 导入统一配置（两个 client 都内置系统代理绕过）
from config import (
    R2_CONFIG, QINIU_CONFIG,
    validate_config, get_r2_client,
    validate_qiniu_config, get_qiniu_client,
)

# ============================================================================
# 配置区域 - 请根据需要修改
# ============================================================================
# 空前缀 = 整桶镜像（同步 R2 全部对象到七牛，R2 有什么七牛就有什么）
# 如只想同步某子目录，填带 / 的前缀，如 'life/'
R2_SOURCE_PREFIX    = ''
QINIU_TARGET_PREFIX = ''
MAX_WORKERS         = 8         # 并发数（跨国链路高延迟，提高并发补吞吐；每 worker 占 R2 读 + 七牛写连接）
DRY_RUN             = False     # 命令行 --dry-run 会覆盖此项
# 小于该阈值读入内存再上传（可被 boto3 自动重试）；大于则直接流式传输
INMEMORY_THRESHOLD  = 50 * 1024 * 1024  # 50 MB
# RequestTimeTooSkewed 重试：七牛端/runner 时钟偶发偏差（跨国链路约 2/6000 概率），
# boto3 默认不重试此 4xx；这里显式重试，短暂等待让时钟自愈后重传。
# 小文件内存数据可直接重试 put；大文件需重新 get_object 拉流（流不可回放）。
CLOCK_SKEW_RETRIES  = 3         # 最多重试次数（不含首次）
CLOCK_SKEW_BACKOFF  = 5          # 每次重试前等待秒数（线性递增：5/10/15s）
# ============================================================================

# 验证两端配置
try:
    validate_config()
    validate_qiniu_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

R2_BUCKET = R2_CONFIG['bucket_name']
QINIU_BUCKET = QINIU_CONFIG['bucket_name']

# 每端独立 client（线程间共享 client 是安全的，连接池内部管理）
r2_client = get_r2_client()
qiniu_client = get_qiniu_client()


def normalize_prefix(p):
    """确保前缀末尾带 /（除非是空前缀即整桶）"""
    if not p:
        return ''
    return p if p.endswith('/') else p + '/'


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


def needs_copy(dst_entry, r2_meta):
    """七牛现有对象是否需要重新上传：不存在 / size 变了 / 简单 MD5 etag 变了"""
    if not dst_entry:
        return True
    if dst_entry.get('size') != r2_meta['size']:
        return True
    se = dst_entry.get('etag', '')
    me = r2_meta.get('etag', '')
    # 多分片 etag 含 '-'，无法跨存储直接比较，仅靠 size 判断
    if se and me and '-' not in se and '-' not in me and se != me:
        return True
    return False


def _is_clock_skewed(exc):
    """判断异常是否为 RequestTimeTooSkewed（请求时钟与服务端偏差过大）"""
    if isinstance(exc, ClientError):
        return exc.response.get('Error', {}).get('Code', '') == 'RequestTimeTooSkewed'
    return False


def copy_one(key, size):
    """从 R2 流式复制单个对象到七牛，返回字节数。

    对 RequestTimeTooSkewed 自动重试（七牛端/runner 时钟偶发偏差，跨国链路
    约 2/6000 概率；boto3 默认不重试此 4xx）。小文件内存数据直接重试 put；
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
                qiniu_client.put_object(
                    Bucket=QINIU_BUCKET, Key=key, Body=data,
                    ContentType=content_type, CacheControl=cache_control,
                )
            else:
                # 大文件：直接把 R2 流喂给七牛，省内存；失败重跑即可
                body = resp['Body']
                try:
                    qiniu_client.put_object(
                        Bucket=QINIU_BUCKET, Key=key, Body=body,
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


def delete_one(key):
    """从七牛删除一个多余对象（R2 已不存在）"""
    qiniu_client.delete_object(Bucket=QINIU_BUCKET, Key=key)
    return key


def get_size_str(n):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if n < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} TB"


def main():
    ap = argparse.ArgumentParser(description='R2 -> 七牛 Kodo 镜像同步（最终一致，含删除）')
    ap.add_argument('--dry-run', action='store_true', default=DRY_RUN,
                    help='只列出待上传/删除清单，不实际传输')
    ap.add_argument('--no-delete', action='store_true',
                    help='只做上传同步，不做删除（保守模式，适用于首次全量冷备）')
    ap.add_argument('--force', action='store_true',
                    help='强制全量重传：跳过 size/etag 增量判断，把 R2 所有对象重新覆盖到七牛'
                         '（用于定期兜底校验；删除多余对象逻辑仍保留以维持镜像一致）')
    args = ap.parse_args()
    dry_run = args.dry_run
    do_delete = not args.no_delete
    force = args.force

    r2_prefix = normalize_prefix(R2_SOURCE_PREFIX)
    qn_prefix = normalize_prefix(QINIU_TARGET_PREFIX)

    print("=" * 80)
    print("R2 -> 七牛 Kodo 镜像同步（最终一致）")
    print("=" * 80)
    print(f"R2 桶:  {R2_BUCKET}   前缀: {r2_prefix}")
    print(f"七牛桶: {QINIU_BUCKET}   前缀: {qn_prefix}   (endpoint: {QINIU_CONFIG['endpoint']})")
    print(f"并发: {MAX_WORKERS}   "
          f"dry-run: {dry_run}   删除多余对象: {do_delete}   force 全量重传: {force}")

    # 1. 列出 R2 源对象
    print(f"\n[1/4] 列出 R2 源对象 ...")
    r2_objects = list_objects_meta(r2_client, R2_BUCKET, r2_prefix)
    # 按 key 前缀过滤（空前缀时保留全部，即整桶镜像）
    r2_objects = {k: v for k, v in r2_objects.items()
                  if k.startswith(r2_prefix)}
    r2_total_size = sum(m['size'] for m in r2_objects.values())
    print(f"      R2 待考虑对象: {len(r2_objects)} 个，"
          f"合计 {get_size_str(r2_total_size)}")

    # 2. 列出七牛现有对象（作为 source of truth，决定上传/删除清单）
    print(f"[2/4] 列出七牛现有对象 ...")
    qn_objects = list_objects_meta(qiniu_client, QINIU_BUCKET, qn_prefix)
    qn_objects = {k: v for k, v in qn_objects.items() if k.startswith(qn_prefix)}
    qn_total_size = sum(m['size'] for m in qn_objects.values())
    print(f"      七牛现有对象: {len(qn_objects)} 个，"
          f"合计 {get_size_str(qn_total_size)}")

    # 免费额度预警（标准存储 10GB）
    if qn_total_size > 10 * 1024 ** 3:
        print(f"      ⚠ 七牛当前存储 {get_size_str(qn_total_size)} 已超 10GB 免费额度，"
              f"超出部分 0.115 元/GB/月 计费！")

    # 3. 计算同步清单
    print(f"[3/4] 对比计算同步清单 ...")
    to_copy = []
    skipped = 0
    for key, meta in r2_objects.items():
        # --force：跳过增量判断，R2 所有对象都重传（覆盖七牛对应 key），兜底校验
        if force or needs_copy(qn_objects.get(key), meta):
            to_copy.append((key, meta))
        else:
            skipped += 1

    to_delete = []
    if do_delete:
        for key in qn_objects:
            if key not in r2_objects:
                to_delete.append(key)

    copy_size = sum(m['size'] for _, m in to_copy)
    del_size = sum(qn_objects[k]['size'] for k in to_delete) if to_delete else 0
    print(f"      待上传: {len(to_copy)} 个 ({get_size_str(copy_size)})；"
          f"跳过(已一致): {skipped} 个")
    print(f"      待删除: {len(to_delete)} 个 ({get_size_str(del_size)})")

    if to_copy:
        print("\n待上传示例（前 5 个）:")
        for i, (key, meta) in enumerate(to_copy[:5], 1):
            print(f"  {i}. {key}  ({get_size_str(meta['size'])})")
        if len(to_copy) > 5:
            print(f"  ... 还有 {len(to_copy) - 5} 个")
    if to_delete:
        print("\n待删除示例（前 5 个）:")
        for i, key in enumerate(to_delete[:5], 1):
            print(f"  {i}. {key}")
        if len(to_delete) > 5:
            print(f"  ... 还有 {len(to_delete) - 5} 个")

    if not to_copy and not to_delete:
        print("\n没有需要同步的内容，七牛已与 R2 完全一致。")
        return

    if dry_run:
        print("\n[dry-run] 不实际传输/删除。去掉 --dry-run 执行真实同步。")
        return

    # 4. 执行：先上传，后删除（避免删错后无备份可恢复）
    start = datetime.now()
    copy_success = copy_failed = del_success = del_failed = 0
    copied_bytes = 0

    if to_copy:
        print(f"\n[4a/4] 上传新/变更对象（并发 {MAX_WORKERS}）...")
        print("=" * 80)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(copy_one, key, meta['size']): (key, meta)
                       for key, meta in to_copy}
            total = len(futures)
            for idx, fut in enumerate(as_completed(futures), 1):
                key, meta = futures[fut]
                try:
                    n = fut.result()
                    print(f"[{idx}/{total}] ✓ ↑ {key} ({get_size_str(n)})")
                    copy_success += 1
                    copied_bytes += n
                except Exception as e:
                    print(f"[{idx}/{total}] ✗ ↑ {key} - {e}")
                    copy_failed += 1

    if to_delete:
        print(f"\n[4b/4] 删除多余对象（并发 {MAX_WORKERS}）...")
        print("=" * 80)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(delete_one, key): key for key in to_delete}
            total = len(futures)
            for idx, fut in enumerate(as_completed(futures), 1):
                key = futures[fut]
                try:
                    fut.result()
                    print(f"[{idx}/{total}] ✓ × {key}")
                    del_success += 1
                except Exception as e:
                    print(f"[{idx}/{total}] ✗ × {key} - {e}")
                    del_failed += 1

    dur = (datetime.now() - start).total_seconds()
    print("\n" + "=" * 80)
    print("镜像同步完成!")
    print(f"上传: 成功 {copy_success} / 失败 {copy_failed}   "
          f"删除: 成功 {del_success} / 失败 {del_failed}")
    print(f"复制大小: {get_size_str(copied_bytes)}")
    print(f"耗时: {dur:.2f} 秒" +
          (f"  速度: {get_size_str(copied_bytes / dur)}/秒" if dur > 0 and copied_bytes else ""))
    if QINIU_CONFIG.get('custom_domain'):
        print(f"七牛访问: {QINIU_CONFIG['custom_domain']}/{qn_prefix}...")
    if copy_failed or del_failed:
        print(f"\n有失败项，重新运行本脚本即可（已成功的不会重传/重删）。")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断。已传输部分已生效，重新运行即可续传（按对象粒度）。")
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
