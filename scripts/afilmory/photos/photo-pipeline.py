#!/usr/bin/env python3
"""
照片管线编排器（CI / 定时任务入口）—— 与 video-pipeline-b2.py 同模式

增量：状态 JSON 存 R2（{R2_PREFIX}/.photo-pipeline-state.json），比对源照片指纹
(size + etag + lastModified)。无新增/变化/删除 → 打印心跳退出，不跑生成/上传；
有变化 → 清理变化源的旧缩略图/预览图（强制 generate-manifest 重建）→ 跑
generate-manifest.py + upload-manifest.py → 回写状态。

存储不变：缩略图/预览图/manifest 都是确定性 key，put 覆盖不增长空间。
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone

os.environ.setdefault('NO_PROXY', '*')

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

from config import R2_CONFIG, validate_config, get_r2_client

# 验证配置
try:
    validate_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

R2_BUCKET = R2_CONFIG['bucket_name']
R2_PREFIX = R2_CONFIG['prefix']
R2_CUSTOM_DOMAIN = R2_CONFIG['custom_domain']

# 产物前缀（与 generate-manifest.py 一致）
THUMB_PREFIX = 'thumbnails'
PREVIEW_PREFIX = 'previews'

PHOTO_EXTS = ('.jpg', '.jpeg', '.png', '.gif', '.heic', '.heif')

# 状态 JSON 存放 key（放在前缀根，不在子目录下）
STATE_KEY = f"{R2_PREFIX}/.photo-pipeline-state.json" if R2_PREFIX else ".photo-pipeline-state.json"

client = get_r2_client()


def list_photos_with_meta():
    """列出源照片 [(key, size, etag, lastModified_iso)]"""
    items = []
    token = None
    prefix = f"{R2_PREFIX}/" if R2_PREFIX and not R2_PREFIX.endswith('/') else (R2_PREFIX or '')
    while True:
        params = {'Bucket': R2_BUCKET, 'MaxKeys': 1000}
        if prefix:
            params['Prefix'] = prefix
        if token:
            params['ContinuationToken'] = token
        resp = client.list_objects_v2(**params)
        for o in resp.get('Contents', []):
            k = o['Key']
            if k.lower().endswith(PHOTO_EXTS) and not k.startswith((f'{THUMB_PREFIX}/', f'{PREVIEW_PREFIX}/')):
                items.append((k, int(o['Size']), o.get('ETag', ''), o['LastModified'].isoformat()))
        if not resp.get('IsTruncated', False):
            break
        token = resp.get('NextContinuationToken')
    return items


def load_state():
    """从 R2 读取状态 {key: {size, etag, lastModified}}；不存在/损坏返回 {}"""
    try:
        obj = client.get_object(Bucket=R2_BUCKET, Key=STATE_KEY)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data.get('photos', {}) if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state_photos):
    """回写状态到 R2"""
    payload = {
        'version': 1,
        'updatedAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'photos': state_photos,
    }
    client.put_object(
        Bucket=R2_BUCKET,
        Key=STATE_KEY,
        Body=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json',
        CacheControl='no-cache',  # 状态会变，不缓存
    )


def try_delete(key):
    """删除对象，不存在/失败返回 False（静默）"""
    try:
        client.delete_object(Bucket=R2_BUCKET, Key=key)
        return True
    except Exception:
        return False


def stale_artifact_keys(src_key):
    """源照片 key 对应的缩略图 + 预览图 key（变化/删除时清理以重建）"""
    base = os.path.splitext(src_key)[0]
    return [
        f"{THUMB_PREFIX}/{base}.webp",
        f"{THUMB_PREFIX}/{base}.jpg",
        f"{PREVIEW_PREFIX}/{base}.webp",
        f"{PREVIEW_PREFIX}/{base}.jpg",
    ]


def run_step(name, cmd):
    """执行一步；非 0 退出则中止管线（不回写状态，下次自动重试）"""
    print(f"\n{'=' * 60}")
    print(name)
    print("=" * 60)
    print("$ " + " ".join(cmd))
    rc = subprocess.run(cmd).returncode
    if rc != 0:
        print(f"\n❌ 步骤失败（{name}，退出码 {rc}），中止管线，不回写状态，下次自动重试")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='照片管线编排器（增量：仅变化时处理）')
    parser.add_argument('--force', action='store_true',
                        help='忽略状态，全部视为变化（全量重建；会删除所有缩略图/预览图后重做，慎用）')
    args = parser.parse_args()

    print("=" * 60)
    print("🖼️  照片管线编排器（生成缩略图/预览图 → manifest → 上传）")
    print("=" * 60)

    print("\n📥 读取上次运行状态...")
    prev = {} if args.force else load_state()
    print(f"   状态记录 {len(prev)} 张照片（{STATE_KEY}）")

    print("\n🔍 扫描源照片...")
    sources = list_photos_with_meta()
    print(f"   找到 {len(sources)} 张源照片")

    cur = {}
    changed, removed = [], []
    for (k, size, etag, lm) in sources:
        cur[k] = {'size': size, 'etag': etag, 'lastModified': lm}
        old = prev.get(k)
        if old is None or (old.get('size'), old.get('etag'), old.get('lastModified')) != (size, etag, lm):
            changed.append(k)
    for k in prev:
        if k not in cur:
            removed.append(k)

    print(f"   新增/变化: {len(changed)} 张")
    print(f"   已删除:    {len(removed)} 张")

    if not changed and not removed:
        print("\n✅ 任务正常运行中，当前没有新增或变化的照片，跳过缩略图/预览图/清单生成。")
        print("=" * 60)
        return

    if changed:
        print("\n🆕 新增/变化的照片：")
        for k in changed:
            print(f"   - {k}")
    if removed:
        print("\n🗑️  已删除的源照片（清理孤立产物）：")
        for k in removed:
            print(f"   - {k}")

    # 清理：变化源删除旧产物以重建；删除源清理孤立产物
    print("\n🧹 清理需要重建/孤立的产物（thumbnails/ 与 previews/）...")
    cleaned = 0
    for k in changed + removed:
        for ak in stale_artifact_keys(k):
            if try_delete(ak):
                cleaned += 1
                print(f"   删除 {ak}")
    if cleaned == 0:
        print("   无需清理（均为新增源，无旧产物）")
    else:
        print(f"   共清理 {cleaned} 个旧产物")

    scripts_dir = os.path.join(SCRIPTS_ROOT, 'photos')
    py = sys.executable

    run_step("1️⃣  生成缩略图/预览图 + manifest.json（已存在的自动跳过）",
             [py, os.path.join(scripts_dir, 'generate-manifest.py')])
    run_step("2️⃣  上传 manifest.json 到 R2",
             [py, os.path.join(scripts_dir, 'upload-manifest.py')])

    print("\n📤 回写状态...")
    save_state(cur)
    print(f"   已更新状态：{len(cur)} 张照片 -> {STATE_KEY}")

    manifest_url = f"{R2_CUSTOM_DOMAIN}/{R2_PREFIX}/manifest.json" if R2_PREFIX else f"{R2_CUSTOM_DOMAIN}/manifest.json"
    print("\n" + "=" * 60)
    print(f"✅ 管线完成！处理变化 {len(changed)}、删除 {len(removed)}")
    print(f"   照片清单: {manifest_url}")
    print("=" * 60)


if __name__ == '__main__':
    main()
