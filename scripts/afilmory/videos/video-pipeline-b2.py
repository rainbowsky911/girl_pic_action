#!/usr/bin/env python3
"""
B2 视频管线编排器（CI / 定时任务入口）

把「压缩 → 抽帧封面 → 生成清单 → 上传清单」串成一条增量管线：
1. 从 B2 读取上次运行的状态 JSON（{B2_PREFIX}/.video-pipeline-state.json）
2. 列出 {B2_PREFIX}/video/ 下所有源视频，与状态比对指纹（size + etag + lastModified）
   - 新增 / 变化  → 需要重新处理
   - 状态里有、源里没有 → 已删除，清理孤立产物
3. 若无任何新增/变化/删除：
   打印「任务正常运行中，当前没有需要更新的视频」后直接退出，
   不跑压缩/抽帧/清单生成（避免无变化时空跑、浪费 CI 与 B2 请求）
4. 若有变化：
   - 删除变化源对应的旧 compressed/ 与 thumbnails/（强制后续脚本重建）
   - 删除已删除源对应的孤立 compressed/ 与 thumbnails/
   - 依次执行：
       compress-videos-b2.py --yes [-w N]   高码率压缩（已压缩的自动跳过）
       generate-video-manifest-b2.py         抽帧封面 + 生成 videos-b2.json
       upload-videos-manifest-b2.py           上传清单到 B2
   - 回写新的状态 JSON 到 B2
   任一脚本非 0 退出则中止且不回写状态，下次自动重试。

状态 JSON 存 B2（CI runner 无持久磁盘），跨运行增量。
原始文件始终保留不动，压缩版与封面都在独立前缀下，不影响源存储。
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

from config import B2_CONFIG, validate_b2_config, get_b2_client

# 验证配置
try:
    validate_b2_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

B2_BUCKET = B2_CONFIG['bucket_name']
B2_PREFIX = B2_CONFIG['prefix']
B2_CUSTOM_DOMAIN = B2_CONFIG['custom_domain']

# 视频专属前缀: {B2_PREFIX}/video/
VIDEO_SUBDIR = 'video'
VIDEO_PREFIX = f"{B2_PREFIX}/{VIDEO_SUBDIR}" if B2_PREFIX else VIDEO_SUBDIR

# 产物前缀（与 compress-videos-b2.py / generate-video-manifest-b2.py 一致）
COMPRESSED_PREFIX = 'compressed'
THUMB_PREFIX = 'thumbnails'

VIDEO_EXTS = ('.mp4', '.mov', '.m4v')

# 状态 JSON 存放 key（放在前缀根，不在 video/ 下，不会被视频扫描命中）
STATE_KEY = f"{B2_PREFIX}/.video-pipeline-state.json" if B2_PREFIX else ".video-pipeline-state.json"

client = get_b2_client()


def list_videos_with_meta():
    """列出源视频 [(key, size, etag, lastModified_iso)]"""
    items = []
    token = None
    prefix = f"{VIDEO_PREFIX}/" if VIDEO_PREFIX and not VIDEO_PREFIX.endswith('/') else (VIDEO_PREFIX or '')
    while True:
        params = {'Bucket': B2_BUCKET, 'MaxKeys': 1000}
        if prefix:
            params['Prefix'] = prefix
        if token:
            params['ContinuationToken'] = token
        resp = client.list_objects_v2(**params)
        for o in resp.get('Contents', []):
            k = o['Key']
            if k.lower().endswith(VIDEO_EXTS):
                items.append((k, int(o['Size']), o.get('ETag', ''), o['LastModified'].isoformat()))
        if not resp.get('IsTruncated', False):
            break
        token = resp.get('NextContinuationToken')
    return items


def load_state():
    """从 B2 读取状态 {key: {size, etag, lastModified}}；不存在/损坏返回 {}"""
    try:
        obj = client.get_object(Bucket=B2_BUCKET, Key=STATE_KEY)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data.get('videos', {}) if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state_videos):
    """回写状态到 B2"""
    payload = {
        'version': 1,
        'updatedAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'videos': state_videos,
    }
    client.put_object(
        Bucket=B2_BUCKET,
        Key=STATE_KEY,
        Body=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json',
        CacheControl='no-cache',  # 状态会变，不缓存
    )


def try_delete(key):
    """删除对象，不存在/失败返回 False（静默）"""
    try:
        client.delete_object(Bucket=B2_BUCKET, Key=key)
        return True
    except Exception:
        return False


def stale_artifact_keys(src_key):
    """源视频 key 对应的 compressed + thumbnail key（变化/删除时清理以重建）"""
    base = os.path.splitext(src_key)[0]
    return [
        f"{COMPRESSED_PREFIX}/{src_key}",
        f"{THUMB_PREFIX}/{base}.webp",
        f"{THUMB_PREFIX}/{base}.jpg",
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
    parser = argparse.ArgumentParser(description='B2 视频管线编排器（增量：仅变化时处理）')
    parser.add_argument('--workers', '-w', type=int, default=2,
                        help='压缩并发数（传给 compress-videos-b2.py，默认 2）')
    parser.add_argument('--force', action='store_true',
                        help='忽略状态，全部视为变化（全量重建；会删除所有 compressed/ 与封面后重做，慎用）')
    args = parser.parse_args()

    print("=" * 60)
    print("🎞️  B2 视频管线编排器（压缩 → 抽帧 → 清单 → 上传）")
    print("=" * 60)

    print("\n📥 读取上次运行状态...")
    prev = {} if args.force else load_state()
    print(f"   状态记录 {len(prev)} 个视频（{STATE_KEY}）")

    print("\n🔍 扫描源视频...")
    sources = list_videos_with_meta()
    print(f"   找到 {len(sources)} 个源视频")

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

    print(f"   新增/变化: {len(changed)} 个")
    print(f"   已删除:    {len(removed)} 个")

    if not changed and not removed:
        print("\n✅ 任务正常运行中，当前没有新增或变化的视频，跳过压缩/抽帧/清单生成。")
        print("=" * 60)
        return

    if changed:
        print("\n🆕 新增/变化的视频：")
        for k in changed:
            print(f"   - {k}")
    if removed:
        print("\n🗑️  已删除的源视频（清理孤立产物）：")
        for k in removed:
            print(f"   - {k}")

    # 清理：变化源删除旧产物以重建；删除源清理孤立产物
    print("\n🧹 清理需要重建/孤立的产物（compressed/ 与 thumbnails/）...")
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

    scripts_dir = os.path.join(SCRIPTS_ROOT, 'videos')
    py = sys.executable

    run_step("1️⃣  压缩高码率视频（已压缩的自动跳过）",
             [py, os.path.join(scripts_dir, 'compress-videos-b2.py'), '--yes', '-w', str(args.workers)])
    run_step("2️⃣  抽帧封面 + 生成 videos-b2.json",
             [py, os.path.join(scripts_dir, 'generate-video-manifest-b2.py')])
    run_step("3️⃣  上传清单到 B2",
             [py, os.path.join(scripts_dir, 'upload-videos-manifest-b2.py')])

    print("\n📤 回写状态...")
    save_state(cur)
    print(f"   已更新状态：{len(cur)} 个视频 -> {STATE_KEY}")

    manifest_url = f"{B2_CUSTOM_DOMAIN}/{B2_PREFIX}/videos.json" if B2_PREFIX else f"{B2_CUSTOM_DOMAIN}/videos.json"
    print("\n" + "=" * 60)
    print(f"✅ 管线完成！处理变化 {len(changed)}、删除 {len(removed)}")
    print(f"   视频清单: {manifest_url}")
    print("=" * 60)


if __name__ == '__main__':
    main()
