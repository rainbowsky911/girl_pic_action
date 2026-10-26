#!/usr/bin/env python3
"""
上传 videos-b2.json 到 Backblaze B2

清单上传到 {B2_PREFIX}/videos.json，访问地址：
  https://b2.dawei666.xyz/life/videos.json

B2 与 R2 是两个独立桶，key 不冲突。前端切换视频源 = 改 CDN_DOMAIN。
"""

import os
import sys

# 绕过 Windows 系统代理直连 B2
os.environ.setdefault('NO_PROXY', '*')

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

# 导入 B2 统一配置
from config import B2_CONFIG, validate_b2_config, get_b2_client

# 验证配置
try:
    validate_b2_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

# 从配置中获取参数
B2_BUCKET = B2_CONFIG['bucket_name']
B2_PREFIX = B2_CONFIG['prefix']
B2_CUSTOM_DOMAIN = B2_CONFIG['custom_domain']

# 初始化 B2 客户端（使用 S3 兼容 API）
client = get_b2_client()


def upload_manifest():
    """上传 videos-b2.json 到 B2 {B2_PREFIX}/videos.json"""
    # 读 B2 版清单（与 R2 的 videos.json 隔离）
    manifest_path = os.path.join(SCRIPTS_ROOT, 'videos-b2.json')

    if not os.path.exists(manifest_path):
        print(f"错误：{manifest_path} 文件不存在")
        print(f"请先运行: python scripts/videos/generate-video-manifest-b2.py")
        print(f"当前目录：{os.getcwd()}")
        exit(1)

    # 目标路径
    target_path = f"{B2_PREFIX}/videos.json"

    print(f"\n上传 videos-b2.json 到 B2")
    print(f"  存储桶: {B2_BUCKET}")
    print(f"  Endpoint: {B2_CONFIG['endpoint']}")
    print(f"  目标路径: {target_path}")

    try:
        with open(manifest_path, 'rb') as f:
            response = client.put_object(
                Bucket=B2_BUCKET,
                Key=target_path,
                Body=f,
                ContentType='application/json',
                CacheControl='public, max-age=300'  # 缓存 5 分钟（清单会变）
            )

        print(f"\n{'=' * 60}")
        print(f"✅ 上传成功！")
        print(f"   ETag: {response['ETag']}")
        print(f"   访问地址: {B2_CUSTOM_DOMAIN}/{target_path}")
        print(f"{'=' * 60}\n")
        print(f"💡 前端切换视频源到 B2：")
        print(f"   把 utils/config.js 的 CDN_DOMAIN 改成 '{B2_CUSTOM_DOMAIN}'")
        print(f"   小程序会自动从 {B2_CUSTOM_DOMAIN}/{B2_PREFIX}/videos.json 拉清单")

    except Exception as e:
        print(f"\n❌ 上传失败: {e}")
        exit(1)

if __name__ == '__main__':
    upload_manifest()
