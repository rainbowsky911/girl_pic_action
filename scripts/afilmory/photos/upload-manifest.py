#!/usr/bin/env python3
"""
上传 manifest.json 到 Cloudflare R2
"""

import os
import sys
import boto3

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

# 导入统一配置
from config import R2_CONFIG, validate_config, get_r2_endpoint

# 验证配置
try:
    validate_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

# 从配置中获取参数
R2_ACCESS_KEY_ID = R2_CONFIG['access_key_id']
R2_SECRET_ACCESS_KEY = R2_CONFIG['secret_access_key']
R2_ENDPOINT = get_r2_endpoint()
R2_BUCKET = R2_CONFIG['bucket_name']
R2_CUSTOM_DOMAIN = R2_CONFIG['custom_domain']
R2_PREFIX = R2_CONFIG['prefix']

# 初始化 R2 客户端（使用 S3 兼容 API）
client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'  # R2 使用 'auto'
)


def upload_manifest():
    """上传 manifest.json + photo-index.json 到 Cloudflare R2"""
    manifest_path = os.path.join(SCRIPTS_ROOT, 'manifest.json')

    if not os.path.exists(manifest_path):
        print(f"错误：{manifest_path} 文件不存在")
        print(f"当前目录：{os.getcwd()}")
        exit(1)

    # 目标路径
    target_path = f"{R2_PREFIX}/manifest.json"

    print(f"\n上传 manifest.json 到 R2")
    print(f"  存储桶: {R2_BUCKET}")
    print(f"  目标路径: {target_path}")

    try:
        with open(manifest_path, 'rb') as f:
            response = client.put_object(
                Bucket=R2_BUCKET,
                Key=target_path,  # 修改为包含目录结构的路径
                Body=f,
                ContentType='application/json',
                CacheControl='public, max-age=300'  # 缓存 5 分钟
            )

        print(f"\n{'=' * 60}")
        print(f"✅ 上传成功！")
        print(f"   ETag: {response['ETag']}")
        print(f"   访问地址: {R2_CUSTOM_DOMAIN}/{target_path}")
        print(f"{'=' * 60}\n")

    except Exception as e:
        print(f"\n❌ 上传失败: {e}")
        exit(1)

    # 照片索引（轻量首屏清单）：网页首屏先拉这个小文件再拉全量 manifest。
    # no-cache：与上传 Worker 的增量写入对齐，上传后刷新即见新索引。
    index_path = os.path.join(SCRIPTS_ROOT, 'photo-index.json')
    index_target = f"{R2_PREFIX}/photo-index.json"
    if not os.path.exists(index_path):
        print(f"ℹ️  {index_path} 不存在，跳过索引上传")
        return
    print(f"\n上传 photo-index.json 到 R2")
    print(f"  目标路径: {index_target}")
    try:
        with open(index_path, 'rb') as f:
            idx_resp = client.put_object(
                Bucket=R2_BUCKET,
                Key=index_target,
                Body=f,
                ContentType='application/json',
                CacheControl='no-cache',  # 小文件 + 上传即时可见
            )
        print(f"   ETag: {idx_resp['ETag']}")
        print(f"   访问地址: {R2_CUSTOM_DOMAIN}/{index_target}")
    except Exception as e:
        print(f"❌ 索引上传失败: {e}")
        # 索引失败不阻断主流程（manifest 已成功）

if __name__ == '__main__':
    upload_manifest()
