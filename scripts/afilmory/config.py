#!/usr/bin/env python3
"""
对象存储统一配置

所有凭证仅从环境变量读取，无硬编码默认值。
在 CI 中通过 load-secrets 步骤从 SECRETS_JSON 注入环境变量。
"""

import os
import sys

# Windows 中文控制台默认 GBK 编码，无法输出 ✓ 等 Unicode 字符，强制 UTF-8
# 已是 UTF-8 时 reconfigure 无副作用
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# Cloudflare R2 配置
R2_CONFIG = {
    'account_id': os.getenv('R2_ACCOUNT_ID', ''),
    'access_key_id': os.getenv('R2_ACCESS_KEY_ID', ''),
    'secret_access_key': os.getenv('R2_SECRET_ACCESS_KEY', ''),
    'bucket_name': os.getenv('R2_BUCKET_NAME', ''),

    # R2 endpoint URL 格式: https://{account_id}.r2.cloudflarestorage.com
    # 会在代码中自动构建，也可以直接通过环境变量指定
    'endpoint': os.getenv('R2_ENDPOINT', ''),

    # 上传配置
    'prefix': os.getenv('R2_PREFIX', 'life'),  # R2中的路径前缀，默认为 life
    'public_url_domain': os.getenv('R2_PUBLIC_DOMAIN', ''),
    'custom_domain': os.getenv('R2_CUSTOM_DOMAIN', ''),
}

# 文件夹时间配置（用于排序）
# 格式: '文件夹名': 'YYYY-MM-DD'
FOLDER_DATES = {
       '生活': '2026-08-01',
       '旅游': '2025-07-01',
       '杭州': '2026-03-01',
       '老家': '2026-01-01',

       '合肥': '2025-11-30',
       '厦门': '2025-10-06',
       '桢桢': '2025-06-02',
       '平潭': '2025-10-02',
       '牛草山': '2025-09-02',
       '周庄': '2025-04-01',
       '哈尔滨': '2025-01-08',
       '绍兴': '2024-05-01',
       '植物园': '2023-12-04',

       '吉卜力': '2026-01-01',
       '婚礼': '2025-01-02',

}

# CDN 配置（向后兼容）
CDN_CONFIG = {
    'domain': os.getenv('CDN_DOMAIN', R2_CONFIG['custom_domain']),
}


def get_r2_endpoint():
    """获取 R2 endpoint URL"""
    # 如果直接配置了 endpoint，优先使用
    if R2_CONFIG['endpoint']:
        return R2_CONFIG['endpoint']

    # 否则根据 account_id 自动构建
    account_id = R2_CONFIG['account_id']
    if account_id:
        return f"https://{account_id}.r2.cloudflarestorage.com"

    return None


def get_r2_client():
    """创建 R2 客户端（S3 兼容 API），内置 Windows 系统代理绕过"""
    import boto3
    os.environ.setdefault('NO_PROXY', '*')
    return boto3.client(
        's3',
        endpoint_url=get_r2_endpoint(),
        aws_access_key_id=R2_CONFIG['access_key_id'],
        aws_secret_access_key=R2_CONFIG['secret_access_key'],
        region_name='auto'  # R2 使用 'auto'
    )


def get_folder_date(folder_name):
    """
    获取文件夹的日期

    Args:
        folder_name: 文件夹名称

    Returns:
        ISO 8601 格式的日期字符串，如果未配置则返回 None
    """
    date_str = FOLDER_DATES.get(folder_name)
    if date_str:
        # 转换为 ISO 8601 格式（添加时间部分）
        return f"{date_str}T00:00:00.000Z"
    return None


def validate_config():
    """验证必需的配置项是否存在"""
    # 必需配置项
    required_fields = {
        'access_key_id': 'R2_ACCESS_KEY_ID',
        'secret_access_key': 'R2_SECRET_ACCESS_KEY',
        'bucket_name': 'R2_BUCKET_NAME',
    }

    # 检查 endpoint 或 account_id 至少有一个
    endpoint = get_r2_endpoint()
    if not endpoint:
        raise ValueError(
            "缺少 R2 endpoint 配置：请设置环境变量 R2_ENDPOINT 或 R2_ACCOUNT_ID"
        )

    # 检查其他必需字段
    missing_fields = []
    for field, env_name in required_fields.items():
        if not R2_CONFIG.get(field):
            missing_fields.append(env_name)

    if missing_fields:
        raise ValueError(f"缺少必需的环境变量: {', '.join(missing_fields)}")

    return True


# ============================================================================
# Backblaze B2 配置（私密桶 + Cloudflare Worker 代理访问）
# 与 R2 完全独立，可并存使用。B2 的 S3 兼容 API 与 R2 调用方式一致，
# 仅 endpoint / region / 凭证不同。
# 流程：浏览器 -> B2 自定义域 -> Cloudflare Worker
#       -> 用 application_key 签名访问私密 B2 桶 -> 返回内容
# ============================================================================
def _env(name, default):
    """读取环境变量；空字符串视为未设置（GitHub Actions 空 Secret 会注入空串，
    此时回退到下方默认值，避免 CI 因空串导致校验失败）。"""
    v = os.environ.get(name)
    return v if v else default


B2_CONFIG = {
    'application_key_id': _env('B2_APPLICATION_KEY_ID', ''),
    'application_key':    _env('B2_APPLICATION_KEY', ''),
    'bucket_name':         _env('B2_BUCKET_NAME', ''),
    'endpoint':            _env('B2_ENDPOINT', ''),
    'region':              _env('B2_REGION', ''),
    'prefix':              _env('B2_PREFIX', 'life'),
    'custom_domain':       _env('B2_CUSTOM_DOMAIN', ''),
}


def get_b2_client():
    """创建 B2 客户端（S3 兼容 API），内置 Windows 系统代理绕过"""
    import boto3
    os.environ.setdefault('NO_PROXY', '*')
    return boto3.client(
        's3',
        endpoint_url=B2_CONFIG['endpoint'],
        aws_access_key_id=B2_CONFIG['application_key_id'],
        aws_secret_access_key=B2_CONFIG['application_key'],
        region_name=B2_CONFIG['region'],
    )


def validate_b2_config():
    """验证 B2 必需配置项是否存在"""
    required_fields = {
        'application_key_id': 'B2_APPLICATION_KEY_ID',
        'application_key':    'B2_APPLICATION_KEY',
        'bucket_name':        'B2_BUCKET_NAME',
        'endpoint':           'B2_ENDPOINT',
    }

    missing_fields = []
    for field, env_name in required_fields.items():
        if not B2_CONFIG.get(field):
            missing_fields.append(env_name)

    if missing_fields:
        raise ValueError(
            f"缺少 B2 配置项: {', '.join(missing_fields)}\n"
            f"请设置环境变量或 SECRETS_JSON 中填入对应 key。"
        )

    return True


# ============================================================================
# 七牛云 Kodo 配置（S3 兼容 API）—— R2 镜像冷备（≤10GB 免费额度内零成本）
# 与 R2/B2 完全独立。用 S3 兼容 API，调用方式与 R2/B2 一致，仅凭证/endpoint 不同。
#
# 注意：此处的 access_key / secret_key 是七牛「S3 兼容」子账号凭证，
#       在七牛控制台 → 对象存储 Kodo → 空间概览 → S3 兼容凭证 页面创建/查看，
#       与七牛主账号的 AK/SK 不是同一套。
#
# 免费额度（实名认证用户，每月抵扣）：
#   - 标准存储 10GB/月（超出 0.115 元/GB/月）
#   - 上传流量 无上限（R2 → 七牛 写入零成本）
#   - PUT/DELETE 10万次/月、GET 100万次/月
#   - 外网流出流量 0.26 元/GB（无免费额度，仅在主动恢复数据时产生）
# 详见 https://www.qiniu.com/prices/kodo
# ============================================================================
QINIU_CONFIG = {
    'access_key_id':     _env('QINIU_ACCESS_KEY_ID', ''),
    'secret_access_key': _env('QINIU_SECRET_ACCESS_KEY', ''),
    'bucket_name':       _env('QINIU_BUCKET_NAME', ''),
    'endpoint':          _env('QINIU_ENDPOINT', ''),
    'region':            _env('QINIU_REGION', ''),
    'prefix':            _env('QINIU_PREFIX', ''),
    'custom_domain':     _env('QINIU_CUSTOM_DOMAIN', ''),
}


def get_qiniu_client():
    """创建七牛 Kodo 客户端（S3 兼容 API），内置 Windows 系统代理绕过"""
    import boto3
    os.environ.setdefault('NO_PROXY', '*')
    return boto3.client(
        's3',
        endpoint_url=QINIU_CONFIG['endpoint'],
        aws_access_key_id=QINIU_CONFIG['access_key_id'],
        aws_secret_access_key=QINIU_CONFIG['secret_access_key'],
        region_name=QINIU_CONFIG['region'],
    )


def validate_qiniu_config():
    """验证七牛必需配置项是否存在"""
    required_fields = {
        'access_key_id':     'QINIU_ACCESS_KEY_ID',
        'secret_access_key': 'QINIU_SECRET_ACCESS_KEY',
        'bucket_name':       'QINIU_BUCKET_NAME',
        'endpoint':          'QINIU_ENDPOINT',
    }

    missing_fields = []
    for field, env_name in required_fields.items():
        if not QINIU_CONFIG.get(field):
            missing_fields.append(env_name)

    if missing_fields:
        raise ValueError(
            f"缺少七牛配置项: {', '.join(missing_fields)}\n"
            f"请设置环境变量或 SECRETS_JSON 中填入对应 key。"
        )

    return True
