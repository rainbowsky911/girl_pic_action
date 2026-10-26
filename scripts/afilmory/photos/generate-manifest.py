#!/usr/bin/env python3
"""
从 Cloudflare R2 扫描照片并生成 manifest.json
"""

import os
import sys
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from PIL import Image
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
except ImportError:
    # 如果没安装 pillow-heif，脚本依然运行，但处理 HEIC 时会按原样报错
    pass
import io

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

# 导入统一配置
from config import R2_CONFIG, validate_config, get_r2_endpoint, get_folder_date

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
R2_PREFIX = R2_CONFIG['prefix']

# 性能配置
MAX_WORKERS = 10  # 并发线程数，可以根据网络情况调整
FETCH_DIMENSIONS = True  # 是否获取真实图片尺寸（False 将大幅提速）

# 缩略图配置
GENERATE_THUMBNAILS = True  # 是否生成缩略图
THUMBNAIL_WIDTH = 800  # 缩略图最大宽度
THUMBNAIL_QUALITY = 75  # 缩略图质量（1-100）
THUMBNAIL_FORMAT = 'WEBP'  # 缩略图格式：WEBP 或 JPEG

# 预览图配置（用于详情页首屏快速加载）
GENERATE_PREVIEWS = True  # 是否生成预览图
PREVIEW_WIDTH = 1920  # 预览图最大宽度（适配大屏手机）
PREVIEW_QUALITY = 80  # 预览图质量（1-100）
PREVIEW_FORMAT = 'WEBP'  # 预览图格式

# 增量：缩略图+预览图都已存在且上次清单有尺寸时，跳过下载/重生成（复用上次结果）
SKIP_EXISTING = True

# 初始化 R2 客户端（使用 S3 兼容 API）
client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'  # R2 使用 'auto'
)


def _object_exists(key):
    """head_object 检查对象是否存在"""
    try:
        client.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except Exception:
        return False


def load_previous_manifest_entries():
    """从 R2 拉取上次的 manifest.json，返回 {s3Key: photo_entry}
    用于复用尺寸/缩略图key，跳过重复下载（增量关键）"""
    try:
        target = f"{R2_PREFIX}/manifest.json" if R2_PREFIX else "manifest.json"
        obj = client.get_object(Bucket=R2_BUCKET, Key=target)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return {p['s3Key']: p for p in data.get('photos', []) if 's3Key' in p}
    except Exception:
        return {}

def apply_exif_orientation(img):
    """
    应用 EXIF 方向信息，返回正确旋转后的图片
    """
    try:
        # 获取 EXIF 数据
        exif = img.getexif()
        if exif is None:
            return img
        
        # EXIF Orientation 标签是 274
        orientation = exif.get(274)
        
        if orientation is None:
            return img
        
        # 根据 orientation 值旋转/翻转图片
        if orientation == 2:
            img = img.transpose(Image.FLIP_LEFT_RIGHT)
        elif orientation == 3:
            img = img.rotate(180, expand=True)
        elif orientation == 4:
            img = img.transpose(Image.FLIP_TOP_BOTTOM)
        elif orientation == 5:
            img = img.rotate(-90, expand=True).transpose(Image.FLIP_LEFT_RIGHT)
        elif orientation == 6:
            img = img.rotate(-90, expand=True)
        elif orientation == 7:
            img = img.rotate(90, expand=True).transpose(Image.FLIP_LEFT_RIGHT)
        elif orientation == 8:
            img = img.rotate(90, expand=True)
        
        return img
    except Exception as e:
        print(f"  ⚠️  应用 EXIF 旋转失败: {e}")
        return img


def get_image_dimensions(file_content):
    """获取图片尺寸（考虑EXIF旋转信息）"""
    try:
        img = Image.open(io.BytesIO(file_content))
        # 应用 EXIF 旋转
        img = apply_exif_orientation(img)
        return img.size  # (width, height)
    except Exception as e:
        print(f"获取图片尺寸失败: {e}")
        return (1200, 900)  # 默认尺寸


def create_and_upload_thumbnail(original_key, img_data):
    """
    创建并上传缩略图
    
    Args:
        original_key: 原图的 S3 key
        img_data: 原图数据
        
    Returns:
        thumbnail_key: 缩略图的 S3 key，失败返回 None
    """
    if not GENERATE_THUMBNAILS:
        return None
        
    try:
        # 打开图片并应用 EXIF 旋转
        img = Image.open(io.BytesIO(img_data))
        img = apply_exif_orientation(img)

        # 动画 GIF 不生成静态缩略图（保留原图动画；这类条目没有 thumbnailKey，
        # 每次增量会被重新处理一遍——GIF 极少，开销可忽略）
        if getattr(img, 'is_animated', False):
            return None

        # 每张图都要有缩略图：长边超 THUMBNAIL_WIDTH 才缩小（只缩不放）；
        # 小图按原尺寸重编码为 webp——统一格式、压掉 PNG 的冗余体积
        long_side = max(img.width, img.height)
        if long_side > THUMBNAIL_WIDTH:
            ratio = THUMBNAIL_WIDTH / long_side
            img = img.resize(
                (max(1, round(img.width * ratio)), max(1, round(img.height * ratio))),
                Image.Resampling.LANCZOS,
            )
        thumbnail = img
        
        # 转换为目标格式（不保留EXIF，因为已经物理旋转）
        output = io.BytesIO()
        if THUMBNAIL_FORMAT == 'WEBP':
            thumbnail.save(output, format='WEBP', quality=THUMBNAIL_QUALITY, method=6, exif=b'')
            ext = '.webp'
            content_type = 'image/webp'
        else:
            thumbnail.save(output, format='JPEG', quality=THUMBNAIL_QUALITY, optimize=True, exif=b'')
            ext = '.jpg'
            content_type = 'image/jpeg'
        
        output.seek(0)
        
        # 生成缩略图的 key（保存在 thumbnails/ 目录）
        # 例如: life/合肥/photo.jpg -> thumbnails/life/合肥/photo.webp
        base_name = os.path.splitext(original_key)[0]
        thumbnail_key = f"thumbnails/{base_name}{ext}"
        
        # 上传缩略图到 R2
        client.put_object(
            Bucket=R2_BUCKET,
            Key=thumbnail_key,
            Body=output,
            ContentType=content_type,
            CacheControl='public, max-age=31536000'  # 长缓存1年（路径固定不变）
        )
        
        return thumbnail_key
        
    except Exception as e:
        print(f"  ⚠️  生成缩略图失败: {e}")
        return None


def create_and_upload_preview(original_key, img_data):
    """
    创建并上传预览图（用于详情页首屏快速加载）
    
    Args:
        original_key: 原图的 S3 key
        img_data: 原图数据
        
    Returns:
        preview_key: 预览图的 S3 key，失败返回 None
    """
    if not GENERATE_PREVIEWS:
        return None
        
    try:
        # 打开图片并应用 EXIF 旋转
        img = Image.open(io.BytesIO(img_data))
        img = apply_exif_orientation(img)

        # 动画 GIF 不生成静态预览图（保留原图动画）
        if getattr(img, 'is_animated', False):
            return None

        # 每张图都要有预览图：长边超 PREVIEW_WIDTH 才缩小（只缩不放）；
        # 小图按原尺寸重编码为 webp——lightbox 永远先载 webp 预览，
        # 不再回退到多 MB 的 PNG/原图直载（曾导致随机看图长时间黑屏）
        long_side = max(img.width, img.height)
        if long_side > PREVIEW_WIDTH:
            ratio = PREVIEW_WIDTH / long_side
            img = img.resize(
                (max(1, round(img.width * ratio)), max(1, round(img.height * ratio))),
                Image.Resampling.LANCZOS,
            )
        preview = img
        
        # 转换为目标格式（不保留EXIF，因为已经物理旋转）
        output = io.BytesIO()
        if PREVIEW_FORMAT == 'WEBP':
            preview.save(output, format='WEBP', quality=PREVIEW_QUALITY, method=6, exif=b'')
            ext = '.webp'
            content_type = 'image/webp'
        else:
            preview.save(output, format='JPEG', quality=PREVIEW_QUALITY, optimize=True, exif=b'')
            ext = '.jpg'
            content_type = 'image/jpeg'
        
        output.seek(0)
        
        # 生成预览图的 key（保存在 previews/ 目录）
        # 例如: life/厦门/photo.jpg -> previews/life/厦门/photo.webp
        base_name = os.path.splitext(original_key)[0]
        preview_key = f"previews/{base_name}{ext}"
        
        # 上传预览图到 R2
        client.put_object(
            Bucket=R2_BUCKET,
            Key=preview_key,
            Body=output,
            ContentType=content_type,
            CacheControl='public, max-age=31536000'  # 长缓存1年（路径固定不变）
        )
        
        return preview_key
        
    except Exception as e:
        print(f"  ⚠️  生成预览图失败: {e}")
        return None


def process_single_photo(obj, prefix, prefix_len, prev_entries=None):
    """
    处理单张照片（用于并发执行）

    Args:
        obj: S3 对象
        prefix: 路径前缀
        prefix_len: 前缀长度
        prev_entries: 上次清单 {s3Key: entry}，用于增量跳过（已存在缩略图/预览图则复用尺寸）

    Returns:
        photo dict 或 None
    """
    key = obj['Key']

    # 只处理图片文件 (加入 HEIC 支持)
    if not key.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.heic', '.heif')):
        return None

    # 跳过缩略图和预览图目录
    if key.startswith('thumbnails/') or key.startswith('previews/'):
        return None

    try:
        # 提取文件夹名
        relative_key = key[prefix_len:] if prefix else key
        folder = '其他'
        if '/' in relative_key:
            folder = relative_key.split('/')[0]

        # 从文件名生成 ID 和标题
        filename = os.path.basename(key)
        file_id = os.path.splitext(filename)[0]
        title = file_id.replace('_', ' ')

        # 增量：缩略图+预览图都已存在且上次清单有尺寸 → 复用，跳过下载与重生成
        if SKIP_EXISTING and prev_entries:
            prev = prev_entries.get(key)
            if prev and prev.get('width') and prev.get('height') \
                    and prev.get('thumbnailKey') and prev.get('previewKey') \
                    and _object_exists(prev['thumbnailKey']) and _object_exists(prev['previewKey']):
                folder_date = get_folder_date(folder)
                date_taken = folder_date if folder_date else obj['LastModified'].isoformat()
                result = {
                    'id': file_id,
                    'title': title,
                    's3Key': key,
                    'folder': folder,
                    'tags': [folder] if folder != '其他' else [],
                    'dateTaken': date_taken,
                    'lastModified': obj['LastModified'].isoformat(),
                    'size': int(obj['Size']),
                    'width': prev['width'],
                    'height': prev['height'],
                    'thumbnailKey': prev['thumbnailKey'],
                    'previewKey': prev['previewKey'],
                }
                if prev.get('hidden'):
                    result['hidden'] = True
                return result

        # 获取图片尺寸
        img_data = None
        if FETCH_DIMENSIONS or GENERATE_THUMBNAILS:
            try:
                obj_response = client.get_object(Bucket=R2_BUCKET, Key=key)
                img_data = obj_response['Body'].read()
                
                if FETCH_DIMENSIONS:
                    width, height = get_image_dimensions(img_data)
                else:
                    width, height = 1200, 900
            except Exception as e:
                print(f"  ⚠️  {key}: 获取数据失败，使用默认值")
                width, height = 1200, 900
                img_data = None
        else:
            width, height = 1200, 900

        # 生成缩略图
        thumbnail_key = None
        if img_data and GENERATE_THUMBNAILS:
            thumbnail_key = create_and_upload_thumbnail(key, img_data)
            if thumbnail_key:
                print(f"  ✓ 生成缩略图: {thumbnail_key}")

        # 生成预览图（用于详情页快速加载）
        preview_key = None
        if img_data and GENERATE_PREVIEWS:
            preview_key = create_and_upload_preview(key, img_data)
            if preview_key:
                print(f"  ✓ 生成预览图: {preview_key}")

        # 确定照片的日期
        folder_date = get_folder_date(folder)
        if folder_date:
            date_taken = folder_date
        else:
            date_taken = obj['LastModified'].isoformat()

        photo = {
            'id': file_id,
            'title': title,
            's3Key': key,
            'folder': folder,
            'tags': [folder] if folder != '其他' else [],
            'dateTaken': date_taken,
            'lastModified': obj['LastModified'].isoformat(),
            'size': int(obj['Size']),
            'width': width,
            'height': height
        }
        
        # 如果生成了缩略图，添加缩略图路径
        if thumbnail_key:
            photo['thumbnailKey'] = thumbnail_key

        # 如果生成了预览图，添加预览图路径
        if preview_key:
            photo['previewKey'] = preview_key

        # 从旧 manifest 继承 hidden 标记（用户通过 /toggle-visibility 设置的隐藏状态）
        old_entry = prev_entries.get(key) if prev_entries else None
        if old_entry and old_entry.get('hidden'):
            photo['hidden'] = True

        return photo

    except Exception as e:
        print(f"  ❌ 处理照片失败 {key}: {e}")
        return None

def scan_photos():
    """扫描 R2 中指定路径前缀下的所有照片（并发处理）"""
    all_objects = []
    continuation_token = None

    # 确保前缀以 / 结尾
    prefix = f"{R2_PREFIX}/" if R2_PREFIX and not R2_PREFIX.endswith('/') else (R2_PREFIX or '')
    prefix_len = len(prefix)

    print(f"\n{'='*60}")
    print(f"📂 扫描 R2 存储桶: {R2_BUCKET}/{prefix}")
    print(f"⚙️  并发线程数: {MAX_WORKERS}")
    print(f"📏 获取图片尺寸: {'是' if FETCH_DIMENSIONS else '否（使用默认值 1200x900）'}")
    print(f"{'='*60}\n")

    # 第一步：快速列出所有文件
    print("🔍 正在列出所有文件...")
    while True:
        params = {
            'Bucket': R2_BUCKET,
            'MaxKeys': 1000
        }

        if prefix:
            params['Prefix'] = prefix

        if continuation_token:
            params['ContinuationToken'] = continuation_token

        response = client.list_objects_v2(**params)

        if 'Contents' not in response:
            break

        all_objects.extend(response['Contents'])

        if not response.get('IsTruncated', False):
            break

        continuation_token = response.get('NextContinuationToken')

    print(f"✅ 找到 {len(all_objects)} 个文件\n")

    # 加载上次清单（用于增量跳过：复用尺寸/缩略图key，避免重复下载）
    prev_entries = load_previous_manifest_entries()
    if prev_entries:
        print(f"ℹ️  上次清单 {len(prev_entries)} 张，已存在缩略图/预览图的照片将跳过下载\n")

    # 第二步：并发处理照片
    print(f"⚡ 开始并发处理照片...\n")
    photos = []
    processed_count = 0
    total_count = len(all_objects)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_obj = {
            executor.submit(process_single_photo, obj, prefix, prefix_len, prev_entries): obj
            for obj in all_objects
        }

        # 处理完成的任务
        for future in as_completed(future_to_obj):
            processed_count += 1
            try:
                photo = future.result()
                if photo:
                    photos.append(photo)
                    # 每处理 10 张照片显示一次进度
                    if processed_count % 10 == 0 or processed_count == total_count:
                        print(f"  进度: {processed_count}/{total_count} ({len(photos)} 张照片)")
            except Exception as e:
                obj = future_to_obj[future]
                print(f"  ❌ 处理失败: {obj['Key']} - {e}")

    print(f"\n{'='*60}")
    print(f"✅ 扫描完成，共找到 {len(photos)} 张照片")
    print(f"{'='*60}\n")

    return photos

def build_photo_index(photos):
    """从 photos 聚合照片索引（轻量首屏清单）。

    每个 folder 一条 {key, count, thumb, lastModified}：
      - count：该 folder 照片数
      - thumb / lastModified：取该 folder lastModified 最大的那张（最新照）的
        thumbnailKey / lastModified。不依赖 photos 是否已排序。
    folder 顺序：按首次出现先后（若 photos 已按 lastModified 倒序，则最新 folder 在前）。
    与 afilmory-web/worker/upload.js 的 buildIndex 结构须保持一致。
    """
    order = []
    by_folder = {}
    for p in photos:
        f = p.get('folder') or '其他'
        if f not in by_folder:
            by_folder[f] = {
                'key': f,
                'count': 0,
                'thumb': p.get('thumbnailKey'),
                'lastModified': p.get('lastModified', ''),
            }
            order.append(f)
        entry = by_folder[f]
        entry['count'] += 1
        lm = p.get('lastModified', '')
        if lm > entry['lastModified']:
            entry['lastModified'] = lm
            entry['thumb'] = p.get('thumbnailKey') or entry['thumb']
    return [by_folder[f] for f in order]


def generate_manifest():
    """生成 manifest.json + photo-index.json"""
    photos = scan_photos()
    
    # 按 lastModified 时间降序排序（最新的在前）
    photos.sort(key=lambda p: p.get('lastModified', ''), reverse=True)

    manifest = {
        'version': '1.0',
        'generatedAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'photos': photos,
        'totalCount': len(photos)
    }

    # 输出到 scripts 根目录，与 upload-manifest.py 读取位置对应
    output_path = os.path.join(SCRIPTS_ROOT, 'manifest.json')

    # 保存到本地
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # 照片索引（轻量首屏清单）：网页首屏先于全量 manifest 拉这个小文件，渲染
    # tag 栏 + 各 folder 首图占位，再异步拉全量 manifest 接管。与 manifest 同源
    # 同次生成；上传 Worker 增量维护（见 afilmory-web/worker/upload.js）。
    photo_index = {
        'version': '1.0',
        'generatedAt': manifest['generatedAt'],
        'totalCount': len(photos),
        'folders': build_photo_index(photos),
    }
    index_path = os.path.join(SCRIPTS_ROOT, 'photo-index.json')
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(photo_index, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ manifest.json 已生成")
    print(f"   文件路径: {os.path.abspath(output_path)}")
    print(f"   照片数量: {len(photos)}")
    print(f"   生成时间: {manifest['generatedAt']}")
    print(f"   索引文件: {os.path.abspath(index_path)}（{len(photo_index['folders'])} 个 folder）")
    print(f"{'='*60}\n")

    return manifest

if __name__ == '__main__':
    generate_manifest()
