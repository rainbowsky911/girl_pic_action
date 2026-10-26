#!/usr/bin/env python3
"""
从 Backblaze B2 扫描视频并生成 videos-b2.json

- 视频存放于 {B2_PREFIX}/video/{folder}/xxx.mp4，folder 复用图片侧目录名
  （婚礼/杭州/桢桢...），以便复用 config.py 的 FOLDER_DATES 排序与小程序 tag
- 用 ffmpeg/ffprobe 自动抽取封面帧并生成 webp 缩略图，上传到 thumbnails/
  抽帧优先走 CDN URL（range seek，不下载整个视频），失败降级为整文件下载
- 输出本地 scripts/videos-b2.json（与 R2 的 videos.json 隔离，互不覆盖）
- 与 generate-manifest.py 完全分离：主页仍只读 manifest.json（图片），
  视频由小程序独立入口读取 videos.json
"""

import os
import sys
import json
import shutil
import subprocess
import tempfile
import io
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# 绕过 Windows 系统代理直连 B2（alilang 等代理客户端未启动时会因
# 系统代理指向 127.0.0.1:xxxxx 而连接失败；B2 为公网资源且直连可达）。
# 用户已显式设置 NO_PROXY 时尊重其配置。
os.environ.setdefault('NO_PROXY', '*')

from PIL import Image

# scripts 根目录（config.py 所在目录）加入 sys.path，脚本可在任意目录运行
SCRIPTS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS_ROOT)

# 导入 B2 统一配置
from config import B2_CONFIG, validate_b2_config, get_b2_client, get_folder_date

# 验证配置
try:
    validate_b2_config()
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

# 从配置中获取参数
B2_ACCESS_KEY_ID = B2_CONFIG['application_key_id']
B2_SECRET_ACCESS_KEY = B2_CONFIG['application_key']
B2_ENDPOINT = B2_CONFIG['endpoint']
B2_BUCKET = B2_CONFIG['bucket_name']
B2_PREFIX = B2_CONFIG['prefix']
B2_CUSTOM_DOMAIN = B2_CONFIG['custom_domain']

# 视频专属扫描前缀: {B2_PREFIX}/video/
VIDEO_SUBDIR = 'video'
if B2_PREFIX:
    VIDEO_PREFIX = f"{B2_PREFIX}/{VIDEO_SUBDIR}"
else:
    VIDEO_PREFIX = VIDEO_SUBDIR

# 压缩版存放前缀（compress-videos-b2.py 产物；原始 key 前加 compressed/）
COMPRESSED_PREFIX = 'compressed'

# 性能配置
MAX_WORKERS = 4  # 视频抽帧较重，并发数适当降低，避免 CDN 并发 seek 压力

# 封面配置
GENERATE_COVERS = True  # 是否生成封面
COVER_SEEK_SECONDS = 1.0  # 抽帧时间点（秒），避开黑屏片头
COVER_WIDTH = 800  # 封面最大宽度（与图片缩略图一致，便于瀑布流复用）
COVER_QUALITY = 75  # 封面质量
COVER_FORMAT = 'WEBP'  # 封面格式：WEBP 或 JPEG
SKIP_EXISTING_COVERS = True  # 已有封面则跳过生成（增量更新）

# 视频后缀白名单（小程序原生播放主要支持 mp4）
VIDEO_EXTS = ('.mp4', '.mov', '.m4v')

# 初始化 B2 客户端（使用 S3 兼容 API）
client = get_b2_client()


def check_ffmpeg():
    """检测 ffmpeg / ffprobe 是否可用

    优先系统 ffmpeg/ffprobe；缺失时降级使用 imageio-ffmpeg pip 包
    （自带 ffmpeg 二进制，无需系统安装/配 PATH，但无独立 ffprobe，
    此时时长/分辨率探测跳过，仅生成封面）。
    Returns: (ffmpeg_path, ffprobe_path) 或 None
    """
    ffmpeg = shutil.which('ffmpeg')
    ffprobe = shutil.which('ffprobe')
    if ffmpeg and ffprobe:
        return ffmpeg, ffprobe

    # 降级：imageio-ffmpeg pip 包自带 ffmpeg 二进制
    try:
        import imageio_ffmpeg
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        print(f"ℹ️  使用 imageio-ffmpeg 提供的 ffmpeg: {ffmpeg_exe}")
        return ffmpeg_exe, ffprobe  # ffprobe 可能为 None，probe 将跳过
    except ImportError:
        pass

    print("⚠️  未检测到 ffmpeg/ffprobe，将跳过封面生成")
    print("   推荐: pip install imageio-ffmpeg（自带 ffmpeg 二进制，无需系统配置）")
    print("   或系统安装 ffmpeg 并加入 PATH；或手动准备封面图到 thumbnails/ 目录后重跑")
    return None


def build_cdn_url(key):
    """根据 s3 key 构造 B2 CDN 公开访问 URL（经 Cloudflare Worker 代理，对中文路径分段编码）"""
    if not B2_CUSTOM_DOMAIN:
        return None
    encoded = '/'.join(quote(p) for p in key.split('/'))
    return f"{B2_CUSTOM_DOMAIN}/{encoded}"


def probe_video(url_or_path, ffprobe):
    """
    用 ffprobe 探测视频时长和分辨率
    Returns: (duration_seconds, width, height) 或 (None, None, None)
    """
    try:
        result = subprocess.run(
            [ffprobe, '-v', 'quiet', '-print_format', 'json',
             '-show_format', '-show_streams', url_or_path],
            capture_output=True, timeout=30
        )
        if result.returncode != 0 or not result.stdout:
            return None, None, None
        data = json.loads(result.stdout)
        duration = None
        if 'format' in data and 'duration' in data['format']:
            try:
                duration = float(data['format']['duration'])
            except (ValueError, TypeError):
                pass
        width = height = None
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'video':
                width = stream.get('width')
                height = stream.get('height')
                break
        return duration, width, height
    except Exception as e:
        print(f"  ⚠️  ffprobe 探测失败: {e}")
        return None, None, None


def extract_frame_from_url(video_cdn_url, seek, ffmpeg):
    """
    用 ffmpeg 直接从 URL 抽帧（range seek，不下载整个文件）
    Returns: PIL.Image 或 None
    """
    try:
        result = subprocess.run(
            [ffmpeg, '-y', '-ss', str(seek), '-i', video_cdn_url,
             '-frames:v', '1', '-f', 'image2pipe', '-vcodec', 'png', 'pipe:1'],
            capture_output=True, timeout=60
        )
        if result.returncode != 0 or not result.stdout:
            return None
        return Image.open(io.BytesIO(result.stdout))
    except Exception as e:
        print(f"  ⚠️  URL 抽帧失败: {e}")
        return None


def extract_frame_from_download(video_key, seek, ffmpeg):
    """
    降级方案：下载整个视频到临时文件再抽帧（URL seek 失败/无 CDN 时使用）
    Returns: PIL.Image 或 None
    """
    tmp_path = None
    try:
        obj = client.get_object(Bucket=B2_BUCKET, Key=video_key)
        suffix = os.path.splitext(video_key)[1] or '.mp4'
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
            for chunk in obj['Body'].iter_chunks(8192):
                tmp.write(chunk)
        result = subprocess.run(
            [ffmpeg, '-y', '-ss', str(seek), '-i', tmp_path,
             '-frames:v', '1', '-f', 'image2pipe', '-vcodec', 'png', 'pipe:1'],
            capture_output=True, timeout=120
        )
        if result.returncode != 0 or not result.stdout:
            return None
        return Image.open(io.BytesIO(result.stdout))
    except Exception as e:
        print(f"  ⚠️  下载抽帧失败: {e}")
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def create_and_upload_cover(video_key, video_cdn_url, ffmpeg):
    """
    抽帧 + 生成封面 + 上传到 thumbnails/
    Returns: cover_key 或 None
    """
    base_name = os.path.splitext(video_key)[0]
    if COVER_FORMAT == 'WEBP':
        ext = '.webp'
        content_type = 'image/webp'
    else:
        ext = '.jpg'
        content_type = 'image/jpeg'
    cover_key = f"thumbnails/{base_name}{ext}"

    # 已有封面则跳过（增量更新）
    if SKIP_EXISTING_COVERS:
        try:
            client.head_object(Bucket=B2_BUCKET, Key=cover_key)
            print(f"  ✓ 封面已存在，跳过: {cover_key}")
            return cover_key
        except Exception:
            pass  # 不存在，继续生成

    # 抽帧：优先 URL range seek，失败降级整文件下载
    img = None
    if video_cdn_url:
        img = extract_frame_from_url(video_cdn_url, COVER_SEEK_SECONDS, ffmpeg)
    if img is None:
        img = extract_frame_from_download(video_key, COVER_SEEK_SECONDS, ffmpeg)
    if img is None:
        print(f"  ⚠️  无法为 {video_key} 抽取封面")
        return None

    # PNG 抽帧可能是 RGBA/P 模式，统一转 RGB
    if img.mode in ('RGBA', 'P', 'LA'):
        img = img.convert('RGB')

    # 缩放（保持宽高比）
    if img.width > COVER_WIDTH:
        ratio = COVER_WIDTH / img.width
        new_height = int(img.height * ratio)
        img = img.resize((COVER_WIDTH, new_height), Image.Resampling.LANCZOS)

    # 保存为目标格式
    output = io.BytesIO()
    if COVER_FORMAT == 'WEBP':
        img.save(output, format='WEBP', quality=COVER_QUALITY, method=6)
    else:
        img.save(output, format='JPEG', quality=COVER_QUALITY, optimize=True)
    output.seek(0)

    client.put_object(
        Bucket=B2_BUCKET,
        Key=cover_key,
        Body=output,
        ContentType=content_type,
        CacheControl='public, max-age=31536000'  # 长缓存1年（路径固定不变）
    )

    return cover_key


def process_single_video(obj, prefix, prefix_len, ffmpeg_info, compressed_keys):
    """
    处理单个视频（用于并发执行）

    Args:
        obj: S3 对象
        prefix: 扫描前缀（life/video/）
        prefix_len: 前缀长度
        ffmpeg_info: (ffmpeg, ffprobe) 或 None
        compressed_keys: compressed/ 前缀下已存在的压缩版 key 集合

    Returns:
        video dict 或 None
    """
    key = obj['Key']

    if not key.lower().endswith(VIDEO_EXTS):
        return None

    # 跳过缩略图/预览图目录（保险，视频不应出现在这些目录）
    if key.startswith('thumbnails/') or key.startswith('previews/'):
        return None

    try:
        # 提取 folder（video/ 之后的第一段），与图片侧 folder 名对齐
        relative_key = key[prefix_len:] if prefix else key
        relative_key = relative_key.lstrip('/')
        folder = '其他'
        if '/' in relative_key:
            folder = relative_key.split('/')[0]

        # 从文件名生成 ID 和标题
        filename = os.path.basename(key)
        file_id = os.path.splitext(filename)[0]
        title = file_id.replace('_', ' ')

        # 播放地址：compressed/ 下有压缩版用压缩版，否则用原始文件
        compressed_key = f"{COMPRESSED_PREFIX}/{key}"
        compressed_info = compressed_keys.get(compressed_key)
        play_key = compressed_key if compressed_info else key

        # size/lastModified：有压缩版时用压缩版的（与实际播放文件一致）
        if compressed_info:
            play_size, play_last_modified = compressed_info
        else:
            play_size, play_last_modified = int(obj['Size']), obj['LastModified'].isoformat()

        cover_key = None
        duration = None
        width = height = None

        if ffmpeg_info:
            ffmpeg, ffprobe = ffmpeg_info
            video_cdn_url = build_cdn_url(key)

            # 探测时长/分辨率（需 ffprobe 且基于 CDN URL，失败/无 ffprobe 则放弃，字段可选）
            if video_cdn_url and ffprobe:
                duration, w, h = probe_video(video_cdn_url, ffprobe)
                if w:
                    width = w
                if h:
                    height = h

            # 生成封面
            if GENERATE_COVERS:
                cover_key = create_and_upload_cover(key, video_cdn_url, ffmpeg)
                if cover_key:
                    print(f"  ✓ 生成封面: {cover_key}")

        # 确定日期（复用图片侧 FOLDER_DATES）
        folder_date = get_folder_date(folder)
        if folder_date:
            date_taken = folder_date
        else:
            date_taken = obj['LastModified'].isoformat()

        video = {
            'id': file_id,
            'title': title,
            'videoKey': play_key,
            'folder': folder,
            'tags': [folder] if folder != '其他' else [],
            'dateTaken': date_taken,
            'lastModified': play_last_modified,
            'size': play_size,
        }

        # 有压缩版时记录原始文件路径（仅管理参考，小程序播放只用 videoKey）
        if play_key != key:
            video['originalKey'] = key

        if cover_key:
            video['coverKey'] = cover_key
        if duration is not None:
            video['duration'] = round(duration, 2)
        if width and height:
            video['width'] = width
            video['height'] = height

        return video

    except Exception as e:
        print(f"  ❌ 处理视频失败 {key}: {e}")
        return None


def list_compressed_keys():
    """列出 compressed/ 前缀下已存在的压缩版 {key: (size, lastModified)}"""
    items = {}
    continuation_token = None
    prefix = f"{COMPRESSED_PREFIX}/{VIDEO_PREFIX}/"
    while True:
        params = {'Bucket': B2_BUCKET, 'MaxKeys': 1000, 'Prefix': prefix}
        if continuation_token:
            params['ContinuationToken'] = continuation_token
        response = client.list_objects_v2(**params)
        if 'Contents' not in response:
            break
        for obj in response['Contents']:
            items[obj['Key']] = (int(obj['Size']), obj['LastModified'].isoformat())
        if not response.get('IsTruncated', False):
            break
        continuation_token = response.get('NextContinuationToken')
    return items


def scan_videos():
    """扫描 B2 中 {B2_PREFIX}/video/ 下的所有视频（并发处理）"""
    all_objects = []
    continuation_token = None

    # 确保前缀以 / 结尾
    prefix = f"{VIDEO_PREFIX}/" if VIDEO_PREFIX and not VIDEO_PREFIX.endswith('/') else (VIDEO_PREFIX or '')
    prefix_len = len(prefix)

    ffmpeg_info = check_ffmpeg()

    print(f"\n{'='*60}")
    print(f"📹 扫描 B2 存储桶: {B2_BUCKET}/{prefix}")
    print(f"   Endpoint: {B2_ENDPOINT}")
    print(f"   访问域名: {B2_CUSTOM_DOMAIN}")
    print(f"⚙️  并发线程数: {MAX_WORKERS}")
    print(f"🎬 ffmpeg: {'可用' if ffmpeg_info else '不可用（跳过封面生成）'}")
    print(f"🖼️  生成封面: {'是' if GENERATE_COVERS else '否'}")
    print(f"⏭️  跳过已有封面: {'是' if SKIP_EXISTING_COVERS else '否'}")
    print(f"{'='*60}\n")

    # 第一步：快速列出所有文件
    print("🔍 正在列出所有文件...")
    while True:
        params = {'Bucket': B2_BUCKET, 'MaxKeys': 1000}
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

    # 先过滤出视频文件（跳过 thumbnails/previews 目录）
    video_objs = [
        o for o in all_objects
        if o['Key'].lower().endswith(VIDEO_EXTS)
        and not o['Key'].startswith(('thumbnails/', 'previews/'))
    ]
    print(f"✅ 找到 {len(video_objs)} 个视频文件\n")

    # 列出已有压缩版（compressed/ 前缀），videoKey 将优先指向压缩版
    compressed_keys = list_compressed_keys()
    if compressed_keys:
        print(f"ℹ️  找到 {len(compressed_keys)} 个压缩版（compressed/），videoKey 将指向压缩版\n")

    # 第二步：并发处理视频
    print(f"⚡ 开始并发处理视频...\n")
    videos = []
    processed_count = 0
    total_count = len(video_objs)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_obj = {
            executor.submit(process_single_video, obj, prefix, prefix_len, ffmpeg_info, compressed_keys): obj
            for obj in video_objs
        }

        for future in as_completed(future_to_obj):
            processed_count += 1
            try:
                video = future.result()
                if video:
                    videos.append(video)
                # 每处理 5 个视频显示一次进度
                if processed_count % 5 == 0 or processed_count == total_count:
                    print(f"  进度: {processed_count}/{total_count} ({len(videos)} 个视频)")
            except Exception as e:
                obj = future_to_obj[future]
                print(f"  ❌ 处理失败: {obj['Key']} - {e}")

    print(f"\n{'='*60}")
    print(f"✅ 扫描完成，共找到 {len(videos)} 个视频")
    print(f"{'='*60}\n")

    return videos


def generate_manifest():
    """生成 videos-b2.json"""
    videos = scan_videos()

    # 排序：优先 folder_date，其次 lastModified，均降序（最新的在前）
    videos.sort(key=lambda v: (
        get_folder_date(v.get('folder')) or '',
        v.get('lastModified', '')
    ), reverse=True)

    manifest = {
        'version': '1.0',
        'generatedAt': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'videos': videos,
        'totalCount': len(videos)
    }

    # 输出到 scripts 根目录，文件名加 -b2 后缀，与 R2 的 videos.json 隔离
    output_path = os.path.join(SCRIPTS_ROOT, 'videos-b2.json')

    # 保存到本地
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ videos-b2.json 已生成")
    print(f"   文件路径: {os.path.abspath(output_path)}")
    print(f"   视频数量: {len(videos)}")
    print(f"   生成时间: {manifest['generatedAt']}")
    print(f"{'='*60}\n")

    return manifest


if __name__ == '__main__':
    generate_manifest()
