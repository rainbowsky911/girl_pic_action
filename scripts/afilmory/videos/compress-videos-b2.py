#!/usr/bin/env python3
"""
B2 视频压缩脚本（双版本模式）

- 扫描 {B2_PREFIX}/video/ 下的视频
- 未压缩的：下载到本地（boto3 直连 B2，不依赖 CDN/Worker）-> ffmpeg 本地探测码率
  -> 码率超阈值（默认 6 Mbps）的 H.264 CRF 26 压缩 -> 上传到 compressed/ 前缀：
     life/video/婚礼/xxx.mp4            原始文件（保留不动）
     compressed/life/video/婚礼/xxx.mp4 压缩版（小程序播放用）
- 已有压缩版的跳过（增量）；码率正常的跳过；下载的文件压缩时直接复用，不重复下载
- 顺带 +faststart（moov 移到头部，起播更快）
- 多线程：--workers 并发（默认 2，每路 ffmpeg 线程数 = CPU核数/并发数，避免超额订阅）

为什么不用 CDN URL 探测码率：CI（GitHub Actions）里 ffmpeg 访问不了
b2.dawei666.xyz（Cloudflare Worker 代理对 ffmpeg 的 range/GET 不可达），
导致 `ffmpeg -i <cdn_url>` 拿不到码率。改为 boto3 直连 B2（带凭证，可靠）下载后
本地探测，彻底绕开 CDN 可达性问题；下载的文件压缩时复用，不浪费。
"""

import os
import sys

# 绕过 Windows 系统代理直连 B2
os.environ.setdefault('NO_PROXY', '*')

import re
import shutil
import argparse
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

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

# 视频专属前缀: {B2_PREFIX}/video/
VIDEO_SUBDIR = 'video'
VIDEO_PREFIX = f"{B2_PREFIX}/{VIDEO_SUBDIR}" if B2_PREFIX else VIDEO_SUBDIR

# 压缩版存放前缀（原始 key 前加 compressed/，与 thumbnails/ 组织方式一致）
COMPRESSED_PREFIX = 'compressed'

# 压缩配置
BITRATE_THRESHOLD = 6000  # 码率阈值（kb/s），超过才压缩
CRF = '26'                # x264 质量（18-23 视觉无损，28 高压缩，26 平衡）
PRESET = 'veryfast'       # 编码速度/压缩率平衡：veryfast 比 medium 快约3-4倍；画质由 CRF 决定不变，仅文件略大（小程序播放够用）
AUDIO_BITRATE = '128k'    # 音频码率

# 视频后缀白名单
VIDEO_EXTS = ('.mp4', '.mov', '.m4v')

# 初始化 B2 客户端（boto3 client 线程安全，list/get/put 可多线程并发调用）
client = get_b2_client()

# 线程安全打印（并行压缩时避免输出交错）
print_lock = threading.Lock()


def tprint(*args, **kwargs):
    """带锁的 print，并行任务输出不交错"""
    with print_lock:
        print(*args, **kwargs)


def check_ffmpeg():
    """检测 ffmpeg（系统优先，imageio-ffmpeg pip 包降级；其 ffmpeg 已验证含 libx264）"""
    ffmpeg = shutil.which('ffmpeg')
    if ffmpeg:
        return ffmpeg
    try:
        import imageio_ffmpeg
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        print(f"ℹ️  使用 imageio-ffmpeg 提供的 ffmpeg: {ffmpeg_exe}")
        return ffmpeg_exe
    except ImportError:
        pass
    print("❌ 未检测到 ffmpeg，请先: pip install imageio-ffmpeg")
    return None


def get_compressed_key(key):
    """原始 key -> 压缩版 key"""
    return f"{COMPRESSED_PREFIX}/{key}"


def build_cdn_url(key):
    """根据 s3 key 构造 B2 CDN 公开访问 URL（经 Cloudflare Worker 代理，对中文路径分段编码）"""
    encoded = '/'.join(quote(p) for p in key.split('/'))
    return f"{B2_CUSTOM_DOMAIN}/{encoded}"


def download_video(key, dest_path, expected_size=0, tag=''):
    """下载视频到 dest_path。优先走 Worker URL（Bandwidth Alliance，B2 出站免费、不计每日额度）；
    失败回退 boto3 直连 B2（计每日下载额度）。每 10% 打印一次进度。
    Returns: (ok, method) method ∈ {'worker','boto3'}"""
    def _report(downloaded, last_pct, method):
        if expected_size > 0:
            pct = min(100, int(downloaded / expected_size * 100))
            if pct >= last_pct[0] + 10:
                last_pct[0] = pct
                tprint(f"{tag} 📥 下载 {pct}%（{downloaded // 1048576}/{expected_size // 1048576} MB）[{method}]")
        return last_pct

    url = build_cdn_url(key)
    # 1) Worker URL（HTTP GET，走 Cloudflare → B2 出站免费）
    try:
        req = Request(url, headers={'User-Agent': 'afilmory-video-pipeline/1.0'})
        with urlopen(req, timeout=60) as resp:
            if resp.status == 200:
                downloaded = 0
                last_pct = [0]
                with open(dest_path, 'wb') as f:
                    while True:
                        chunk = resp.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        last_pct = _report(downloaded, last_pct, 'worker')
                return True, 'worker'
    except (URLError, HTTPError, TimeoutError, OSError) as e:
        tprint(f"{tag} ⚠️  Worker 下载失败({e}), 回退 boto3 直连")
    except Exception as e:
        tprint(f"{tag} ⚠️  Worker 下载异常({e}), 回退 boto3 直连")

    # 2) 兜底：boto3 直连 B2（计额度，但稳定）
    obj = client.get_object(Bucket=B2_BUCKET, Key=key)
    downloaded = 0
    last_pct = [0]
    with open(dest_path, 'wb') as f:
        for chunk in obj['Body'].iter_chunks(8 * 1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            last_pct = _report(downloaded, last_pct, 'boto3')
    return True, 'boto3'


def probe_bitrate_local(path, ffmpeg, tag=''):
    """用 ffmpeg -i 探测本地文件码率与时长（读 moov 元数据，快）
    Returns: (bitrate_kb_s, duration_seconds) 任一未知为 None"""
    try:
        r = subprocess.run([ffmpeg, '-i', path], capture_output=True, timeout=60)
        info = r.stderr.decode('utf-8', errors='ignore')
        bm = re.search(r'bitrate: (\d+) kb/s', info)
        bitrate = int(bm.group(1)) if bm else None
        dm = re.search(r'Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)', info)
        duration = None
        if dm:
            duration = int(dm.group(1)) * 3600 + int(dm.group(2)) * 60 + float(dm.group(3))
        return bitrate, duration
    except Exception as e:
        tprint(f"{tag} ⚠️  码率探测失败: {e}")
        return None, None


def _run_encode(cmd, duration=None, tag=''):
    """运行 ffmpeg 编码，解析 -progress 输出每 10% 打印一次进度；超时 1800s 杀进程。
    Returns: (returncode, err_str)；超时 returncode=None。"""
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        tprint(f"{tag} ❌ 启动 ffmpeg 失败: {e}")
        return None, ''

    # 单独线程排空 stderr，避免管道缓冲塞满导致 ffmpeg 卡死
    err_buf = []

    def _drain():
        try:
            for chunk in iter(lambda: proc.stderr.read(65536), b''):
                err_buf.append(chunk)
        except Exception:
            pass
    threading.Thread(target=_drain, daemon=True).start()

    start = time.time()
    last_pct = 0
    timed_out = False
    for line in proc.stdout:
        s = line.decode('utf-8', errors='ignore').strip()
        if s.startswith('out_time=') and duration:
            m = re.match(r'out_time=(\d+):(\d+):(\d+(?:\.\d+)?)', s)
            if m:
                t = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
                pct = min(99, int(t / duration * 100))
                if pct >= last_pct + 10:
                    last_pct = pct
                    tprint(f"{tag} 🔄 压缩 {pct}%")
        if time.time() - start > 1800:
            proc.kill()
            tprint(f"{tag} ❌ 压缩超时(1800s)")
            timed_out = True
            break
    proc.wait()
    err = b''.join(err_buf).decode('utf-8', errors='ignore')
    if timed_out:
        return None, err
    return proc.returncode, err


def list_by_prefix(prefix):
    """列出指定前缀下的所有 key 集合"""
    keys = set()
    continuation_token = None
    while True:
        params = {'Bucket': B2_BUCKET, 'MaxKeys': 1000}
        if prefix:
            params['Prefix'] = prefix
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        response = client.list_objects_v2(**params)
        if 'Contents' not in response:
            break
        for obj in response['Contents']:
            keys.add(obj['Key'])
        if not response.get('IsTruncated', False):
            break
        continuation_token = response.get('NextContinuationToken')
    return keys


def list_videos():
    """列出 {B2_PREFIX}/video/ 下的所有原始视频 [(key, size)]"""
    videos = []
    continuation_token = None
    prefix = f"{VIDEO_PREFIX}/" if VIDEO_PREFIX and not VIDEO_PREFIX.endswith('/') else (VIDEO_PREFIX or '')

    print(f"\n🔍 扫描 B2 存储桶: {B2_BUCKET}/{prefix}")

    while True:
        params = {'Bucket': B2_BUCKET, 'MaxKeys': 1000}
        if prefix:
            params['Prefix'] = prefix
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        response = client.list_objects_v2(**params)
        if 'Contents' not in response:
            break

        for obj in response['Contents']:
            key = obj['Key']
            if key.lower().endswith(VIDEO_EXTS):
                videos.append((key, int(obj['Size'])))

        if not response.get('IsTruncated', False):
            break
        continuation_token = response.get('NextContinuationToken')

    print(f"✅ 找到 {len(videos)} 个原始视频\n")
    return videos


def compress_video(key, old_size, ffmpeg, enc_threads=None, tag=''):
    """
    下载 -> 本地探测码率 -> 高码率则压缩 -> 上传到 compressed/ 前缀（原始文件不动）
    下载的文件压缩时直接复用，不重复下载；不依赖 CDN URL（CI 中 Worker 代理对 ffmpeg 不可达）。
    enc_threads: 限制 libx264 线程数（并行压缩时按 CPU/并发数分配，避免超额订阅）
    tag: 进度标签（如 "[3/12]"），并行时区分输出
    Returns: (success, new_size)
    """
    tmp_in = tmp_out = None
    try:
        # 1. 下载（优先 Worker URL 走联盟免费出站，失败回退 boto3 直连）
        tprint(f"{tag} 📥 下载中（{old_size / 1024 / 1024:.1f} MB）...")
        suffix = os.path.splitext(key)[1] or '.mp4'
        fd, tmp_in = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        _, method = download_video(key, tmp_in, expected_size=old_size, tag=tag)
        tprint(f"{tag} 📥 下载完成（{method}）")

        # 2. 本地探测码率 + 时长
        bitrate, duration = probe_bitrate_local(tmp_in, ffmpeg, tag=tag)
        if bitrate is None:
            tprint(f"{tag} ⚠️  码率未知，跳过")
            return False, old_size
        if bitrate <= BITRATE_THRESHOLD:
            tprint(f"{tag} ✓ {bitrate} kb/s，码率正常，跳过")
            return False, old_size

        # 3. 压缩（复用已下载的 tmp_in，-progress 解析进度）
        tprint(f"{tag} 🎯 {bitrate} kb/s -> 压缩中（CRF {CRF}, {PRESET}）...")
        fd, tmp_out = tempfile.mkstemp(suffix='.mp4')
        os.close(fd)
        cmd = [
            ffmpeg, '-y', '-i', tmp_in,
            '-c:v', 'libx264', '-crf', CRF, '-preset', PRESET,
        ]
        if enc_threads:
            cmd += ['-threads', str(enc_threads)]
        cmd += [
            '-c:a', 'aac', '-b:a', AUDIO_BITRATE,
            '-movflags', '+faststart',
            '-progress', 'pipe:1',  # 结构化进度到 stdout，解析百分比
            tmp_out
        ]
        rc, err = _run_encode(cmd, duration=duration, tag=tag)
        if rc != 0:
            tprint(f"{tag} ❌ 压缩失败: {err[-200:]}")
            return False, old_size

        new_size = os.path.getsize(tmp_out)
        if new_size == 0:
            tprint(f"{tag} ❌ 压缩产物为空，跳过")
            return False, old_size

        # 保护：压缩后没明显变小就不传（理论上阈值过滤后不会发生）
        if new_size >= old_size * 0.9:
            tprint(f"{tag} ⚠️  压缩后体积未明显减小（{new_size / 1024 / 1024:.1f} MB），不传压缩版")
            return False, old_size

        # 4. 上传到 compressed/ 前缀（原始文件保留不动）
        compressed_key = get_compressed_key(key)
        tprint(f"{tag} 📤 上传压缩版（{new_size / 1024 / 1024:.1f} MB）-> {compressed_key}")
        with open(tmp_out, 'rb') as f:
            client.put_object(
                Bucket=B2_BUCKET,
                Key=compressed_key,
                Body=f,
                ContentType='video/mp4',
                CacheControl='public, max-age=31536000'  # 路径固定内容不变，长缓存1年
            )

        return True, new_size

    except Exception as e:
        tprint(f"{tag} ❌ 处理失败: {e}")
        return False, old_size
    finally:
        for p in (tmp_in, tmp_out):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass


def main():
    parser = argparse.ArgumentParser(description='B2 视频压缩工具（双版本模式）')
    parser.add_argument('--yes', '-y', action='store_true', help='自动确认，跳过交互（CI/自动化用）')
    parser.add_argument('--workers', '-w', type=int, default=2,
                        help='压缩并发数（默认 2；每路 ffmpeg 线程数 = CPU核数/并发数，避免超额订阅）')
    args = parser.parse_args()

    print("=" * 60)
    print("🗜️  B2 视频压缩工具（双版本模式：原始保留，压缩版存 compressed/）")
    print("=" * 60)

    ffmpeg = check_ffmpeg()
    if not ffmpeg:
        sys.exit(1)

    videos = list_videos()
    if not videos:
        print("没有找到视频，退出。")
        return

    # 列出已有压缩版（增量跳过）
    compressed_prefix = f"{COMPRESSED_PREFIX}/{VIDEO_PREFIX}/"
    existing_compressed = list_by_prefix(compressed_prefix)
    if existing_compressed:
        print(f"ℹ️  已有 {len(existing_compressed)} 个压缩版，将跳过\n")

    # 候选 = 未压缩的视频（已压缩的跳过）
    candidates = []
    for key, size in videos:
        if get_compressed_key(key) in existing_compressed:
            print(f"  ⏭️  {key}: 已有压缩版，跳过")
        else:
            candidates.append((key, size))

    if not candidates:
        print("\n没有需要压缩的视频，退出。")
        return

    # 确认
    print(f"\n⚠️  将处理以下 {len(candidates)} 个未压缩视频（下载 -> 探测码率 -> 高码率才压缩）:")
    for key, size in candidates:
        print(f"  - {key} ({size / 1024 / 1024:.1f} MB)")
    if not args.yes:
        confirm = input("确认继续? (y/N): ").strip().lower()
        if confirm != 'y':
            print("已取消。")
            return

    # 并行处理（下载/上传为网络 I/O，可与编码 CPU 重叠；每路 ffmpeg 限制线程数避免 CPU 超额订阅）
    workers = max(1, args.workers)
    cpu = os.cpu_count() or 4
    enc_threads = max(1, cpu // workers) if workers > 1 else None
    n = len(candidates)

    print("\n" + "=" * 60)
    print(f"🗜️  {workers} 路并发，每路 ffmpeg 线程数 = {'自动' if enc_threads is None else enc_threads}")
    print("=" * 60)

    state = {'done': 0, 'success': 0, 'saved': 0}
    state_lock = threading.Lock()

    def _compress_one(item):
        i, (key, old_size) = item
        tag = f"[{i + 1}/{n}]"
        tprint(f"{tag} {key}")
        try:
            success, new_size = compress_video(key, old_size, ffmpeg, enc_threads=enc_threads, tag=tag)
        except Exception as e:
            tprint(f"{tag} ❌ 异常: {e}")
            success, new_size = False, old_size
        with state_lock:
            state['done'] += 1
            if success:
                saved = old_size - new_size
                state['success'] += 1
                state['saved'] += saved
                tprint(f"{tag} ✅ 完成: {old_size / 1024 / 1024:.1f} MB -> {new_size / 1024 / 1024:.1f} MB"
                       f"（-{saved / 1024 / 1024:.1f} MB, -{saved / old_size * 100:.0f}%）"
                       f"  [{state['done']}/{n}]")
            else:
                tprint(f"{tag} 跳过  [{state['done']}/{n}]")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_compress_one, item) for item in enumerate(candidates)]
        for fut in as_completed(futs):
            fut.result()  # _compress_one 内部已捕获异常，正常不会抛

    success_count = state['success']
    total_saved = state['saved']

    # 总结
    print("\n" + "=" * 60)
    print(f"✅ 压缩完成! 成功 {success_count}/{n}，播放侧共节省 {total_saved / 1024 / 1024:.1f} MB")
    print("=" * 60)

    if success_count > 0:
        print("\n💡 后续步骤:")
        print("   1. 重跑 scripts/videos/generate-video-manifest-b2.py（videoKey 会自动指向压缩版）")
        print("   2. 重跑 scripts/videos/upload-videos-manifest-b2.py 上传新清单")


if __name__ == '__main__':
    main()
