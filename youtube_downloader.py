from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from task_store import safe_filename


YOUTUBE_HOST_RE = re.compile(r"(^|\.)youtube\.com$|(^|\.)youtu\.be$", re.IGNORECASE)
YOUTUBE_PATH_RE = re.compile(r"^/(watch|shorts|embed|live)(/|$)", re.IGNORECASE)


class YouTubeDownloadCancelled(RuntimeError):
    pass


class YouTubeDownloadError(RuntimeError):
    pass


@dataclass
class YouTubeDownloadResult:
    path: Path
    file_name: str
    file_size: int
    title: str


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def is_youtube_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme.lower() not in {"http", "https"} or not YOUTUBE_HOST_RE.search(host):
        return False
    if host.endswith("youtu.be"):
        return bool(parsed.path.strip("/"))
    return bool(YOUTUBE_PATH_RE.search(parsed.path or ""))


def youtube_quality_height() -> int:
    return max(144, env_int("WALRUS_YOUTUBE_MAX_HEIGHT", 720))


def youtube_concurrent_fragments() -> int:
    return max(1, min(16, env_int("WALRUS_YOUTUBE_CONCURRENT_FRAGMENTS", 4)))


def ffmpeg_available() -> bool:
    return bool(shutil.which("ffmpeg"))


def format_selector(max_height: int | None = None) -> str:
    height = max_height or youtube_quality_height()
    if ffmpeg_available():
        return (
            f"bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a]/"
            f"bestvideo[height<={height}]+bestaudio/"
            f"best[height<={height}][ext=mp4]/best[height<={height}]/best"
        )
    return f"best[height<={height}][ext=mp4]/best[height<={height}]/best"


def compact_youtube_error(error: Exception | str, language: str = "fa") -> str:
    text = str(error)
    lower = text.lower()
    fa = language == "fa"

    if "cancelled" in lower:
        return "دانلود لغو شد." if fa else "Download cancelled."
    if "private" in lower:
        return "این ویدیو خصوصی است و قابل دانلود نیست." if fa else "This video is private."
    if "sign in" in lower or "login" in lower or "cookies" in lower:
        return (
            "یوتیوب برای این ویدیو ورود/کوکی می‌خواهد. فایل cookies.txt را بفرست و روی آن /youtube_cookies بزن، بعد لینک را دوباره ارسال کن."
            if fa
            else "YouTube requires sign-in or cookies for this video. Send cookies.txt, reply to it with /youtube_cookies, then retry the link."
        )
    if "age" in lower:
        return (
            "این ویدیو محدودیت سنی دارد و بدون ورود قابل دانلود نیست."
            if fa
            else "This video is age-restricted."
        )
    if "not available in your country" in lower or "region" in lower:
        return (
            "این ویدیو برای موقعیت فعلی سرور در دسترس نیست."
            if fa
            else "This video is not available from the server region."
        )
    if "copyright" in lower:
        return "این ویدیو به دلیل محدودیت کپی‌رایت در دسترس نیست." if fa else text[:220]
    if "ssl" in lower or "eof occurred" in lower:
        return (
            "اتصال به یوتیوب قطع شد. دوباره امتحان کن."
            if fa
            else "The YouTube connection was interrupted. Try again."
        )
    if "unsupported url" in lower:
        return "این لینک یوتیوب پشتیبانی نمی‌شود." if fa else "Unsupported YouTube URL."

    limit = 220
    return text[:limit] + ("..." if len(text) > limit else "")


def _best_size(info: dict) -> int:
    for key in ("filesize", "filesize_approx"):
        value = info.get(key)
        if isinstance(value, int) and value > 0:
            return value

    requested = info.get("requested_formats") or []
    total = 0
    for item in requested:
        if not isinstance(item, dict):
            continue
        value = item.get("filesize") or item.get("filesize_approx") or 0
        if isinstance(value, int) and value > 0:
            total += value
    return total


def _unique_path(directory: Path, file_name: str) -> Path:
    path = directory / file_name
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    for index in range(1, 1000):
        candidate = directory / f"{stem} {index}{suffix}"
        if not candidate.exists():
            return candidate
    return directory / f"{stem}_{os.getpid()}{suffix}"


def _downloaded_file(output_dir: Path, task_id: str) -> Path:
    candidates = []
    for path in output_dir.glob(f"yt_{task_id}*"):
        if not path.is_file():
            continue
        if path.suffix in {".part", ".ytdl", ".tmp", ".temp"}:
            continue
        candidates.append(path)

    if not candidates:
        raise YouTubeDownloadError("yt-dlp finished but no downloaded file was found.")
    return max(candidates, key=lambda item: item.stat().st_mtime)


def cleanup_youtube_partials(output_dir: Path, task_id: str) -> None:
    for path in output_dir.glob(f"yt_{task_id}*"):
        try:
            if path.is_file():
                path.unlink()
        except OSError:
            pass


def download_youtube(
    *,
    url: str,
    output_dir: Path,
    task_id: str,
    should_cancel,
    progress,
    check_size,
    language: str = "fa",
    cookies_path: Path | None = None,
) -> YouTubeDownloadResult:
    if not is_youtube_url(url):
        raise YouTubeDownloadError("Unsupported YouTube URL.")

    try:
        import yt_dlp
    except Exception as error:
        raise YouTubeDownloadError(f"yt-dlp is not installed: {error}") from error

    output_dir.mkdir(parents=True, exist_ok=True)
    outtmpl = str(output_dir / f"yt_{task_id}.%(ext)s")

    def progress_hook(status: dict) -> None:
        if should_cancel():
            raise YouTubeDownloadCancelled("Cancelled by user.")

        state = status.get("status")
        if state == "downloading":
            downloaded = int(status.get("downloaded_bytes") or 0)
            total = int(status.get("total_bytes") or status.get("total_bytes_estimate") or 0)
            if total > 0:
                check_size(total)
            if downloaded > 0:
                check_size(downloaded)
            progress(downloaded, total)
        elif state == "finished":
            total = int(status.get("total_bytes") or status.get("downloaded_bytes") or 0)
            if total > 0:
                check_size(total)
                progress(total, total)

    options = {
        "format": format_selector(),
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 5,
        "fragment_retries": 10,
        "socket_timeout": 30,
        "continuedl": True,
        "concurrent_fragment_downloads": youtube_concurrent_fragments(),
        "merge_output_format": "mp4",
        "progress_hooks": [progress_hook],
    }
    if cookies_path and cookies_path.exists():
        options["cookiefile"] = str(cookies_path)

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False)
            if should_cancel():
                raise YouTubeDownloadCancelled("Cancelled by user.")
            if not isinstance(info, dict):
                raise YouTubeDownloadError("Could not read YouTube metadata.")

            estimated_size = _best_size(info)
            if estimated_size > 0:
                check_size(estimated_size)
                progress(0, estimated_size)

            info = ydl.extract_info(url, download=True)
    except YouTubeDownloadCancelled:
        cleanup_youtube_partials(output_dir, task_id)
        raise
    except Exception as error:
        cleanup_youtube_partials(output_dir, task_id)
        raise YouTubeDownloadError(compact_youtube_error(error, language)) from error

    path = _downloaded_file(output_dir, task_id)
    title = str((info or {}).get("title") or "youtube").strip() or "youtube"
    final_name = safe_filename(f"{title}_{task_id}{path.suffix or '.mp4'}", f"youtube_{task_id}.mp4")
    final_path = _unique_path(output_dir, final_name)
    if final_path != path:
        path.replace(final_path)
        path = final_path

    file_size = path.stat().st_size
    check_size(file_size)
    return YouTubeDownloadResult(
        path=path,
        file_name=path.name,
        file_size=file_size,
        title=title,
    )
