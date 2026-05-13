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


@dataclass
class YouTubeFormatInfo:
    format_id: str
    label: str
    ext: str
    filesize: int
    type: str  # "video", "audio"


@dataclass
class YouTubeVideoInfo:
    title: str
    duration: int
    thumbnail: str
    formats: list[YouTubeFormatInfo]
    url: str


def _human_size(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "?"
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size_bytes} B"


def _human_duration(seconds: int) -> str:
    if seconds <= 0:
        return "?"
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def fetch_youtube_formats(
    url: str,
    cookies_path: Path | None = None,
) -> YouTubeVideoInfo:
    """Quickly fetch available formats for a YouTube video without downloading."""
    if not is_youtube_url(url):
        raise YouTubeDownloadError("Unsupported YouTube URL.")

    try:
        import yt_dlp
    except Exception as error:
        raise YouTubeDownloadError(f"yt-dlp is not installed: {error}") from error

    options = {
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 30,
        "noplaylist": True,
        "age_limit": 100,
    }
    has_cookies = bool(cookies_path and cookies_path.exists())
    if has_cookies:
        options["cookiefile"] = str(cookies_path)

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as error:
        full_error = f"{type(error).__name__}: {error}"
        print(f"YouTube fetch_formats failed: {full_error}", flush=True)
        if has_cookies:
            print(f"YouTube cookies were provided from: {cookies_path}", flush=True)
        raise YouTubeDownloadError(
            compact_youtube_error(error, has_cookies=has_cookies)
        ) from error

    if not isinstance(info, dict):
        raise YouTubeDownloadError("Could not read YouTube metadata.")

    title = str(info.get("title") or "YouTube Video").strip()
    duration = int(info.get("duration") or 0)
    thumbnail = str(info.get("thumbnail") or "")

    seen_labels: set[str] = set()
    formats: list[YouTubeFormatInfo] = []
    has_ffmpeg = ffmpeg_available()

    raw_formats = info.get("formats") or []

    # Collect video formats
    video_heights: dict[int, dict] = {}
    for fmt in raw_formats:
        if not isinstance(fmt, dict):
            continue
        height = int(fmt.get("height") or 0)
        vcodec = str(fmt.get("vcodec") or "none")
        if height <= 0 or vcodec == "none":
            continue
        ext = str(fmt.get("ext") or "mp4")
        filesize = int(fmt.get("filesize") or fmt.get("filesize_approx") or 0)
        if height not in video_heights or filesize > video_heights[height].get("filesize", 0):
            video_heights[height] = {
                "format_id": str(fmt.get("format_id") or ""),
                "height": height,
                "ext": ext,
                "filesize": filesize,
                "fps": int(fmt.get("fps") or 0),
            }

    for height in sorted(video_heights.keys(), reverse=True):
        v = video_heights[height]
        fps_text = f" {v['fps']}fps" if v["fps"] and v["fps"] > 30 else ""
        size_text = f" ~{_human_size(v['filesize'])}" if v["filesize"] > 0 else ""
        label = f"🎬 {height}p{fps_text}{size_text}"
        if label not in seen_labels:
            seen_labels.add(label)
            # For video, we use our format_selector with max_height to get best combo
            if has_ffmpeg:
                fmt_str = (
                    f"bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a]/"
                    f"bestvideo[height<={height}]+bestaudio/"
                    f"best[height<={height}][ext=mp4]/best[height<={height}]/"
                    f"bestvideo+bestaudio/best"
                )
            else:
                fmt_str = f"best[height<={height}][ext=mp4]/best[height<={height}]/best"
            formats.append(YouTubeFormatInfo(
                format_id=fmt_str,
                label=label,
                ext="mp4",
                filesize=v["filesize"],
                type="video",
            ))

    # Collect best audio format
    best_audio: dict | None = None
    for fmt in raw_formats:
        if not isinstance(fmt, dict):
            continue
        vcodec = str(fmt.get("vcodec") or "none")
        acodec = str(fmt.get("acodec") or "none")
        if vcodec != "none" or acodec == "none":
            continue
        filesize = int(fmt.get("filesize") or fmt.get("filesize_approx") or 0)
        abr = float(fmt.get("abr") or 0)
        if best_audio is None or abr > float(best_audio.get("abr") or 0):
            best_audio = {
                "format_id": str(fmt.get("format_id") or ""),
                "ext": str(fmt.get("ext") or "m4a"),
                "filesize": filesize,
                "abr": abr,
            }

    if best_audio:
        size_text = f" ~{_human_size(best_audio['filesize'])}" if best_audio["filesize"] > 0 else ""
        abr_text = f" {int(best_audio['abr'])}kbps" if best_audio["abr"] > 0 else ""
        label = f"🎵 Audio only{abr_text}{size_text}"
        formats.append(YouTubeFormatInfo(
            format_id=f"bestaudio[ext=m4a]/bestaudio/best",
            label=label,
            ext=best_audio["ext"],
            filesize=best_audio["filesize"],
            type="audio",
        ))

    # If no formats found, add a default
    if not formats:
        formats.append(YouTubeFormatInfo(
            format_id=format_selector(),
            label=f"🎬 Best available",
            ext="mp4",
            filesize=0,
            type="video",
        ))

    return YouTubeVideoInfo(
        title=title,
        duration=duration,
        thumbnail=thumbnail,
        formats=formats,
        url=url,
    )


def validate_youtube_cookies(cookies_path: Path) -> dict:
    """Validate YouTube cookies by trying to access a restricted test."""
    result: dict = {
        "exists": False,
        "valid_format": False,
        "lines": 0,
        "domains": 0,
        "youtube_domains": 0,
        "working": False,
        "error": None,
    }

    if not cookies_path.exists():
        result["error"] = "Cookie file does not exist."
        return result

    result["exists"] = True
    try:
        text = cookies_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        result["error"] = f"Cannot read cookie file: {e}"
        return result

    lines = [l.strip() for l in text.splitlines() if l.strip() and not l.strip().startswith("#")]
    result["lines"] = len(lines)

    domains: set[str] = set()
    youtube_domains: set[str] = set()
    google_domains: set[str] = set()
    for line in lines:
        parts = line.split("\t")
        if len(parts) >= 7:
            domain = parts[0].strip().lstrip(".")
            domains.add(domain)
            if "youtube" in domain.lower():
                youtube_domains.add(domain)
            if "google" in domain.lower():
                google_domains.add(domain)

    result["domains"] = len(domains)
    result["youtube_domains"] = len(youtube_domains)
    result["google_domains"] = len(google_domains)
    result["domain_list"] = sorted(domains)
    result["has_google"] = len(google_domains) > 0
    result["has_youtube"] = len(youtube_domains) > 0
    result["valid_format"] = len(lines) > 0 and (len(youtube_domains) > 0 or len(google_domains) > 0)

    if not result["valid_format"]:
        result["error"] = "Cookie file has no YouTube/Google cookie entries."
        return result

    if not result["has_google"]:
        result["error"] = (
            "Cookie file is missing google.com cookies. "
            "Age-restricted videos need cookies from BOTH google.com and youtube.com. "
            "Re-export cookies with all domains included."
        )

    # Try to use cookies with yt-dlp to extract info on a known video
    try:
        import yt_dlp
        options = {
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 15,
            "noplaylist": True,
            "cookiefile": str(cookies_path),
        }
        # Just try extracting info for a common video
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info("https://www.youtube.com/watch?v=jNQXAC9IVRw", download=False)
            if isinstance(info, dict) and info.get("title"):
                result["working"] = True
            else:
                result["error"] = "yt-dlp returned no data with these cookies."
    except Exception as e:
        error_text = str(e).lower()
        if "sign in" in error_text or "login" in error_text or "cookies" in error_text:
            result["error"] = "Cookies are expired or invalid. Export fresh cookies."
        else:
            result["working"] = True  # Video-specific error, cookies themselves may be fine
            result["error"] = None

    return result


def compact_youtube_error(error: Exception | str, language: str = "fa", has_cookies: bool = False) -> str:
    text = str(error)
    lower = text.lower()
    fa = language == "fa"

    if "cancelled" in lower:
        return "دانلود لغو شد." if fa else "Download cancelled."
    if "private" in lower:
        return "این ویدیو خصوصی است و قابل دانلود نیست." if fa else "This video is private."
    if "age" in lower:
        if has_cookies:
            return (
                "این ویدیو محدودیت سنی دارد. کوکی‌های فعلی کافی نیستند. "
                "مطمئن شو کوکی‌ها شامل دامنه‌های google.com و youtube.com هستند و حساب Google‌ات تأیید سن دارد. "
                "کوکی جدید بگیر و /youtube_cookies بزن."
                if fa
                else "This video is age-restricted. Current cookies are not sufficient. "
                "Make sure cookies include both google.com and youtube.com domains, "
                "and your Google account has age verification. Re-export and /youtube_cookies."
            )
        return (
            "این ویدیو محدودیت سنی دارد. فایل cookies.txt از مرورگری که داخل Google/YouTube لاگین است بگیر "
            "(هم دامنه google.com و هم youtube.com)، بفرست و /youtube_cookies بزن."
            if fa
            else "This video is age-restricted. Export cookies.txt from a browser logged into Google/YouTube "
            "(include both google.com and youtube.com domains), send it and use /youtube_cookies."
        )
    if "sign in" in lower or "login" in lower or "cookies" in lower:
        if has_cookies:
            return (
                "کوکی‌ها موجود هستند ولی یوتیوب هنوز قبول نمی‌کنه. "
                "احتمالاً کوکی‌ها منقضی شده‌اند یا حساب Google تأیید سن ندارد. "
                "مراحل: 1) در مرورگر وارد Google شو 2) در تنظیمات Google تأیید سن کن "
                "3) کوکی جدید بگیر و /youtube_cookies بزن. "
                f"\n\nخطای اصلی: {text[:200]}"
                if fa
                else "Cookies exist but YouTube still requires sign-in. "
                "Cookies may be expired or Google account lacks age verification. "
                "Steps: 1) Sign into Google in browser 2) Verify age in Google settings "
                "3) Re-export cookies and /youtube_cookies. "
                f"\n\nOriginal error: {text[:200]}"
            )
        return (
            "یوتیوب برای این ویدیو ورود/کوکی می‌خواهد. فایل cookies.txt را بفرست و روی آن /youtube_cookies بزن، بعد لینک را دوباره ارسال کن."
            if fa
            else "YouTube requires sign-in or cookies for this video. Send cookies.txt, reply to it with /youtube_cookies, then retry the link."
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
    chosen_format: str | None = None,
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

    selected_format = chosen_format or format_selector()
    is_audio = selected_format.startswith("bestaudio")
    merge_format = "mp4" if not is_audio else None

    options = {
        "format": selected_format,
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 5,
        "fragment_retries": 10,
        "socket_timeout": 30,
        "continuedl": True,
        "age_limit": 100,
        "concurrent_fragment_downloads": youtube_concurrent_fragments(),
        "progress_hooks": [progress_hook],
    }
    if merge_format:
        options["merge_output_format"] = merge_format
    if cookies_path and cookies_path.exists():
        options["cookiefile"] = str(cookies_path)

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            # If format was already chosen via picker, skip metadata-only pass
            if not chosen_format:
                info = ydl.extract_info(url, download=False)
                if should_cancel():
                    raise YouTubeDownloadCancelled("Cancelled by user.")
                if not isinstance(info, dict):
                    raise YouTubeDownloadError("Could not read YouTube metadata.")

                estimated_size = _best_size(info)
                if estimated_size > 0:
                    check_size(estimated_size)
                    progress(0, estimated_size)

            try:
                info = ydl.extract_info(url, download=True)
            except Exception as fmt_error:
                fmt_err_text = str(fmt_error).lower()
                if "format" in fmt_err_text and "not available" in fmt_err_text and chosen_format:
                    # Retry with most permissive format
                    print(f"YouTube format retry: '{chosen_format}' not available, falling back.", flush=True)
                    cleanup_youtube_partials(output_dir, task_id)
                    fallback = "bestvideo+bestaudio/best" if ffmpeg_available() else "best"
                    options["format"] = fallback
                    with yt_dlp.YoutubeDL(options) as ydl2:
                        info = ydl2.extract_info(url, download=True)
                else:
                    raise
    except YouTubeDownloadCancelled:
        cleanup_youtube_partials(output_dir, task_id)
        raise
    except Exception as error:
        cleanup_youtube_partials(output_dir, task_id)
        raise YouTubeDownloadError(compact_youtube_error(error, language)) from error

    path = _downloaded_file(output_dir, task_id)
    title = str((info or {}).get("title") or "youtube").strip() or "youtube"
    default_ext = ".m4a" if is_audio else (path.suffix or ".mp4")
    final_name = safe_filename(f"{title}_{task_id}{default_ext}", f"youtube_{task_id}{default_ext}")
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
