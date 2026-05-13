from __future__ import annotations

import asyncio
import os
import re
import signal
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from html import escape
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv
from pyrogram import Client, enums, filters, idle
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from rubpy import Client as RubikaClient
import requests

from task_store import (
    DATA_DIR,
    DOWNLOAD_DIR,
    SESSION_DIR,
    apply_runtime_settings,
    append_task,
    build_status_text,
    clear_processing,
    cleanup_local_file,
    ensure_storage_dirs,
    find_failed_entry,
    has_rubika_session,
    human_size,
    human_duration,
    human_speed,
    find_queued_task,
    is_cancelled,
    load_processing,
    load_runtime_settings,
    load_worker_pid,
    ltr_code,
    mark_cancelled,
    normalize_language,
    normalize_upload_filename,
    pop_telegram_events,
    processing_task_is_active,
    queue_size,
    read_failed_entries,
    read_queue_tasks,
    remove_queued_task,
    runtime_path,
    safe_filename,
    save_runtime_settings,
    split_name,
    t as tr,
    write_failed_entries,
)
from youtube_downloader import (
    YouTubeDownloadCancelled,
    YouTubeFormatInfo,
    YouTubeVideoInfo,
    _human_duration as yt_human_duration,
    cleanup_youtube_partials,
    compact_youtube_error,
    download_youtube,
    fetch_youtube_formats,
    is_youtube_url,
    validate_youtube_cookies,
)


load_dotenv()

def env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(
            f"Warning: ignoring invalid integer value for {name}: {raw!r}",
            flush=True,
        )
        return default


API_ID = env_int("API_ID")
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_TELEGRAM_ID = env_int("OWNER_TELEGRAM_ID")
RUBIKA_CONNECT_TIMEOUT = env_int("RUBIKA_CONNECT_TIMEOUT", 60)
TELEGRAM_SESSION = str(
    runtime_path(
        os.getenv("TELEGRAM_SESSION", "walrus").strip() or "walrus",
        SESSION_DIR,
    )
)
MAX_FILE_BYTES = env_int("WALRUS_MAX_FILE_BYTES", 8 * 1024 * 1024 * 1024)
MIN_FREE_BYTES = env_int("WALRUS_MIN_FREE_BYTES", 512 * 1024 * 1024)
ALLOW_FILE_URLS = os.getenv("WALRUS_ALLOW_FILE_URLS", "").strip().lower() in {"1", "true", "yes"}

ensure_storage_dirs()


if not API_ID or not API_HASH or not BOT_TOKEN:
    raise RuntimeError("Please set API_ID, API_HASH and BOT_TOKEN as Space secrets.")


def telegram_session_files() -> list[Path]:
    path = Path(TELEGRAM_SESSION)
    candidates = [path]

    if path.suffix == "":
        candidates.append(Path(f"{path}.session"))
    else:
        candidates.append(path.with_suffix(".session"))

    for session_path in list(candidates):
        candidates.extend(
            [
                Path(f"{session_path}-journal"),
                Path(f"{session_path}-shm"),
                Path(f"{session_path}-wal"),
            ]
        )

    unique_candidates = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


def clear_telegram_session_files(reason: str) -> None:
    removed = []
    for path in telegram_session_files():
        try:
            if path.exists():
                path.unlink()
                removed.append(path.name)
        except OSError as error:
            print(f"Failed to remove Telegram session file {path}: {error}", flush=True)

    if removed:
        print(
            f"Cleared Telegram session files after {reason}: {', '.join(removed)}",
            flush=True,
        )
    else:
        print(f"No Telegram session files found to clear after {reason}.", flush=True)


def is_auth_key_duplicated(error: Exception) -> bool:
    text = str(error)
    return (
        type(error).__name__ == "AuthKeyDuplicated"
        or "AUTH_KEY_DUPLICATED" in text
        or "AuthKeyDuplicated" in text
    )


app = Client(
    TELEGRAM_SESSION,
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    max_concurrent_transmissions=5,
)

ACTIVE_DOWNLOADS: dict[str, dict] = {}
COMMANDS_READY = False
AUTH_SETUPS: dict[int, dict] = {}
CHANNEL_CHOICES: dict[int, dict[str, dict]] = {}
RECENT_CALLBACKS: dict[str, float] = {}
BASE_DIR = Path(__file__).resolve().parent
RUBIKA_AUTH_HELPER = BASE_DIR / "rubika_auth_helper.py"
YOUTUBE_COOKIES_FILE = DATA_DIR / "youtube_cookies.txt"
YOUTUBE_FORMAT_CHOICES: dict[str, dict] = {}  # token -> {url, format_id, label, chat_id, ...}

BTN_STATUS = "📊 Status"
BTN_TRANSFERS = "📋 Transfers"
BTN_CLEANUP = "🧹 Cleanup"
BTN_CANCEL = "🛑 Cancel"
BTN_SETTINGS = "⚙️ Settings"
MENU_BUTTONS = {BTN_STATUS, BTN_TRANSFERS, BTN_CLEANUP, BTN_CANCEL, BTN_SETTINGS}
VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".webm",
    ".flv",
    ".m4v",
}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
AUDIO_EXTENSIONS = {".mp3", ".wav", ".ogg", ".m4a", ".flac", ".aac"}
DOCUMENT_EXTENSIONS = {
    ".pdf",
    ".txt",
    ".csv",
    ".json",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
}
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz"}
DIRECT_FILE_EXTENSIONS = (
    VIDEO_EXTENSIONS
    | IMAGE_EXTENSIONS
    | AUDIO_EXTENSIONS
    | DOCUMENT_EXTENSIONS
    | ARCHIVE_EXTENSIONS
)
DIRECT_FILE_CONTENT_TYPES = {
    "application/pdf",
    "application/zip",
    "application/x-zip-compressed",
    "application/x-rar-compressed",
    "application/x-7z-compressed",
    "application/x-tar",
    "application/gzip",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/msword",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
}
URL_PATTERN = re.compile(r"(?P<url>(?:https?|file)://\S+)", re.IGNORECASE)
DIRECT_DOWNLOAD_MAX_RETRIES = 5
DIRECT_DOWNLOAD_RETRY_DELAY = 3
SPEEDTEST_DOWNLOAD_URL = os.getenv(
    "WALRUS_SPEEDTEST_DOWNLOAD_URL",
    "https://speed.cloudflare.com/__down?bytes=1000000",
).strip()
SPEEDTEST_RUBIKA_URL = os.getenv("WALRUS_SPEEDTEST_RUBIKA_URL", "https://rubika.ir").strip()
SPEEDTEST_YOUTUBE_URL = os.getenv(
    "WALRUS_SPEEDTEST_YOUTUBE_URL",
    "https://www.youtube.com/generate_204",
).strip()

MENU_ACTION_KEYS = {
    "status": "btn_status",
    "transfers": "btn_transfers",
    "cleanup": "btn_cleanup",
    "cancel": "btn_cancel",
    "settings": "btn_settings",
}
MENU_ACTION_BY_TEXT = {
    tr(language, key): action
    for language in ("fa", "en")
    for action, key in MENU_ACTION_KEYS.items()
}
MENU_BUTTONS = set(MENU_ACTION_BY_TEXT)


def current_language() -> str:
    return normalize_language(load_runtime_settings().get("language"))


def text_for(key: str, language: str | None = None, **kwargs) -> str:
    return tr(language or current_language(), key, **kwargs)


def menu_keyboard(language: str | None = None) -> ReplyKeyboardMarkup:
    lang = normalize_language(language or current_language())
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(tr(lang, "btn_status")), KeyboardButton(tr(lang, "btn_transfers"))],
            [KeyboardButton(tr(lang, "btn_cleanup")), KeyboardButton(tr(lang, "btn_cancel"))],
            [KeyboardButton(tr(lang, "btn_settings"))],
        ],
        resize_keyboard=True,
    )


MENU_KEYBOARD = menu_keyboard()

BOT_COMMAND_LABELS = {
    "en": {
        "start": "Open the main menu",
        "settings": "View Rubika upload settings",
        "status": "Show queue and storage status",
        "transfers": "List active and queued transfers",
        "set_rubika": "Start Rubika number setup",
        "youtube_cookies": "Save YouTube cookies.txt",
        "clear_youtube_cookies": "Remove YouTube cookies",
        "check_cookie": "Check if YouTube cookies are valid",
        "speedtest": "Test current Space network",
        "retry": "Retry a failed transfer",
        "retry_all": "Retry all failed transfers",
        "cleanup": "Clean safe download leftovers",
        "cancel": "Cancel a transfer",
    },
    "fa": {
        "start": "باز کردن منوی اصلی",
        "settings": "نمایش تنظیمات روبیکا",
        "status": "نمایش وضعیت صف و حافظه",
        "transfers": "نمایش انتقال‌های فعال و صف",
        "set_rubika": "شروع تنظیم شماره روبیکا",
        "youtube_cookies": "ذخیره کوکی یوتیوب",
        "clear_youtube_cookies": "حذف کوکی یوتیوب",
        "check_cookie": "بررسی اعتبار کوکی یوتیوب",
        "speedtest": "تست سرعت شبکه",
        "retry": "تلاش دوباره برای انتقال ناموفق",
        "retry_all": "تلاش دوباره برای همه انتقال‌های ناموفق",
        "cleanup": "پاک‌سازی فایل‌های امن",
        "cancel": "لغو یک انتقال",
    },
}


def bot_commands(language: str | None = None) -> list[BotCommand]:
    lang = normalize_language(language or current_language())
    labels = BOT_COMMAND_LABELS[lang]
    return [BotCommand(command, description) for command, description in labels.items()]


MENU_BUTTON_FILTER = filters.create(
    lambda _filter, _client, message: (message.text or "").strip() in MENU_BUTTONS
)


async def ensure_bot_commands(client: Client) -> None:
    global COMMANDS_READY
    language = current_language()
    if COMMANDS_READY == language:
        return

    try:
        await client.set_bot_commands(bot_commands(language))
        COMMANDS_READY = language
    except Exception:
        pass


def is_owner(user_id: int | None) -> bool:
    if not OWNER_TELEGRAM_ID:
        return True
    return bool(user_id and user_id == OWNER_TELEGRAM_ID)


async def ensure_authorized_message(message: Message) -> bool:
    if getattr(message.from_user, "is_bot", False):
        return False

    user_id = getattr(message.from_user, "id", None)
    if is_owner(user_id):
        return True

    print(
        "Ignoring unauthorized message "
        f"user_id={user_id} owner_id={OWNER_TELEGRAM_ID} "
        f"text={(message.text or message.caption or '')[:80]!r}",
        flush=True,
    )
    return False


async def ensure_authorized_callback(callback_query: CallbackQuery) -> bool:
    if is_owner(getattr(callback_query.from_user, "id", None)):
        return True

    print(
        "Ignoring unauthorized callback "
        f"user_id={getattr(callback_query.from_user, 'id', None)} "
        f"owner_id={OWNER_TELEGRAM_ID} data={(callback_query.data or '')[:80]!r}",
        flush=True,
    )
    try:
        await callback_query.answer()
    except Exception:
        pass
    return False


def callback_seen(callback_query: CallbackQuery) -> bool:
    callback_id = getattr(callback_query, "id", None)
    if not callback_id:
        return False

    now = time.time()
    for key, seen_at in list(RECENT_CALLBACKS.items()):
        if now - seen_at > 60:
            RECENT_CALLBACKS.pop(key, None)

    if callback_id in RECENT_CALLBACKS:
        return True
    RECENT_CALLBACKS[callback_id] = now
    return False


def build_menu_text() -> str:
    settings = load_settings_with_phone()
    lang = settings["language"]
    if lang == "fa":
        intro = "📤 <b>فایل، لینک مستقیم یا لینک یوتیوب بفرست</b> تا بعد از دانلود در روبیکا آپلود شود."
        session_label = "نشست روبیکا"
        destination_label = "مقصد"
    else:
        intro = "📤 <b>Send a file, direct link, or YouTube link</b> and I will upload it to Rubika."
        session_label = "Rubika Session"
        destination_label = "Destination"
    return "\n".join(
        [
            "<b>⛵️ WalrusHF v1.2.3</b>",
            intro,
            "",
            f"📱 <b>{session_label}:</b> {ltr_code(settings['rubika_session'])}",
            f"📬 <b>{destination_label}:</b> {ltr_code(format_destination_label(settings))}",
        ]
    )


def main_action_keyboard() -> InlineKeyboardMarkup:
    lang = current_language()
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(tr(lang, "btn_status"), callback_data="menu:status"),
                InlineKeyboardButton(tr(lang, "btn_transfers"), callback_data="menu:transfers"),
            ],
            [
                InlineKeyboardButton(tr(lang, "btn_cleanup"), callback_data="menu:cleanup"),
                InlineKeyboardButton(tr(lang, "btn_cancel"), callback_data="menu:cancel"),
            ],
            [InlineKeyboardButton(tr(lang, "btn_settings"), callback_data="menu:settings")],
        ]
    )


def status_summary_keyboard(has_cleanup: bool) -> InlineKeyboardMarkup:
    lang = current_language()
    details = "📋 جزئیات" if lang == "fa" else "📋 Details"
    confirm = "🧹 تایید پاک‌سازی" if lang == "fa" else "🧹 Confirm Cleanup"
    rows = [[InlineKeyboardButton(details, callback_data="menu:transfers")]]
    if has_cleanup:
        rows.append([InlineKeyboardButton(confirm, callback_data="cleanup:confirm")])
    rows.append([InlineKeyboardButton(tr(lang, "btn_settings"), callback_data="menu:settings")])
    return InlineKeyboardMarkup(rows)


def cleanup_keyboard(has_candidates: bool) -> InlineKeyboardMarkup | None:
    if not has_candidates:
        return None
    label = "✅ تایید پاک‌سازی" if current_language() == "fa" else "✅ Confirm cleanup"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data="cleanup:confirm")]]
    )


def format_destination_label(settings: dict) -> str:
    return str(settings.get("rubika_target_title") or "Saved Messages")


def rubika_session_exists() -> bool:
    return has_rubika_session(load_runtime_settings()["rubika_session"])


def rubika_session_phone(session_name: str) -> str | None:
    candidates = [runtime_path(session_name, SESSION_DIR)]
    candidates.append(Path(f"{candidates[0]}.rp"))

    for path in candidates:
        if not path.exists() or not path.is_file():
            continue
        try:
            with sqlite3.connect(path) as connection:
                row = connection.execute("select phone from session limit 1").fetchone()
        except sqlite3.Error:
            continue
        if row and row[0]:
            return normalize_phone_number(str(row[0]))

    return None


def load_settings_with_phone() -> dict:
    settings = load_runtime_settings()
    if settings.get("rubika_phone"):
        try:
            normalized_phone = normalize_phone_number(settings["rubika_phone"])
        except ValueError:
            return settings
        if normalized_phone != settings["rubika_phone"]:
            settings["rubika_phone"] = normalized_phone
            return save_runtime_settings(settings)
        return settings

    phone = rubika_session_phone(settings["rubika_session"])
    if not phone:
        return settings

    settings["rubika_phone"] = phone
    return save_runtime_settings(settings)


def settings_action_keyboard() -> InlineKeyboardMarkup:
    lang = current_language()
    next_lang = "en" if lang == "fa" else "fa"
    language_label = "🌐 English" if next_lang == "en" else "🌐 فارسی"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(tr(lang, "btn_change_account"), callback_data="settings:session")],
            [InlineKeyboardButton(tr(lang, "btn_destination"), callback_data="settings:destination")],
            [InlineKeyboardButton(language_label, callback_data=f"settings:language:{next_lang}")],
        ]
    )


def destination_action_keyboard() -> InlineKeyboardMarkup:
    lang = current_language()
    saved = "☁️ پیام‌های ذخیره‌شده" if lang == "fa" else "☁️ Saved Messages"
    channel = "📣 انتخاب کانال" if lang == "fa" else "📣 Choose Channel"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(saved, callback_data="destination:saved")],
            [InlineKeyboardButton(channel, callback_data="destination:channels")],
            [InlineKeyboardButton(tr(lang, "btn_back"), callback_data="destination:back")],
        ]
    )


def channel_picker_keyboard(chat_id: int, channels: list[dict]) -> InlineKeyboardMarkup:
    choices: dict[str, dict] = {}
    rows = []

    for channel in channels[:8]:
        token = uuid.uuid4().hex[:8]
        choices[token] = channel
        title = truncate_button_label(channel.get("title") or "Untitled Channel")
        rows.append(
            [InlineKeyboardButton(f"📣 {title}", callback_data=f"destination:set:{token}")]
        )

    CHANNEL_CHOICES[chat_id] = choices
    rows.append([InlineKeyboardButton("↩️ Back", callback_data="destination:menu")])
    return InlineKeyboardMarkup(rows)


def auth_setup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✖️ Cancel Setup", callback_data="auth:cancel")]]
    )


def build_settings_text(note: str | None = None) -> str:
    settings = load_settings_with_phone()
    lang = settings["language"]
    active_phone = settings.get("rubika_phone") or "Not set"
    if lang == "fa":
        lines = [
            "<b>⚙️ تنظیمات روبیکا</b>",
            "",
            "اینجا حساب، مقصد آپلود و زبان ربات را کنترل می‌کنی.",
            "",
            f"📱 <b>حساب فعلی:</b> {ltr_code(settings['rubika_session'])}",
            f"☎️ <b>شماره فعال:</b> {ltr_code(active_phone)}",
            f"📬 <b>مقصد آپلود:</b> {ltr_code(format_destination_label(settings))}",
            f"🌐 <b>زبان:</b> {ltr_code(tr(lang, 'language_name'))}",
            "",
            "انتقال‌هایی که قبلا وارد صف شده‌اند مقصد و زبان زمان ثبت خودشان را نگه می‌دارند.",
        ]
        if note:
            lines.extend(["", note])
        return "\n".join(lines)

    lines = [
        "<b>⚙️ Rubika Settings</b>",
        "",
        "Control which Rubika account receives uploads.",
        "",
        f"📱 <b>Current Account:</b> {ltr_code(settings['rubika_session'])}",
        f"☎️ <b>Active Phone:</b> {ltr_code(active_phone)}",
        f"📬 <b>Upload Destination:</b> {ltr_code(format_destination_label(settings))}",
        f"🌐 <b>Language:</b> {ltr_code(tr(lang, 'language_name'))}",
    ]

    lines.extend(
        [
            "",
            "Use the buttons below to change the Rubika account or upload destination.",
            "Already queued transfers keep the destination they were queued with.",
        ]
    )

    if note:
        lines.extend(["", note])

    return "\n".join(lines)


async def send_settings_panel(message: Message, note: str | None = None) -> None:
    await message.reply_text(
        build_settings_text(note),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=settings_action_keyboard(),
    )


async def send_settings_panel_to_chat(chat_id: int, note: str | None = None) -> None:
    await app.send_message(
        chat_id,
        build_settings_text(note),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=settings_action_keyboard(),
    )


async def render_settings_panel(message: Message, note: str | None = None) -> None:
    text = build_settings_text(note)
    markup = settings_action_keyboard()
    try:
        await message.edit_text(
            text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=markup,
        )
    except Exception:
        await message.reply_text(
            text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=markup,
        )


def truncate_button_label(text: str, max_length: int = 38) -> str:
    text = " ".join(str(text or "").split()).strip() or "Untitled"
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 1].rstrip()}…"


def build_destination_text(note: str | None = None) -> str:
    settings = load_runtime_settings()
    lang = settings["language"]
    if lang == "fa":
        lines = [
            "<b>📬 مقصد آپلود</b>",
            "",
            f"مقصد فعلی: {ltr_code(format_destination_label(settings))}",
            "",
            "انتخاب کن فایل‌های بعدی کجا آپلود شوند.",
            "انتقال‌هایی که قبلا وارد صف شده‌اند تغییر نمی‌کنند.",
        ]
        if note:
            lines.extend(["", note])
        return "\n".join(lines)

    lines = [
        "<b>📬 Upload Destination</b>",
        "",
        f"Current: {ltr_code(format_destination_label(settings))}",
        "",
        "Choose where future uploads should go.",
        "Already queued transfers will not be changed.",
    ]

    if note:
        lines.extend(["", note])

    return "\n".join(lines)


async def send_destination_panel(message: Message, note: str | None = None) -> None:
    await message.reply_text(
        build_destination_text(note),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=destination_action_keyboard(),
    )


def reset_destination_settings() -> dict:
    settings = load_runtime_settings()
    settings["rubika_target"] = "me"
    settings["rubika_target_title"] = "Saved Messages"
    settings["rubika_target_type"] = "saved"
    return save_runtime_settings(settings)


def rubika_update_to_plain(value):
    if isinstance(value, dict):
        return {key: rubika_update_to_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [rubika_update_to_plain(item) for item in value]

    for attr in ("to_dict", "original_update"):
        try:
            data = getattr(value, attr)
        except Exception:
            data = None
        if isinstance(data, dict):
            return rubika_update_to_plain(data)

    return value


def nested_text_value(payload: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for value in payload.values():
        if isinstance(value, dict):
            found = nested_text_value(value, keys)
            if found:
                return found
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    found = nested_text_value(item, keys)
                    if found:
                        return found

    return None


def collect_channel_destinations(payload) -> list[dict]:
    channels: list[dict] = []
    seen: set[str] = set()

    def visit(value) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
            return

        if not isinstance(value, dict):
            return

        guid = value.get("channel_guid") or value.get("object_guid")
        if isinstance(guid, str) and guid.startswith("c0") and guid not in seen:
            seen.add(guid)
            title = nested_text_value(
                value,
                ("title", "channel_title", "name", "first_name", "username"),
            )
            channels.append(
                {
                    "guid": guid,
                    "title": title or f"Channel {len(channels) + 1}",
                    "type": "channel",
                }
            )

        for item in value.values():
            if isinstance(item, (dict, list)):
                visit(item)

    visit(rubika_update_to_plain(payload))
    return channels


async def load_rubika_channels(session_name: str) -> list[dict]:
    client = RubikaClient(name=session_name)
    entered = False
    try:
        await asyncio.wait_for(client.__aenter__(), timeout=RUBIKA_CONNECT_TIMEOUT)
        entered = True
        chats = await client.get_chats()
        return collect_channel_destinations(chats)
    finally:
        if entered:
            await client.__aexit__(None, None, None)


def auth_state(chat_id: int) -> dict | None:
    return AUTH_SETUPS.get(chat_id)


def track_auth_temp_message(chat_id: int, message_id: int) -> None:
    state = auth_state(chat_id)
    if not state:
        return
    temp_message_ids = state.setdefault("temp_message_ids", [])
    if message_id not in temp_message_ids:
        temp_message_ids.append(message_id)


async def cleanup_auth_temp_messages(chat_id: int) -> None:
    state = auth_state(chat_id)
    if not state:
        return

    temp_message_ids = state.get("temp_message_ids", [])
    if not temp_message_ids:
        return

    state["temp_message_ids"] = []
    try:
        await app.delete_messages(chat_id, temp_message_ids)
    except Exception:
        pass


async def cleanup_auth_input_message(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


async def send_auth_temp_message(
    message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | ReplyKeyboardMarkup | None,
) -> Message:
    sent = await message.reply_text(text, reply_markup=reply_markup)
    track_auth_temp_message(message.chat.id, sent.id)
    return sent


async def send_auth_temp_message_to_chat(
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | ReplyKeyboardMarkup | None,
) -> Message | None:
    try:
        sent = await app.send_message(chat_id, text, reply_markup=reply_markup)
    except Exception:
        return None
    track_auth_temp_message(chat_id, sent.id)
    return sent


def clear_auth_setup(chat_id: int) -> None:
    AUTH_SETUPS.pop(chat_id, None)


def stop_auth_process(chat_id: int) -> None:
    state = AUTH_SETUPS.get(chat_id)
    process = state.get("process") if state else None
    if process and process.poll() is None:
        process.terminate()


def normalize_phone_number(phone_number: str) -> str:
    phone = re.sub(r"[^\d+]", "", phone_number.strip())
    if phone.startswith("00"):
        phone = phone[2:]
    elif phone.startswith("+"):
        phone = phone[1:]

    if phone.startswith("0"):
        phone = f"98{phone[1:]}"
    elif phone.startswith("9") and len(phone) == 10:
        phone = f"98{phone}"

    if not re.fullmatch(r"\d{7,15}", phone):
        raise ValueError("Invalid phone number.")

    return phone


async def prompt_rubika_phone_setup(message: Message, first_setup: bool = False) -> None:
    stop_auth_process(message.chat.id)
    await cleanup_auth_temp_messages(message.chat.id)
    clear_auth_setup(message.chat.id)
    setup_id = uuid.uuid4().hex
    AUTH_SETUPS[message.chat.id] = {
        "setup_id": setup_id,
        "stage": "await_phone",
        "session_name": load_runtime_settings()["rubika_session"],
    }
    lines = []
    if first_setup:
        lines.extend(
            [
                "⚠️ First setup: no Rubika account session exists yet.",
                "We need to create the Rubika session before uploads can work.",
                "",
            ]
        )

    lines.extend(
        [
            "1. Send the Rubika phone number you want to log in with.",
            "2. I will request the Rubika OTP.",
            "3. Send the OTP code here when it arrives.",
            "",
            "If Rubika asks for an account password first, I will ask for that before the OTP.",
            "The stored Rubika session is replaced only after successful login.",
        ]
    )
    await send_auth_temp_message(message, "\n".join(lines), auth_setup_keyboard())


async def cancel_auth_setup(message: Message) -> None:
    state = AUTH_SETUPS.get(message.chat.id)
    if not state:
        await send_settings_panel(message, note="⚪️ No Rubika setup is in progress.")
        return

    stop_auth_process(message.chat.id)
    await cleanup_auth_temp_messages(message.chat.id)
    clear_auth_setup(message.chat.id)
    await send_settings_panel(message, note="⚪️ Rubika number setup cancelled.")


async def start_rubika_auth_process(message: Message, phone_number: str) -> None:
    existing_state = AUTH_SETUPS.get(message.chat.id, {})
    setup_id = existing_state.get("setup_id") or uuid.uuid4().hex
    temp_message_ids = list(existing_state.get("temp_message_ids", []))
    normalized_phone = normalize_phone_number(phone_number)
    digits_only = normalized_phone[1:] if normalized_phone.startswith("+") else normalized_phone
    if not digits_only.isdigit() or len(digits_only) < 10:
        await cleanup_auth_input_message(message)
        await cleanup_auth_temp_messages(message.chat.id)
        await send_auth_temp_message(
            message,
            "⚠️ Please send a valid Rubika phone number.",
            auth_setup_keyboard(),
        )
        return

    session_name = load_runtime_settings()["rubika_session"]
    processing_task = load_processing()
    if processing_task_is_active(processing_task) and has_rubika_session(session_name):
        await cleanup_auth_input_message(message)
        await cleanup_auth_temp_messages(message.chat.id)
        await send_settings_panel(
            message,
            note="⚠️ Wait for the current upload to finish before changing the Rubika number.",
        )
        clear_auth_setup(message.chat.id)
        return

    stop_auth_process(message.chat.id)
    try:
        process = subprocess.Popen(
            [sys.executable, str(RUBIKA_AUTH_HELPER), session_name, normalized_phone],
            cwd=str(BASE_DIR),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError as error:
        await cleanup_auth_temp_messages(message.chat.id)
        clear_auth_setup(message.chat.id)
        await send_settings_panel(
            message,
            note=f"❌ Could not start Rubika login helper: {error}",
        )
        return

    AUTH_SETUPS[message.chat.id] = {
        "setup_id": setup_id,
        "stage": "waiting_for_otp",
        "session_name": session_name,
        "phone_number": normalized_phone,
        "process": process,
        "log_tail": [],
        "temp_message_ids": temp_message_ids,
    }

    asyncio.create_task(monitor_rubika_auth_process(message.chat.id, setup_id, process))
    await cleanup_auth_input_message(message)
    await cleanup_auth_temp_messages(message.chat.id)
    await send_auth_temp_message(
        message,
        "📨 Requesting Rubika OTP now...",
        auth_setup_keyboard(),
    )


async def monitor_rubika_auth_process(chat_id: int, setup_id: str, process) -> None:
    state = AUTH_SETUPS.get(chat_id)
    if not state or state.get("setup_id") != setup_id or state.get("process") is not process:
        return

    if not process or not process.stdout:
        current = AUTH_SETUPS.get(chat_id)
        if current and current.get("setup_id") == setup_id:
            await cleanup_auth_temp_messages(chat_id)
            clear_auth_setup(chat_id)
        await send_settings_panel_to_chat(
            chat_id,
            note="❌ Rubika setup could not start.",
        )
        return

    success = False
    cancelled = False
    error_text: str | None = None

    while True:
        line = await asyncio.to_thread(process.stdout.readline)
        if not line:
            if process.poll() is not None:
                break
            continue

        text = line.strip()
        if not text:
            continue

        if text.startswith("__AUTH_PASSKEY_PROMPT__:"):
            hint = text.split(":", 1)[1].strip()
            current = AUTH_SETUPS.get(chat_id)
            if (
                not current
                or current.get("setup_id") != setup_id
                or current.get("process") is not process
            ):
                return
            current["stage"] = "await_passkey"
            await cleanup_auth_temp_messages(chat_id)
            lines = [
                "🔑 Rubika requires the account password before it can send the OTP.",
            ]
            if hint:
                lines.append(f"Hint: {hint}")
            lines.extend(["", "Send the Rubika account password here."])
            await send_auth_temp_message_to_chat(
                chat_id,
                "\n".join(lines),
                auth_setup_keyboard(),
            )
            continue

        if text == "__AUTH_OTP_PROMPT__":
            current = AUTH_SETUPS.get(chat_id)
            if (
                not current
                or current.get("setup_id") != setup_id
                or current.get("process") is not process
            ):
                return
            current["stage"] = "await_otp"
            await cleanup_auth_temp_messages(chat_id)
            await send_auth_temp_message_to_chat(
                chat_id,
                "🔐 Rubika OTP request was sent. Send the verification code here.",
                auth_setup_keyboard(),
            )
            continue

        if text.startswith("__AUTH_PROMPT__:"):
            prompt_text = text.split(":", 1)[1].strip() or "Rubika requested verification input."
            current = AUTH_SETUPS.get(chat_id)
            if (
                not current
                or current.get("setup_id") != setup_id
                or current.get("process") is not process
            ):
                return
            current["stage"] = "await_extra_input"
            await cleanup_auth_temp_messages(chat_id)
            await send_auth_temp_message_to_chat(
                chat_id,
                "\n".join(
                    [
                        "🔐 Rubika is waiting for verification input.",
                        prompt_text,
                        "",
                        "Send the requested code here.",
                    ]
                ),
                auth_setup_keyboard(),
            )
            continue

        if text == "__AUTH_SUCCESS__":
            success = True
            break

        if text == "__AUTH_CANCELLED__":
            cancelled = True
            break

        if text.startswith("__AUTH_ERROR__:"):
            error_text = text.split(":", 1)[1].strip()
            break

        current = AUTH_SETUPS.get(chat_id)
        if (
            current is not None
            and current.get("setup_id") == setup_id
            and current.get("process") is process
        ):
            log_tail = current.setdefault("log_tail", [])
            log_tail.append(text)
            del log_tail[:-5]

    current = AUTH_SETUPS.get(chat_id)
    active_phone = current.get("phone_number") if current else None
    if current and current.get("setup_id") == setup_id and current.get("process") is process:
        await cleanup_auth_temp_messages(chat_id)
        clear_auth_setup(chat_id)
    else:
        return

    if success:
        if active_phone:
            settings = load_runtime_settings()
            settings["rubika_phone"] = active_phone
            save_runtime_settings(settings)
        await send_settings_panel_to_chat(
            chat_id,
            note="✅ Rubika number updated and the current session was replaced successfully.",
        )
        return

    if cancelled:
        await send_settings_panel_to_chat(
            chat_id,
            note="⚪️ Rubika number setup cancelled.",
        )
        return

    if not error_text:
        error_text = "Rubika setup failed."

    await send_settings_panel_to_chat(
        chat_id,
        note=f"❌ Rubika login failed: {error_text}",
    )


async def submit_rubika_auth_input(message: Message, value: str, next_text: str) -> None:
    state = AUTH_SETUPS.get(message.chat.id)
    process = state.get("process") if state else None
    if not state or not process or not process.stdin:
        return

    process.stdin.write(value.strip() + "\n")
    process.stdin.flush()
    state["stage"] = "waiting_for_helper"
    await cleanup_auth_input_message(message)
    await cleanup_auth_temp_messages(message.chat.id)
    await send_auth_temp_message(
        message,
        next_text,
        auth_setup_keyboard(),
    )


async def maybe_handle_auth_input(message: Message) -> bool:
    state = AUTH_SETUPS.get(message.chat.id)
    if not state:
        return False

    text = (message.text or "").strip()
    if not text or text.startswith("/") or text in MENU_BUTTONS:
        return False

    if state.get("stage") == "await_phone":
        await start_rubika_auth_process(message, text)
        return True

    if state.get("stage") == "await_passkey":
        await submit_rubika_auth_input(
            message,
            text,
            "⏳ Checking the Rubika password and requesting OTP...",
        )
        return True

    if state.get("stage") == "await_otp":
        await submit_rubika_auth_input(
            message,
            text,
            "⏳ Verifying the Rubika OTP and creating the session...",
        )
        return True

    if state.get("stage") == "await_extra_input":
        await submit_rubika_auth_input(
            message,
            text,
            "⏳ Sending Rubika verification input...",
        )
        return True

    return False


async def send_menu(message: Message) -> None:
    await message.reply_text(
        build_menu_text(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=main_action_keyboard(),
    )


def iter_download_files() -> list[Path]:
    if not DOWNLOAD_DIR.exists():
        return []
    return sorted(path for path in DOWNLOAD_DIR.iterdir() if path.is_file())


def sum_file_sizes(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def protected_download_paths() -> set[Path]:
    protected: set[Path] = set()

    for active in ACTIVE_DOWNLOADS.values():
        path = active.get("download_path")
        if path:
            protected.add(Path(path).resolve())

    for task in read_queue_tasks():
        path = task.get("path")
        if path:
            protected.add(Path(path).resolve())

    processing_task = load_processing()
    if (
        processing_task
        and processing_task_is_active(processing_task)
        and not cancel_requested(processing_task)
        and processing_task.get("path")
    ):
        protected.add(Path(processing_task["path"]).resolve())

    return protected


def cleanup_candidates() -> list[Path]:
    protected = protected_download_paths()
    candidates = []

    for path in iter_download_files():
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if resolved not in protected:
            candidates.append(path)

    return candidates


def stale_processing_task() -> dict | None:
    processing_task = load_processing()
    if not processing_task:
        return None
    if processing_task_is_active(processing_task) and not cancel_requested(processing_task):
        return None
    return processing_task


def dead_failed_entries() -> list[dict]:
    entries = []
    for entry in read_failed_entries():
        task = entry.get("task") or {}
        path = Path(task.get("path", ""))
        if not task.get("path") or not path.exists():
            entries.append(entry)
    return entries


def prune_dead_failed_entries() -> int:
    entries = read_failed_entries()
    kept = []
    removed = 0

    for entry in entries:
        task = entry.get("task") or {}
        path = Path(task.get("path", ""))
        if not task.get("path") or not path.exists():
            removed += 1
            continue
        kept.append(entry)

    if removed:
        write_failed_entries(kept)

    return removed


def compact_task_card(prefix: str, task: dict, status: str = "") -> str:
    task_id = task.get("task_id", "-")
    file_name = Path(task.get("file_name") or task.get("path") or "file").name
    stem, suffix = split_name(file_name)
    display_name = safe_filename(f"{stem[:30]}{suffix}", "file")
    size = human_size(int(task.get("file_size", 0) or 0))
    lines = [
        f"{prefix} <b>ID:</b> {ltr_code(task_id)}",
        f"📄 <b>File:</b> {ltr_code(display_name)}",
        f"📦 <b>Size:</b> {ltr_code(size)}",
    ]

    if status:
        lines.append(status)

    return "\n".join(lines)


def compact_button_label(prefix: str, task: dict) -> str:
    task_id = task.get("task_id", "-")
    file_name = Path(task.get("file_name") or task.get("path") or "file").name
    stem, suffix = split_name(file_name)
    display_name = safe_filename(f"{stem[:18]}{suffix}", "file")
    return f"{prefix} {display_name} - {task_id}"


def cancel_requested(task: dict | None) -> bool:
    if not task:
        return False

    task_id = task.get("task_id", "")
    return bool(task.get("cancelled")) or bool(task_id and is_cancelled(task_id))


def visible_active_downloads() -> list[dict]:
    return [task for task in ACTIVE_DOWNLOADS.values() if not cancel_requested(task)]


def visible_processing_task() -> dict | None:
    processing_task = load_processing()
    if not processing_task_is_active(processing_task) or cancel_requested(processing_task):
        return None
    return processing_task


def cancellable_tasks() -> list[tuple[str, dict]]:
    tasks: list[tuple[str, dict]] = []

    for active in visible_active_downloads():
        tasks.append(("⬇️", active))

    processing_task = visible_processing_task()
    if processing_task:
        tasks.append(("🚀", processing_task))

    for task in read_queue_tasks():
        tasks.append(("⏳", task))

    return tasks


def retryable_failed_tasks() -> list[dict]:
    tasks = []
    seen_task_ids: set[str] = set()

    for entry in reversed(read_failed_entries()):
        task = entry.get("task") or {}
        task_id = task.get("task_id")
        if not task_id or task_id in seen_task_ids:
            continue
        path = Path(task.get("path", ""))
        if path.exists():
            tasks.append(task)
            seen_task_ids.add(task_id)

    return tasks


def build_cancel_keyboard() -> InlineKeyboardMarkup | None:
    rows = []

    for prefix, task in cancellable_tasks()[:12]:
        task_id = task.get("task_id")
        if not task_id:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    compact_button_label(prefix, task),
                    callback_data=f"cancel:{task_id}",
                )
            ]
        )

    if not rows:
        return None

    return InlineKeyboardMarkup(rows)


def transfers_action_keyboard() -> InlineKeyboardMarkup:
    rows = []
    retryable_failed = retryable_failed_tasks()

    for _prefix, task in cancellable_tasks()[:8]:
        task_id = task.get("task_id")
        if not task_id:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    compact_button_label("🛑 Cancel", task),
                    callback_data=f"cancel:{task_id}",
                )
            ]
        )

    if retryable_failed:
        rows.append(
            [
                InlineKeyboardButton(
                    "🔁 Retry All Failed",
                    callback_data="retry_all",
                )
            ]
        )

    for task in retryable_failed[:8]:
        task_id = task.get("task_id")
        if not task_id:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    compact_button_label("🔁 Retry", task),
                    callback_data=f"retry:{task_id}",
                )
            ]
        )

    rows.extend(
        [
            [
                InlineKeyboardButton("📊 Status", callback_data="menu:status"),
                InlineKeyboardButton("🧹 Cleanup", callback_data="menu:cleanup"),
            ],
            [InlineKeyboardButton("🛑 Cancel List", callback_data="menu:cancel")],
        ]
    )

    return InlineKeyboardMarkup(rows)


def status_action_keyboard(task_id: str, action: str = "cancel") -> InlineKeyboardMarkup:
    if action == "retry":
        button = InlineKeyboardButton(tr(current_language(), "btn_retry"), callback_data=f"retry:{task_id}")
    else:
        button = InlineKeyboardButton(tr(current_language(), "btn_cancel"), callback_data=f"cancel:{task_id}")

    return InlineKeyboardMarkup([[button]])


async def send_cancel_picker(message: Message) -> None:
    keyboard = build_cancel_keyboard()
    lang = current_language()
    if not keyboard:
        await message.reply_text(
            "🛑 انتقال فعالی برای لغو وجود ندارد." if lang == "fa" else "🛑 There are no active transfers to cancel.",
            reply_markup=menu_keyboard(),
        )
        return

    title = "🛑 لغو انتقال" if lang == "fa" else "🛑 Cancel Transfer"
    body = "یک انتقال را انتخاب کن:" if lang == "fa" else "Choose one transfer:"
    await message.reply_text(
        "\n".join(
            [
                f"<b>{title}</b>",
                "",
                body,
            ]
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=keyboard,
    )


async def send_status_summary(message: Message) -> None:
    has_cleanup = bool(cleanup_candidates() or stale_processing_task() or dead_failed_entries())
    await message.reply_text(
        build_status_summary(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_summary_keyboard(has_cleanup),
    )


async def send_transfers_summary(message: Message) -> None:
    await message.reply_text(
        build_transfers_summary(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=transfers_action_keyboard(),
    )


async def send_cleanup_preview(message: Message) -> None:
    candidates = cleanup_candidates()
    has_cleanup = bool(candidates or stale_processing_task() or dead_failed_entries())
    await message.reply_text(
        build_cleanup_preview(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=cleanup_keyboard(has_cleanup),
    )


async def run_cleanup(message: Message) -> None:
    candidates = cleanup_candidates()
    total_size = sum_file_sizes(candidates)
    stale_task = stale_processing_task()
    removed_count = 0

    for path in candidates:
        try:
            path.unlink()
            removed_count += 1
        except OSError:
            pass

    cleared_stale_state = False
    if stale_task:
        clear_processing()
        cleared_stale_state = True

    pruned_failed_count = prune_dead_failed_entries()
    lang = current_language()
    if lang == "fa":
        lines = [
            "<b>🧹 پاک‌سازی انجام شد</b>",
            "",
            f"فایل‌های حذف‌شده: <b>{removed_count}</b>",
            f"فضای آزادشده: <b>{human_size(total_size)}</b>",
            f"وضعیت آپلود گیرکرده پاک شد: <b>{1 if cleared_stale_state else 0}</b>",
            f"رکوردهای ناموفق مرده حذف شد: <b>{pruned_failed_count}</b>",
        ]
    else:
        lines = [
            "<b>🧹 Cleanup Complete</b>",
            "",
            f"Removed files: <b>{removed_count}</b>",
            f"Freed space: <b>{human_size(total_size)}</b>",
            f"Cleared stale upload state: <b>{1 if cleared_stale_state else 0}</b>",
            f"Pruned dead failed records: <b>{pruned_failed_count}</b>",
        ]

    await message.reply_text(
        "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=main_action_keyboard(),
    )


def build_status_summary() -> str:
    queued = read_queue_tasks()
    active_downloads = visible_active_downloads()
    processing = visible_processing_task()
    failed_entries = read_failed_entries()
    files = iter_download_files()
    candidates = cleanup_candidates()
    settings = load_runtime_settings()
    lang = settings["language"]
    if lang == "fa":
        lines = [
            "<b>📊 وضعیت WalrusHF</b>",
            "",
            f"📱 <b>نشست روبیکا:</b> {ltr_code(settings['rubika_session'])}",
            f"📬 <b>مقصد:</b> {ltr_code(format_destination_label(settings))}",
            f"🌐 <b>زبان:</b> {ltr_code(tr(lang, 'language_name'))}",
            f"🍪 <b>کوکی یوتیوب:</b> {ltr_code('ذخیره شده' if youtube_cookies_exist() else 'ندارد')}",
            "",
            f"⬇️ <b>دانلودهای فعال:</b> {ltr_code(str(len(active_downloads)))}",
            f"🚀 <b>آپلودهای فعال:</b> {ltr_code(str(1 if processing else 0))}",
            f"⏳ <b>در صف:</b> {ltr_code(str(len(queued)))}",
            f"❌ <b>ناموفق:</b> {ltr_code(str(len(failed_entries)))}",
            f"📁 <b>فایل‌های دانلودشده:</b> {ltr_code(f'{len(files)} / {human_size(sum_file_sizes(files))}')}",
            f"🧹 <b>قابل پاک‌سازی:</b> {ltr_code(f'{len(candidates)} / {human_size(sum_file_sizes(candidates))}')}",
        ]
        return "\n".join(lines)

    lines = [
        "<b>📊 WalrusHF Status</b>",
        "",
        f"📱 <b>Rubika Session:</b> {ltr_code(settings['rubika_session'])}",
        f"📬 <b>Destination:</b> {ltr_code(format_destination_label(settings))}",
        "",
        f"⬇️ <b>Active Downloads:</b> {ltr_code(str(len(active_downloads)))}",
        f"🚀 <b>Active Uploads:</b> {ltr_code(str(1 if processing else 0))}",
        f"⏳ <b>Queued:</b> {ltr_code(str(len(queued)))}",
        f"❌ <b>Failed:</b> {ltr_code(str(len(failed_entries)))}",
        f"📁 <b>Downloaded Files:</b> {ltr_code(f'{len(files)} / {human_size(sum_file_sizes(files))}')}",
        f"🧹 <b>Cleanup Available:</b> {ltr_code(f'{len(candidates)} / {human_size(sum_file_sizes(candidates))}')}",
    ]

    return "\n".join(lines)


def build_transfers_summary() -> str:
    queued = read_queue_tasks()
    active_downloads = visible_active_downloads()
    processing = visible_processing_task()
    failed_entries = read_failed_entries()
    lang = current_language()
    lines = ["<b>📋 انتقال‌ها</b>" if lang == "fa" else "<b>📋 Transfers</b>", ""]

    if active_downloads:
        lines.append("<b>⬇️ در حال دانلود</b>" if lang == "fa" else "<b>⬇️ Downloading</b>")
        for active in active_downloads[:5]:
            download_percent = active.get("download_percent", 0)
            label = "دانلود" if lang == "fa" else "Download"
            status = f"⬇️ <b>{label}:</b> {ltr_code(f'{download_percent}%')}"
            lines.append(compact_task_card("•", active, status))
            lines.append("")
        lines.append("")

    if processing:
        lines.append("<b>🚀 در حال آپلود</b>" if lang == "fa" else "<b>🚀 Uploading</b>")
        upload_percent = processing.get("upload_percent", 0)
        label = "آپلود" if lang == "fa" else "Upload"
        status = f"⬆️ <b>{label}:</b> {ltr_code(f'{upload_percent}%')}"
        if processing.get("attempt_text"):
            attempt = "تلاش" if lang == "fa" else "Attempt"
            status += f"\n🔁 <b>{attempt}:</b> {ltr_code(processing['attempt_text'])}"
        lines.append(compact_task_card("•", processing, status))
        lines.append("")

    if queued:
        lines.append("<b>⏳ صف آپلود</b>" if lang == "fa" else "<b>⏳ Upload Queue</b>")
        for index, task in enumerate(queued[:8], start=1):
            lines.append(compact_task_card(f"{index}.", task))
            lines.append("")
        if len(queued) > 8:
            lines.append(f"... and {len(queued) - 8} more")
        lines.append("")

    retryable_failed = retryable_failed_tasks()

    if retryable_failed:
        lines.append("<b>❌ انتقال‌های ناموفق قابل تلاش دوباره</b>" if lang == "fa" else "<b>❌ Retryable Failed Transfers</b>")
        for task in retryable_failed[:5]:
            note = "دکمه تلاش دوباره را بزن." if lang == "fa" else "Tap a Retry button below."
            lines.append(compact_task_card("•", task, note))
            lines.append("")
        if len(retryable_failed) > 5:
            lines.append(f"... and {len(retryable_failed) - 5} more")
        lines.append("")

    if len(lines) == 2:
        lines.append("الان انتقال فعالی وجود ندارد." if lang == "fa" else "No active transfers right now.")

    return "\n".join(lines)


def build_cleanup_preview() -> str:
    candidates = cleanup_candidates()
    total_size = sum_file_sizes(candidates)
    stale_task = stale_processing_task()
    dead_failed_count = len(dead_failed_entries())
    lang = current_language()
    if lang == "fa":
        lines = [
            "<b>🧹 پاک‌سازی</b>",
            "",
            f"🗑 <b>فایل‌های قابل حذف:</b> {ltr_code(str(len(candidates)))}",
            f"💾 <b>فضای آزادشونده:</b> {ltr_code(human_size(total_size))}",
            f"🚀 <b>وضعیت آپلود گیرکرده:</b> {ltr_code('1' if stale_task else '0')}",
            f"❌ <b>رکوردهای ناموفق مرده:</b> {ltr_code(str(dead_failed_count))}",
        ]
        if candidates or stale_task or dead_failed_count:
            lines.extend(["", "فقط مواردی پاک می‌شوند که فعال، در صف، یا قابل تلاش دوباره نیستند."])
        else:
            lines.append("چیزی برای پاک‌سازی نیست.")
        return "\n".join(lines)

    lines = [
        "<b>🧹 Cleanup</b>",
        "",
        f"🗑 <b>Files to remove:</b> {ltr_code(str(len(candidates)))}",
        f"💾 <b>Space to free:</b> {ltr_code(human_size(total_size))}",
        f"🚀 <b>Stale upload state:</b> {ltr_code('1' if stale_task else '0')}",
        f"❌ <b>Dead failed records:</b> {ltr_code(str(dead_failed_count))}",
    ]

    if candidates or stale_task or dead_failed_count:
        lines.extend(
            [
                "",
                "This only removes files and records that are not active, queued, or retryable.",
            ]
        )
    else:
        lines.append("Nothing to clean up.")

    return "\n".join(lines)


def get_media(message: Message):
    media_types = [
        ("video", message.video),
        ("document", message.document),
        ("audio", message.audio),
        ("voice", message.voice),
        ("photo", message.photo),
        ("animation", message.animation),
        ("video_note", message.video_note),
        ("sticker", message.sticker),
    ]

    for media_type, media in media_types:
        if media:
            return media_type, media

    return None, None


def extract_direct_urls(text: str | None) -> list[str]:
    if not text:
        return []

    matches = URL_PATTERN.finditer(text.strip())
    urls: list[str] = []
    seen: set[str] = set()

    for match in matches:
        url = match.group("url").rstrip('.,!?)"]}>\'')
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)

    return urls


def path_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    return Path(unquote(parsed.path or "")).name


def summarize_batch_item(result: dict) -> str:
    lang = current_language()
    icon_map = {
        "queued": "✅",
        "cancelled": "🛑",
        "failed": "❌",
    }
    status_map = (
        {"queued": "در صف", "cancelled": "لغو شد", "failed": "ناموفق"}
        if lang == "fa"
        else {"queued": "Queued", "cancelled": "Cancelled", "failed": "Failed"}
    )
    icon = icon_map.get(result.get("status"), "•")
    status = status_map.get(result.get("status"), "Updated")
    file_name = safe_filename(result.get("file_name"), "file.bin")
    task_id = result.get("task_id", "-")
    return f"{icon} {ltr_code(file_name)} {ltr_code(task_id)} {status}"


def build_batch_summary_text(results: list[dict]) -> str:
    lang = current_language()
    queued = sum(1 for result in results if result.get("status") == "queued")
    cancelled = sum(1 for result in results if result.get("status") == "cancelled")
    failed = sum(1 for result in results if result.get("status") == "failed")

    if lang == "fa":
        lines = [
            "<b>📦 پردازش گروهی تمام شد</b>",
            "",
            f"🔗 <b>لینک‌ها:</b> {ltr_code(str(len(results)))}",
            f"✅ <b>در صف:</b> {ltr_code(str(queued))}",
            f"🛑 <b>لغوشده:</b> {ltr_code(str(cancelled))}",
            f"❌ <b>ناموفق:</b> {ltr_code(str(failed))}",
        ]
    else:
        lines = [
            "<b>📦 Batch Finished</b>",
            "",
            f"🔗 <b>Links:</b> {ltr_code(str(len(results)))}",
            f"✅ <b>Queued:</b> {ltr_code(str(queued))}",
            f"🛑 <b>Cancelled:</b> {ltr_code(str(cancelled))}",
            f"❌ <b>Failed:</b> {ltr_code(str(failed))}",
        ]

    if results:
        lines.extend(["", "<b>موارد</b>" if lang == "fa" else "<b>Items</b>"])
        for result in results[:8]:
            lines.append(summarize_batch_item(result))
        if len(results) > 8:
            lines.append(f"... and {len(results) - 8} more")

    return "\n".join(lines)


def is_direct_file_filename(name: str) -> bool:
    return Path(name).suffix.lower() in DIRECT_FILE_EXTENSIONS


def is_supported_file_content_type(content_type: str) -> bool:
    media_type = content_type.split(";", 1)[0].strip().lower()
    return (
        media_type.startswith(("video/", "audio/", "image/"))
        or media_type in DIRECT_FILE_CONTENT_TYPES
    )


def youtube_cookies_exist() -> bool:
    return YOUTUBE_COOKIES_FILE.exists() and YOUTUBE_COOKIES_FILE.stat().st_size > 0


def valid_youtube_cookie_text(text: str) -> bool:
    normalized = text.replace("\r\n", "\n").strip()
    if not normalized:
        return False
    if "# Netscape HTTP Cookie File" in normalized:
        return True
    return ".youtube.com" in normalized or "\tyoutube.com\t" in normalized


def youtube_cookie_help_text(language: str | None = None) -> str:
    lang = normalize_language(language or current_language())
    if lang == "fa":
        return "\n".join(
            [
                "<b>🍪 کوکی یوتیوب</b>",
                "",
                "برای ویدیوهایی که یوتیوب پیام ورود یا bot-check می‌دهد، فایل cookies.txt لازم است.",
                "",
                "روش استفاده:",
                "1. با افزونه‌هایی مثل Get cookies.txt از مرورگری که داخل YouTube لاگین است، cookies.txt بگیر.",
                "2. فایل را در تلگرام برای ربات بفرست.",
                "3. روی همان فایل ریپلای کن و بنویس /youtube_cookies",
                "",
                "برای حذف کوکی‌ها: /clear_youtube_cookies",
            ]
        )
    return "\n".join(
        [
            "<b>🍪 YouTube Cookies</b>",
            "",
            "Some YouTube videos require a cookies.txt file because YouTube asks for sign-in or bot checks.",
            "",
            "How to use:",
            "1. Export cookies.txt from a browser signed in to YouTube.",
            "2. Send the file to this bot.",
            "3. Reply to that file with /youtube_cookies.",
            "",
            "Remove cookies with /clear_youtube_cookies.",
        ]
    )


async def save_youtube_cookies_from_message(message: Message) -> tuple[bool, str]:
    command_text = message.text or ""
    inline_text = command_text.split(maxsplit=1)[1] if len(command_text.split(maxsplit=1)) > 1 else ""
    source_text = inline_text

    if not source_text and message.reply_to_message and message.reply_to_message.text:
        source_text = message.reply_to_message.text

    if source_text:
        if not valid_youtube_cookie_text(source_text):
            return False, "متن کوکی معتبر نیست." if current_language() == "fa" else "The cookie text does not look valid."
        YOUTUBE_COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
        YOUTUBE_COOKIES_FILE.write_text(source_text.strip() + "\n", encoding="utf-8")
        return True, ""

    replied = message.reply_to_message
    document = replied.document if replied else None
    if not document:
        return False, ""

    file_size = int(getattr(document, "file_size", 0) or 0)
    if file_size > 2 * 1024 * 1024:
        return False, "فایل کوکی خیلی بزرگ است." if current_language() == "fa" else "The cookie file is too large."

    temp_path = YOUTUBE_COOKIES_FILE.with_suffix(".tmp")
    downloaded = await app.download_media(replied, file_name=str(temp_path))
    if not downloaded:
        return False, "دانلود فایل کوکی ناموفق بود." if current_language() == "fa" else "Could not download the cookie file."

    text = Path(downloaded).read_text(encoding="utf-8", errors="replace")
    if not valid_youtube_cookie_text(text):
        Path(downloaded).unlink(missing_ok=True)
        return False, "فایل کوکی معتبر نیست." if current_language() == "fa" else "The cookie file does not look valid."

    YOUTUBE_COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    Path(downloaded).replace(YOUTUBE_COOKIES_FILE)
    return True, ""


def max_file_size_text() -> str:
    return human_size(MAX_FILE_BYTES) if MAX_FILE_BYTES > 0 else "unlimited"


def ensure_file_size_allowed(file_size: int, context: str = "file") -> None:
    if MAX_FILE_BYTES > 0 and file_size > MAX_FILE_BYTES:
        raise RuntimeError(
            f"The {context} is too large for this Space limit "
            f"({human_size(file_size)} > {max_file_size_text()})."
        )


def ensure_download_space(expected_size: int = 0) -> None:
    try:
        usage = shutil.disk_usage(DOWNLOAD_DIR)
    except OSError:
        return

    needed = max(0, expected_size) + max(0, MIN_FREE_BYTES)
    if needed > 0 and usage.free < needed:
        raise RuntimeError(
            "Not enough Space disk available for this transfer "
            f"({human_size(usage.free)} free, need about {human_size(needed)})."
        )


def measure_http_latency(url: str, timeout: float = 10.0) -> dict:
    started = time.monotonic()
    try:
        response = requests.get(url, timeout=(5, timeout), stream=True)
        try:
            for _chunk in response.iter_content(chunk_size=1):
                break
        finally:
            response.close()
        elapsed_ms = int((time.monotonic() - started) * 1000)
        return {
            "ok": response.status_code < 500,
            "status": response.status_code,
            "latency_ms": elapsed_ms,
            "error": "",
        }
    except Exception as error:
        return {
            "ok": False,
            "status": 0,
            "latency_ms": 0,
            "error": str(error)[:140],
        }


def measure_download_speed(url: str, timeout: float = 20.0) -> dict:
    started = time.monotonic()
    downloaded = 0
    try:
        with requests.get(url, timeout=(5, timeout), stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if not chunk:
                    continue
                downloaded += len(chunk)
                if downloaded >= 1024 * 1024:
                    break
        elapsed = max(0.001, time.monotonic() - started)
        mbps = (downloaded * 8) / elapsed / 1_000_000
        return {
            "ok": downloaded > 0,
            "bytes": downloaded,
            "elapsed": elapsed,
            "mbps": mbps,
            "error": "",
        }
    except Exception as error:
        return {
            "ok": False,
            "bytes": downloaded,
            "elapsed": max(0.001, time.monotonic() - started),
            "mbps": 0.0,
            "error": str(error)[:140],
        }


def network_score(download: dict, rubika: dict, youtube: dict) -> tuple[int, str]:
    score = 0
    mbps = float(download.get("mbps") or 0)
    rubika_latency = int(rubika.get("latency_ms") or 9999)
    youtube_latency = int(youtube.get("latency_ms") or 9999)

    if download.get("ok"):
        score += 35 if mbps >= 20 else 28 if mbps >= 10 else 20 if mbps >= 3 else 10
    if rubika.get("ok"):
        score += 30 if rubika_latency <= 250 else 22 if rubika_latency <= 600 else 12
    if youtube.get("ok"):
        score += 25 if youtube_latency <= 350 else 18 if youtube_latency <= 800 else 10
    score += 10
    score = max(0, min(100, score))

    grade = "عالی" if score >= 85 else "خوب" if score >= 70 else "متوسط" if score >= 50 else "ضعیف"
    return score, grade


def build_speedtest_text(download: dict, rubika: dict, youtube: dict) -> str:
    lang = current_language()
    score, grade_fa = network_score(download, rubika, youtube)
    grade_en = "Excellent" if score >= 85 else "Good" if score >= 70 else "Fair" if score >= 50 else "Poor"
    download_speed = f"{float(download.get('mbps') or 0):.2f} Mbps"
    rubika_ping = f"{rubika.get('latency_ms') or '-'} ms"
    youtube_ping = f"{youtube.get('latency_ms') or '-'} ms"

    if lang == "fa":
        lines = [
            "<b>🚦 تست سرعت WalrusHF</b>",
            "",
            f"📥 <b>دانلود سرور:</b> {ltr_code(download_speed)}",
            f"🔷 <b>پینگ روبیکا:</b> {ltr_code(rubika_ping)}",
            f"▶️ <b>پینگ یوتیوب:</b> {ltr_code(youtube_ping)}",
            "",
            f"🏁 <b>امتیاز شبکه:</b> {ltr_code(f'{score}/100')} - {grade_fa}",
        ]
        if not download.get("ok"):
            lines.append(f"⚠️ خطای تست دانلود: {download.get('error')}")
        if not rubika.get("ok"):
            lines.append(f"⚠️ خطای روبیکا: {rubika.get('error')}")
        if not youtube.get("ok"):
            lines.append(f"⚠️ خطای یوتیوب: {youtube.get('error')}")
        lines.append("")
        lines.append("این تست وضعیت شبکه Space را می‌سنجد؛ سرعت نهایی آپلود روبیکا ممکن است با محدودیت خود روبیکا فرق کند.")
        return "\n".join(lines)

    lines = [
        "<b>🚦 WalrusHF Speed Test</b>",
        "",
        f"📥 <b>Server download:</b> {ltr_code(download_speed)}",
        f"🔷 <b>Rubika ping:</b> {ltr_code(rubika_ping)}",
        f"▶️ <b>YouTube ping:</b> {ltr_code(youtube_ping)}",
        "",
        f"🏁 <b>Network score:</b> {ltr_code(f'{score}/100')} - {grade_en}",
    ]
    if not download.get("ok"):
        lines.append(f"⚠️ Download test error: {download.get('error')}")
    if not rubika.get("ok"):
        lines.append(f"⚠️ Rubika error: {rubika.get('error')}")
    if not youtube.get("ok"):
        lines.append(f"⚠️ YouTube error: {youtube.get('error')}")
    lines.append("")
    lines.append("This measures the Space network. Final Rubika upload speed can still be limited by Rubika itself.")
    return "\n".join(lines)


async def run_speedtest() -> str:
    download, rubika, youtube = await asyncio.gather(
        asyncio.to_thread(measure_download_speed, SPEEDTEST_DOWNLOAD_URL),
        asyncio.to_thread(measure_http_latency, SPEEDTEST_RUBIKA_URL),
        asyncio.to_thread(measure_http_latency, SPEEDTEST_YOUTUBE_URL),
    )
    return build_speedtest_text(download, rubika, youtube)


def build_url_download_filename(url: str, task_id: str, fallback_suffix: str = ".bin") -> str:
    original_name = normalize_upload_filename(path_name_from_url(url), f"file{fallback_suffix}")
    stem, suffix = split_name(original_name or "file")

    if suffix.lower() not in DIRECT_FILE_EXTENSIONS:
        suffix = fallback_suffix if fallback_suffix in DIRECT_FILE_EXTENSIONS else ".bin"

    unique_name = f"{(stem or 'file')[:120]}_{task_id}{suffix}"
    return safe_filename(unique_name, f"file_{task_id}{suffix}")


class DirectDownloadCancelled(RuntimeError):
    pass


def is_transient_download_error(error_text: str) -> bool:
    return any(
        key in error_text
        for key in [
            "timeout",
            "timed out",
            "connection reset",
            "remote disconnected",
            "temporarily unavailable",
            "incomplete read",
            "chunkedencodingerror",
            "connection aborted",
            "502",
            "503",
            "504",
        ]
    )


def wait_for_direct_retry(seconds: int, should_cancel) -> None:
    for _ in range(seconds):
        if should_cancel():
            raise DirectDownloadCancelled("Cancelled by user.")
        time.sleep(1)


def response_total_size(response: requests.Response, downloaded: int) -> int:
    content_range = response.headers.get("content-range", "").strip()
    if content_range and "/" in content_range:
        total_text = content_range.rsplit("/", 1)[-1].strip()
        if total_text.isdigit():
            return int(total_text)

    content_length = int(response.headers.get("content-length") or 0)
    if response.status_code == 206 and content_length > 0:
        return downloaded + content_length
    return content_length


def build_download_filename(message: Message, media_type: str, media) -> str:
    original_name = getattr(media, "file_name", None)
    default_extensions = {
        "video": ".mp4",
        "audio": ".mp3",
        "voice": ".ogg",
        "photo": ".jpg",
        "animation": ".mp4",
        "video_note": ".mp4",
        "sticker": ".webp",
        "document": ".bin",
    }
    default_extension = default_extensions.get(media_type, ".bin")

    if not original_name:
        file_unique_id = getattr(media, "file_unique_id", None) or "file"
        original_name = f"{file_unique_id}{default_extension}"

    original_name = normalize_upload_filename(
        original_name,
        f"file{default_extension}",
    )
    stem, suffix = split_name(original_name)

    unique_name = f"{stem}_{message.id}{suffix or '.bin'}"
    return safe_filename(unique_name)


async def safe_edit_status(
    status_message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await status_message.edit_text(
            text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except Exception:
        pass


async def edit_status_by_task(
    client: Client,
    task: dict,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await client.edit_message_text(
            chat_id=task["chat_id"],
            message_id=task["status_message_id"],
            text=text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except Exception:
        pass


def inline_keyboard_from_payload(markup: dict | None) -> InlineKeyboardMarkup | None:
    if not markup:
        return None

    rows = []
    for row in markup.get("inline_keyboard", []):
        buttons = []
        for button in row:
            text = str(button.get("text") or "")
            callback_data = button.get("callback_data")
            if text and callback_data:
                buttons.append(
                    InlineKeyboardButton(text, callback_data=str(callback_data))
                )
        if buttons:
            rows.append(buttons)

    return InlineKeyboardMarkup(rows) if rows else None


async def handle_worker_telegram_event(event: dict) -> None:
    event_type = event.get("type")
    payload = event.get("payload") or {}

    try:
        if event_type == "edit_message_text":
            await app.edit_message_text(
                chat_id=payload["chat_id"],
                message_id=payload["message_id"],
                text=payload.get("text", ""),
                parse_mode=enums.ParseMode.HTML,
                reply_markup=inline_keyboard_from_payload(payload.get("reply_markup")),
            )
        elif event_type == "send_message":
            await app.send_message(
                chat_id=payload["chat_id"],
                text=payload.get("text", ""),
                parse_mode=enums.ParseMode.HTML,
                reply_to_message_id=payload.get("reply_to_message_id"),
            )
    except Exception as error:
        print(
            "Telegram event bridge failed "
            f"type={event_type} task={event.get('task_id', '-')} error={error}",
            flush=True,
        )


async def worker_telegram_event_loop() -> None:
    while True:
        for event in pop_telegram_events():
            await handle_worker_telegram_event(event)
        await asyncio.sleep(1)


async def cancel_task_by_id(client: Client, message: Message, task_id: str) -> None:
    active = ACTIVE_DOWNLOADS.get(task_id)
    if active:
        active["cancelled"] = True
        text = build_status_text(
            task_id=task_id,
            file_name=active["file_name"],
            file_size=active["file_size"],
            stage="🛑 Cancelling",
            download_percent=active.get("download_percent", 0),
            upload_percent=active.get("upload_percent", 0),
            upload_status="Stopping the transfer.",
        )
        await edit_status_by_task(client, active, text)
        await message.reply_text(f"🛑 Cancel requested: {task_id}", reply_markup=menu_keyboard())
        return

    queued_task = remove_queued_task(task_id)
    if queued_task:
        cleanup_download_artifact(queued_task.get("path", ""))
        text = build_status_text(
            task_id=task_id,
            file_name=queued_task.get("file_name", Path(queued_task.get("path", "")).name or "file"),
            file_size=int(queued_task.get("file_size", 0)),
            stage="🛑 Cancelled",
            download_percent=100,
            upload_percent=0,
            upload_status="Removed from the queue.",
        )
        await edit_status_by_task(client, queued_task, text)
        await message.reply_text(f"🗑 Removed from queue: {task_id}", reply_markup=menu_keyboard())
        return

    processing_task = load_processing()
    if processing_task and processing_task.get("task_id") == task_id:
        mark_cancelled(task_id)
        worker_stopped = stop_rubika_worker()
        text = build_status_text(
            task_id=task_id,
            file_name=processing_task.get("file_name", Path(processing_task.get("path", "")).name or "file"),
            file_size=int(processing_task.get("file_size", 0)),
            stage="🛑 Cancelling",
            download_percent=100,
            upload_percent=int(processing_task.get("upload_percent", 0)),
            upload_status=(
                "Stopping the upload worker."
                if worker_stopped
                else "Stopping at the next safe checkpoint."
            ),
            attempt_text=processing_task.get("attempt_text"),
        )
        await edit_status_by_task(client, processing_task, text)
        await message.reply_text(f"🛑 Cancel requested: {task_id}", reply_markup=menu_keyboard())
        return

    if is_cancelled(task_id):
        await message.reply_text(f"🛑 Already cancelled: {task_id}", reply_markup=menu_keyboard())
        return

    await message.reply_text(f"🔎 Task not found: {task_id}", reply_markup=menu_keyboard())


def resolve_task_from_reply(status_message_id: int | None) -> tuple[str | None, dict | None]:
    if status_message_id is None:
        return None, None

    for task_id, payload in ACTIVE_DOWNLOADS.items():
        if payload["status_message_id"] == status_message_id:
            return task_id, payload

    queued_task = find_queued_task(
        lambda task: task.get("status_message_id") == status_message_id
    )
    if queued_task:
        return queued_task.get("task_id"), queued_task

    processing_task = load_processing()
    if processing_task and processing_task.get("status_message_id") == status_message_id:
        return processing_task.get("task_id"), processing_task

    return None, None


def cleanup_download_artifact(path_like: str) -> None:
    try:
        cleanup_local_file(path_like)
    except Exception:
        pass


def stop_rubika_worker() -> bool:
    pid = load_worker_pid()
    if not pid:
        return False

    try:
        os.kill(pid, signal.SIGTERM)
        return True
    except OSError:
        return False


def make_download_progress_callback(task_id: str, status_message: Message, task_meta: dict):
    loop = asyncio.get_running_loop()
    state = {
        "last_percent": -1,
        "last_update": 0.0,
        "last_bytes": 0,
        "last_sample_at": time.monotonic(),
        "speed_bps": 0.0,
    }

    def progress(current: int, total: int, client: Client, *_args) -> None:
        active = ACTIVE_DOWNLOADS.get(task_id)
        if active and active.get("cancelled"):
            client.stop_transmission()
            return

        if total <= 0:
            return

        percent = int((current * 100) / total)
        percent = min(100, max(0, percent))
        now = time.monotonic()

        delta_bytes = max(0, current - state["last_bytes"])
        delta_time = max(0.0, now - state["last_sample_at"])
        if delta_bytes > 0 and delta_time > 0:
            instant_speed = delta_bytes / delta_time
            state["speed_bps"] = (
                instant_speed
                if state["speed_bps"] <= 0
                else (state["speed_bps"] * 0.65) + (instant_speed * 0.35)
            )
            state["last_bytes"] = current
            state["last_sample_at"] = now

        speed_text = human_speed(state["speed_bps"]) if state["speed_bps"] > 0 else None
        eta_text = None
        remaining = max(0, total - current)
        if remaining > 0 and state["speed_bps"] > 0:
            eta_text = human_duration(remaining / state["speed_bps"])

        should_emit = (
            percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 10
            or now - state["last_update"] >= 2
        )

        if not should_emit:
            return

        state["last_percent"] = percent
        state["last_update"] = now
        if active is not None:
            active["download_percent"] = percent

        text = build_status_text(
            task_id=task_id,
            file_name=task_meta["file_name"],
            file_size=task_meta["file_size"],
            stage="⬇️ Downloading",
            download_percent=percent,
            upload_percent=0,
            upload_status="The file will enter the upload queue after download.",
            speed_text=speed_text,
            eta_text=eta_text,
            language=task_meta.get("language"),
        )
        loop.create_task(
            safe_edit_status(
                status_message,
                text,
                reply_markup=status_action_keyboard(task_id, "cancel"),
            )
        )

    return progress


def make_direct_download_progress_callback(task_id: str, status_message: Message, task_meta: dict):
    loop = asyncio.get_running_loop()
    state = {
        "last_percent": -1,
        "last_update": 0.0,
        "last_bytes": 0,
        "last_sample_at": time.monotonic(),
        "speed_bps": 0.0,
    }

    def progress(current: int, total: int) -> None:
        active = ACTIVE_DOWNLOADS.get(task_id)
        if active and active.get("cancelled"):
            raise DirectDownloadCancelled("Cancelled by user.")

        if total > 0:
            task_meta["file_size"] = total
            if active is not None:
                active["file_size"] = total
            percent = min(100, max(0, int((current * 100) / total)))
        else:
            percent = 0

        now = time.monotonic()
        delta_bytes = max(0, current - state["last_bytes"])
        delta_time = max(0.0, now - state["last_sample_at"])
        if delta_bytes > 0 and delta_time > 0:
            instant_speed = delta_bytes / delta_time
            state["speed_bps"] = (
                instant_speed
                if state["speed_bps"] <= 0
                else (state["speed_bps"] * 0.65) + (instant_speed * 0.35)
            )
            state["last_bytes"] = current
            state["last_sample_at"] = now

        speed_text = human_speed(state["speed_bps"]) if state["speed_bps"] > 0 else None
        eta_text = None
        if total > 0:
            remaining = max(0, total - current)
            if remaining > 0 and state["speed_bps"] > 0:
                eta_text = human_duration(remaining / state["speed_bps"])

        should_emit = (
            percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 10
            or now - state["last_update"] >= 2
        )

        if not should_emit:
            return

        state["last_percent"] = percent
        state["last_update"] = now
        if active is not None:
            active["download_percent"] = percent

        text = build_status_text(
            task_id=task_id,
            file_name=task_meta["file_name"],
            file_size=task_meta["file_size"],
            stage="⬇️ Downloading",
            download_percent=percent,
            upload_percent=0,
            upload_status="Downloading the file from the link.",
            speed_text=speed_text,
            eta_text=eta_text,
            language=task_meta.get("language"),
        )
        loop.call_soon_threadsafe(
            lambda: loop.create_task(
                safe_edit_status(
                    status_message,
                    text,
                    reply_markup=status_action_keyboard(task_id, "cancel"),
                )
            )
        )

    return progress


def make_youtube_download_progress_callback(task_id: str, status_message: Message, task_meta: dict):
    loop = asyncio.get_running_loop()
    state = {
        "last_percent": -1,
        "last_update": 0.0,
        "last_bytes": 0,
        "last_sample_at": time.monotonic(),
        "speed_bps": 0.0,
    }

    def progress(current: int, total: int) -> None:
        active = ACTIVE_DOWNLOADS.get(task_id)
        if active and active.get("cancelled"):
            raise YouTubeDownloadCancelled("Cancelled by user.")

        if total > 0:
            task_meta["file_size"] = total
            if active is not None:
                active["file_size"] = total
            percent = min(100, max(0, int((current * 100) / total)))
        else:
            percent = 0

        now = time.monotonic()
        delta_bytes = max(0, current - state["last_bytes"])
        delta_time = max(0.0, now - state["last_sample_at"])
        if delta_bytes > 0 and delta_time > 0:
            instant_speed = delta_bytes / delta_time
            state["speed_bps"] = (
                instant_speed
                if state["speed_bps"] <= 0
                else (state["speed_bps"] * 0.65) + (instant_speed * 0.35)
            )
            state["last_bytes"] = current
            state["last_sample_at"] = now

        speed_text = human_speed(state["speed_bps"]) if state["speed_bps"] > 0 else None
        eta_text = None
        if total > 0:
            remaining = max(0, total - current)
            if remaining > 0 and state["speed_bps"] > 0:
                eta_text = human_duration(remaining / state["speed_bps"])

        should_emit = (
            percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 5
            or now - state["last_update"] >= 2
        )
        if not should_emit:
            return

        state["last_percent"] = percent
        state["last_update"] = now
        if active is not None:
            active["download_percent"] = percent

        language = task_meta.get("language")
        text = build_status_text(
            task_id=task_id,
            file_name=task_meta["file_name"],
            file_size=task_meta["file_size"],
            stage=tr(language, "stage_downloading"),
            download_percent=percent,
            upload_percent=0,
            upload_status=tr(language, "note_downloading_youtube"),
            speed_text=speed_text,
            eta_text=eta_text,
            language=language,
        )
        loop.call_soon_threadsafe(
            lambda: loop.create_task(
                safe_edit_status(
                    status_message,
                    text,
                    reply_markup=status_action_keyboard(task_id, "cancel"),
                )
            )
        )

    return progress


def download_file_url(
    url: str,
    download_path: Path,
    progress,
    should_cancel,
    task_id: str,
) -> Path:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "file":
        if not ALLOW_FILE_URLS:
            raise RuntimeError("file:// URLs are disabled in this Hugging Face Space.")
        source_path = Path(unquote(parsed.path or ""))
        if not source_path.exists() or not source_path.is_file():
            raise RuntimeError("Local file URL not found.")
        if not is_direct_file_filename(source_path.name):
            raise RuntimeError("The file URL must point to a supported file type.")

        total = source_path.stat().st_size
        ensure_file_size_allowed(total, "local file")
        ensure_download_space(total)
        copied = 0
        progress(0, total)
        with source_path.open("rb") as source, download_path.open("wb") as target:
            while True:
                if should_cancel():
                    raise DirectDownloadCancelled("Cancelled by user.")
                chunk = source.read(1024 * 256)
                if not chunk:
                    break
                target.write(chunk)
                copied += len(chunk)
                progress(copied, total)
        progress(total, total)
        return download_path

    if scheme not in {"http", "https"}:
        raise RuntimeError("Only http(s):// direct file URLs are supported in this Space.")

    last_error: Exception | None = None

    for attempt in range(1, DIRECT_DOWNLOAD_MAX_RETRIES + 1):
        if should_cancel():
            raise DirectDownloadCancelled("Cancelled by user.")

        existing_size = download_path.stat().st_size if download_path.exists() else 0
        if existing_size > 0:
            download_path.unlink(missing_ok=True)

        try:
            with requests.get(
                url,
                stream=True,
                timeout=(15, 120),
            ) as response:
                response.raise_for_status()

                content_type = response.headers.get("content-type", "")
                if not (
                    is_supported_file_content_type(content_type)
                    or is_direct_file_filename(path_name_from_url(response.url))
                    or is_direct_file_filename(download_path.name)
                ):
                    raise RuntimeError("The URL must point to a direct supported file.")

                total = response_total_size(response, 0)
                if total > 0:
                    ensure_file_size_allowed(total, "direct URL file")
                    ensure_download_space(total)
                    progress(0, total)

                downloaded = 0
                with download_path.open("wb") as target:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if should_cancel():
                            raise DirectDownloadCancelled("Cancelled by user.")
                        if not chunk:
                            continue
                        target.write(chunk)
                        downloaded += len(chunk)
                        ensure_file_size_allowed(downloaded, "direct URL file")
                        progress(downloaded, total)

                if total > 0 and downloaded < total:
                    raise RuntimeError(
                        f"Download interrupted at {downloaded} of {total} bytes."
                    )

                progress(total or downloaded, total or downloaded)
                return download_path
        except Exception as error:
            if isinstance(error, DirectDownloadCancelled):
                raise

            last_error = error
            if attempt >= DIRECT_DOWNLOAD_MAX_RETRIES:
                break

            if not is_transient_download_error(str(error).lower()):
                break

            wait_for_direct_retry(DIRECT_DOWNLOAD_RETRY_DELAY * attempt, should_cancel)

    raise last_error if last_error else RuntimeError("Download failed.")


async def queue_downloaded_file(
    task_id: str,
    message: Message,
    status: Message,
    file_name: str,
    file_size: int,
    media_type: str,
    started_at: float,
    downloaded_path: Path,
    caption: str = "",
    source: str = "telegram",
    source_url: str | None = None,
    upload_file_name: str | None = None,
    runtime_settings: dict | None = None,
) -> None:
    file_name = normalize_upload_filename(file_name, downloaded_path.name)
    queue_position = queue_size() + (1 if load_processing() else 0) + 1
    task = {
        "task_id": task_id,
        "type": "local_file",
        "path": str(downloaded_path),
        "caption": caption,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "file_name": file_name,
        "file_size": file_size,
        "media_type": media_type,
        "started_at": started_at,
        "source": source,
    }
    if source_url:
        task["source_url"] = source_url
    if upload_file_name:
        task["upload_file_name"] = normalize_upload_filename(
            upload_file_name,
            downloaded_path.name,
        )
    apply_runtime_settings(task, runtime_settings)

    append_task(task)

    await safe_edit_status(
        status,
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=file_size,
            stage=tr(task["language"], "stage_upload_queue"),
            download_percent=100,
            upload_percent=0,
            upload_status=tr(task["language"], "note_waiting_upload"),
            queue_position=queue_position,
            language=task["language"],
        ),
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client: Client, message: Message):
    try:
        print(
            f"/start received chat_id={message.chat.id} "
            f"user_id={getattr(message.from_user, 'id', '-')}",
            flush=True,
        )
        if not await ensure_authorized_message(message):
            return
        await ensure_bot_commands(client)
        if not rubika_session_exists():
            await prompt_rubika_phone_setup(message, first_setup=True)
            return
        await send_menu(message)
    except Exception as error:
        print(f"/start handler failed: {type(error).__name__}: {error}", flush=True)
        try:
            await message.reply_text(
                "⚠️ /start failed inside the bot. Check the Space logs for the exact error.",
                reply_markup=menu_keyboard(),
            )
        except Exception as reply_error:
            print(f"/start error reply failed: {reply_error}", flush=True)
        return


@app.on_message(filters.private & filters.command("settings"))
async def settings_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await send_settings_panel(message)


@app.on_message(filters.private & filters.command("set_rubika"))
async def set_rubika_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    if len(message.command or []) < 2:
        await prompt_rubika_phone_setup(message)
        return

    await start_rubika_auth_process(message, " ".join(message.command[1:]))


@app.on_message(filters.private & filters.command("status"))
async def status_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await send_status_summary(message)


@app.on_message(filters.private & filters.command("transfers"))
async def transfers_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await send_transfers_summary(message)


@app.on_message(filters.private & filters.command("cleanup"))
async def cleanup_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    command = message.command or []
    confirm = len(command) > 1 and command[1].lower() == "confirm"

    if not confirm:
        await send_cleanup_preview(message)
        return

    await run_cleanup(message)


async def retry_task_by_id(client: Client, message: Message, task_id: str) -> None:
    if task_id in ACTIVE_DOWNLOADS:
        await message.reply_text(f"⬇️ This transfer is still downloading: {task_id}")
        return

    if find_queued_task(lambda task: task.get("task_id") == task_id):
        await message.reply_text(f"⏳ This transfer is already queued: {task_id}")
        return

    processing_task = visible_processing_task()
    if processing_task and processing_task.get("task_id") == task_id:
        await message.reply_text(f"🚀 This transfer is already uploading: {task_id}")
        return

    failed_entry = find_failed_entry(task_id)
    if not failed_entry:
        await message.reply_text(f"🔎 Failed transfer not found: {task_id}")
        return

    task = dict(failed_entry.get("task") or {})
    path = Path(task.get("path", ""))
    if not path.exists():
        await message.reply_text(
            "\n".join(
                [
                    f"⚠️ Local file not found: {task_id}",
                    "It was probably cleaned up. Please send the file again.",
                ]
            ),
            reply_markup=menu_keyboard(),
        )
        return

    task["upload_percent"] = 0
    task["attempt_text"] = None
    task["started_at"] = time.time()
    task["file_size"] = int(task.get("file_size") or path.stat().st_size)
    apply_runtime_settings(task)
    append_task(task)

    queue_position = queue_size() + (1 if load_processing() else 0)
    text = build_status_text(
        task_id=task_id,
        file_name=task.get("file_name", path.name),
        file_size=int(task.get("file_size", 0)),
        stage="🔁 Queued Again",
        download_percent=100,
        upload_percent=0,
        upload_status="The transfer was added back to the upload queue.",
        queue_position=queue_position,
    )
    await edit_status_by_task(
        client,
        task,
        text,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    await message.reply_text(
        f"🔁 Added back to queue: {task_id}",
        reply_markup=menu_keyboard(),
    )


async def retry_all_failed_tasks(client: Client, message: Message) -> None:
    retryable_tasks = retryable_failed_tasks()
    if not retryable_tasks:
        await message.reply_text(
            "🔎 No retryable failed transfers were found.",
            reply_markup=menu_keyboard(),
        )
        return

    queued_count = 0
    skipped_count = 0

    for task in retryable_tasks:
        task_id = task.get("task_id", "")
        if not task_id:
            skipped_count += 1
            continue

        if task_id in ACTIVE_DOWNLOADS:
            skipped_count += 1
            continue

        if find_queued_task(lambda queued: queued.get("task_id") == task_id):
            skipped_count += 1
            continue

        processing_task = visible_processing_task()
        if processing_task and processing_task.get("task_id") == task_id:
            skipped_count += 1
            continue

        path = Path(task.get("path", ""))
        if not path.exists():
            skipped_count += 1
            continue

        retry_task = dict(task)
        retry_task["upload_percent"] = 0
        retry_task["attempt_text"] = None
        retry_task["speed_text"] = None
        retry_task["eta_text"] = None
        retry_task["started_at"] = time.time()
        retry_task["file_size"] = int(retry_task.get("file_size") or path.stat().st_size)
        apply_runtime_settings(retry_task)
        append_task(retry_task)
        queued_count += 1

        queue_position = queue_size() + (1 if load_processing() else 0)
        text = build_status_text(
            task_id=task_id,
            file_name=retry_task.get("file_name", path.name),
            file_size=int(retry_task.get("file_size", 0)),
            stage="🔁 Queued Again",
            download_percent=100,
            upload_percent=0,
            upload_status="The transfer was added back to the upload queue.",
            queue_position=queue_position,
        )
        await edit_status_by_task(
            client,
            retry_task,
            text,
            reply_markup=status_action_keyboard(task_id, "cancel"),
        )

    if queued_count == 0:
        await message.reply_text(
            "⚠️ No failed transfers were added back to the queue.",
            reply_markup=menu_keyboard(),
        )
        return

    lines = [
        "<b>🔁 Retry All Complete</b>",
        "",
        f"Added back to queue: <b>{queued_count}</b>",
    ]
    if skipped_count:
        lines.append(f"Skipped: <b>{skipped_count}</b>")

    await message.reply_text(
        "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=menu_keyboard(),
    )


@app.on_message(filters.private & filters.command("retry"))
async def retry_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    if len(message.command) < 2:
        await message.reply_text(
            "🔁 Open Transfers and use a Retry button, or run /retry_all.",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=main_action_keyboard(),
        )
        return

    task_id = message.command[1].strip()
    await retry_task_by_id(client, message, task_id)


@app.on_message(filters.private & filters.command("retry_all"))
async def retry_all_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await retry_all_failed_tasks(client, message)


@app.on_message(filters.private & filters.command("youtube_cookies"))
async def youtube_cookies_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    saved, error = await save_youtube_cookies_from_message(message)
    lang = current_language()
    if saved:
        text = (
            "✅ کوکی یوتیوب ذخیره شد. حالا لینک یوتیوب را دوباره بفرست."
            if lang == "fa"
            else "✅ YouTube cookies saved. Send the YouTube link again now."
        )
        await message.reply_text(text, reply_markup=menu_keyboard())
        return

    if error:
        await message.reply_text(f"⚠️ {error}\n\n{youtube_cookie_help_text(lang)}", parse_mode=enums.ParseMode.HTML)
        return

    await message.reply_text(
        youtube_cookie_help_text(lang),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=menu_keyboard(),
    )


@app.on_message(filters.private & filters.command("clear_youtube_cookies"))
async def clear_youtube_cookies_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    try:
        YOUTUBE_COOKIES_FILE.unlink(missing_ok=True)
    except OSError as error:
        await message.reply_text(f"⚠️ {error}", reply_markup=menu_keyboard())
        return

    text = "✅ کوکی یوتیوب حذف شد." if current_language() == "fa" else "✅ YouTube cookies removed."
    await message.reply_text(text, reply_markup=menu_keyboard())


@app.on_message(filters.private & filters.command("check_cookie"))
async def check_cookie_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    lang = current_language()
    checking = "⏳ در حال بررسی کوکی یوتیوب..." if lang == "fa" else "⏳ Checking YouTube cookies..."
    status = await message.reply_text(checking)

    try:
        result = await asyncio.to_thread(validate_youtube_cookies, YOUTUBE_COOKIES_FILE)
    except Exception as error:
        await status.edit_text(f"⚠️ Error: {error}", reply_markup=menu_keyboard())
        return

    if lang == "fa":
        lines = ["<b>🍪 بررسی کوکی یوتیوب</b>", ""]
        lines.append(f"📁 <b>فایل:</b> {'✅ موجود' if result['exists'] else '❌ وجود ندارد'}")
        if result["exists"]:
            lines.append(f"📝 <b>تعداد خطوط:</b> <code>{result['lines']}</code>")
            lines.append(f"🌐 <b>کل دامنه‌ها:</b> <code>{result['domains']}</code>")
            lines.append(f"▶️ <b>دامنه‌های YouTube:</b> {'✅' if result.get('has_youtube') else '❌'} <code>{result.get('youtube_domains', 0)}</code>")
            lines.append(f"🔍 <b>دامنه‌های Google:</b> {'✅' if result.get('has_google') else '❌'} <code>{result.get('google_domains', 0)}</code>")
            domain_list = result.get("domain_list") or []
            if domain_list:
                lines.append(f"📋 <b>لیست دامنه‌ها:</b> <code>{', '.join(domain_list[:10])}</code>")
            lines.append(f"✅ <b>فرمت معتبر:</b> {'بله' if result['valid_format'] else '❌ خیر'}")
            lines.append(f"🔑 <b>عملکرد:</b> {'✅ فعال' if result['working'] else '❌ غیرفعال'}")
            if not result.get("has_google"):
                lines.extend(["", "⚠️ <b>هشدار:</b> کوکی‌های google.com وجود ندارند!"])
                lines.append("ویدیوهای دارای محدودیت سنی نیاز به کوکی هر دو دامنه google.com و youtube.com دارند.")
                lines.append("لطفاً کوکی را با ابزاری بگیر که <b>همه دامنه‌ها</b> را اکسپورت کند.")
        if result.get("error"):
            lines.extend(["", f"⚠️ <b>خطا:</b> {escape(str(result['error']))}"])
        if not result["exists"]:
            lines.extend(["", "💡 فایل cookies.txt را بفرست و روی آن /youtube_cookies بزن."])
        elif not result["working"] and not result.get("error"):
            lines.extend(["", "💡 کوکی‌ها منقضی شده‌اند. کوکی جدید از مرورگر بگیر و دوباره بفرست."])
        elif result["working"] and result.get("has_google"):
            lines.extend(["", "✅ کوکی‌ها سالم و فعال هستند و شامل هر دو دامنه YouTube و Google هستند."])
        elif result["working"]:
            lines.extend(["", "✅ کوکی‌ها فعال هستند ولی ممکن است برای ویدیوهای دارای محدودیت سنی کافی نباشند."])
    else:
        lines = ["<b>🍪 YouTube Cookie Check</b>", ""]
        lines.append(f"📁 <b>File:</b> {'✅ Found' if result['exists'] else '❌ Not found'}")
        if result["exists"]:
            lines.append(f"📝 <b>Lines:</b> <code>{result['lines']}</code>")
            lines.append(f"🌐 <b>Total domains:</b> <code>{result['domains']}</code>")
            lines.append(f"▶️ <b>YouTube domains:</b> {'✅' if result.get('has_youtube') else '❌'} <code>{result.get('youtube_domains', 0)}</code>")
            lines.append(f"🔍 <b>Google domains:</b> {'✅' if result.get('has_google') else '❌'} <code>{result.get('google_domains', 0)}</code>")
            domain_list = result.get("domain_list") or []
            if domain_list:
                lines.append(f"📋 <b>Domains:</b> <code>{', '.join(domain_list[:10])}</code>")
            lines.append(f"✅ <b>Valid format:</b> {'Yes' if result['valid_format'] else '❌ No'}")
            lines.append(f"🔑 <b>Working:</b> {'✅ Active' if result['working'] else '❌ Inactive'}")
            if not result.get("has_google"):
                lines.extend(["", "⚠️ <b>Warning:</b> google.com cookies are missing!"])
                lines.append("Age-restricted videos need cookies from BOTH google.com and youtube.com.")
                lines.append("Re-export cookies with a tool that exports <b>all domains</b>.")
        if result.get("error"):
            lines.extend(["", f"⚠️ <b>Error:</b> {escape(str(result['error']))}"])
        if not result["exists"]:
            lines.extend(["", "💡 Send a cookies.txt file and reply to it with /youtube_cookies."])
        elif not result["working"] and not result.get("error"):
            lines.extend(["", "💡 Cookies are expired. Export fresh cookies from your browser."])
        elif result["working"] and result.get("has_google"):
            lines.extend(["", "✅ Cookies are valid and include both YouTube and Google domains."])
        elif result["working"]:
            lines.extend(["", "✅ Cookies work but may not be sufficient for age-restricted videos."])

    await status.edit_text(
        "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=menu_keyboard(),
    )


@app.on_callback_query(filters.regex(r"^ytfmt:"))
async def youtube_format_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return

    token = (callback_query.data or "").split(":", 1)[-1]
    choice = YOUTUBE_FORMAT_CHOICES.pop(token, None)
    if not choice:
        await callback_query.answer("⚠️ Selection expired. Send the link again.")
        return

    await callback_query.answer("✅ Starting download...")
    url = choice["url"]
    format_id = choice["format_id"]
    label = choice.get("label", "")

    # Remove the format picker message
    try:
        await callback_query.message.delete()
    except Exception:
        pass

    # Create a synthetic message-like object from the original chat
    message = callback_query.message
    await process_youtube_url(message, url, chosen_format=format_id)


@app.on_message(filters.private & filters.command("speedtest"))
async def speedtest_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    lang = current_language()
    waiting = "⏳ در حال تست شبکه Space..." if lang == "fa" else "⏳ Testing the Space network..."
    status = await message.reply_text(waiting)
    try:
        text = await run_speedtest()
        await status.edit_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=main_action_keyboard())
    except Exception as error:
        failed = "تست سرعت ناموفق بود" if lang == "fa" else "Speed test failed"
        await status.edit_text(f"⚠️ {failed}: {error}", reply_markup=menu_keyboard())


@app.on_message(filters.private & MENU_BUTTON_FILTER)
async def menu_button_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    text = (message.text or "").strip()
    action = MENU_ACTION_BY_TEXT.get(text)

    if action == "status":
        await status_handler(client, message)
    elif action == "transfers":
        await transfers_handler(client, message)
    elif action == "cleanup":
        await cleanup_handler(client, message)
    elif action == "cancel":
        await send_cancel_picker(message)
    elif action == "settings":
        await settings_handler(client, message)


@app.on_callback_query(filters.regex(r"^menu:"))
async def menu_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    action = (callback_query.data or "").split(":", 1)[1].strip()
    await callback_query.answer()

    if action == "status":
        await send_status_summary(callback_query.message)
    elif action == "transfers":
        await send_transfers_summary(callback_query.message)
    elif action == "cleanup":
        await send_cleanup_preview(callback_query.message)
    elif action == "cancel":
        await send_cancel_picker(callback_query.message)
    elif action == "settings":
        await render_settings_panel(callback_query.message)


@app.on_callback_query(filters.regex(r"^settings:"))
async def settings_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    parts = (callback_query.data or "").split(":")
    action = parts[1] if len(parts) > 1 else ""
    await callback_query.answer()

    if action == "session":
        await prompt_rubika_phone_setup(callback_query.message)
    elif action == "destination":
        await send_destination_panel(callback_query.message)
    elif action == "language":
        settings = load_runtime_settings()
        requested_language = parts[2] if len(parts) > 2 else ""
        settings["language"] = (
            normalize_language(requested_language)
            if requested_language
            else ("en" if settings.get("language") == "fa" else "fa")
        )
        save_runtime_settings(settings)
        global COMMANDS_READY
        COMMANDS_READY = False
        await ensure_bot_commands(client)
        note = (
            "✅ Language changed to English."
            if settings["language"] == "en"
            else "✅ زبان ربات فارسی شد."
        )
        await render_settings_panel(callback_query.message, note=note)


@app.on_callback_query(filters.regex(r"^destination:"))
async def destination_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return

    parts = (callback_query.data or "").split(":")
    action = parts[1] if len(parts) > 1 else ""
    chat_id = callback_query.message.chat.id

    if action == "menu":
        await callback_query.answer()
        await send_destination_panel(callback_query.message)
        return

    if action == "back":
        await callback_query.answer()
        await send_settings_panel(callback_query.message)
        return

    if action == "saved":
        reset_destination_settings()
        CHANNEL_CHOICES.pop(chat_id, None)
        await callback_query.answer("Destination set to Saved Messages.")
        await send_settings_panel(
            callback_query.message,
            note="✅ Upload destination changed to Saved Messages.",
        )
        return

    if action == "channels":
        await callback_query.answer("Loading channels...")
        if not rubika_session_exists():
            await send_destination_panel(
                callback_query.message,
                note="⚠️ Set up the Rubika account first, then choose a channel.",
            )
            return

        settings = load_runtime_settings()
        try:
            channels = await load_rubika_channels(settings["rubika_session"])
        except Exception as error:
            await send_destination_panel(
                callback_query.message,
                note=f"⚠️ Could not load Rubika channels: {escape(str(error))}",
            )
            return

        if not channels:
            CHANNEL_CHOICES.pop(chat_id, None)
            await send_destination_panel(
                callback_query.message,
                note=(
                    "No recent channels were found. Open the channel in Rubika "
                    "with this account, then try again."
                ),
            )
            return

        await callback_query.message.reply_text(
            "<b>📣 Choose Channel</b>\n\nSelect a channel for future uploads.",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=channel_picker_keyboard(chat_id, channels),
        )
        return

    if action == "set" and len(parts) > 2:
        token = parts[2]
        channel = CHANNEL_CHOICES.get(chat_id, {}).get(token)
        if not channel:
            await callback_query.answer(
                "This channel list expired. Please load channels again.",
                show_alert=True,
            )
            return

        settings = load_runtime_settings()
        settings["rubika_target"] = channel["guid"]
        settings["rubika_target_title"] = channel["title"]
        settings["rubika_target_type"] = "channel"
        save_runtime_settings(settings)
        CHANNEL_CHOICES.pop(chat_id, None)
        await callback_query.answer("Destination updated.")
        await send_settings_panel(
            callback_query.message,
            note=f"✅ Upload destination changed to {escape(channel['title'])}.",
        )
        return

    await callback_query.answer("Unknown destination action.", show_alert=True)


@app.on_callback_query(filters.regex(r"^auth:cancel$"))
async def auth_cancel_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    await callback_query.answer("Rubika setup cancelled.")
    await cancel_auth_setup(callback_query.message)


@app.on_callback_query(filters.regex(r"^cleanup:confirm$"))
async def cleanup_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    await callback_query.answer("Cleanup started.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await run_cleanup(callback_query.message)


@app.on_callback_query(filters.regex(r"^cancel:"))
async def cancel_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    task_id = (callback_query.data or "").split(":", 1)[1].strip()
    await callback_query.answer("Cancel requested.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await cancel_task_by_id(client, callback_query.message, task_id)


@app.on_callback_query(filters.regex(r"^retry:"))
async def retry_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    task_id = (callback_query.data or "").split(":", 1)[1].strip()
    await callback_query.answer("Retry queued.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await retry_task_by_id(client, callback_query.message, task_id)


@app.on_callback_query(filters.regex(r"^retry_all$"))
async def retry_all_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    if callback_seen(callback_query):
        await callback_query.answer()
        return
    await callback_query.answer("Retrying all failed transfers.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await retry_all_failed_tasks(client, callback_query.message)


@app.on_message(filters.private & filters.command("cancel"))
async def cancel_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    task_id = None
    if message.command and len(message.command) > 1:
        task_id = message.command[1].strip()

    if not task_id and message.reply_to_message:
        task_id, _ = resolve_task_from_reply(message.reply_to_message.id)

    if not task_id:
        await send_cancel_picker(message)
        return

    await cancel_task_by_id(client, message, task_id)


@app.on_message(
    filters.private
    & (
        filters.video
        | filters.document
        | filters.audio
        | filters.voice
        | filters.photo
        | filters.animation
        | filters.video_note
        | filters.sticker
    )
)
async def media_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    media_type, media = get_media(message)
    if not media:
        await message.reply_text("⚠️ This message cannot be processed.")
        return

    task_id = uuid.uuid4().hex[:10]
    file_name = build_download_filename(message, media_type, media)
    file_size = int(getattr(media, "file_size", 0) or 0)
    download_path = DOWNLOAD_DIR / file_name
    started_at = time.time()
    runtime_settings = load_runtime_settings()

    try:
        if file_size > 0:
            ensure_file_size_allowed(file_size, "Telegram file")
            ensure_download_space(file_size)
        else:
            ensure_download_space()
    except RuntimeError as error:
        await message.reply_text(f"⚠️ {error}", reply_markup=menu_keyboard())
        return

    status = await message.reply_text(
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=file_size,
            stage=tr(runtime_settings["language"], "stage_preparing_download"),
            download_percent=0,
            upload_percent=0,
            upload_status=tr(runtime_settings["language"], "note_file_download_soon"),
            language=runtime_settings["language"],
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    ACTIVE_DOWNLOADS[task_id] = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "download_path": str(download_path),
        "file_name": file_name,
        "file_size": file_size,
        "started_at": started_at,
        "cancelled": False,
        "download_percent": 0,
        "upload_percent": 0,
        "language": runtime_settings["language"],
    }

    try:
        downloaded = await client.download_media(
            message,
            file_name=str(download_path),
            progress=make_download_progress_callback(
                task_id,
                status,
                {
                    "file_name": file_name,
                    "file_size": file_size,
                    "language": runtime_settings["language"],
                },
            ),
            progress_args=(client,),
        )

        if ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled"):
            raise RuntimeError("Cancelled by user.")

        if not downloaded:
            raise RuntimeError("Download failed.")

        downloaded_path = Path(downloaded)
        if not downloaded_path.exists():
            raise RuntimeError("Downloaded file not found.")

        await queue_downloaded_file(
            task_id=task_id,
            message=message,
            status=status,
            file_name=file_name,
            file_size=file_size,
            media_type=media_type,
            started_at=started_at,
            downloaded_path=downloaded_path,
            caption=message.caption or "",
            runtime_settings=runtime_settings,
        )

    except Exception as e:
        active = ACTIVE_DOWNLOADS.get(task_id, {})
        was_cancelled = active.get("cancelled") or "cancelled by user" in str(e).lower()
        cleanup_download_artifact(str(download_path))

        if was_cancelled:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=file_size,
                    stage=tr(runtime_settings["language"], "stage_cancelled"),
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status=tr(runtime_settings["language"], "note_cancelled"),
                    language=runtime_settings["language"],
                ),
            )
        else:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=file_size,
                    stage=tr(runtime_settings["language"], "stage_download_failed"),
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status=tr(runtime_settings["language"], "note_download_failed"),
                    note=str(e),
                    language=runtime_settings["language"],
                ),
            )
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


async def process_direct_file_url(message: Message, url: str) -> dict:
    task_id = uuid.uuid4().hex[:10]
    fallback_suffix = Path(path_name_from_url(url)).suffix.lower()
    if fallback_suffix not in DIRECT_FILE_EXTENSIONS:
        fallback_suffix = ".bin"

    file_name = build_url_download_filename(url, task_id, fallback_suffix)
    download_path = DOWNLOAD_DIR / file_name
    started_at = time.time()
    runtime_settings = load_runtime_settings()
    task_meta = {
        "file_name": file_name,
        "file_size": 0,
        "language": runtime_settings["language"],
    }

    status = await message.reply_text(
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=0,
            stage=tr(runtime_settings["language"], "stage_preparing_download"),
            download_percent=0,
            upload_percent=0,
            upload_status=tr(runtime_settings["language"], "note_link_download_soon"),
            language=runtime_settings["language"],
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    ACTIVE_DOWNLOADS[task_id] = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "download_path": str(download_path),
        "file_name": file_name,
        "file_size": 0,
        "started_at": started_at,
        "cancelled": False,
        "download_percent": 0,
        "upload_percent": 0,
        "language": runtime_settings["language"],
    }

    try:
        downloaded_path = await asyncio.to_thread(
            download_file_url,
            url,
            download_path,
            make_direct_download_progress_callback(task_id, status, task_meta),
            lambda: ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled", False),
            task_id,
        )

        if ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled"):
            raise DirectDownloadCancelled("Cancelled by user.")

        if not downloaded_path.exists():
            raise RuntimeError("Downloaded file not found.")

        file_size = task_meta["file_size"] or downloaded_path.stat().st_size
        await queue_downloaded_file(
            task_id=task_id,
            message=message,
            status=status,
            file_name=file_name,
            file_size=file_size,
            media_type="file",
            started_at=started_at,
            downloaded_path=downloaded_path,
            caption="",
            source="direct_url",
            source_url=url,
            upload_file_name=file_name,
            runtime_settings=runtime_settings,
        )
        return {"task_id": task_id, "file_name": file_name, "status": "queued"}
    except Exception as e:
        active = ACTIVE_DOWNLOADS.get(task_id, {})
        was_cancelled = active.get("cancelled") or isinstance(e, DirectDownloadCancelled)
        cleanup_download_artifact(str(download_path))

        if was_cancelled:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=task_meta.get("file_size", 0),
                    stage=tr(runtime_settings["language"], "stage_cancelled"),
                    download_percent=active.get("download_percent", 0),
                    upload_percent=0,
                    upload_status=tr(runtime_settings["language"], "note_cancelled"),
                    language=runtime_settings["language"],
                ),
            )
            return {"task_id": task_id, "file_name": file_name, "status": "cancelled"}
        else:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=task_meta.get("file_size", 0),
                    stage=tr(runtime_settings["language"], "stage_download_failed"),
                    download_percent=active.get("download_percent", 0),
                    upload_percent=0,
                    upload_status=tr(runtime_settings["language"], "note_download_failed"),
                    note=str(e),
                    language=runtime_settings["language"],
                ),
            )
            return {"task_id": task_id, "file_name": file_name, "status": "failed"}
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


async def process_youtube_url(message: Message, url: str, chosen_format: str | None = None) -> dict:
    task_id = uuid.uuid4().hex[:10]
    started_at = time.time()
    runtime_settings = load_runtime_settings()
    language = runtime_settings["language"]

    # ── Quality picker: if no format chosen yet, show available qualities ──
    if not chosen_format:
        fetching = (
            "⏳ در حال بررسی ویدیو..." if language == "fa"
            else "⏳ Fetching video info..."
        )
        picker_msg = await message.reply_text(fetching)
        try:
            video_info = await asyncio.to_thread(
                fetch_youtube_formats,
                url,
                cookies_path=YOUTUBE_COOKIES_FILE if youtube_cookies_exist() else None,
            )
        except Exception as error:
            note = compact_youtube_error(error, language)
            error_title = "❌ خطا در بررسی ویدیو" if language == "fa" else "❌ Failed to fetch video"
            await picker_msg.edit_text(
                f"<b>{error_title}</b>\n\n{escape(note)}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=menu_keyboard(),
            )
            return {"task_id": task_id, "file_name": f"youtube_{task_id}.mp4", "status": "failed"}

        # Build quality picker keyboard
        duration_text = yt_human_duration(video_info.duration)
        title_text = escape(video_info.title[:80])
        if language == "fa":
            header = (
                f"<b>▶️ {title_text}</b>\n"
                f"⏱ مدت: <code>{duration_text}</code>\n\n"
                f"📥 کیفیت مورد نظر را انتخاب کن:"
            )
        else:
            header = (
                f"<b>▶️ {title_text}</b>\n"
                f"⏱ Duration: <code>{duration_text}</code>\n\n"
                f"📥 Choose quality:"
            )

        rows = []
        for fmt in video_info.formats[:8]:
            token = uuid.uuid4().hex[:10]
            YOUTUBE_FORMAT_CHOICES[token] = {
                "url": url,
                "format_id": fmt.format_id,
                "label": fmt.label,
                "chat_id": message.chat.id,
            }
            rows.append([InlineKeyboardButton(fmt.label, callback_data=f"ytfmt:{token}")])

        await picker_msg.edit_text(
            header,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return {"task_id": task_id, "file_name": f"youtube_{task_id}.mp4", "status": "picking_quality"}

    # ── Download with chosen format ──
    file_name = f"youtube_{task_id}.mp4"
    task_meta = {
        "file_name": file_name,
        "file_size": 0,
        "language": language,
    }

    status = await message.reply_text(
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=0,
            stage=tr(language, "stage_youtube_prepare"),
            download_percent=0,
            upload_percent=0,
            upload_status=tr(language, "note_youtube_download_soon"),
            language=language,
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    ACTIVE_DOWNLOADS[task_id] = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "download_path": str(DOWNLOAD_DIR / file_name),
        "file_name": file_name,
        "file_size": 0,
        "started_at": started_at,
        "cancelled": False,
        "download_percent": 0,
        "upload_percent": 0,
        "language": language,
    }

    space_checked = {"done": False}

    def check_youtube_size(size: int) -> None:
        if size > 0:
            ensure_file_size_allowed(size, "YouTube video")
            if not space_checked["done"]:
                ensure_download_space(size)
                space_checked["done"] = True
        else:
            if not space_checked["done"]:
                ensure_download_space()
                space_checked["done"] = True

    try:
        result = await asyncio.to_thread(
            download_youtube,
            url=url,
            output_dir=DOWNLOAD_DIR,
            task_id=task_id,
            should_cancel=lambda: ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled", False),
            progress=make_youtube_download_progress_callback(task_id, status, task_meta),
            check_size=check_youtube_size,
            language=language,
            cookies_path=YOUTUBE_COOKIES_FILE if youtube_cookies_exist() else None,
            chosen_format=chosen_format,
        )

        if ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled"):
            raise YouTubeDownloadCancelled("Cancelled by user.")

        task_meta["file_name"] = result.file_name
        task_meta["file_size"] = result.file_size
        ACTIVE_DOWNLOADS[task_id]["download_path"] = str(result.path)
        ACTIVE_DOWNLOADS[task_id]["file_name"] = result.file_name
        ACTIVE_DOWNLOADS[task_id]["file_size"] = result.file_size

        media_type = "audio" if chosen_format and chosen_format.startswith("bestaudio") else "video"
        await queue_downloaded_file(
            task_id=task_id,
            message=message,
            status=status,
            file_name=result.file_name,
            file_size=result.file_size,
            media_type=media_type,
            started_at=started_at,
            downloaded_path=result.path,
            caption="",
            source="youtube",
            source_url=url,
            upload_file_name=result.file_name,
            runtime_settings=runtime_settings,
        )
        return {"task_id": task_id, "file_name": result.file_name, "status": "queued"}
    except Exception as error:
        active = ACTIVE_DOWNLOADS.get(task_id, {})
        was_cancelled = active.get("cancelled") or isinstance(error, YouTubeDownloadCancelled)
        cleanup_youtube_partials(DOWNLOAD_DIR, task_id)

        if was_cancelled:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=task_meta["file_name"],
                    file_size=task_meta.get("file_size", 0),
                    stage=tr(language, "stage_cancelled"),
                    download_percent=active.get("download_percent", 0),
                    upload_percent=0,
                    upload_status=tr(language, "note_cancelled"),
                    language=language,
                ),
            )
            return {"task_id": task_id, "file_name": task_meta["file_name"], "status": "cancelled"}

        note = compact_youtube_error(error, language)
        await safe_edit_status(
            status,
            build_status_text(
                task_id=task_id,
                file_name=task_meta["file_name"],
                file_size=task_meta.get("file_size", 0),
                stage=tr(language, "stage_download_failed"),
                download_percent=active.get("download_percent", 0),
                upload_percent=0,
                upload_status=tr(language, "note_download_failed"),
                note=note,
                language=language,
            ),
        )
        return {"task_id": task_id, "file_name": task_meta["file_name"], "status": "failed"}
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


@app.on_message(filters.private & filters.text)
async def direct_file_url_handler(_client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return

    text = (message.text or "").strip()
    if await maybe_handle_auth_input(message):
        return

    if not text or text in MENU_BUTTONS or text.startswith("/"):
        return

    urls = extract_direct_urls(text)
    if not urls:
        return

    if len(urls) > 1:
        found_text = (
            f"🔗 {len(urls)} لینک پیدا شد. دانلودها شروع می‌شوند."
            if current_language() == "fa"
            else f"🔗 Found {len(urls)} links. Starting downloads now."
        )
        await message.reply_text(
            found_text,
            reply_markup=menu_keyboard(),
        )

    results = await asyncio.gather(
        *(
            process_youtube_url(message, url)
            if is_youtube_url(url)
            else process_direct_file_url(message, url)
            for url in urls
        )
    )

    if len(urls) > 1:
        await message.reply_text(
            build_batch_summary_text(results),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=menu_keyboard(),
        )


async def start_telegram_client() -> None:
    print(
        "Telegram bot starting "
        f"session={TELEGRAM_SESSION} owner_id={OWNER_TELEGRAM_ID or 'open'}",
        flush=True,
    )
    try:
        await app.start()
    except Exception as error:
        if not is_auth_key_duplicated(error):
            raise

        print(
            "Telegram session hit AUTH_KEY_DUPLICATED; clearing bot session and retrying once.",
            flush=True,
        )
        clear_telegram_session_files("AUTH_KEY_DUPLICATED")
        await app.start()

    me = await app.get_me()
    print(
        "Telegram bot started "
        f"username=@{me.username or '-'} id={me.id}",
        flush=True,
    )


async def main() -> None:
    await start_telegram_client()
    asyncio.create_task(worker_telegram_event_loop())
    await idle()
    print("Telegram bot stopping", flush=True)
    await app.stop()


if __name__ == "__main__":
    app.run(main())
