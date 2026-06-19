"""
EbookManager Bot — v12
=======================
New over v11:
  Caption Template System:
  - Multiple named caption templates for PDF delivery
  - Per-group template assignment (assigned and trigger chats)
  - Sensitive groups can use a minimal/clean template (no source links)
  - Safe user mention: shows plain name (no hyperlink) when user has no username
    to avoid ban-triggering clickable links in sensitive groups
  - Built-in templates: default, minimal, branded, silent, clean, request_style
  - Commands:
      /list_templates               — show all templates
      /preview_template <name>      — preview a template
      /set_group_template <name> [chat] [thread]  — assign template to group
      /get_group_template [chat] [thread]          — check which template a group uses
      /set_dm_template <name>       — set template used for DM/private deliveries
      /set_search_purge <seconds>   — how long search result messages stay (default 60s)
      /companion_status             — show all companion client statuses
      /companion_add_source <n> <src>    — assign a source to a companion
      /companion_remove_source <n> <src> — remove source from a companion
      /companion_restart <n>        — reconnect a companion client
      /disable <ref>                — remove source + delete ALL its books from DB
      /disable <ref> --keep         — remove source only, keep books in DB
      /add_template <name> <text>   — add a custom template (owner only)
      /del_template <name>          — delete a custom template (owner only)
"""

import os
import re
import asyncio
import sqlite3
import time
import json
import hashlib
import tempfile
import logging
import traceback
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime, timezone
from telethon import TelegramClient, events, types, errors
from telethon.tl.custom import Button
from dotenv import load_dotenv

load_dotenv()

def _parse_purge(raw: str) -> int:
    raw = str(raw).strip().lower()
    if raw.endswith('m'):
        try: return max(60, int(raw[:-1]) * 60)
        except: pass
    if raw.endswith('h'):
        try: return max(60, int(raw[:-1]) * 3600)
        except: pass
    try: return max(60, int(raw) * 3600)
    except: pass
    return 72 * 3600

def _fmt_purge(seconds: int) -> str:
    if seconds < 3600:
        return f'{seconds // 60}m'
    elif seconds % 3600 == 0:
        return f'{seconds // 3600}h'
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f'{h}h {m}m'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

API_ID    = int(os.getenv('API_ID'))
API_HASH  = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
_BOT_USERNAME: list[str] = ['']   # filled after bot_client.start()
OWNER_ID  = int(os.getenv('OWNER_ID'))

BOT_USERNAME:    str       = ''
BACKUP_GROUP_ID: list[int] = [0]
ANALYTICS_GROUP: list[int] = [0]
REQUEST_GROUP:   list[int] = [0]
BRAND_CHANNEL:   list[str] = ['@CuriousCrewReturn']
SOURCE_CREDIT:   list[str] = ['@boipoka_group']
SOURCE_GROUPS:   list[str] = []
SEARCH_MODE:     list[str] = ['public']
DM_PURGE_SECS:   int       = 0
DM_PURGE_SECS_REF: list[int] = [0]
# Template used for DM deliveries.  Defaults to 'default' if not set.
# Set via /set_dm_template <name>  or  settings.json → "dm_template": "..."
DM_TEMPLATE_REF: list[str] = ['default']
# How long search result messages stay before auto-delete (seconds).
# Default 60s.  0 = use group purge_hrs.  Set via /set_search_purge or settings.json.
SEARCH_RESULT_PURGE_SECS: list[int] = [60]

ADMIN_IDS: set[int] = set()
ALL_STAFF: set[int] = {OWNER_ID}

_DL_SEMAPHORE: asyncio.Semaphore = None
_DL_MAX = 3

_SEARCH_EXECUTOR: ThreadPoolExecutor = None
_active_downloads: dict = {}
_dl_dedup_cache: dict[tuple, float] = {}  # (user_id, book_id) → timestamp
_DL_DEDUP_WINDOW = 8.0  # seconds — ignore duplicate taps within this window

_scrap_lock: asyncio.Lock = None
_scrap_running:  list[bool]  = [False]
_scrap_cancel:   list[bool]  = [False]
_scrap_who:      list[int]   = [0]
_scrap_started:  list[float] = [0.0]
_scrap_current:  list[str]   = ['']
_scrap_queue:    list        = []

_scrap_log: list[tuple] = []
_SCRAP_LOG_MAX = 200

_BOT_START_TIME: float = time.time()  # uptime tracking

# ── Feature: Keyword alerts (Feature 1) ──────────────────────────────────────
# {keyword: [(chat_id, thread_id), ...]}  — where to post the alert
KEYWORD_ALERTS: dict[str, list] = {}

# ── Feature: VIP users (Feature 11) ─────────────────────────────────────────
VIP_USERS: set[int] = set()
VIP_DL_LIMIT_MULTIPLIER = 3   # VIPs get 3× daily download limit

# ── VIP per-user custom daily limits: {user_id: int} ─────────────────────────
VIP_CUSTOM_LIMITS: dict[int, int] = {}

# ── VIP Permissions — what VIPs are allowed to do (all True by default) ──────
# These are EXTRA privileges on top of normal users.
VIP_PERMS: dict[str, bool] = {
    'bypass_search_cooldown':   True,   # no search cooldown between queries
    'bypass_book_cooldown':     True,   # no per-book re-download cooldown
    'bypass_flood_check':       True,   # not flood-muted automatically
    'higher_dl_limit':          True,   # use VIP_DL_LIMIT_MULTIPLIER or custom limit
    'priority_download':        True,   # jumps download queue (future)
    'request_unlimited':        True,   # no request-rate limit
}

# ── VIP Card appearance (owner can change via /vip_card_style) ────────────────
VIP_CARD_STYLE: dict[str, str] = {
    'header':  '🌟 VIP Members 🌟',
    'border':  '━',
    'emoji':   '⭐',
    'footer':  'Powered by the bot 🤖',
}

# ── Feature: Book aliases (Feature 5) ────────────────────────────────────────
# {book_id: [alias1, alias2, ...]}
BOOK_ALIASES: dict[int, list[str]] = {}

# ── Feature: Broadcast report config (Feature 16) ────────────────────────────
# Chats that receive broadcast reports: {(chat_id, thread_id): {'daily': bool, 'weekly': bool}}
BROADCAST_REPORT_CHATS: dict[tuple, dict] = {}

# ── Feature: Auto-report config (Feature 17) ─────────────────────────────────
REPORT_TIME_UTC: list[str]  = ['00:00']   # HH:MM, loaded from settings.json
REPORT_TZ_NAME:  list[str]  = ['UTC']     # tz name for display only

# ── Feature: Book of the day (Feature 7) ─────────────────────────────────────
# {(chat_id, thread_id): True}  — where to post book of the day
BOTD_CHATS: dict[tuple, bool] = {}
_BOTD_LAST_DATE: list[str] = ['']  # YYYY-MM-DD of last posted BOTD

def _log_scrap(src_label: str, event_type: str, detail: str = ''):
    _scrap_log.append((time.time(), src_label, event_type, detail))
    if len(_scrap_log) > _SCRAP_LOG_MAX:
        del _scrap_log[0]

AUTO_SCRAP_INTERVAL_H: list[int] = [int(os.getenv('AUTO_SCRAP_INTERVAL_H', '0'))]
# ↑ 0 means disabled.  Override in settings.json via "auto_scrap_interval_h": 72
# The list wrapper lets us hot-reload from settings without breaking references.

# ─────────────────────────────────────────────────────────────────────────────
# COMPANION CLIENT SYSTEM
#
# Some source groups block the main userbot account.  A companion is a
# secondary Telegram account (user account, NOT bot) that can join those
# groups and scrape/listen for new files on behalf of the main system.
#
# Config in settings.json:
#   "companions": [
#     {
#       "name":       "Companion A",           ← display name
#       "api_id":     12345678,
#       "api_hash":   "abc123...",
#       "session":    "companion_a",            ← .session filename (no .session ext)
#       "sources":    ["@BlockedGroup1", "-100123456789"]
#     }
#   ]
#
# The companion's session file must already exist (log in once manually).
# Each companion runs its own TelegramClient in the same event loop.
# All DB writes go to the shared ebooks.db — companions are transparent.
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CompanionClient:
    name:    str
    api_id:  int
    api_hash: str
    session: str          # session file path (no .session suffix)
    sources: list[str]    # source identifiers this companion handles
    client:  object = None     # TelegramClient, filled on start
    me:      object = None     # GetMe result
    running: bool  = False
    error:   str   = ''

# Registry — populated from settings.json at startup
COMPANION_CLIENTS: list[CompanionClient] = []

def _load_companions(data: dict):
    """Parse companion config from settings dict and populate COMPANION_CLIENTS."""
    COMPANION_CLIENTS.clear()
    for entry in data.get('companions', []):
        try:
            name     = str(entry.get('name', 'Companion')).strip()
            api_id   = int(entry['api_id'])
            api_hash = str(entry['api_hash']).strip()
            session  = str(entry.get('session', f'companion_{name.lower().replace(" ","_")}')).strip()
            sources  = [str(s).strip() for s in entry.get('sources', []) if s]
            COMPANION_CLIENTS.append(CompanionClient(
                name=name, api_id=api_id, api_hash=api_hash,
                session=session, sources=sources
            ))
        except Exception as e:
            log.warning(f'_load_companions: bad entry {entry}: {e}')
    if COMPANION_CLIENTS:
        log.info(f'Loaded {len(COMPANION_CLIENTS)} companion(s): '
                 f'{[c.name for c in COMPANION_CLIENTS]}')

def _save_companions():
    """Persist current companion config (sources list) back to settings.json."""
    data = _load_settings()
    data['companions'] = [
        {
            'name':     c.name,
            'api_id':   c.api_id,
            'api_hash': c.api_hash,
            'session':  c.session,
            'sources':  c.sources,
        }
        for c in COMPANION_CLIENTS
    ]
    try:
        tmp = SETTINGS_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, SETTINGS_FILE)
    except Exception as ex:
        log.warning(f'_save_companions: {ex}')

def _get_client_for_source(source_ref: str, main_user_client) -> object:
    """
    Return the TelegramClient that should be used to scrape `source_ref`.

    Checks companions first — if any running companion owns this source,
    use it.  Falls back to the main user_client.
    """
    norm = _normalize_id_for_compare(source_ref)
    for comp in COMPANION_CLIENTS:
        if not comp.running or not comp.client:
            continue
        for s in comp.sources:
            sn = _normalize_id_for_compare(s)
            if sn == norm:
                return comp.client
    return main_user_client

def _companion_owns_source(source_ref: str) -> 'CompanionClient | None':
    """Return the companion that owns this source, or None."""
    norm = _normalize_id_for_compare(source_ref)
    for comp in COMPANION_CLIENTS:
        for s in comp.sources:
            if _normalize_id_for_compare(s) == norm:
                return comp
    return None

_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(_BASE_DIR, 'ebooks.db')
DB_SCAN    = os.path.join(_BASE_DIR, 'scanbook.db')   # shadow DB for fresh scrapes
DB_SCAN_WAL= DB_SCAN + '-wal'
COLLECTION_DB = os.path.join(_BASE_DIR, 'collection.db')  # user collections
DM_DB         = os.path.join(_BASE_DIR, 'dm.db')           # tracks unlocked DM channels
# Username of the userbot account — needed for "say hello first" workaround.
# Set in settings.json → "userbot_username": "MrRobotCrew"
USERBOT_USERNAME: list[str] = [os.getenv('USERBOT_USERNAME', '')]

# ── Shadow-DB state ────────────────────────────────────────────────────────────
_shadow_lock: asyncio.Lock = None   # initialised in start_clients()
_shadow_active: list[bool] = [False]   # True while a fresh-to-shadow scrape runs

def _make_db_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=25000')
    conn.execute('PRAGMA cache_size=-32000')
    conn.execute('PRAGMA temp_store=MEMORY')
    conn.execute('PRAGMA mmap_size=33554432')
    conn.execute('PRAGMA wal_autocheckpoint=1000')
    return conn

def db_connect() -> sqlite3.Connection:
    """Connect to the live ebooks.db (bot-facing, never wiped while bot runs)."""
    return _make_db_conn(DB_PATH)

def db_scan_connect() -> sqlite3.Connection:
    """Connect to the shadow scanbook.db used during fresh full scrapes."""
    return _make_db_conn(DB_SCAN)


def _promote_shadow_db():
    """
    Atomically promote scanbook.db → ebooks.db and migrate stats/auxiliary
    tables from the old DB so no history is lost.

    Steps:
      1. Checkpoint + close WAL on shadow DB
      2. Copy download_log, search_log, bot_events, botd_history, feedback,
         book_aliases from old ebooks.db into scanbook.db (they are not part
         of the fresh book scan and must be preserved).
      3. Rename ebooks.db → ebooks.db.bak  (atomic fallback)
      4. Rename scanbook.db → ebooks.db
      5. Delete .bak
    """
    try:
        # Checkpoint shadow WAL so the file is self-contained
        conn_s = _make_db_conn(DB_SCAN)
        conn_s.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        conn_s.close()

        # Migrate stat/auxiliary tables from current ebooks.db into shadow
        if os.path.exists(DB_PATH):
            conn_old = _make_db_conn(DB_PATH)
            conn_new = _make_db_conn(DB_SCAN)
            _migrate_tables = [
                'download_log', 'search_log', 'bot_events',
                'botd_history', 'feedback', 'book_aliases', 'cleanup_queue',
            ]
            for tname in _migrate_tables:
                try:
                    rows = conn_old.execute(f'SELECT * FROM {tname}').fetchall()
                    if not rows:
                        continue
                    # Get column count from first row
                    ph = ','.join('?' * len(rows[0]))
                    # INSERT OR IGNORE to avoid PK conflicts from overlapping data
                    conn_new.executemany(
                        f'INSERT OR IGNORE INTO {tname} VALUES ({ph})', rows
                    )
                    conn_new.commit()
                    log.info(f'_promote_shadow_db: migrated {len(rows)} rows from {tname}')
                except Exception as mig_err:
                    log.warning(f'_promote_shadow_db: migrate {tname}: {mig_err}')
            conn_old.close()
            conn_new.close()

        bak_path = DB_PATH + '.bak'
        if os.path.exists(DB_PATH):
            os.rename(DB_PATH, bak_path)
        os.rename(DB_SCAN, DB_PATH)
        # Also move WAL file if it exists
        if os.path.exists(DB_SCAN_WAL):
            try:
                wal_dst = DB_PATH + '-wal'
                if os.path.exists(wal_dst):
                    os.remove(wal_dst)
                os.rename(DB_SCAN_WAL, wal_dst)
            except Exception:
                pass
        # Remove backup now that promotion succeeded
        if os.path.exists(bak_path):
            try: os.remove(bak_path)
            except Exception: pass
        log.info('_promote_shadow_db: scanbook.db promoted to ebooks.db successfully')
        return True
    except Exception as e:
        log.error(f'_promote_shadow_db FAILED: {e}')
        return False

CACHE_DIR     = os.path.join(_BASE_DIR, 'search_cache')
SETTINGS_FILE = os.path.join(_BASE_DIR, 'settings.json')
os.makedirs(CACHE_DIR, exist_ok=True)

_SEARCH_CACHE: dict[str, tuple] = {}
_SEARCH_CACHE_MAX   = 500
_SEARCH_CACHE_TTL   = 1800
_SEARCH_CACHE_LOCK  = None

# Tracks search result messages so they can be deleted when cache expires.
# {query_hash: (message_id, chat_id)}  — one entry per active search window.
_SEARCH_MSG_REGISTRY: dict[str, tuple] = {}

ASSIGNED_CHATS: dict[tuple, int] = {}
TRIGGER_CHATS:  dict[tuple, int] = {}

# ── Per-group template assignment ─────────────────────────────────────────────
# Keys match ASSIGNED_CHATS / TRIGGER_CHATS keys: (chat_id, thread_id)
# Value: template name string
GROUP_TEMPLATES: dict[tuple, str] = {}

download_cooldowns: dict[tuple, float] = {}
search_cooldowns:   dict[int, float]   = {}
_DISABLE_PENDING:   dict[int, dict]    = {}   # pending /disable confirmations keyed by admin user_id

SPAM_CFG: dict[str, list] = {
    'search_cooldown':    [3],
    'daily_dl_limit':     [30],
    'page_cooldown':      [1.5],
    'request_max':        [3],
    'request_window':     [600],
    'query_min_len':      [2],
    'query_max_len':      [100],
    'chat_rate_limit':    [20],
    'chat_rate_window':   [60],
    'flood_msgs':         [5],
    'flood_window':       [10],
    'flood_mute':         [300],
    'warn_on_cooldown':   [True],
}

def SEARCH_COOLDOWN_SECS()  -> float: return SPAM_CFG['search_cooldown'][0]
def DAILY_DL_LIMIT()        -> int:   return SPAM_CFG['daily_dl_limit'][0]
def PAGE_COOLDOWN_SECS()    -> float: return SPAM_CFG['page_cooldown'][0]
def REQUEST_COOLDOWN_MAX()  -> int:   return SPAM_CFG['request_max'][0]
def REQUEST_COOLDOWN_SECS() -> int:   return SPAM_CFG['request_window'][0]
def QUERY_MIN_LEN()         -> int:   return SPAM_CFG['query_min_len'][0]
def QUERY_MAX_LEN()         -> int:   return SPAM_CFG['query_max_len'][0]
def CHAT_RATE_LIMIT_N()     -> int:   return SPAM_CFG['chat_rate_limit'][0]
def CHAT_RATE_LIMIT_SECS()  -> int:   return SPAM_CFG['chat_rate_window'][0]
def FLOOD_MSGS()            -> int:   return SPAM_CFG['flood_msgs'][0]
def FLOOD_WINDOW_SECS()     -> int:   return SPAM_CFG['flood_window'][0]
def FLOOD_MUTE_SECS()       -> int:   return SPAM_CFG['flood_mute'][0]
def WARN_ON_COOLDOWN()      -> bool:  return SPAM_CFG['warn_on_cooldown'][0]

DAILY_DL_WINDOW = 86400

_flood_tracker:  dict[int, list[float]] = {}
_flood_muted:    dict[int, float]       = {}
_daily_dl_log:   dict[int, list[float]] = {}
_request_log:    dict[int, list[float]] = {}
_chat_rate_log:  dict[int, list[float]] = {}
_page_cooldowns: dict[int, float]       = {}

OWN_IDS: set[int] = set()

pending_requests: dict[int, tuple] = {}

_fresh_confirm: dict[int, tuple] = {}
_FRESH_CONFIRM_TTL = 60

# ─────────────────────────────────────────────────────────────────────────────
# Caption Template System
# ─────────────────────────────────────────────────────────────────────────────
#
# Template variables (replaced at render time):
#   {book_name}        — filename without extension
#   {user_mention}     — plain name OR @username (NO hyperlink, safe for all groups)
#   {brand}            — BRAND_CHANNEL[0]
#   {source}           — SOURCE_CREDIT[0]  (global credit line, e.g. "CCR Library")
#   {book_source}      — the actual source group name/username where this specific
#                        book came from (e.g. "@MyBookGroup" or "My Book Channel")
#                        Falls back to {source} if the source label is unknown.
#   {book_source_link} — same as {book_source} but as a t.me link when a username
#                        is available, e.g. [My Book Channel](t.me/MyBookGroup)
#                        Falls back to plain {book_source} if no username.
#   {purge_time}       — formatted purge duration (e.g. "10 minutes")
#   {separator}        — ─────────────────────────────
#
# Hyperlink mentions (tg://user?id=...) are ONLY used in the 'default' template
# which is never auto-assigned to sensitive groups.  All other templates use
# plain-text mentions so Telegram cannot flag them as spam links.
#

_SEP = '─' * 30

# Built-in templates — stored as raw strings with {placeholders}.
# Admins can add custom templates via /add_template.
_BUILTIN_TEMPLATES: dict[str, str] = {

    # ── default ──────────────────────────────────────────────────────────────
    # Full branding, hyperlink mention.  Safe for public/open groups.
    'default': (
        '📚 **{book_name}**\n'
        '{separator}\n'
        '👤 Delivered to: {user_mention_link}\n'
        '🌐 Powered by: {brand}\n'
        '📦 Source: {book_source}\n'
        '{separator}\n'
        '⏳ _This file will be deleted in {purge_time}._\n'
        '💡 _আরও বই খুঁজতে লিখো `.বই <নাম>` বা `.boi <n>`_'
    ),

    # ── dm ────────────────────────────────────────────────────────────────────
    # Used for private/DM deliveries by default.
    # Set a different one via /set_dm_template or settings.json → "dm_template"
    'dm': (
        '📚 **{book_name}**\n'
        '{separator}\n'
        '📦 Source: {book_source_link}\n'
        '🌐 {brand}\n'
        '{separator}\n'
        '⏳ _Auto-deletes in {purge_time}_\n'
        '💡 _আরও বই পেতে `.বই <নাম>` লিখো_'
    ),

    # ── minimal ──────────────────────────────────────────────────────────────
    # Only book name + plain requester mention.
    # Perfect for sensitive groups where links can trigger bans.
    'minimal': (
        '📚 **{book_name}**\n'
        '👤 Requested by: {user_mention}'
    ),

    # ── branded ──────────────────────────────────────────────────────────────
    # Brand + source visible, but plain-text mention (no hyperlink).
    'branded': (
        '📚 **{book_name}**\n'
        '{separator}\n'
        '👤 For: {user_mention}\n'
        '🌐 {brand} • 📦 {source}\n'
        '{separator}\n'
        '⏳ _Deletes in {purge_time}._'
    ),

    # ── silent ───────────────────────────────────────────────────────────────
    # Absolute bare minimum: just book name, no mention, no branding.
    # Use for ultra-sensitive groups.
    'silent': (
        '📚 **{book_name}**\n'
        '⏳ _{purge_time} auto-delete_'
    ),

    # ── clean ─────────────────────────────────────────────────────────────────
    # Nice formatting, brand visible, plain mention, no source link.
    'clean': (
        '📖 **{book_name}**\n'
        '{separator}\n'
        '✅ Delivered to {user_mention}\n'
        '🏷 {brand}\n'
        '⏳ _Auto-deletes in {purge_time}_'
    ),

    # ── request_style ─────────────────────────────────────────────────────────
    # Looks like a fulfilled request — clear "requested by" language.
    'request_style': (
        '📬 **Your book is here!**\n'
        '{separator}\n'
        '📖 {book_name}\n'
        '👤 Requested by: {user_mention}\n'
        '{separator}\n'
        '⏳ _File auto-deletes in {purge_time}_\n'
        '💡 _আরও বই খুঁজতে লিখো `.বই <নাম>`_'
    ),

    # ── no_brand ──────────────────────────────────────────────────────────────
    # No branding at all, plain mention, no links anywhere.
    'no_brand': (
        '📚 **{book_name}**\n'
        '👤 For: {user_mention}\n'
        '⏳ _Deletes in {purge_time}_'
    ),

    # ── boi_mohol ─────────────────────────────────────────────────────────────
    # Aesthetic / girly style for Boi Mohol group 🌸
    # Note: {user_mention} is the @username or first-name link — kept on its
    # own line so long fancy names don't break the layout.
    'boi_mohol': (
        '╭── 🌸 বই মহল 🌸 ──╮\n'
        '📖 **{book_name}**\n'
        '╰─────── ──────╯\n'
        '🎀 {user_full_mention}\n'
        'তোমার বইটা এসে গেছে ✨💕\n'
        '\n'
        '⏳ {purge_time} পরে মুছে যাবে 🍂\n'
        '🌼 @Boi_Mohol · `.বই <নাম>` লিখে খোঁজো'
    ),
}

# Custom templates added by admins at runtime — persisted in settings.json
_CUSTOM_TEMPLATES: dict[str, str] = {}


def get_template(name: str) -> str | None:
    """Return template text by name (custom first, then builtin). None if not found."""
    return _CUSTOM_TEMPLATES.get(name) or _BUILTIN_TEMPLATES.get(name)


def list_templates() -> list[str]:
    """Return sorted list of all template names."""
    names = set(_BUILTIN_TEMPLATES) | set(_CUSTOM_TEMPLATES)
    return sorted(names)


def _safe_mention(user_id: int, first_name: str, username: str | None) -> str:
    """
    Return a safe plain-text user reference — no hyperlinks, no tg:// URIs.

    Format:
      - Has @username  →  FirstName (@username)
      - No username    →  FirstName

    This is deliberately NOT a hyperlink.  Clickable tg://user?id=... mentions
    in caption text can trigger Telegram's anti-spam scanner in some groups,
    resulting in the bot being banned.  Plain text is always safe.
    """
    name = (first_name or 'User').strip()
    if username:
        return f'{name} (@{username})'
    return name


def _hyperlink_mention(user_id: int, first_name: str) -> str:
    """Return a Markdown hyperlink mention using first name only."""
    return f'[{first_name or "User"}](tg://user?id={user_id})'


def _full_name_mention(user_id: int, first_name: str, last_name=None) -> str:
    """Hyperlink with full name (first + last). Use as {user_full_mention} in templates."""
    full = (first_name or "User").strip()
    if last_name and str(last_name).strip():
        full = f"{full} {str(last_name).strip()}"
    return f'[{full}](tg://user?id={user_id})'


def _resolve_book_source(src_chat_id, book_id=None) -> tuple[str, str]:
    """
    Return (plain_label, link_label) for the ORIGINAL source of a book.

    plain_label  — e.g. "@mybookchannel"  or  "My Book Channel"
    link_label   — "[plain_label](t.me/username)" when username is known,
                   plain_label otherwise

    Resolution order:
      1. books.orig_chat_id  — the original source channel, never overwritten by backup
      2. src_chat_id arg     — current chat_id (may be backup group after backup)
      3. scrape_progress.scrape_label for whichever chat was resolved
      4. SOURCE_CREDIT global fallback
    """
    fallback = SOURCE_CREDIT[0] or 'CCR Library'

    # Step 1: if book_id given, prefer orig_chat_id from books table
    resolved_chat_id = src_chat_id
    if book_id:
        try:
            conn = db_connect(); c = conn.cursor()
            c.execute('SELECT orig_chat_id, chat_id FROM books WHERE id=?', (int(book_id),))
            row = c.fetchone(); conn.close()
            if row:
                orig, curr = row[0], row[1]
                # Use orig_chat_id if set and not pointing to backup group
                if orig and orig != 0 and orig != BACKUP_GROUP_ID[0]:
                    resolved_chat_id = orig
                elif curr and BACKUP_GROUP_ID[0] and curr != BACKUP_GROUP_ID[0]:
                    resolved_chat_id = curr
                elif curr and not BACKUP_GROUP_ID[0]:
                    resolved_chat_id = curr
        except Exception:
            pass

    if not resolved_chat_id:
        return fallback, fallback

    # Step 2: look up scrape_label for resolved_chat_id
    try:
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT scrape_label FROM scrape_progress WHERE chat_id=?', (int(resolved_chat_id),))
        row = c.fetchone(); conn.close()
        label = (row[0] or '').strip() if row else ''
    except Exception:
        label = ''

    if not label:
        return fallback, fallback

    # Build link: if label looks like a username (no spaces, alnum+underscore), linkify it
    uname = label.lstrip('@').strip()
    if ' ' not in uname and uname and all(c2.isalnum() or c2 == '_' for c2 in uname):
        plain = f'@{uname}'
        link  = f'[{plain}](t.me/{uname})'
    else:
        plain = label
        link  = label
    return plain, link


def render_caption(
    template_name: str,
    fname: str,
    user_id: int,
    first_name: str,
    username: str | None,
    purge_secs: int = 600,
    last_name=None,
    src_chat_id=None,
    book_id=None,
) -> str:
    """
    Render a caption from the named template.

    src_chat_id — current chat_id of the book (may be backup group)
    book_id     — if given, used to look up orig_chat_id for accurate {book_source}
    """
    tmpl = get_template(template_name) or get_template('default')
    book_name = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', fname, flags=re.IGNORECASE).strip()
    book_name = re.sub(r'[_\-+\[\]()\.\{\}]', ' ', book_name).strip()

    purge_time = _fmt_purge(purge_secs)
    book_source_plain, book_source_link = _resolve_book_source(src_chat_id, book_id=book_id)

    return tmpl.format(
        book_name         = book_name,
        user_mention      = _safe_mention(user_id, first_name, username),
        user_mention_link = _hyperlink_mention(user_id, first_name),
        user_full_mention = _full_name_mention(user_id, first_name, last_name),
        brand             = BRAND_CHANNEL[0],
        source            = SOURCE_CREDIT[0],
        book_source       = book_source_plain,
        book_source_link  = book_source_link,
        purge_time        = purge_time,
        separator         = _SEP,
    )


def get_group_template(chat_id, thread_id) -> str:
    """Return the template name assigned to this chat/thread, or 'default'."""
    if thread_id and (chat_id, thread_id) in GROUP_TEMPLATES:
        return GROUP_TEMPLATES[(chat_id, thread_id)]
    if (chat_id, None) in GROUP_TEMPLATES:
        return GROUP_TEMPLATES[(chat_id, None)]
    return 'default'


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _normalize_chat_id(raw: str):
    s = str(raw).strip()
    try: return int(s)
    except ValueError: return s

def _is_numeric_id(s: str) -> bool:
    return s.lstrip('-').isdigit()

def _normalize_id_for_compare(x) -> int | str:
    s = str(x).strip()
    try:
        n = int(s)
        if n < 0:
            abs_str = str(abs(n))
            if abs_str.startswith('100') and len(abs_str) > 10:
                return int(abs_str[3:])
            return abs(n)
        return n
    except ValueError:
        return s.lower().lstrip('@')

def _to_full_channel_id(bare_id: int) -> int:
    if bare_id <= 0:
        return bare_id
    return int(f'-100{bare_id}')

async def _resolve_source(user_client, src: str):
    src = str(src).strip()

    if _is_numeric_id(src):
        chat_id = int(src)
        try:
            return await user_client.get_entity(chat_id)
        except Exception:
            pass

        from telethon.tl.types import PeerChannel, PeerChat
        bare = _normalize_id_for_compare(chat_id)
        try:
            entity = await user_client.get_entity(PeerChannel(int(bare)))
            return entity
        except Exception:
            pass
        try:
            entity = await user_client.get_entity(PeerChat(int(bare)))
            return entity
        except Exception:
            pass

        class _MinimalEntity:
            is_minimal = True
            def __init__(self, cid):
                raw = _normalize_id_for_compare(str(cid))
                self.id         = raw if isinstance(raw, int) else int(str(cid).lstrip('-'))
                self.username   = None
                self.title      = str(cid)
                self.noforwards = False
                self._full_id   = cid
        try:
            async for _ in user_client.iter_messages(chat_id, limit=1):
                break
            log.info(f'_resolve_source: MinimalEntity fallback for {chat_id}')
            return _MinimalEntity(chat_id)
        except Exception as e3:
            raise Exception(
                f'Cannot reach chat {src}. '
                f'The user account must JOIN this channel/group before scraping. '
                f'Error: {e3}'
            )
    else:
        username = src.lstrip('@')
        try:
            return await user_client.get_entity(username)
        except Exception as e:
            raise Exception(f'Cannot resolve username "{src}": {e}')

def _tid(raw: str):
    s = raw.strip()
    if not s or s == '0': return None
    try: return int(s)
    except: return None

def _parse_env_chats(env_key: str, reg: dict):
    for part in os.getenv(env_key,'').split(','):
        part = part.strip()
        if not part: continue
        bits = part.split(':')
        try:
            cid  = _normalize_chat_id(bits[0].strip())
            tid  = _tid(bits[1]) if len(bits) > 1 else None
            psec = _parse_purge(bits[2].strip()) if len(bits) > 2 and bits[2].strip() else 72*3600
            reg[(cid, tid)] = psec
        except Exception: pass

def _load_settings():
    if not os.path.exists(SETTINGS_FILE): return {}
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except Exception as ex:
        log.warning(f'load settings.json: {ex}')
        return {}

def _save_settings():
    # Load whatever is on disk (preserves keys we don't manage)
    existing: dict = {}
    try:
        with open(SETTINGS_FILE) as f:
            existing = json.load(f)
    except Exception:
        pass

    # Only overwrite the keys this code owns — never touch unknown keys
    existing['backup_group_id'] = BACKUP_GROUP_ID[0]
    existing['analytics_group'] = ANALYTICS_GROUP[0]
    existing['request_group']   = REQUEST_GROUP[0]
    existing['brand_channel']   = BRAND_CHANNEL[0]
    existing['source_credit']   = SOURCE_CREDIT[0]
    existing['sources']         = SOURCE_GROUPS
    existing['search_mode']     = SEARCH_MODE[0]
    existing['dm_purge_secs']          = DM_PURGE_SECS_REF[0]
    existing['dm_template']             = DM_TEMPLATE_REF[0]
    existing['search_result_purge_secs'] = SEARCH_RESULT_PURGE_SECS[0]
    existing['userbot_username']        = USERBOT_USERNAME[0]
    existing['assigned_chats']  = [
        {'chat_id': c, 'thread_id': t, 'purge_seconds': p}
        for (c, t), p in ASSIGNED_CHATS.items()
    ]
    existing['trigger_chats']   = [
        {'chat_id': c, 'thread_id': t, 'purge_seconds': p}
        for (c, t), p in TRIGGER_CHATS.items()
    ]
    existing['extra_admins']    = list(ADMIN_IDS - {OWNER_ID})
    existing['spam_cfg']        = {k: v[0] for k, v in SPAM_CFG.items()}
    existing['custom_templates'] = _CUSTOM_TEMPLATES
    existing['group_templates'] = [
        {'chat_id': c, 'thread_id': t, 'template': tmpl}
        for (c, t), tmpl in GROUP_TEMPLATES.items()
    ]
    existing['keyword_alerts']  = {kw: v for kw, v in KEYWORD_ALERTS.items()}
    existing['vip_users']       = list(VIP_USERS)
    existing['vip_custom_limits'] = {str(k): v for k, v in VIP_CUSTOM_LIMITS.items()}
    existing['vip_perms']        = dict(VIP_PERMS)
    existing['vip_card_style']   = dict(VIP_CARD_STYLE)
    existing['book_aliases']    = {str(k): v for k, v in BOOK_ALIASES.items()}
    existing['broadcast_report_chats'] = [
        {'chat_id': c, 'thread_id': t, 'daily': v.get('daily', True), 'weekly': v.get('weekly', True)}
        for (c, t), v in BROADCAST_REPORT_CHATS.items()
    ]
    existing['botd_chats'] = [
        {'chat_id': c, 'thread_id': t}
        for (c, t) in BOTD_CHATS
    ]
    if 'report_time' not in existing:
        existing['report_time'] = REPORT_TIME_UTC[0]  # write default if missing
    else:
        existing['report_time'] = REPORT_TIME_UTC[0]
    if 'report_timezone' not in existing:
        existing['report_timezone'] = REPORT_TZ_NAME[0]
    else:
        existing['report_timezone'] = REPORT_TZ_NAME[0]

    try:
        tmp = SETTINGS_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        os.replace(tmp, SETTINGS_FILE)
    except Exception as ex:
        log.warning(f'save settings.json: {ex}')

def _apply_settings(data: dict):
    if 'backup_group_id' in data and data['backup_group_id']:
        BACKUP_GROUP_ID[0] = int(data['backup_group_id'])
    if 'analytics_group' in data:
        ANALYTICS_GROUP[0] = int(data['analytics_group'])
    if 'request_group' in data and data['request_group']:
        REQUEST_GROUP[0] = int(data['request_group'])
    if 'brand_channel' in data and data['brand_channel']:
        BRAND_CHANNEL[0] = data['brand_channel']
    if 'source_credit' in data and data['source_credit']:
        SOURCE_CREDIT[0] = data['source_credit']
    for s in data.get('sources', []):
        if s and s not in SOURCE_GROUPS:
            SOURCE_GROUPS.append(s)
    if 'search_mode' in data:
        SEARCH_MODE[0] = data['search_mode']
    if 'dm_purge_secs' in data:
        DM_PURGE_SECS_REF[0] = int(data['dm_purge_secs'])
    if 'dm_template' in data and data['dm_template']:
        DM_TEMPLATE_REF[0] = str(data['dm_template']).strip()
    if 'search_result_purge_secs' in data:
        try: SEARCH_RESULT_PURGE_SECS[0] = max(0, int(data['search_result_purge_secs']))
        except Exception: pass
    if 'userbot_username' in data and data['userbot_username']:
        USERBOT_USERNAME[0] = str(data['userbot_username']).strip().lstrip('@')
    for e in data.get('assigned_chats', []):
        raw = e.get('purge_seconds') or e.get('purge_hours', 72)
        psec = raw * 3600 if isinstance(raw, int) and raw <= 168 else int(raw)
        ASSIGNED_CHATS[(e['chat_id'], e.get('thread_id'))] = psec
    for e in data.get('trigger_chats', []):
        raw = e.get('purge_seconds') or e.get('purge_hours', 72)
        psec = raw * 3600 if isinstance(raw, int) and raw <= 168 else int(raw)
        TRIGGER_CHATS[(e['chat_id'], e.get('thread_id'))] = psec
    for aid in data.get('extra_admins', []):
        try:
            ADMIN_IDS.add(int(aid))
            ALL_STAFF.add(int(aid))
        except Exception: pass
    for k, raw_val in data.get('spam_cfg', {}).items():
        if k not in SPAM_CFG:
            continue
        try:
            current = SPAM_CFG[k][0]
            if isinstance(current, bool):
                SPAM_CFG[k][0] = bool(raw_val)
            elif isinstance(current, float):
                SPAM_CFG[k][0] = float(raw_val)
            else:
                SPAM_CFG[k][0] = int(raw_val)
        except Exception:
            pass
    # ── Restore template system ────────────────────────────────────────────
    for name, text in data.get('custom_templates', {}).items():
        _CUSTOM_TEMPLATES[name] = text
    for e in data.get('group_templates', []):
        try:
            GROUP_TEMPLATES[(e['chat_id'], e.get('thread_id'))] = e['template']
        except Exception:
            pass
    # Keyword alerts
    for kw, targets in data.get('keyword_alerts', {}).items():
        KEYWORD_ALERTS[kw.lower().strip()] = [tuple(t) if isinstance(t, list) else t for t in targets]
    # VIP users
    for uid in data.get('vip_users', []):
        try: VIP_USERS.add(int(uid))
        except: pass
    # VIP custom limits
    for uid_str, lim in data.get('vip_custom_limits', {}).items():
        try: VIP_CUSTOM_LIMITS[int(uid_str)] = int(lim)
        except: pass
    # VIP permissions
    for perm, val in data.get('vip_perms', {}).items():
        if perm in VIP_PERMS:
            VIP_PERMS[perm] = bool(val)
    # VIP card style
    for k, v in data.get('vip_card_style', {}).items():
        if k in VIP_CARD_STYLE:
            VIP_CARD_STYLE[k] = str(v)
    # Book aliases
    for bid_str, aliases in data.get('book_aliases', {}).items():
        try: BOOK_ALIASES[int(bid_str)] = aliases
        except: pass
    # Broadcast report chats
    for e in data.get('broadcast_report_chats', []):
        try:
            key = (e['chat_id'], e.get('thread_id'))
            BROADCAST_REPORT_CHATS[key] = {'daily': e.get('daily', True), 'weekly': e.get('weekly', True)}
        except: pass
    # Book of the day chats
    for e in data.get('botd_chats', []):
        try: BOTD_CHATS[(e['chat_id'], e.get('thread_id'))] = True
        except: pass
    # Report time + timezone
    if 'report_time' in data and data['report_time']:
        REPORT_TIME_UTC[0] = str(data['report_time']).strip()
    if 'report_timezone' in data and data['report_timezone']:
        REPORT_TZ_NAME[0] = str(data['report_timezone']).strip()
    # Auto-scrape interval — configurable via JSON (overrides env var)
    if 'auto_scrap_interval_h' in data:
        try:
            new_interval = int(data['auto_scrap_interval_h'])
            if new_interval != AUTO_SCRAP_INTERVAL_H[0]:
                log.info(f'Auto-scrape interval changed: {AUTO_SCRAP_INTERVAL_H[0]}h → {new_interval}h')
            AUTO_SCRAP_INTERVAL_H[0] = new_interval
        except Exception:
            pass
    # Companions — load on startup only (not hot-reloaded; restarts needed for new companions)
    if 'companions' in data and not COMPANION_CLIENTS:
        _load_companions(data)

# ─────────────────────────────────────────────────────────────────────────────
# Scrape hang watchdog — auto-cancels if scrape has been running > 4 h with
# no progress (checked every 5 min).  Prevents permanent "stuck" state.
# ─────────────────────────────────────────────────────────────────────────────
_SCRAP_WATCHDOG_TIMEOUT = 4 * 3600   # 4 hours max per job
_SCRAP_WATCHDOG_INTERVAL = 300        # check every 5 minutes
_scrap_last_progress: list[float] = [0.0]  # updated on each msg check

async def _scrape_watchdog():
    """Cancel a scrape that hasn't made progress in _SCRAP_WATCHDOG_TIMEOUT seconds."""
    await asyncio.sleep(60)
    log.info('Scrape watchdog started')
    while True:
        await asyncio.sleep(_SCRAP_WATCHDOG_INTERVAL)
        if not _scrap_running[0]:
            _scrap_last_progress[0] = 0.0
            continue
        if _scrap_last_progress[0] == 0.0:
            _scrap_last_progress[0] = time.time()
            continue
        stall = time.time() - _scrap_last_progress[0]
        if stall > _SCRAP_WATCHDOG_TIMEOUT:
            log.error(f'Scrape watchdog: no progress for {int(stall)}s — force-cancelling')
            _scrap_cancel[0] = True
            _scrap_running[0] = False
            _scrap_current[0] = ''
            try:
                await report(
                    f'⚠️ **Scrape auto-cancelled by watchdog**\n'
                    f'No progress for `{int(stall)//3600}h {(int(stall)%3600)//60}m`.\n'
                    f'Use `/scrap` to restart manually.'
                )
            except Exception:
                pass
            _scrap_last_progress[0] = 0.0

# ─────────────────────────────────────────────────────────────────────────────
# JSON hot-reload watchdog
# Polls settings.json every 15 s; if mtime changed, reloads and notifies
# analytics group with a diff of what changed.
# ─────────────────────────────────────────────────────────────────────────────
_SETTINGS_MTIME: list[float] = [0.0]

def _snapshot_settings() -> dict:
    """Capture current in-memory state as a plain dict for diffing."""
    return {
        'backup_group_id':          BACKUP_GROUP_ID[0],
        'analytics_group':          ANALYTICS_GROUP[0],
        'request_group':            REQUEST_GROUP[0],
        'brand_channel':            BRAND_CHANNEL[0],
        'source_credit':            SOURCE_CREDIT[0],
        'sources':                  list(SOURCE_GROUPS),
        'search_mode':              SEARCH_MODE[0],
        'dm_purge_secs':            DM_PURGE_SECS_REF[0],
        'dm_template':              DM_TEMPLATE_REF[0],
        'search_result_purge_secs': SEARCH_RESULT_PURGE_SECS[0],
        'auto_scrap_interval_h':    AUTO_SCRAP_INTERVAL_H[0],
        'userbot_username':         USERBOT_USERNAME[0],
        'spam_cfg':                 {k: v[0] for k, v in SPAM_CFG.items()},
        'extra_admins':             list(ADMIN_IDS - {OWNER_ID}),
        'keyword_alerts':           list(KEYWORD_ALERTS.keys()),
        'vip_users':                list(VIP_USERS),
        'vip_custom_limits':        dict(VIP_CUSTOM_LIMITS),
        'vip_perms':                dict(VIP_PERMS),
        'report_time':              REPORT_TIME_UTC[0],
        'broadcast_chats':          len(BROADCAST_REPORT_CHATS),
        'botd_chats':               len(BOTD_CHATS),
        'assigned_chats':           len(ASSIGNED_CHATS),
        'trigger_chats':            len(TRIGGER_CHATS),
        'group_templates':          dict(GROUP_TEMPLATES),
        'custom_templates':         list(_CUSTOM_TEMPLATES.keys()),
        'companions':               [(c.name, list(c.sources)) for c in COMPANION_CLIENTS],
    }

async def _settings_watchdog():
    """Background task: detect external edits to settings.json and hot-reload."""
    await asyncio.sleep(5)  # let startup settle
    _SETTINGS_MTIME[0] = os.path.getmtime(SETTINGS_FILE) if os.path.exists(SETTINGS_FILE) else 0.0
    log.info('Settings watchdog started (poll every 15s)')
    while True:
        await asyncio.sleep(15)
        try:
            if not os.path.exists(SETTINGS_FILE):
                continue
            mtime = os.path.getmtime(SETTINGS_FILE)
            if mtime <= _SETTINGS_MTIME[0]:
                continue
            _SETTINGS_MTIME[0] = mtime

            # Read new data
            try:
                with open(SETTINGS_FILE) as f:
                    new_data = json.load(f)
            except Exception as ex:
                log.warning(f'Watchdog: settings.json parse error: {ex}')
                await report(f'⚠️ **settings.json edit detected but JSON is invalid!**\n`{ex}`\n_Fix the file and save again._')
                continue

            before = _snapshot_settings()

            # Clear mutable collections before re-applying
            SOURCE_GROUPS.clear()
            ASSIGNED_CHATS.clear()
            TRIGGER_CHATS.clear()
            GROUP_TEMPLATES.clear()
            _CUSTOM_TEMPLATES.clear()
            KEYWORD_ALERTS.clear()
            VIP_USERS.clear()
            VIP_CUSTOM_LIMITS.clear()
            BOOK_ALIASES.clear()
            BROADCAST_REPORT_CHATS.clear()
            BOTD_CHATS.clear()
            for k in SPAM_CFG:
                pass  # keep keys, values reset by _apply_settings

            _apply_settings(new_data)

            # Hot-reload companion source assignments (not new companions — those need restart)
            if 'companions' in new_data:
                for entry in new_data.get('companions', []):
                    new_name    = str(entry.get('name', '')).strip()
                    new_sources = [str(s).strip() for s in entry.get('sources', [])]
                    for comp in COMPANION_CLIENTS:
                        if comp.name == new_name and comp.sources != new_sources:
                            old_srcs = list(comp.sources)
                            comp.sources = new_sources
                            log.info(f'Watchdog: updated companion "{comp.name}" sources: '
                                     f'{old_srcs} → {new_sources}')

            after = _snapshot_settings()

            # Build human-readable diff
            diff_lines = []
            for key in sorted(set(before) | set(after)):
                bv, av = before.get(key), after.get(key)
                if bv != av:
                    diff_lines.append(f'• `{key}`: `{bv}` → `{av}`')

            src_before = before.get('sources', [])
            src_after  = after.get('sources', [])
            added_src  = [s for s in src_after  if s not in src_before]
            removed_src= [s for s in src_before if s not in src_after]
            if added_src:
                diff_lines.append(f'• Sources added: `{", ".join(added_src)}`')
            if removed_src:
                diff_lines.append(f'• Sources removed: `{", ".join(removed_src)}`')

            diff_text = '\n'.join(diff_lines) if diff_lines else '_No tracked changes detected_'
            log.info(f'settings.json hot-reloaded. Changes: {len(diff_lines)}')

            await report(
                f'🔄 **settings.json reloaded** _(external edit detected)_\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'{diff_text}\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'🕐 {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
            )
        except Exception as ex:
            log.warning(f'Settings watchdog error: {ex}')


_BN_VOWEL_MARKS = set('ািীুূেৈোৌৃঁংঃ্')
_RE_BENGALI      = re.compile('[\u0980-\u09FF]')
# Vowel marks only — NOT nukta (়) which changes consonant identity
_RE_STRIP_VOWELS = re.compile('[ািীুূেৈোৌৃঁংঃ্]')
_RE_STRIP_SERIAL = re.compile(r'^[\d\u09e6-\u09ef\s\.]+')

# ── Pre-compiled normalisation regexes ────────────────────────────────────────
_RE_NORM_PUNCT = re.compile(
    r'[_\-+\[\]()\.\{\}/\\*\^$!?@#%&;:,\'\"` ~<>|='
    '\u0964\u0965'   # Bengali danda / double danda
    '\u2013\u2014'   # en-dash / em-dash
    '\u2018\u2019\u201c\u201d'
    '\u00b7\u2022\u25a0\uff5c'
    ']'
)
_RE_NORM_SPACE = re.compile(r'\s+')

# ── Orthographic variant pairs  (A ↔ B) ──────────────────────────────────────
# Kept SHORT and HIGH-CONFIDENCE only. Huge tables → exponential variant explosion.
_BN_VARIANTS: list[tuple[str, str]] = [
    # Consonant/cluster
    ('য়',           'য'),
    ('ক্ষ',          'খ'),
    # Vowel ি/ী ু/ূ — the single most common user error
    ('ি',            'ী'),
    ('ু',            'ূ'),
    # ণ/ন ষ/স শ/স — extremely common confusion
    ('ণ',            'ন'),
    ('ষ',            'স'),
    ('শ',            'স'),
    # Author names — only most common ones
    ('রবীন্দ্রনাথ', 'রবিন্দ্রনাথ'),
    ('রবীন্দ্র',     'রবিন্দ্র'),
    ('বিভূতিভূষণ',  'বিভুতিভূষণ'),
    ('শরৎচন্দ্র',   'শরতচন্দ্র'),
    ('জীবনানন্দ',   'জিবনানন্দ'),
    ('হুমায়ূন',     'হুমায়ুন'),
    ('সুনীল',        'সুনিল'),
    ('মানিক',        'মাণিক'),
    ('বাংলা',        'বাঙলা'),
    # Common collection/volume words
    ('সমগ্র',        'রচনাসমগ্র'),
    ('রচনাবলি',     'রচনাবলী'),
]

# ── Latin → Bengali transliteration (for ASCII queries) ───────────────────────
_BN_TRANSLIT: list[tuple[str, str]] = [
    ('রবীন্দ্রনাথ', 'rabindranath'),
    ('রবীন্দ্রনাথ', 'rabindra nath'),
    ('রবীন্দ্র',    'rabindra'),
    ('ঠাকুর',        'tagore'),
    ('শরৎচন্দ্র',   'sarat chandra'),
    ('শরৎচন্দ্র',   'sharatchandra'),
    ('বিভূতিভূষণ',  'bibhutibhushan'),
    ('বঙ্কিমচন্দ্র','bankimchandra'),
    ('জীবনানন্দ',   'jibanananda'),
    ('মানিক',        'manik'),
    ('হুমায়ূন',     'humayun'),
    ('আহমেদ',        'ahmed'),
    ('সুনীল',        'sunil'),
    ('সমরেশ',        'samaresh'),
    ('বাংলা',        'bangla'),
    ('বাংলা',        'bengali'),
    ('সমগ্র',        'samagra'),
    ('রচনাবলী',      'rachanabali'),
]

# ── In-process backfill state — only run once per process lifetime ─────────────
_stripped_backfill_done: list[bool] = [False]


def _strip_bn_vowels(text: str) -> str:
    return _RE_STRIP_VOWELS.sub('', text)


def _bn_variants(text: str) -> list[str]:
    """
    Generate spelling variants of a single Bengali word/phrase.
    Capped at 8 variants to prevent exponential blowup on long queries.
    """
    variants: set[str] = {text}
    for src, dst in _BN_VARIANTS:
        for v in list(variants):
            if src in v:
                variants.add(v.replace(src, dst))
            if dst in v:
                variants.add(v.replace(dst, src))
            if len(variants) >= 8:
                break
        if len(variants) >= 8:
            break
    return list(variants)


def _transliterate_query(query: str) -> list[str]:
    """Map ASCII query to candidate Bengali strings. Returns empty if already Bengali."""
    q_low = query.lower().strip()
    if _RE_BENGALI.search(q_low):
        return []
    q_norm = q_low.replace(' ', '')
    results = []
    seen: set[str] = set()
    for bn, lat in _BN_TRANSLIT:
        lat_norm = lat.lower().replace(' ', '')
        if lat_norm in q_norm or q_norm in lat_norm:
            if bn not in seen:
                results.append(bn); seen.add(bn)
        elif ' ' in lat and all(w in q_low for w in lat.split()):
            if bn not in seen:
                results.append(bn); seen.add(bn)
    return results


def _strip_serial_prefix(name: str) -> str:
    return _RE_STRIP_SERIAL.sub('', name).strip()


def _ensure_stripped_backfill(conn: sqlite3.Connection):
    """
    One-time background backfill of stripped_name for any books that are missing it.
    Runs at most once per process lifetime to avoid per-query overhead.
    """
    if _stripped_backfill_done[0]:
        return
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM books WHERE stripped_name='' OR stripped_name IS NULL")
        missing = c.fetchone()[0]
        if missing > 0:
            log.info(f'[search] backfilling stripped_name for {missing} books…')
            c.execute("SELECT id, search_name FROM books WHERE stripped_name='' OR stripped_name IS NULL")
            rows = c.fetchall()
            conn.executemany(
                'UPDATE books SET stripped_name=? WHERE id=?',
                [(_RE_STRIP_VOWELS.sub('', (sn or '')), bid) for bid, sn in rows]
            )
            conn.commit()
            log.info(f'[search] stripped_name backfill done ({missing} rows)')
        _stripped_backfill_done[0] = True
    except Exception as e:
        log.warning(f'[search] backfill error: {e}')


# ─────────────────────────────────────────────────────────────────────────────
# normalize_name  — MUST NOT strip Bengali nukta (়) or hasanta (্)
# Those are part of consonant identity, not decoration.
# ─────────────────────────────────────────────────────────────────────────────
def normalize_name(name: str) -> str:
    if not name:
        return ''
    name = os.path.splitext(name)[0]
    name = _RE_NORM_PUNCT.sub(' ', name)
    return _RE_NORM_SPACE.sub(' ', name).strip().lower()


# ── Stopwords: never use these as FTS AND terms (they match everything) ───────
_STOPWORDS = frozenset(['the','a','an','of','in','on','at','to','by','for',
                        'and','or','is','it','its','as','be','was','are',
                        'with','from','this','that','how','এর','এবং','বা','ও'])

# ── Numeric token pattern — FTS5 unicode61 doesn't index pure numbers ─────────
_RE_PURE_NUMERIC = re.compile(r'^\d+$')

def _is_fts_indexable(word: str) -> bool:
    """Return False for tokens FTS5 unicode61 would skip (pure numbers, stopwords)."""
    if not word:
        return False
    # FTS5 unicode61 DOES index numbers in newer SQLite builds, but to be safe
    # we handle numeric-containing words separately via file_name LIKE scan
    return True   # we keep all words in OR but handle AND specially


# ─────────────────────────────────────────────────────────────────────────────
# _score_result  — deterministic integer 0-100
# ─────────────────────────────────────────────────────────────────────────────
def _score_result(norm_name: str, query_words: list[str],
                  strip_words: list[str], norm_query: str) -> int:
    if not query_words or not norm_name:
        return 0

    sname   = _strip_bn_vowels(norm_name)
    sq      = _strip_bn_vowels(norm_query)
    serial  = _strip_serial_prefix(norm_name)
    sserial = _strip_bn_vowels(serial)

    # Split name into individual words for proper word-boundary matching
    name_words = set(norm_name.split())
    sname_words = set(sname.split())

    # ── Tier 1: exact ─────────────────────────────────────────────────────────
    if norm_name == norm_query:                                   return 100
    if serial    == norm_query and serial:                        return 96
    if sserial   == sq         and sq:                            return 92
    if sname     == sq         and sq:                            return 90

    # ── Tier 2: starts-with ───────────────────────────────────────────────────
    if norm_name.startswith(norm_query):                          return 85
    if serial.startswith(norm_query) and serial:                  return 83
    if sq and sserial.startswith(sq):                             return 78
    if sq and sname.startswith(sq):                               return 76

    # ── Tier 3: contains full query as substring ──────────────────────────────
    if norm_query in norm_name:                                   return 72
    if serial and norm_query in serial:                           return 70
    if sq and sq in sserial:                                      return 62
    if sq and sq in sname:                                        return 58

    # ── Tier 4: all meaningful words present (order-independent) ─────────────
    # Filter out stopwords for matching — "the 48 laws of power" should match
    # "48 laws of power" because "the" and "of" are stopwords
    meaningful_qwords = [w for w in query_words if w not in _STOPWORDS]
    if not meaningful_qwords:
        meaningful_qwords = query_words  # all were stopwords, use all

    total = len(meaningful_qwords)

    # Word-boundary match (split) — much more accurate than substring
    em_split = sum(1 for w in meaningful_qwords if w in name_words)
    # Substring match as fallback (catches partial words like "law" in "laws")
    em_sub   = sum(1 for w in meaningful_qwords if w in norm_name)
    sm_split = sum(1 for w in [_strip_bn_vowels(w) for w in meaningful_qwords] if w in sname_words)
    sm_sub   = sum(1 for w in [_strip_bn_vowels(w) for w in meaningful_qwords] if w in sname)

    best_split = max(em_split, sm_split)
    best_sub   = max(em_sub,   sm_sub)

    if best_split == total:
        # All meaningful words present as whole words — very strong match
        longest = max((len(w) for w in meaningful_qwords if w in name_words), default=0)
        return min(68 + min(longest, 6), 80)

    if best_sub == total:
        # All meaningful words present as substrings
        longest = max((len(w) for w in meaningful_qwords if w in norm_name), default=0)
        return min(55 + min(longest, 8), 68)

    if best_split == 0 and best_sub == 0:
        return 0

    # Partial match — score proportionally, prefer split matches
    best = max(best_split * 1.2, best_sub)
    ratio = min(best / total, 1.0)
    longest = max(
        (len(w) for w in meaningful_qwords if w in norm_name),
        default=max((len(w) for w in [_strip_bn_vowels(w) for w in meaningful_qwords] if w in sname), default=0)
    )
    return max(1, int(ratio * 45) + min(longest, 5))


# ─────────────────────────────────────────────────────────────────────────────
# smart_search  — the main search function, runs in _SEARCH_EXECUTOR thread
# ─────────────────────────────────────────────────────────────────────────────
MAX_RESULTS = 100
PER_PAGE    = 5

def smart_search(query: str) -> list[tuple]:
    t0 = time.monotonic()

    norm_query  = normalize_name(query)
    query_words = [w for w in norm_query.split() if w]

    if not query_words:
        return []

    strip_words = [_strip_bn_vowels(w) for w in query_words]
    has_bengali = bool(_RE_BENGALI.search(norm_query))
    sq          = _strip_bn_vowels(norm_query)

    # ── Meaningful words: strip stopwords for AND logic ───────────────────────
    meaningful_words = [w for w in query_words if w not in _STOPWORDS] or query_words

    # ── Numeric words: FTS5 unicode61 may not index pure numbers ─────────────
    # We handle these via file_name LIKE scan to guarantee they're found
    numeric_words = [w for w in query_words if _RE_PURE_NUMERIC.match(w)]

    # ── Build FTS variant word set (capped at 20 unique terms) ───────────────
    fts_words: set[str] = set(query_words)
    for w in sorted(query_words, key=len, reverse=True)[:4]:
        for v in _bn_variants(w):
            vn = normalize_name(v)
            fts_words.update(vn.split())
        if len(fts_words) >= 20:
            break

    fts_strip_words = {_strip_bn_vowels(w) for w in fts_words if w}
    translit_candidates = _transliterate_query(norm_query) if not has_bengali else []
    for bn_cand in translit_candidates:
        bn_norm = normalize_name(bn_cand)
        fts_words.update(bn_norm.split())
        fts_strip_words.update(_strip_bn_vowels(w) for w in bn_norm.split() if w)

    conn = db_connect()
    c    = conn.cursor()

    scored: dict[int, tuple] = {}

    def _add(row, s: int):
        bid = row[0]
        if s > 0 and (bid not in scored or s > scored[bid][5]):
            scored[bid] = (bid, row[1], row[2], row[3], (row[4] or 'pdf'), s)

    # ──────────────────────────────────────────────────────────────────────────
    # Strategy 1 — FTS5 AND (meaningful non-stopword words only)
    # Skips numeric-only words since FTS5 may not index them.
    # ──────────────────────────────────────────────────────────────────────────
    fts_and_words = [w for w in meaningful_words if not _RE_PURE_NUMERIC.match(w)]
    if len(fts_and_words) >= 1:
        fts_and_expr = ' AND '.join('"' + w + '"*' for w in fts_and_words)
        try:
            c.execute(
                'SELECT b.id, b.file_name, b.file_size, b.is_restricted, b.file_ext, f.search_name'
                ' FROM books_fts f JOIN books b ON b.id = f.rowid'
                ' WHERE f MATCH ? ORDER BY rank LIMIT 300',
                (fts_and_expr,)
            )
            for row in c.fetchall():
                s = _score_result(row[5] or normalize_name(row[1]),
                                  query_words, strip_words, norm_query)
                _add(row, s)
        except Exception as ex:
            log.warning(f'[search] FTS AND: {ex}')

    # ──────────────────────────────────────────────────────────────────────────
    # Strategy 2 — FTS5 OR (all variant words)
    # ──────────────────────────────────────────────────────────────────────────
    if fts_words:
        sorted_fts = sorted(fts_words, key=len, reverse=True)[:20]
        fts_or_expr = ' OR '.join('"' + w + '"*' for w in sorted_fts if w)
        try:
            c.execute(
                'SELECT b.id, b.file_name, b.file_size, b.is_restricted, b.file_ext, f.search_name'
                ' FROM books_fts f JOIN books b ON b.id = f.rowid'
                ' WHERE f MATCH ? ORDER BY rank LIMIT 500',
                (fts_or_expr,)
            )
            for row in c.fetchall():
                if row[0] in scored:
                    continue
                s = _score_result(row[5] or normalize_name(row[1]),
                                  query_words, strip_words, norm_query)
                _add(row, s)
        except Exception as ex:
            log.warning(f'[search] FTS OR: {ex}')

    # ──────────────────────────────────────────────────────────────────────────
    # Strategy 3 — file_name LIKE scan for numeric tokens and short queries
    # This is the safety net that guarantees "48 laws of power" finds results
    # even if FTS5 didn't index "48".
    # Runs when: numeric words present, OR query is short (≤2 words), OR FTS got < 3 hits
    # ──────────────────────────────────────────────────────────────────────────
    run_like = numeric_words or len(query_words) <= 2 or len(scored) < 3
    if run_like:
        # Build LIKE patterns from meaningful words (longest first for specificity)
        like_words = sorted(meaningful_words, key=len, reverse=True)[:4]
        like_conditions = ' AND '.join('(file_name LIKE ? OR search_name LIKE ?)' for _ in like_words)
        like_params = []
        for w in like_words:
            pat = f'%{w}%'
            like_params.extend([pat, pat])
        already = tuple(scored.keys()) if scored else (-1,)
        ph_excl = ','.join('?' * len(already))
        try:
            c.execute(
                'SELECT id, file_name, file_size, is_restricted, file_ext, search_name'
                f' FROM books WHERE ({like_conditions}) AND id NOT IN ({ph_excl})'
                ' LIMIT 400',
                like_params + list(already)
            )
            for row in c.fetchall():
                s = _score_result(row[5] or normalize_name(row[1]),
                                  query_words, strip_words, norm_query)
                _add(row, s)
        except Exception as ex:
            log.warning(f'[search] LIKE scan: {ex}')

    # ──────────────────────────────────────────────────────────────────────────
    # Strategy 4 — Bengali vowel-stripped scan (only when needed)
    # ──────────────────────────────────────────────────────────────────────────
    need_strip_scan = has_bengali or bool(translit_candidates)
    if need_strip_scan and len(scored) < 5:
        c.execute('PRAGMA query_only=OFF')
        _ensure_stripped_backfill(conn)
        c.execute('PRAGMA query_only=ON')

        strip_patterns: list[str] = []
        if sq and len(sq) >= 2:
            strip_patterns.append(f'%{sq}%')
        for sw in sorted(fts_strip_words, key=len, reverse=True)[:6]:
            if sw and len(sw) >= 2:
                p = f'%{sw}%'
                if p not in strip_patterns:
                    strip_patterns.append(p)

        if strip_patterns:
            already = tuple(scored.keys()) if scored else (-1,)
            ph_excl = ','.join('?' * len(already))
            cond    = ' OR '.join('stripped_name LIKE ?' for _ in strip_patterns)
            try:
                c.execute(
                    'SELECT id, file_name, file_size, is_restricted, file_ext, search_name'
                    f' FROM books WHERE ({cond}) AND id NOT IN ({ph_excl}) LIMIT 400',
                    strip_patterns + list(already)
                )
                for row in c.fetchall():
                    s = _score_result(row[5] or normalize_name(row[1]),
                                      query_words, strip_words, norm_query)
                    if s == 0 and sq:
                        sn = _strip_bn_vowels(row[5] or normalize_name(row[1]))
                        if sq in sn:
                            s = max(1, int(len(sq) / max(len(sn), 1) * 50))
                    _add(row, s)
            except Exception as ex:
                log.warning(f'[search] strip scan: {ex}')

    try:
        conn.close()
    except Exception:
        pass

    if not scored:
        elapsed = time.monotonic() - t0
        log.debug(f'[search] "{query}" → 0 results in {elapsed*1000:.0f}ms')
        return []

    ranked = sorted(scored.values(), key=lambda r: (-r[5], len(r[1]), r[1]))
    result = ranked[:MAX_RESULTS]

    elapsed = time.monotonic() - t0
    log.debug(f'[search] "{query}" → {len(result)} results in {elapsed*1000:.0f}ms '
              f'(scored={len(scored)})')
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Spam / rate-limit helpers (unchanged from v11)
# ─────────────────────────────────────────────────────────────────────────────

def _rolling_count(log: list, window_secs: float) -> int:
    cutoff = time.time() - window_secs
    log[:] = [t for t in log if t >= cutoff]
    return len(log)


def spam_check_search(user_id: int, chat_id: int) -> tuple:
    if is_staff(user_id):
        return True, ''
    now = time.time()

    # VIP flood bypass
    if _is_vip(user_id) and VIP_PERMS.get('bypass_flood_check'):
        pass  # skip flood check entirely for VIP
    else:
        muted_until = _flood_muted.get(user_id, 0)
        if now < muted_until:
            mins = int((muted_until - now) / 60) + 1
            return False, f'muted:{mins}'

        log_entry = _flood_tracker.setdefault(user_id, [])
        log_entry.append(now)
        log_entry[:] = [t for t in log_entry if t >= now - FLOOD_WINDOW_SECS()]
        if len(log_entry) > FLOOD_MSGS():
            _flood_muted[user_id] = now + FLOOD_MUTE_SECS()
            _flood_tracker[user_id] = []
            return False, f'flood:{FLOOD_MUTE_SECS() // 60}'

    # VIP search cooldown bypass
    if not (_is_vip(user_id) and VIP_PERMS.get('bypass_search_cooldown')):
        if now - search_cooldowns.get(user_id, 0) < SEARCH_COOLDOWN_SECS():
            return False, 'cooldown'

    chat_log = _chat_rate_log.setdefault(chat_id, [])
    if _rolling_count(chat_log, CHAT_RATE_LIMIT_SECS()) >= CHAT_RATE_LIMIT_N():
        return False, 'chat_rate'
    chat_log.append(now)

    search_cooldowns[user_id] = now
    return True, ''


def spam_check_download(user_id: int, book_id: int) -> tuple:
    if is_staff(user_id):
        return True, ''
    now = time.time()

    # VIP book cooldown bypass
    if not (_is_vip(user_id) and VIP_PERMS.get('bypass_book_cooldown')):
        key = (user_id, book_id)
        last_dl = download_cooldowns.get(key, 0)
        if now - last_dl < 600:
            remaining = int(600 - (now - last_dl))
            return False, f'book_cooldown:{remaining}'

    dl_log = _daily_dl_log.setdefault(user_id, [])
    if _rolling_count(dl_log, DAILY_DL_WINDOW) >= _get_daily_limit(user_id):
        return False, f'daily_limit:{_get_daily_limit(user_id)}'

    key = (user_id, book_id)
    download_cooldowns[key] = now
    dl_log.append(now)
    return True, ''


def spam_check_request(user_id: int) -> tuple:
    if is_staff(user_id):
        return True, ''
    # VIP unlimited requests
    if _is_vip(user_id) and VIP_PERMS.get('request_unlimited'):
        return True, ''
    now = time.time()
    req_log = _request_log.setdefault(user_id, [])
    if _rolling_count(req_log, REQUEST_COOLDOWN_SECS()) >= REQUEST_COOLDOWN_MAX():
        return False, f'request_limit:{REQUEST_COOLDOWN_SECS() // 60}'
    req_log.append(now)
    return True, ''


def spam_check_page(user_id: int) -> bool:
    if is_staff(user_id): return True
    now = time.time()
    if now - _page_cooldowns.get(user_id, 0) < PAGE_COOLDOWN_SECS():
        return False
    _page_cooldowns[user_id] = now
    return True


def sanitize_query(query: str):
    q = query.strip()
    if len(q) < QUERY_MIN_LEN(): return None
    return q[:QUERY_MAX_LEN()]


def cleanup_spam_state():
    now = time.time()
    stale = [u for u, t in _flood_muted.items() if t < now]
    for u in stale: del _flood_muted[u]
    for u in list(_flood_tracker):
        _flood_tracker[u] = [t for t in _flood_tracker[u] if t >= now - FLOOD_WINDOW_SECS()]
        if not _flood_tracker[u]: del _flood_tracker[u]
    for u in list(_daily_dl_log):
        _daily_dl_log[u] = [t for t in _daily_dl_log[u] if t >= now - DAILY_DL_WINDOW]
        if not _daily_dl_log[u]: del _daily_dl_log[u]
    for u in list(_request_log):
        _request_log[u] = [t for t in _request_log[u] if t >= now - REQUEST_COOLDOWN_SECS()]
        if not _request_log[u]: del _request_log[u]
    for c in list(_chat_rate_log):
        _chat_rate_log[c] = [t for t in _chat_rate_log[c] if t >= now - CHAT_RATE_LIMIT_SECS()]
        if not _chat_rate_log[c]: del _chat_rate_log[c]
    stale_p = [u for u, t in _page_cooldowns.items() if now - t > 10]
    for u in stale_p: del _page_cooldowns[u]
    stale_s = [u for u, t in search_cooldowns.items() if now - t > SEARCH_COOLDOWN_SECS() + 5]
    for u in stale_s: del search_cooldowns[u]


_VALID_MIMES: dict[str, str] = {
    # PDF
    'application/pdf':                  '.pdf',
    'application/x-pdf':                '.pdf',
    # EPUB
    'application/epub+zip':             '.epub',
    'application/epub':                 '.epub',
    # MOBI / Kindle
    'application/x-mobipocket-ebook':   '.mobi',
    'application/mobi':                 '.mobi',
    'application/x-mobi8-ebook':        '.azw3',
    'application/vnd.amazon.ebook':     '.azw',
    'application/x-kindle-book':        '.kfx',
    'application/x-kfx-ebook':          '.kfx',
    # DjVu
    'image/vnd.djvu':                   '.djvu',
    'image/x-djvu':                     '.djvu',
    # FictionBook
    'application/x-fictionbook+xml':    '.fb2',
    'application/x-fictionbook':        '.fb2',
    # LIT (Microsoft)
    'application/x-ms-reader':          '.lit',
    # Comic book archives
    'application/vnd.comicbook+zip':    '.cbz',
    'application/x-cbz':                '.cbz',
    'application/vnd.comicbook-rar':    '.cbr',
    'application/x-cbr':                '.cbr',
    # RTF / TXT ebooks
    'application/rtf':                  '.rtf',
    'text/rtf':                         '.rtf',
    # DOC/DOCX (sometimes used for ebooks)
    'application/msword':               '.doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
    # Generic fallback for octet-stream — extension-based detection handles this
    'application/octet-stream':         '',
}

# All extensions recognised as ebooks
_EBOOK_EXTS = frozenset({
    '.pdf', '.epub', '.mobi', '.azw', '.azw3', '.kfx',
    '.djvu', '.fb2', '.lit', '.cbz', '.cbr', '.rtf',
    '.doc', '.docx',
})
_VALID_EXTS = _EBOOK_EXTS  # alias for backward compat

def _extract_file_info(msg) -> 'tuple[str, str, int] | None':
    if not msg.file:
        return None

    fname: str | None = getattr(msg.file, 'name', None) or None
    if not fname:
        doc = getattr(getattr(msg, 'media', None), 'document', None)
        if doc:
            for attr in getattr(doc, 'attributes', []):
                fn = getattr(attr, 'file_name', None)
                if fn:
                    fname = fn.strip(); break

    mime: str = (getattr(msg.file, 'mime_type', None) or '').lower().strip()
    if not mime:
        doc = getattr(getattr(msg, 'media', None), 'document', None)
        if doc:
            mime = (getattr(doc, 'mime_type', None) or '').lower().strip()

    ext = os.path.splitext(fname)[1].lower() if fname else ''
    # If extension not recognised, try MIME lookup
    if ext not in _VALID_EXTS:
        ext = _VALID_MIMES.get(mime, '')
    # For octet-stream with a known extension in the filename, trust the extension
    if not ext and fname:
        ext = os.path.splitext(fname)[1].lower()
    if ext not in _VALID_EXTS:
        return None

    fsize: int = getattr(msg.file, 'size', None) or 0
    if not fsize:
        doc = getattr(getattr(msg, 'media', None), 'document', None)
        if doc:
            fsize = getattr(doc, 'size', None) or 0

    if fname:
        fname = os.path.basename(fname).strip()
        if not fname.lower().endswith(ext):
            fname = fname + ext
    else:
        fname = f'unknown_{fsize}{ext}'

    return fname, ext, fsize

def is_staff(uid: int) -> bool: return uid in ALL_STAFF
def is_owner(uid: int) -> bool: return uid == OWNER_ID

def get_sender_id(event) -> int | None:
    if hasattr(event, 'sender_id') and event.sender_id:
        return event.sender_id
    if hasattr(event, 'from_id') and event.from_id:
        return getattr(event.from_id, 'user_id', None)
    return None


def _is_channel_post(event) -> bool:
    msg = getattr(event, 'message', event)
    if getattr(msg, 'post', False):
        return True
    fwd = getattr(msg, 'fwd_from', None)
    if fwd is not None:
        if getattr(fwd, 'channel_post', None):
            return True
        from telethon.tl.types import PeerChannel as _PC
        if isinstance(getattr(fwd, 'from_id', None), _PC):
            return True
    return False

def get_event_thread_id(event) -> int | None:
    if not event.reply_to:
        return None
    top = getattr(event.reply_to, 'reply_to_top_id', None)
    if top:
        return top
    mid = getattr(event.reply_to, 'reply_to_msg_id', None)
    if not mid:
        return None
    if getattr(event.reply_to, 'forum_topic', False):
        return mid
    cid = getattr(event, 'chat_id', None)
    if cid and ((cid, mid) in ASSIGNED_CHATS or (cid, mid) in TRIGGER_CHATS):
        return mid
    return None

def get_assignment(chat_id, thread_id, reg: dict):
    if thread_id and (chat_id, thread_id) in reg:
        return reg[(chat_id, thread_id)]
    if (chat_id, None) in reg:
        return reg[(chat_id, None)]
    return None

_delete_queue: list = []  # (message_id, chat_id, delete_at, use_user_client)

def _is_protected_chat(chat_id) -> bool:
    """Return True if this chat_id must never have messages deleted from it.
    Protects source channels and the backup group from accidental deletion.
    """
    if not chat_id:
        return False
    # Never touch the backup group
    if BACKUP_GROUP_ID[0] and chat_id == BACKUP_GROUP_ID[0]:
        return True
    # Never touch source channels
    norm = _normalize_id_for_compare(chat_id)
    for s in SOURCE_GROUPS:
        s_norm = _normalize_id_for_compare(s)
        if s_norm == norm:
            return True
    return False

def schedule_delete(message_id: int, chat_id: int, delay: int, use_user_client: bool = False):
    """Buffer deletion requests; flushed by _flush_delete_queue every cleanup cycle.
    Guards: never deletes from source channels or backup group.
    Minimum delay of 60s so the file is visible before it disappears.
    """
    if _is_protected_chat(chat_id):
        log.debug(f'schedule_delete: skipped protected chat {chat_id} msg {message_id}')
        return
    # Enforce minimum 60s so user actually receives the file before deletion
    safe_delay = max(delay, 60)
    _delete_queue.append((message_id, chat_id, int(time.time()) + safe_delay, 1 if use_user_client else 0))

def _flush_delete_queue():
    """Call from the cleanup loop to drain the in-memory buffer in one transaction."""
    if not _delete_queue:
        return
    batch = _delete_queue[:]
    del _delete_queue[:]
    try:
        conn = db_connect()
        conn.executemany(
            'INSERT INTO cleanup_queue(message_id,chat_id,delete_at,use_user_client) VALUES(?,?,?,?)',
            batch
        )
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f'_flush_delete_queue: {e}')
        # Put them back so they're not lost
        _delete_queue.extend(batch)

def chat_label(event) -> str:
    if event.chat:
        return (getattr(event.chat,'title',None)
                or getattr(event.chat,'username',None)
                or str(event.chat_id))
    return str(event.chat_id)

def ts_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def user_mention_md(user_id: int, name: str) -> str:
    return f'[{name}](tg://user?id={user_id})'

def check_access(event) -> tuple[bool, str]:
    sender_id = get_sender_id(event)
    tid = get_event_thread_id(event)
    if event.is_private:
        if sender_id and is_staff(sender_id):
            return True, 'dm'
        if SEARCH_MODE[0] == 'public':
            return True, 'dm'
        return False, 'denied'
    in_trig = get_assignment(event.chat_id, tid, TRIGGER_CHATS) is not None
    in_asgn = get_assignment(event.chat_id, tid, ASSIGNED_CHATS) is not None
    if in_trig:  return True, 'trigger'
    if in_asgn:  return True, 'assigned'
    return False, 'denied'

# ═══════════════════════════════════════════════════════════════════════════════
# COLLECTION SYSTEM  — collection.db
#
# Users can build named reading lists (collections) of books, share them as
# a code like  #col123456789_42  (user_id + collection_id),  browse and add
# books from inline mode, and view/manage everything via button UI.
#
# Schema:
#   collections(id, user_id, name, emoji, is_public, created_at, updated_at)
#   collection_items(id, collection_id, book_id, book_name, added_at)
#
# Collection share code: #col{user_id}_{collection_id}
# ═══════════════════════════════════════════════════════════════════════════════

def col_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(COLLECTION_DB, timeout=20, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=15000')
    return conn


def setup_collections_db():
    conn = col_connect(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS collections(
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        name       TEXT    NOT NULL,
        emoji      TEXT    DEFAULT '📚',
        is_public  INTEGER DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS col_user ON collections(user_id)')
    c.execute('''CREATE TABLE IF NOT EXISTS collection_items(
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id INTEGER NOT NULL,
        book_id       INTEGER NOT NULL,
        book_name     TEXT    DEFAULT '',
        added_at      INTEGER NOT NULL,
        UNIQUE(collection_id, book_id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS ci_col ON collection_items(collection_id)')
    c.execute('CREATE INDEX IF NOT EXISTS ci_book ON collection_items(book_id)')
    conn.commit(); conn.close()


# ── Share code helpers ────────────────────────────────────────────────────────
def col_share_code(user_id: int, col_id: int) -> str:
    return f'#col{user_id}_{col_id}'

def col_parse_code(code: str) -> tuple[int, int] | None:
    """Parse #col{uid}_{cid} → (uid, cid) or None."""
    m = re.match(r'#col(\d+)_(\d+)', code.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None

def col_get(col_id: int) -> dict | None:
    conn = col_connect(); c = conn.cursor()
    c.execute('SELECT id,user_id,name,emoji,is_public,created_at,updated_at FROM collections WHERE id=?', (col_id,))
    r = c.fetchone(); conn.close()
    if not r: return None
    return dict(id=r[0], user_id=r[1], name=r[2], emoji=r[3],
                is_public=r[4], created_at=r[5], updated_at=r[6])

def col_items(col_id: int) -> list[dict]:
    conn = col_connect(); c = conn.cursor()
    c.execute('SELECT id,book_id,book_name,added_at FROM collection_items WHERE collection_id=? ORDER BY added_at DESC', (col_id,))
    rows = c.fetchall(); conn.close()
    return [dict(id=r[0], book_id=r[1], book_name=r[2], added_at=r[3]) for r in rows]

def col_user_list(user_id: int) -> list[dict]:
    conn = col_connect(); c = conn.cursor()
    c.execute('''SELECT c.id, c.name, c.emoji, c.is_public, c.updated_at,
                        COUNT(ci.id) as cnt
                 FROM collections c
                 LEFT JOIN collection_items ci ON ci.collection_id=c.id
                 WHERE c.user_id=?
                 GROUP BY c.id ORDER BY c.updated_at DESC''', (user_id,))
    rows = c.fetchall(); conn.close()
    return [dict(id=r[0], name=r[1], emoji=r[2], is_public=r[3],
                 updated_at=r[4], count=r[5]) for r in rows]

def col_add_book(col_id: int, book_id: int, book_name: str) -> bool:
    """Add book to collection. Returns True if newly added, False if already there."""
    try:
        conn = col_connect(); c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO collection_items(collection_id,book_id,book_name,added_at) VALUES(?,?,?,?)',
                  (col_id, book_id, book_name, int(time.time())))
        added = c.rowcount > 0
        if added:
            c.execute('UPDATE collections SET updated_at=? WHERE id=?', (int(time.time()), col_id))
        conn.commit(); conn.close()
        return added
    except Exception: return False

def col_remove_book(col_id: int, book_id: int):
    conn = col_connect(); c = conn.cursor()
    c.execute('DELETE FROM collection_items WHERE collection_id=? AND book_id=?', (col_id, book_id))
    conn.commit(); conn.close()

def col_create(user_id: int, name: str, emoji: str = '📚') -> int:
    now = int(time.time())
    conn = col_connect(); c = conn.cursor()
    c.execute('INSERT INTO collections(user_id,name,emoji,is_public,created_at,updated_at) VALUES(?,?,?,0,?,?)',
              (user_id, name[:60], emoji, now, now))
    col_id = c.lastrowid
    conn.commit(); conn.close()
    return col_id

def col_delete(col_id: int, user_id: int) -> bool:
    conn = col_connect(); c = conn.cursor()
    c.execute('DELETE FROM collection_items WHERE collection_id=?', (col_id,))
    c.execute('DELETE FROM collections WHERE id=? AND user_id=?', (col_id, user_id))
    ok = c.rowcount > 0
    conn.commit(); conn.close()
    return ok

def col_rename(col_id: int, user_id: int, new_name: str) -> bool:
    conn = col_connect(); c = conn.cursor()
    c.execute('UPDATE collections SET name=?, updated_at=? WHERE id=? AND user_id=?',
              (new_name[:60], int(time.time()), col_id, user_id))
    ok = c.rowcount > 0
    conn.commit(); conn.close()
    return ok

def col_toggle_public(col_id: int, user_id: int) -> bool | None:
    """Toggle public flag. Returns new value or None if not found."""
    conn = col_connect(); c = conn.cursor()
    c.execute('SELECT is_public FROM collections WHERE id=? AND user_id=?', (col_id, user_id))
    r = c.fetchone()
    if not r: conn.close(); return None
    new_val = 1 - r[0]
    c.execute('UPDATE collections SET is_public=? WHERE id=?', (new_val, col_id))
    conn.commit(); conn.close()
    return bool(new_val)


# ── UI builders ───────────────────────────────────────────────────────────────
MAX_COLLECTIONS_PER_USER = 20
MAX_ITEMS_PER_COLLECTION = 100

def _col_list_text(user_id: int) -> tuple[str, list]:
    """Build collection list message text + buttons."""
    cols = col_user_list(user_id)
    if not cols:
        text = (
            '📚 **My Collections**\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '_You have no collections yet._\n\n'
            'Tap **＋ New Collection** to create one!\n'
            '_You can save books here while browsing._'
        )
        btns = [[Button.inline('➕ New Collection', b'col_new')]]
        return text, btns

    lines = ['📚 **My Collections**\n━━━━━━━━━━━━━━━━━━━━']
    for c in cols:
        pub = '🌐' if c['is_public'] else '🔒'
        lines.append(f'{c["emoji"]} **{c["name"]}** {pub} — `{c["count"]} books`')

    text = '\n'.join(lines)
    btns = []
    # One button per collection (open it)
    row = []
    for c in cols[:10]:
        label = f'{c["emoji"]} {c["name"][:18]}'
        row.append(Button.inline(label, f'col_open_{c["id"]}'.encode()))
        if len(row) == 2:
            btns.append(row); row = []
    if row: btns.append(row)
    btns.append([
        Button.inline('➕ New', b'col_new'),
        Button.inline('❌ Close', b'col_close'),
    ])
    return text, btns


def _col_detail_text(col: dict, items: list) -> tuple[str, list]:
    """Build collection detail message text + buttons."""
    pub    = '🌐 Public' if col['is_public'] else '🔒 Private'
    code   = col_share_code(col['user_id'], col['id'])
    header = (
        f'{col["emoji"]} **{col["name"]}**\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'{pub}  •  📚 {len(items)} book{"s" if len(items) != 1 else ""}\n'
    )
    if col['is_public']:
        header += (
            f'\n🔗 **Share code:** `{code}`\n'
            f'_Anyone can paste this code in any chat to view this collection_\n'
        )
    else:
        header += (
            f'\n🔗 **Share code:** `{code}`\n'
            f'_Make this collection public so others can view it_\n'
        )
    header += '━━━━━━━━━━━━━━━━━━━━\n'

    if not items:
        body = '_No books yet — tap ➕ on any search result to add books!_\n'
    else:
        book_lines = []
        for i, it in enumerate(items[:15], 1):
            bname = re.sub(r'\.(pdf|epub|mobi|azw3?|djvu|fb2)$', '', it['book_name'],
                           flags=re.IGNORECASE).strip()
            bname = bname[:50] + ('…' if len(bname) > 50 else '')
            book_lines.append(f'`{i}.` {bname}')
        body = '\n'.join(book_lines) + '\n'
        if len(items) > 15:
            body += f'_…and {len(items)-15} more_\n'

    text = header + body
    cid  = col['id']
    btns = []
    # Download buttons for first 5 books
    for it in items[:5]:
        bname_short = re.sub(r'\.(pdf|epub|mobi|azw3?|djvu)$', '', it['book_name'],
                              flags=re.IGNORECASE).strip()[:32]
        btns.append([Button.inline(
            f'📥 {bname_short}',
            f'get_{it["book_id"]}_0'.encode()
        )])
    if len(items) > 5:
        btns.append([Button.inline(
            f'📥 All {len(items)} books →',
            f'col_dlall_{cid}'.encode()
        )])
    # Management row
    btns.append([
        Button.inline('🗑 Remove', f'col_rm_sel_{cid}'.encode()),
        Button.inline('✏️ Rename', f'col_rename_{cid}'.encode()),
    ])
    btns.append([
        Button.inline('🌐 Make Public' if not col['is_public'] else '🔒 Make Private',
                      f'col_toggle_{cid}'.encode()),
        Button.inline('🗑 Delete', f'col_del_{cid}'.encode()),
    ])
    btns.append([
        Button.inline('◀️ My Collections', b'col_list'),
        Button.inline('✖ Close', b'col_close'),
    ])
    return text, btns


def _setup_db():
    """Wrapper that initialises both ebooks.db and collection.db."""
    setup_db()
    setup_collections_db()
    setup_dm_db()


# ─────────────────────────────────────────────────────────────────────────────
# DM.DB  —  tracks which users have already received a DM from the userbot.
#
# When the userbot tries to send a file to a user in DM, Telegram requires
# that the user has previously messaged the bot (or the bot has messaged them).
# For restricted userbots (like @MrRobotCrew) Telegram raises
# UserIsBlockedError / PeerFloodError if the channel isn't "opened".
#
# Solution:
#   1. Bot sends the user a "tap here to start" message with a deep link.
#   2. User taps → /start → userbot can now DM them.
#   3. We record user_id in dm.db so we never need to do this again.
# ─────────────────────────────────────────────────────────────────────────────

def dm_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DM_DB, timeout=15, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn

def setup_dm_db():
    conn = dm_connect(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS dm_unlocked(
        user_id    INTEGER PRIMARY KEY,
        username   TEXT    DEFAULT '',
        first_name TEXT    DEFAULT '',
        unlocked_at INTEGER NOT NULL,
        method     TEXT    DEFAULT 'auto'   -- 'auto', 'manual', 'start'
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS dmu_ts ON dm_unlocked(unlocked_at)')
    conn.commit(); conn.close()

def dm_is_unlocked(user_id: int) -> bool:
    """Return True if we know DM is already open for this user."""
    try:
        conn = dm_connect(); c = conn.cursor()
        c.execute('SELECT 1 FROM dm_unlocked WHERE user_id=?', (user_id,))
        r = c.fetchone(); conn.close()
        return r is not None
    except Exception:
        return False

def dm_mark_unlocked(user_id: int, username: str = '', first_name: str = '', method: str = 'auto'):
    """Record that DM channel is open for this user."""
    try:
        conn = dm_connect(); c = conn.cursor()
        c.execute(
            'INSERT OR REPLACE INTO dm_unlocked(user_id,username,first_name,unlocked_at,method)'
            ' VALUES(?,?,?,?,?)',
            (user_id, username or '', first_name or '', int(time.time()), method)
        )
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f'dm_mark_unlocked: {e}')

async def _ensure_dm_open(user_client, bot_client, user_id: int,
                           first_name: str = '', username: str = '') -> bool:
    """
    Ensure the userbot can DM this user.

    If already unlocked → return True immediately.
    Otherwise → have the BOT send a "tap to start" message to open the channel,
    then wait up to 10s for the user to tap (or proceed anyway since the
    bot message doesn't need the userbot to message first).

    For the actual delivery: we try user_client.send_file first; if that raises
    UserIsBlockedError / PeerFloodError, we fall back to bot_client.
    """
    if dm_is_unlocked(user_id):
        return True

    # Try sending via user_client directly first — may already work
    try:
        await user_client.get_input_entity(user_id)
        # If we can get the entity, the DM may already be openable
        # We'll attempt send in deliver_book; mark optimistically
        dm_mark_unlocked(user_id, username, first_name, method='entity_known')
        return True
    except Exception:
        pass

    # Tell the bot to send a greeting so user taps /start
    bot_un = _BOT_USERNAME[0]
    if bot_client and bot_un:
        try:
            greeting = (
                f'👋 Hi **{first_name or "there"}**!\n\n'
                f'To receive your book, please tap **Start** below 👇\n'
                f'_(This is a one-time step so we can send files to you directly.)_'
            )
            await bot_client.send_message(
                user_id, greeting,
                buttons=Button.url('▶️ Start', f'https://t.me/{bot_un}?start=dmopen'),
                parse_mode='md'
            )
            log.info(f'_ensure_dm_open: sent greeting to {user_id} via bot')
        except Exception as eg:
            log.debug(f'_ensure_dm_open: bot greeting failed for {user_id}: {eg}')

    return False   # caller should fall back to bot delivery


# ── Register collection command alias ────────────────────────────────────────
# Handled inside handle_command() below via cmd check.

# ─────────────────────────────────────────────────────────────────────────────
# Database setup
# ─────────────────────────────────────────────────────────────────────────────
def _setup_schema(conn: sqlite3.Connection):
    """Create all tables/indexes/triggers on an already-opened connection.
    Called for both the live DB and the shadow scanbook.db."""
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT, search_name TEXT, stripped_name TEXT DEFAULT '',
        file_size INTEGER,
        message_id INTEGER, chat_id INTEGER, file_ext TEXT,
        is_restricted INTEGER DEFAULT 0,
        orig_chat_id INTEGER DEFAULT 0,
        UNIQUE(file_name, file_size)
    )''')
    try:
        c.execute("ALTER TABLE books ADD COLUMN stripped_name TEXT DEFAULT ''")
        conn.commit()
    except Exception:
        pass
    # orig_chat_id: stores the ORIGINAL source channel — never overwritten by backup updates
    try:
        c.execute('ALTER TABLE books ADD COLUMN orig_chat_id INTEGER DEFAULT 0')
        conn.commit()
        # Backfill: for existing rows set orig_chat_id = chat_id
        c.execute('UPDATE books SET orig_chat_id = chat_id WHERE orig_chat_id = 0 OR orig_chat_id IS NULL')
        conn.commit()
    except Exception:
        pass
    c.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS books_fts USING fts5(
        search_name, content='books', content_rowid='id', tokenize='unicode61'
    )''')
    c.execute('''CREATE TRIGGER IF NOT EXISTS books_ai AFTER INSERT ON books BEGIN
        INSERT INTO books_fts(rowid,search_name) VALUES(new.id,new.search_name);
    END;''')
    c.execute('''CREATE TABLE IF NOT EXISTS cleanup_queue(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER, chat_id INTEGER, delete_at INTEGER,
        use_user_client INTEGER DEFAULT 0
    )''')
    try:
        c.execute('ALTER TABLE cleanup_queue ADD COLUMN use_user_client INTEGER DEFAULT 0')
        conn.commit()
    except Exception:
        pass

    c.execute('''CREATE TABLE IF NOT EXISTS scrape_progress(
        chat_id INTEGER PRIMARY KEY,
        last_msg_id INTEGER DEFAULT 0,
        last_scraped_at INTEGER DEFAULT 0,
        scrape_label TEXT DEFAULT '',
        last_scrape_mode TEXT DEFAULT 'resume'
    )''')
    for col, default in [
        ('last_scraped_at',  '0'),
        ('scrape_label',     "''"),
        ('last_scrape_mode', "'resume'"),
    ]:
        try:
            c.execute(f'ALTER TABLE scrape_progress ADD COLUMN {col} TEXT DEFAULT {default}')
            conn.commit()
        except Exception:
            pass

    c.execute('''CREATE TABLE IF NOT EXISTS download_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT DEFAULT '',
        first_name TEXT DEFAULT '',
        book_id INTEGER NOT NULL,
        book_name TEXT DEFAULT '',
        chat_id INTEGER DEFAULT 0,
        ts INTEGER NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS dl_log_user ON download_log(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS dl_log_book ON download_log(book_id)')
    c.execute('CREATE INDEX IF NOT EXISTS dl_log_ts   ON download_log(ts)')

    c.execute('''CREATE TABLE IF NOT EXISTS search_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT DEFAULT '',
        first_name TEXT DEFAULT '',
        query TEXT NOT NULL,
        chat_id INTEGER DEFAULT 0,
        ts INTEGER NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS sr_log_user  ON search_log(user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS sr_log_query ON search_log(query)')
    c.execute('CREATE INDEX IF NOT EXISTS sr_log_ts    ON search_log(ts)')

    try:
        c.execute('ALTER TABLE search_log ADD COLUMN result_count INTEGER DEFAULT -1')
        conn.commit()
    except Exception:
        pass

    c.execute('CREATE INDEX IF NOT EXISTS dl_log_chat_ts ON download_log(chat_id, ts)')
    c.execute('CREATE INDEX IF NOT EXISTS sr_log_chat_ts ON search_log(chat_id, ts)')
    c.execute('CREATE INDEX IF NOT EXISTS dl_log_ts_uid  ON download_log(ts, user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS sr_log_ts_uid  ON search_log(ts, user_id)')

    c.execute('''CREATE TABLE IF NOT EXISTS bot_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
        ts INTEGER NOT NULL,
        detail TEXT DEFAULT ''
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS ev_type_ts ON bot_events(event_type, ts)')

    c.execute('''CREATE TABLE IF NOT EXISTS book_aliases(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER NOT NULL,
        alias TEXT NOT NULL,
        UNIQUE(book_id, alias)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS ba_book ON book_aliases(book_id)')
    c.execute('CREATE INDEX IF NOT EXISTS ba_alias ON book_aliases(alias)')

    c.execute('''CREATE TABLE IF NOT EXISTS feedback(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT DEFAULT '',
        first_name TEXT DEFAULT '',
        message TEXT NOT NULL,
        ts INTEGER NOT NULL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS fb_ts ON feedback(ts)')

    c.execute('''CREATE TABLE IF NOT EXISTS botd_history(
        date TEXT PRIMARY KEY,
        book_id INTEGER NOT NULL,
        book_name TEXT DEFAULT ''
    )''')

    conn.commit()


def setup_db():
    conn = db_connect()
    _setup_schema(conn)
    conn.close()
    setup_collections_db()

def warmup_db():
    try:
        conn = db_connect()
        conn.execute('SELECT COUNT(*) FROM books').fetchone()
        conn.execute("SELECT COUNT(*) FROM books_fts WHERE books_fts MATCH 'a*'").fetchone()
        conn.close()
        log.info('DB warmed up — all pages loaded into RAM cache')
    except Exception as e:
        log.warning(f'warmup_db: {e}')


def warmup_search_cache():
    loaded = 0
    now    = time.time()
    try:
        for fn in os.listdir(CACHE_DIR):
            if not fn.endswith('.json'):
                continue
            fp = os.path.join(CACHE_DIR, fn)
            try:
                mtime = os.path.getmtime(fp)
                if now - mtime > _SEARCH_CACHE_TTL:
                    continue
                with open(fp) as f:
                    data = json.load(f)
                query_hash = fn[:-5]
                _SEARCH_CACHE[query_hash] = (data, mtime)
                loaded += 1
                if loaded >= _SEARCH_CACHE_MAX:
                    break
            except Exception:
                pass
    except Exception as e:
        log.warning(f'warmup_search_cache: {e}')
    if loaded:
        log.info(f'Search cache warmed: {loaded} entries loaded into memory')


_analytics_client_ref: list = []

async def report(text: str, is_error: bool = False):
    if not ANALYTICS_GROUP[0] or not _analytics_client_ref:
        return
    prefix = '🚨 **ERROR**\n' if is_error else ''
    try:
        await _analytics_client_ref[0].send_message(
            ANALYTICS_GROUP[0], prefix + text, parse_mode='md'
        )
    except Exception as e:
        log.warning(f'report failed: {e}')

async def report_search(user_id, name, username, chat_title, chat_id, query, thread_id=None, result_count=-1):
    uref = user_mention_md(user_id, name)
    uname_str = f'@{username}' if username else '—'
    thread_str = f'`{thread_id}`' if thread_id else '—'
    try:
        conn = db_connect(); c = conn.cursor()
        c.execute(
            'INSERT INTO search_log(user_id, username, first_name, query, chat_id, ts, result_count) VALUES(?,?,?,?,?,?,?)',
            (user_id, username or '', name or '', query, chat_id, int(time.time()), result_count)
        )
        conn.commit(); conn.close()
    except Exception as db_e:
        log.debug(f'search_log insert: {db_e}')
    if not ANALYTICS_GROUP[0]:
        return
    await report(
        f'🔍 **Search**\n'
        f'👤 {uref} ({uname_str}) · 🆔 `{user_id}`\n'
        f'📍 Chat: `{chat_id}` | {chat_title}\n'
        f'🧵 Thread: {thread_str}\n'
        f'🔎 Query: `{query}`\n'
        f'🕐 {ts_str()}'
    )

async def report_download(user_id, name, username, chat_title, chat_id, book_name, book_id):
    uref = user_mention_md(user_id, name)
    uname_str = f'@{username}' if username else '—'
    try:
        conn = db_connect(); c = conn.cursor()
        c.execute(
            'INSERT INTO download_log(user_id, username, first_name, book_id, book_name, chat_id, ts) VALUES(?,?,?,?,?,?,?)',
            (user_id, username or '', name or '', book_id, book_name, chat_id, int(time.time()))
        )
        conn.commit(); conn.close()
    except Exception as db_e:
        log.debug(f'download_log insert: {db_e}')
    if not ANALYTICS_GROUP[0]:
        return
    vip_badge = ' ⭐ VIP' if _is_vip(user_id) else ''
    await report(
        f'📥 **Download**{vip_badge}\n'
        f'👤 {uref} ({uname_str}) · 🆔 `{user_id}`\n'
        f'📍 Chat: `{chat_id}` | {chat_title}\n'
        f'📖 Book: `{book_name}` (id:{book_id})\n'
        f'🕐 {ts_str()}'
    )

async def report_error(context: str, exc: Exception):
    tb = traceback.format_exc()[-800:]
    await report(
        f'**Context:** {context}\n'
        f'**Error:** `{exc}`\n'
        f'```\n{tb}\n```',
        is_error=True
    )

# ─────────────────────────────────────────────────────────────────────────────
# Book indexing
# ─────────────────────────────────────────────────────────────────────────────
async def index_books(user_client: TelegramClient, source_ref, status_cb=None,
                      cancel_ref: list = None, from_date: int = None,
                      scrape_mode: str = 'resume', target_db: str = None,
                      companion_client=None):
    """
    Scrape a source and index books.

    companion_client — if provided, use this client instead of user_client
                       for iter_messages (for sources blocked to main account).
    target_db        — write to this DB path instead of the live DB_PATH.
    """
    # Choose the right client for this source
    scrape_client = companion_client or _get_client_for_source(str(source_ref), user_client)
    _db_conn_fn = (lambda: _make_db_conn(target_db)) if target_db else db_connect

    try:
        entity = await _resolve_source(scrape_client, source_ref)
    except Exception as e:
        orig_err = str(e)
        # ── Auto-companion fallback ──────────────────────────────────────────
        # If the main/assigned client can't reach this source, try all running
        # companions automatically. If one succeeds, save the assignment.
        _fallback_found = False
        for _comp in COMPANION_CLIENTS:
            if not _comp.running or not _comp.client:
                continue
            if _comp.client is scrape_client:
                continue  # already tried this one
            try:
                entity = await _resolve_source(_comp.client, source_ref)
                # Success — assign this source to the companion permanently
                _src_str = str(source_ref)
                if _src_str not in _comp.sources:
                    _comp.sources.append(_src_str)
                    _save_companions()
                    log.info(f'Auto-assigned "{_src_str}" to companion "{_comp.name}"')
                    if status_cb:
                        await status_cb(
                            f'🤝 Main client blocked from `{_src_str}` — '
                            f'auto-assigned to companion **{_comp.name}**. '
                            f'Assignment saved.'
                        )
                    try:
                        await report(
                            f'🤝 **Auto-companion assignment**\n'
                            f'Source `{_src_str}` was blocked for main client.\n'
                            f'→ Assigned to **{_comp.name}** automatically.\n'
                            f'_Original error: {orig_err[:200]}_\n'
                            f'🕐 {ts_str()}'
                        )
                    except Exception:
                        pass
                scrape_client = _comp.client
                _fallback_found = True
                break
            except Exception:
                continue

        if not _fallback_found:
            if status_cb:
                await status_cb(
                    f'⚠️ Cannot resolve `{source_ref}`: {orig_err}\n'
                    f'_Tried {len([c for c in COMPANION_CLIENTS if c.running])} companion(s) — all failed._'
                )
            _log_scrap(str(source_ref), 'error', f'resolve failed (no companion could help): {orig_err}')
            return

    src_chat_id = entity.id
    norm_cid = _normalize_id_for_compare(str(src_chat_id))
    if isinstance(norm_cid, int):
        src_chat_id = norm_cid
    label = getattr(entity, 'username', None) or getattr(entity, 'title', None) or str(source_ref)
    _scrap_current[0] = label

    conn = _db_conn_fn()
    c = conn.cursor()
    c.execute('SELECT last_msg_id, last_scraped_at FROM scrape_progress WHERE chat_id=?', (src_chat_id,))
    row = c.fetchone()
    last_id      = row[0] if row else 0
    last_scraped = row[1] if row else 0
    conn.close()

    me = await scrape_client.get_me()
    is_restr = getattr(entity, 'noforwards', False)
    count = 0; new_last = last_id; checked = 0

    iter_kwargs: dict = {'reverse': True}

    if scrape_mode == 'from_date' and from_date:
        iter_kwargs['offset_date'] = datetime.fromtimestamp(from_date, tz=timezone.utc)
        from_dt_str = datetime.fromtimestamp(from_date, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        if status_cb:
            await status_cb(
                f'📅 **From-date scrape** of `{label}`\n'
                f'_Starting from: {from_dt_str}_\n'
                f'_(Messages after this date will be re-checked)_'
            )
        _log_scrap(label, 'start', f'mode=from_date, date={from_dt_str}')
    else:
        iter_kwargs['min_id'] = last_id
        if last_id > 0 and status_cb:
            last_dt = datetime.fromtimestamp(last_scraped, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if last_scraped else 'unknown'
            await status_cb(
                f'🔄 Resuming `{label}` from msg `{last_id}`\n'
                f'_Last scraped: {last_dt}_'
            )
        elif status_cb:
            await status_cb(f'🆕 Fresh scrape of `{label}`…')
        _log_scrap(label, 'start', f'mode={scrape_mode}, min_id={last_id}')

    conn_loop = _db_conn_fn()
    cur_loop  = conn_loop.cursor()

    # Telegram request rate-limiter for scraping: pause briefly every N messages
    # to avoid hitting Telegram's flood limits and getting the account restricted.
    _SCRAPE_PAUSE_EVERY = 200   # messages between short pauses
    _SCRAPE_PAUSE_SECS  = 1.5   # seconds per pause

    try:
        iter_target = getattr(entity, '_full_id', entity)
        async for msg in scrape_client.iter_messages(iter_target, **iter_kwargs):
            if cancel_ref and cancel_ref[0]:
                _log_scrap(label, 'cancelled', f'stopped at msg {msg.id}')
                if status_cb:
                    await status_cb(f'🛑 Cancelled `{label}` at msg `{msg.id}`. `{count}` new books saved so far.')
                break

            checked += 1
            if not msg.file: continue
            if msg.sender_id in OWN_IDS or msg.sender_id == me.id:
                new_last = max(new_last, msg.id); continue

            file_info = _extract_file_info(msg)
            if not file_info:
                continue
            fname, ext, fsize = file_info
            is_restr_this = 1 if is_restr else 0

            try:
                cur_loop.execute(
                    'SELECT id, is_restricted FROM books WHERE file_name=? AND file_size=?',
                    (fname, fsize)
                )
                existing = cur_loop.fetchone()

                if not existing:
                    cur_loop.execute(
                        'INSERT INTO books(file_name,search_name,stripped_name,file_size,'
                        'message_id,chat_id,orig_chat_id,file_ext,is_restricted) VALUES(?,?,?,?,?,?,?,?,?)',
                        (fname, normalize_name(fname),
                         _RE_STRIP_VOWELS.sub('', normalize_name(fname)),
                         fsize, msg.id, src_chat_id, src_chat_id, ext, is_restr_this)
                    )
                    count += 1
                    # Fire keyword alerts only when writing to the live DB
                    if not target_db and KEYWORD_ALERTS and cur_loop.lastrowid:
                        asyncio.get_event_loop().create_task(
                            _check_keyword_alerts(fname, cur_loop.lastrowid)
                        )
                    if count % 100 == 0:
                        conn_loop.commit()
                elif existing[1] == 1 and is_restr_this == 0:
                    cur_loop.execute(
                        'UPDATE books SET message_id=?, chat_id=?, is_restricted=0 WHERE id=?',
                        (msg.id, src_chat_id, existing[0])
                    )
                    conn_loop.commit()
                    log.info(f'Upgraded restricted→unrestricted: {fname}')
                    _log_scrap(label, 'upgrade', fname)
            except Exception as db_err:
                log.warning(f'DB insert/update failed for {fname}: {db_err}')

            new_last = max(new_last, msg.id)

            # Yield to the event loop every 10 messages so downloads,
            # searches and callbacks are never starved during a long scrape.
            if checked % 10 == 0:
                _scrap_last_progress[0] = time.time()  # heartbeat for watchdog
                await asyncio.sleep(0)

            # Telegram flood protection: pause briefly every N messages
            if checked % _SCRAPE_PAUSE_EVERY == 0:
                await asyncio.sleep(_SCRAPE_PAUSE_SECS)

            if status_cb and checked % 100 == 0:
                await status_cb(f'⏳ `{label}`: scanned {checked} msgs, {count} new books…')

    except Exception as e:
        log.warning(f'index_books {source_ref}: {e}')
        _log_scrap(label, 'error', str(e))
    finally:
        try:
            conn_loop.commit()
            conn_loop.close()
        except Exception:
            pass

    now_ts = int(time.time())
    conn = _db_conn_fn()
    conn.execute(
        '''INSERT OR REPLACE INTO scrape_progress
           (chat_id, last_msg_id, last_scraped_at, scrape_label, last_scrape_mode)
           VALUES(?,?,?,?,?)''',
        (src_chat_id, new_last, now_ts, label, scrape_mode)
    )
    conn.commit(); conn.close()

    _log_scrap(label, 'done', f'{count} new, last_msg={new_last}, mode={scrape_mode}')
    _scrap_current[0] = ''

    if status_cb:
        mode_tag = {'from_date': '📅 from-date', 'full': '🔁 full', 'resume': '▶️ resume'}.get(scrape_mode, scrape_mode)
        await status_cb(
            f'✅ `{label}` [{mode_tag}]: **{count}** new books. '
            f'(scanned {checked} msgs, up to `{new_last}`)'
        )

async def _wipe_books(scope: str = 'all', chat_id: int = None) -> dict:
    conn = db_connect(); c = conn.cursor()

    if scope == 'all':
        c.execute('SELECT COUNT(*) FROM books')
        books_deleted = c.fetchone()[0]
        c.execute('DELETE FROM books')
        c.execute("DELETE FROM sqlite_sequence WHERE name='books'")
        c.execute('DROP TABLE IF EXISTS books_fts')
        c.execute('''CREATE VIRTUAL TABLE books_fts USING fts5(
            search_name, content='books', content_rowid='id', tokenize='unicode61'
        )''')
        c.execute('''CREATE TRIGGER IF NOT EXISTS books_ai AFTER INSERT ON books BEGIN
            INSERT INTO books_fts(rowid,search_name) VALUES(new.id,new.search_name);
        END;''')
        c.execute('UPDATE scrape_progress SET last_msg_id=0, last_scraped_at=0, last_scrape_mode="full"')
    else:
        c.execute('SELECT COUNT(*) FROM books WHERE chat_id=?', (chat_id,))
        books_deleted = c.fetchone()[0]
        c.execute('DELETE FROM books WHERE chat_id=?', (chat_id,))
        c.execute('UPDATE scrape_progress SET last_msg_id=0, last_scraped_at=0, last_scrape_mode="full" WHERE chat_id=?',
                  (chat_id,))
        c.execute("INSERT INTO books_fts(books_fts) VALUES('rebuild')")

    conn.commit(); conn.close()

    cache_cleared = 0
    for fn in os.listdir(CACHE_DIR):
        fp = os.path.join(CACHE_DIR, fn)
        if os.path.isfile(fp):
            try: os.remove(fp); cache_cleared += 1
            except: pass
    _SEARCH_CACHE.clear()

    _log_scrap('_wipe_', 'fresh-wipe', f'scope={scope}, deleted={books_deleted}')
    return {'books_deleted': books_deleted, 'cache_cleared': cache_cleared}


# ─────────────────────────────────────────────────────────────────────────────
# Scrape job orchestrator
# ─────────────────────────────────────────────────────────────────────────────
async def _run_scrap_job(user_client, reply_fn, targets: list[str],
                         job_label: str = 'manual', started_by: int = 0,
                         scrape_mode: str = 'resume',
                         from_date_map: dict = None,
                         target_db: str = None):
    async with _scrap_lock:
        _scrap_running[0] = True
        _scrap_cancel[0]  = False
        _scrap_who[0]     = started_by
        _scrap_started[0] = time.time()

        try:
            mode_icon = {'resume': '▶️', 'from_date': '📅', 'full': '🔁'}.get(scrape_mode, '⚙️')
            total = len(targets)
            await reply_fn(
                f'🚀 **Scrape job started** [{job_label}] {mode_icon}\n'
                f'📦 {total} source(s) queued\n'
                f'_Use `/scrap_cancel` to stop gracefully._'
            )
            _log_scrap('_job_', 'start', f'{total} sources, mode={scrape_mode}, by={started_by}')

            for i, src in enumerate(targets, 1):
                if _scrap_cancel[0]:
                    await reply_fn(f'🛑 **Job cancelled** after {i-1}/{total} sources.')
                    break
                fd = (from_date_map or {}).get(src)
                # Use companion client if this source is assigned to one
                _comp = _companion_owns_source(src)
                _scrape_client = (_comp.client if _comp and _comp.running and _comp.client
                                  else user_client)

                async def _cb(m, _rf=reply_fn): await _rf(m)
                await index_books(user_client, src, status_cb=_cb,
                                  cancel_ref=_scrap_cancel,
                                  from_date=fd, scrape_mode=scrape_mode,
                                  target_db=target_db,
                                  companion_client=_scrape_client if _scrape_client is not user_client else None)

                if _scrap_cancel[0]:
                    await reply_fn(f'🛑 **Job cancelled** mid-source ({i}/{total}).')
                    break

            # Run FTS rebuild in thread so it never blocks the event loop
            _fts_db = target_db or DB_PATH
            def _fts_rebuild():
                try:
                    conn = _make_db_conn(_fts_db)
                    conn.execute("INSERT INTO books_fts(books_fts) VALUES('rebuild')")
                    conn.commit(); conn.close()
                except Exception as e:
                    log.warning(f'FTS rebuild: {e}')

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(_SEARCH_EXECUTOR, _fts_rebuild)

            elapsed = int(time.time() - _scrap_started[0])
            h, m = elapsed // 3600, (elapsed % 3600) // 60
            elapsed_str = f'{h}h {m}m' if h else f'{m}m {elapsed%60}s'

            if not _scrap_cancel[0]:
                await reply_fn(
                    f'✅ **Scrape job complete** [{job_label}]\n'
                    f'⏱ Elapsed: `{elapsed_str}`\n'
                    f'_Use `/scrap_status` to see full results._'
                )
            _log_scrap('_job_', 'done', f'elapsed={elapsed_str}')

        except Exception as job_err:
            log.error(f'_run_scrap_job crashed: {job_err}', exc_info=True)
            try:
                await reply_fn(f'❌ **Scrape job crashed**: `{job_err}`\n_Bot is still running._')
            except Exception:
                pass
            try:
                await report_error('_run_scrap_job', job_err)
            except Exception:
                pass
        finally:
            # ALWAYS reset — prevents permanent stuck state after any crash
            _scrap_running[0] = False
            _scrap_cancel[0]  = False
            _scrap_who[0]     = 0
            _scrap_current[0] = ''

# ─────────────────────────────────────────────────────────────────────────────
# Caption builder — now uses template system
# ─────────────────────────────────────────────────────────────────────────────
def build_caption(fname: str, user_id: int, first_name: str,
                  username: str = None,
                  template_name: str = 'default',
                  purge_secs: int = 600,
                  last_name: str = None,
                  src_chat_id=None,
                  book_id=None) -> str:
    """
    Build a delivery caption using the named template.

    book_id     — pass this so {book_source} resolves via orig_chat_id,
                  even after the book has been backed up and chat_id changed.
    src_chat_id — current chat_id (used as fallback if orig_chat_id not set)
    """
    return render_caption(
        template_name=template_name,
        fname=fname,
        user_id=user_id,
        first_name=first_name,
        username=username,
        purge_secs=purge_secs,
        last_name=last_name,
        src_chat_id=src_chat_id,
        book_id=book_id,
    )


async def handle_search(event, interaction_client, user_client, mode: str):
    sender_id  = get_sender_id(event)
    sender     = event.sender
    first_name = getattr(sender, 'first_name', 'User') or 'User'
    last_name  = getattr(sender, 'last_name',  None)
    username   = getattr(sender, 'username', None)
    tid        = get_event_thread_id(event)
    # Thread-awareness: if user is replying inside a sub-thread, deliver there
    if not tid and event.reply_to:
        _rt = event.reply_to
        _top = getattr(_rt, 'reply_to_top_id', None)
        _mid = getattr(_rt, 'reply_to_msg_id', None)
        _is_topic = getattr(_rt, 'forum_topic', False)
        if _top and not _is_topic:
            tid = _top  # user is in a reply thread — deliver there
    registry   = TRIGGER_CHATS if mode == 'trigger' else ASSIGNED_CHATS
    purge_hrs  = get_assignment(event.chat_id, tid, registry) if not event.is_private else None

    # ── Detect if searcher replied to another user's message ─────────────────
    # If so, the FIRST book download from this query will mention that user
    # with a hyperlinked full name so they get a notification.
    _mentioned_user: dict | None = None
    if not event.is_private and event.reply_to:
        _reply_msg_id = getattr(event.reply_to, 'reply_to_msg_id', None)
        # Don't treat a forum topic header as a user reply
        _is_forum_topic = getattr(event.reply_to, 'forum_topic', False)
        if _reply_msg_id and not _is_forum_topic:
            try:
                _replied_msg = await interaction_client.get_messages(
                    event.chat_id, ids=_reply_msg_id
                )
                if _replied_msg and _replied_msg.sender_id:
                    _rsid = _replied_msg.sender_id
                    # Ignore if replying to own message or to a bot/channel
                    if _rsid != sender_id:
                        _rs = _replied_msg.sender
                        _rfn = getattr(_rs, 'first_name', '') or ''
                        _rln = getattr(_rs, 'last_name',  '') or ''
                        _run = getattr(_rs, 'username',   None)
                        _is_bot = getattr(_rs, 'bot', False)
                        if not _is_bot and _rsid > 0:
                            _mentioned_user = {
                                'user_id':    _rsid,
                                'first_name': _rfn,
                                'last_name':  _rln,
                                'username':   _run,
                            }
            except Exception:
                pass

    query = event.text.strip()
    if mode == 'trigger':
        query = re.sub(r'^([/\\.](boi|find|search|kitab)|[/\\.]বই|।বই)\s*', '', query, flags=re.IGNORECASE).strip()

    query = sanitize_query(query) if query else None
    if not query:
        if not event.is_private:
            sent = await interaction_client.send_message(
                event.chat_id,
                f'💡 `.বই <নাম>` · `।বই <নাম>` · `.boi <n>`\n'
                f'_কমপক্ষে {QUERY_MIN_LEN()} অক্ষর লিখুন_',
                reply_to=tid, parse_mode='md'
            )
            schedule_delete(sent.id, sent.chat_id, 30)
            schedule_delete(event.id, event.chat_id, 0)
        return

    allowed, reason = spam_check_search(sender_id, event.chat_id)
    if not allowed:
        if WARN_ON_COOLDOWN() and not event.is_private:
            if reason.startswith('flood:'):
                mins = reason.split(':')[1]
                msg = f'⚠️ Slow down! You are sending too many requests. Ignored for `{mins}m`.'
            elif reason.startswith('muted:'):
                mins = reason.split(':')[1]
                msg = f'⏳ You are temporarily muted for `{mins}m` due to spam.'
            elif reason == 'cooldown':
                msg = None
            elif reason == 'chat_rate':
                msg = f'⚠️ This chat is too busy right now. Please wait a moment.'
            else:
                msg = None
            if msg:
                sent = await interaction_client.send_message(
                    event.chat_id, msg, reply_to=tid, parse_mode='md'
                )
                schedule_delete(sent.id, event.chat_id, 8)
        schedule_delete(event.id, event.chat_id, 0)
        return

    if not event.is_private:
        schedule_delete(event.id, event.chat_id, 0)
    elif DM_PURGE_SECS_REF[0]:
        schedule_delete(event.id, event.chat_id, DM_PURGE_SECS_REF[0])

    asyncio.create_task(report_search(
        sender_id, first_name, username,
        chat_label(event), event.chat_id, query, tid
    ))

    query_hash = hashlib.md5(query.lower().encode()).hexdigest()

    results = None
    now = time.time()
    async with _SEARCH_CACHE_LOCK:
        entry = _SEARCH_CACHE.get(query_hash)
        if entry and (now - entry[1]) < _SEARCH_CACHE_TTL:
            results = entry[0]

    if results is None:
        loop    = asyncio.get_event_loop()
        results = await loop.run_in_executor(_SEARCH_EXECUTOR, smart_search, query)

        async with _SEARCH_CACHE_LOCK:
            if len(_SEARCH_CACHE) >= _SEARCH_CACHE_MAX:
                oldest = min(_SEARCH_CACHE.items(), key=lambda x: x[1][1])
                del _SEARCH_CACHE[oldest[0]]
            _SEARCH_CACHE[query_hash] = (results, time.time())

        cache_path = os.path.join(CACHE_DIR, f'{query_hash}.json')
        try:
            with open(cache_path, 'w') as f:
                json.dump(results, f)
        except Exception:
            pass

        # ── Fuzzy fallback 1: vowel-stripped query (Bengali only) ────────────
        if not results and query and _RE_BENGALI.search(query):
            _sq = _RE_STRIP_VOWELS.sub('', normalize_name(query))
            if _sq and _sq != normalize_name(query) and len(_sq) >= 2:
                results = await loop.run_in_executor(_SEARCH_EXECUTOR, smart_search, _sq)

        # ── Fuzzy fallback 2: transliteration (ASCII queries) ─────────────
        if not results and query and not _RE_BENGALI.search(query):
            for _bn_cand in _transliterate_query(normalize_name(query)):
                _tr = await loop.run_in_executor(_SEARCH_EXECUTOR, smart_search, _bn_cand)
                if _tr:
                    results = _tr
                    break

        # ── Fuzzy fallback 3: individual words — longest first ────────────
        if not results and query:
            results = []
            _seen_ids: set = set()
            _words = sorted(
                [w for w in query.split() if len(w) >= 2],
                key=len, reverse=True
            )
            for _w in _words[:4]:
                _wr = await loop.run_in_executor(_SEARCH_EXECUTOR, smart_search, _w)
                for _r in _wr:
                    if _r[0] not in _seen_ids:
                        results.append(_r)
                        _seen_ids.add(_r[0])
                if results:
                    break

    if not results:
        suggestion_lines = ''
        try:
            sugg_conn = db_connect(); sugg_c = sugg_conn.cursor()
            qwords = [w for w in normalize_name(query).split() if w]
            all_sugg_words: set[str] = set(qwords)
            for w in qwords:
                for vv in _bn_variants(w):
                    vvn = normalize_name(vv)
                    if vvn:
                        all_sugg_words.update(vvn.split())
            sugg_words = list(all_sugg_words)[:5]
            if sugg_words:
                fts_sugg = ' OR '.join(f'"{w}"*' for w in sugg_words)
                sugg_c.execute(
                    'SELECT b.file_name FROM books b JOIN books_fts ON b.id=books_fts.rowid'
                    ' WHERE books_fts MATCH ? ORDER BY bm25(books_fts) LIMIT 5',
                    (fts_sugg,)
                )
                suggestions = sugg_c.fetchall()
                if suggestions:
                    suggestion_lines = '\n\n💡 **মিলতে পারে এমন বই:**\n' + '\n'.join(
                        f'• `{re.sub(r".(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$", "", s[0], flags=re.IGNORECASE).strip()}`'
                        for s in suggestions
                    )
            sugg_conn.close()
        except Exception:
            pass

        _has_bn = bool(_RE_BENGALI.search(query or ''))
        _tips = (
            '• বানান একটু বদলে দেখুন (যেমন: ী → ি, ূ → ু)\n'
            '• লেখকের নাম দিয়ে খুঁজুন\n'
            '• ইংরেজিতেও খুঁজে দেখুন'
            if _has_bn else
            '• Try spelling in Bengali: `.বই রবীন্দ্রনাথ`\n'
            '• Try author name separately\n'
            '• Try shorter keywords'
        )
        zero_msg = (
            f'❌ **কোনো বই পাওয়া যায়নি** — `{query}`\n\n'
            f'💡 **চেষ্টা করুন:**\n'
            f'{_tips}\n'
            f'• `.request {query}` লিখে বইটা চেয়ে নিন'
            + suggestion_lines
        )
        sent = await interaction_client.send_message(
            event.chat_id, zero_msg,
            reply_to=tid, parse_mode='md'
        )
        # Use SEARCH_RESULT_PURGE_SECS for zero-result message too
        _srp = SEARCH_RESULT_PURGE_SECS[0]
        delay = (DM_PURGE_SECS_REF[0] or 120) if event.is_private else (_srp if _srp else (purge_hrs or 3600))
        schedule_delete(sent.id, sent.chat_id, delay)
        return

    await send_page(event, interaction_client, query_hash, results, 0, mode,
                    mentioned_user=_mentioned_user)


def _make_book_buttons(formats: list) -> list:
    if not formats:
        return []

    _EBOOK_ICONS = {
        'epub': '📕', 'mobi': '📗', 'azw': '📗', 'azw3': '📗',
        'kfx': '📗', 'pdf': '📄', 'djvu': '📄', 'fb2': '📘',
        'lit': '📘', 'cbz': '📙', 'cbr': '📙', 'rtf': '📝',
        'doc': '📝', 'docx': '📝',
    }

    first   = formats[0]
    display = re.sub(
        r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$',
        '', first['name'], flags=re.IGNORECASE
    ).strip()
    display = re.sub(r'[_\-+\[\]()\.\{\}]', ' ', display).strip()
    # Full-width title row — truncate generously since nothing shares this row
    if len(display) > 58:
        display = display[:55] + '\u2026'

    best     = next((f for f in formats if not f['is_restricted']), formats[0])
    title_cb = f'get_{best["book_id"]}'.encode()
    save_cb  = f'col_picker_{best["book_id"]}'.encode()

    # Row 1 — full-width title (whole row to itself = no truncation issue)
    rows = [[Button.inline(display, data=title_cb)]]

    # Row 2 — format/size buttons  +  compact ➕ at the very end
    fmt_btns = []
    for f in formats:
        fmt      = (f['ext'] or 'pdf').lower().replace('.', '')
        icon     = _EBOOK_ICONS.get(fmt, '📄')
        lock     = '🔒' if f['is_restricted'] else ''
        size_str = f'{f["size"]/(1024*1024):.1f}MB' if f['size'] else '?'
        cb       = f'get_{f["book_id"]}'.encode()
        fmt_btns.append(Button.inline(f'{icon}{fmt.upper()} {size_str}{lock}', data=cb))

    fmt_btns.append(Button.inline('➕', data=save_cb))
    rows.append(fmt_btns)
    return rows


async def send_page(event, interaction_client, query_hash, results, page,
                    mode='assigned', tid_override=None, mentioned_user: dict | None = None):
    """
    Send or edit a search results page.

    mentioned_user — if the searcher replied to another user's message,
                     pass that user's info here.  Their hyperlinked name is
                     injected into the result header for the FIRST book
                     download from this query (so they get a notification).
                     Format: {'user_id': int, 'first_name': str,
                              'last_name': str, 'username': str|None}
    """

    all_cards_map:  dict[str, list] = {}
    all_cards_order: list[str]      = []

    for row in results:
        book_id  = row[0]
        name     = row[1]
        size     = row[2]
        is_restr = row[3]
        ext      = (row[4] if len(row) > 4 else 'pdf') or 'pdf'
        score    = row[5] if len(row) > 5 else 0
        norm_key = normalize_name(name)

        fmt_dict = {
            'book_id':       book_id,
            'name':          name,
            'size':          size,
            'is_restricted': is_restr,
            'ext':           ext,
            'score':         score,
        }
        if norm_key not in all_cards_map:
            all_cards_map[norm_key] = []
            all_cards_order.append(norm_key)
        all_cards_map[norm_key].append(fmt_dict)

    for key in all_cards_order:
        all_cards_map[key].sort(key=lambda f: (
            1 if f['is_restricted'] else 0,
            0 if (f['ext'] or '').lower().replace('.', '') == 'pdf' else 1,
        ))

    n_cards = len(all_cards_order)
    total   = max(1, (n_cards + PER_PAGE - 1) // PER_PAGE)
    page    = max(0, min(page, total - 1))
    card_keys_page = all_cards_order[page * PER_PAGE:(page + 1) * PER_PAGE]

    if tid_override is not None:
        tid = tid_override
    elif hasattr(event, 'reply_to'):
        tid = get_event_thread_id(event)
    else:
        tid = None

    is_private = getattr(event, 'is_private', False)
    registry   = TRIGGER_CHATS if mode == 'trigger' else ASSIGNED_CHATS
    purge_secs = get_assignment(event.chat_id, tid, registry) if not is_private else None

    use_btns = bool(BOT_TOKEN)

    # ── Build mention header if searcher replied to another user ─────────────
    mention_line    = ''
    mention_entities: list = []
    if mentioned_user and page == 0 and not is_private:
        _muid  = mentioned_user['user_id']
        _mfn   = mentioned_user['first_name'] or 'User'
        _mln   = mentioned_user.get('last_name') or ''
        _full  = f'{_mfn} {_mln}'.strip()
        mention_line = f'👤 {_full}\n'
        mention_entities = []   # filled after send via entities param

    unique_count = n_cards
    text = (
        mention_line
        + f'\U0001f4da **Search Results**\n'
        f'\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501'
        f'\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n'
        f'Page {page+1} of {total}  \u2022  {unique_count} book(s) found\n'
        + (f'_(showing top {unique_count} ranked results)_\n'
           if len(results) >= MAX_RESULTS else '')
    )
    buttons = []

    for norm_key in card_keys_page:
        formats = all_cards_map[norm_key]

        if use_btns:
            buttons.extend(_make_book_buttons(formats))
        else:
            display = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$',
                             '', formats[0]['name'], flags=re.IGNORECASE).strip()
            icon    = '\U0001f512' if all(f['is_restricted'] for f in formats) else '\U0001f4d6'
            parts   = []
            for f in formats:
                fmt      = (f['ext'] or 'pdf').lower().replace('.', '').upper()
                size_str = f'{f["size"] / (1024 * 1024):.1f} MB' if f['size'] else '?'
                parts.append(f'{fmt} {size_str} /get_{f["book_id"]}')
            text += f'{icon} **{display}**\n\u2514 ' + '  |  '.join(parts) + '\n\n'

    if use_btns and total > 1:
        nav = []
        if page > 0:
            nav.append(Button.inline('« First', data=f'page_{query_hash}_0'.encode()))
            nav.append(Button.inline('‹ Prev',  data=f'page_{query_hash}_{page-1}'.encode()))
        nav.append(Button.inline(f' {page+1} / {total} ', data=f'page_{query_hash}_{page}'.encode()))
        if page < total - 1:
            nav.append(Button.inline('Next ›',  data=f'page_{query_hash}_{page+1}'.encode()))
            nav.append(Button.inline('Last »',  data=f'page_{query_hash}_{total-1}'.encode()))
        if nav: buttons.append(nav)

    if hasattr(event, 'answer'):
        try: await event.edit(text, buttons=buttons or None)
        except Exception: pass
        return

    # ── Build proper mention entity for the header line ───────────────────────
    # We parse the markdown text first to get its entities, then inject the
    # InputMessageEntityMentionName on top — this way ALL formatting works.
    _extra_entities = []
    if mentioned_user and page == 0 and not is_private:
        from telethon.tl.types import InputMessageEntityMentionName, MessageEntityBold
        _muid = mentioned_user['user_id']
        _mfn  = mentioned_user['first_name'] or 'User'
        _mln  = mentioned_user.get('last_name') or ''
        _full = f'{_mfn} {_mln}'.strip()
        # Offset inside text: '👤 ' = emoji(2 chars in UTF-16/Telegram) + space
        # Telegram counts entities in UTF-16 code units, but Python telethon
        # handles the conversion internally when we give byte offset of the string.
        # Safe approach: use len() on the Python string prefix.
        _offset = len('👤 ')
        try:
            peer = await interaction_client.get_input_entity(_muid)
            _extra_entities.append(
                InputMessageEntityMentionName(
                    offset=_offset, length=len(_full), user_id=peer
                )
            )
            _extra_entities.append(
                MessageEntityBold(offset=_offset, length=len(_full))
            )
        except Exception:
            pass  # name still shows as plain text — acceptable fallback

    sent = await interaction_client.send_message(
        event.chat_id, text,
        buttons=buttons or None,
        reply_to=tid,
        parse_mode='md',
        # Telethon merges these with the parsed markdown entities
        formatting_entities=_extra_entities if _extra_entities else None,
    )
    # ── Search result purge time ──────────────────────────────────────────────
    # Priority: SEARCH_RESULT_PURGE_SECS (global) → group purge_secs → 3600s
    _srp = SEARCH_RESULT_PURGE_SECS[0]
    if is_private:
        delay = DM_PURGE_SECS_REF[0] or 600
    elif _srp > 0:
        delay = _srp
    else:
        delay = purge_secs or 3600
    schedule_delete(sent.id, sent.chat_id, delay)
    # Store (msg_id, chat_id, mentioned_user) so button callback can read it
    _SEARCH_MSG_REGISTRY[query_hash] = (sent.id, sent.chat_id, mentioned_user)

# ─────────────────────────────────────────────────────────────────────────────
# Book delivery — now passes template + username to build_caption
# ─────────────────────────────────────────────────────────────────────────────
class _NullCtx:
    async def __aenter__(self): return self
    async def __aexit__(self, *_): pass

async def deliver_book(target_chat_id, book_id, user_client, interaction_client,
                       caption, reply_to_id, access_hash=None,
                       requester_id=0, requester_name='', requester_username='',
                       request_chat_id=0, request_chat_title='',
                       thread_id=None, query_hash=None,
                       group_purge_secs: int = 0,
                       caption_entities: list | None = None):
    # ── Duplicate-tap guard: ignore second delivery of same book by same user
    # within _DL_DEDUP_WINDOW seconds (e.g. double-click on the button)
    _dedup_key = (requester_id, book_id, target_chat_id)
    _now = time.time()
    _last = _dl_dedup_cache.get(_dedup_key, 0)
    if _now - _last < _DL_DEDUP_WINDOW:
        log.debug(f'deliver_book: dedup dropped uid={requester_id} book={book_id}')
        return False
    _dl_dedup_cache[_dedup_key] = _now
    # Prune old entries to avoid memory leak
    if len(_dl_dedup_cache) > 2000:
        cutoff = _now - _DL_DEDUP_WINDOW * 10
        for k in [k for k, v in _dl_dedup_cache.items() if v < cutoff]:
            del _dl_dedup_cache[k]
    _progress_reply = thread_id if thread_id is not None else reply_to_id
    conn = db_connect(); c = conn.cursor()
    c.execute('SELECT file_name,message_id,chat_id,is_restricted FROM books WHERE id=?', (book_id,))
    row = c.fetchone(); conn.close()
    if not row: return False

    file_name, src_msg_id, src_chat_id, is_restricted = row

    if is_restricted:
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT id,message_id,chat_id FROM books WHERE file_name=? AND is_restricted=0 ORDER BY id DESC LIMIT 1', (file_name,))
        alt = c.fetchone(); conn.close()
        if alt:
            book_id, src_msg_id, src_chat_id = alt
            is_restricted = False

    asyncio.create_task(report_download(
        requester_id, requester_name, requester_username,
        request_chat_title, request_chat_id, file_name, book_id
    ))

    target_entity = target_chat_id
    if access_hash and isinstance(target_chat_id, int) and target_chat_id > 0:
        try:
            target_entity = await user_client.get_entity(
                types.InputPeerUser(user_id=target_chat_id, access_hash=access_hash)
            )
        except Exception: pass

    if not is_restricted:
        fetch_chat_id = _to_full_channel_id(src_chat_id) if isinstance(src_chat_id, int) and src_chat_id > 0 else src_chat_id
        _is_dm_delivery = isinstance(target_chat_id, int) and target_chat_id > 0

        # For DM deliveries via userbot, ensure the channel is open first.
        # If not, bot greets the user and we fall back to bot delivery.
        if _is_dm_delivery and USERBOT_USERNAME[0]:
            _dm_ok = await _ensure_dm_open(
                user_client, interaction_client, target_chat_id,
                first_name=requester_name, username=requester_username
            )
            _try_order = [(user_client, target_entity), (interaction_client, target_chat_id)]
            if not _dm_ok:
                # Bot can send without restriction — put it first
                _try_order = [(interaction_client, target_chat_id), (user_client, target_entity)]
        else:
            _try_order = [(user_client, target_entity), (interaction_client, target_chat_id)]

        for client, tgt in _try_order:
            try:
                src = await client.get_messages(fetch_chat_id, ids=src_msg_id)
                if src and src.media:
                    sent = await client.send_file(
                        tgt, src.media, caption=caption,
                        reply_to=reply_to_id, parse_mode='md',
                        formatting_entities=caption_entities or None,
                    )
                    _dl_delay = (DM_PURGE_SECS_REF[0] or 600) if _is_dm_delivery else (group_purge_secs or 3600)
                    _sent_by_user = (client is user_client)
                    schedule_delete(sent.id, target_chat_id, _dl_delay, use_user_client=_is_dm_delivery and _sent_by_user)
                    # Mark DM as unlocked since send succeeded
                    if _is_dm_delivery:
                        dm_mark_unlocked(target_chat_id, requester_username, requester_name,
                                         method='send_success')
                    return True
            except Exception as _se:
                log.debug(f'deliver_book send attempt failed: {_se}')
                continue
        return False

    dl_display = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', file_name, flags=re.IGNORECASE).strip()
    dl_display = re.sub(r'[_\-+\[\]()\.\{\}]', ' ', dl_display).strip()
    if len(dl_display) > 40: dl_display = dl_display[:37] + '…'

    if _DL_SEMAPHORE and _DL_SEMAPHORE._value == 0:
        try:
            qmsg = await interaction_client.send_message(
                target_chat_id,
                f'⏳ **Queued**\n📖 `{dl_display}`\n_Waiting for a download slot…_',
                reply_to=_progress_reply, parse_mode='md'
            )
            schedule_delete(qmsg.id, target_chat_id, 120)
        except Exception: pass

    async with (_DL_SEMAPHORE or _NullCtx()):
        notify     = [None]
        notify_txt = ['']
        last_edit  = [0.0]
        EDIT_INTERVAL = 1.5
        RESEND_AFTER  = 8

        def _progress_bar(pct: float, width: int = 14) -> str:
            filled = int(width * pct / 100)
            return '█' * filled + '░' * (width - filled)

        async def _send_progress(text: str):
            if notify[0]:
                try: await interaction_client.delete_messages(target_chat_id, notify[0].id)
                except Exception: pass
            try:
                notify[0] = await interaction_client.send_message(
                    target_chat_id, text, reply_to=_progress_reply, parse_mode='md'
                )
            except Exception: pass

        async def _update_progress(stage: str, pct: float, speed_kb: float = 0, eta_s: float = 0):
            bar       = _progress_bar(pct)
            speed_str = f'{speed_kb:.0f} KB/s' if speed_kb >= 1 else ''
            eta_str   = f'ETA {int(eta_s)}s'   if eta_s > 0   else ''
            meta      = ' • '.join(filter(None, [speed_str, eta_str]))
            text = (
                f'🔒 **Restricted File**\n📖 `{dl_display}`\n'
                f'`{bar}` **{pct:.0f}%**\n_{stage}_'
                + (f'\n`{meta}`' if meta else '')
            )
            notify_txt[0] = text
            now = time.time()
            if not notify[0]:
                await _send_progress(text); return
            if now - last_edit[0] >= EDIT_INTERVAL:
                last_edit[0] = now
                try:
                    await interaction_client.edit_message(target_chat_id, notify[0].id, text, parse_mode='md')
                except errors.MessageNotModifiedError: pass
                except (errors.MessageIdInvalidError, errors.ChatNotFoundError):
                    await _send_progress(text)
                except Exception: pass

        resend_running = [True]

        async def _resend_watcher():
            await asyncio.sleep(5)
            while resend_running[0]:
                await asyncio.sleep(RESEND_AFTER)
                if not notify[0] or not notify_txt[0]: continue
                try:
                    kwargs = dict(limit=3)
                    if reply_to_id: kwargs['reply_to'] = reply_to_id
                    latest = await interaction_client.get_messages(target_chat_id, **kwargs)
                    if latest:
                        latest_id = max(m.id for m in latest)
                        if latest_id > notify[0].id + 1:
                            await _send_progress(notify_txt[0])
                except Exception: pass

        asyncio.create_task(_resend_watcher())
        try: await _update_progress('Preparing download…', 0)
        except Exception: pass

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = None
            try:
                ent = await user_client.get_entity(
                    _to_full_channel_id(src_chat_id) if isinstance(src_chat_id, int) and src_chat_id > 0 else src_chat_id
                )
                msg = await user_client.get_messages(ent, ids=src_msg_id)
                if not msg or not msg.media:
                    raise RuntimeError('Message or media not found')

                total_size = getattr(msg.file, 'size', 0) or 0
                fname_hint = getattr(msg.file, 'name', None) or file_name
                local_path = os.path.join(tmpdir, fname_hint)

                dl_start   = time.time()
                bytes_done = [0]
                real_size  = total_size

                with open(local_path, 'wb') as out_f:
                    async for chunk in user_client.iter_download(msg.media, request_size=512*1024):
                        out_f.write(chunk)
                        bytes_done[0] += len(chunk)
                        now     = time.time()
                        elapsed = max(now - dl_start, 0.001)
                        spd_kb  = bytes_done[0] / elapsed / 1024
                        if real_size:
                            pct   = min(bytes_done[0] / real_size * 100, 99.9)
                            rem   = max((real_size - bytes_done[0]) / max(bytes_done[0] / elapsed, 1), 0)
                            stage = f'Downloading… {bytes_done[0]/1024/1024:.1f} / {real_size/1024/1024:.1f} MB'
                        else:
                            pct = 50.0; rem = 0
                            stage = f'Downloading… {bytes_done[0]/1024/1024:.1f} MB'
                        asyncio.create_task(_update_progress(stage, pct, spd_kb, rem))

                dl_elapsed = time.time() - dl_start
                act_size   = os.path.getsize(local_path)
                log.info(f'Downloaded {file_name} in {dl_elapsed:.1f}s @ {act_size/dl_elapsed/1024:.0f} KB/s')

            except Exception as e:
                resend_running[0] = False
                log.warning(f'Restricted dl: {e}')
                await report_error('deliver_book restricted download', e)
                if notify[0]:
                    try:
                        await interaction_client.edit_message(
                            target_chat_id, notify[0].id,
                            f'❌ **Download failed**\n📖 `{dl_display}`\n_Try again later._',
                            parse_mode='md'
                        )
                        schedule_delete(notify[0].id, target_chat_id, 30)
                    except Exception: pass
                return False

            await _update_progress('Uploading to backup…', 100)
            up_start = [time.time()]

            async def on_upload(sent_bytes: int, total_bytes: int):
                now = time.time(); elapsed = max(now - up_start[0], 0.001)
                pct = min((sent_bytes / total_bytes * 100) if total_bytes else 99, 99.9)
                spd_kb = sent_bytes / elapsed / 1024
                rem = ((total_bytes - sent_bytes) / max(sent_bytes / elapsed, 1)) if sent_bytes > 0 else 0
                await _update_progress(
                    f'Uploading… {sent_bytes/1024/1024:.1f} / {total_bytes/1024/1024:.1f} MB',
                    pct, spd_kb, rem
                )

            # Resolve original source label BEFORE backup (src_chat_id is still correct here)
            _orig_source_plain, _orig_source_link = _resolve_book_source(src_chat_id, book_id=book_id)
            # Embed source info in the backup caption so it's self-documenting
            # Format: #*#source_info: <chat_id> | <label>#*#
            _src_tag = (
                f'\n#*#source_info: {src_chat_id} | {_orig_source_plain}#*#'
            )
            _bak_caption = f'📦 {file_name}{_src_tag}'

            bak = None
            try:
                bak = await user_client.send_file(BACKUP_GROUP_ID[0], local_path,
                                                   caption=_bak_caption,
                                                   progress_callback=on_upload)
                conn = db_connect(); c = conn.cursor()
                # Update message_id and chat_id for retrieval, but KEEP orig_chat_id intact
                # so {book_source} keeps resolving to the real source even after backup
                c.execute(
                    'UPDATE books SET message_id=?, chat_id=?, is_restricted=0'
                    ' WHERE id=? AND (orig_chat_id IS NULL OR orig_chat_id=0 OR orig_chat_id=?)',
                    (bak.id, BACKUP_GROUP_ID[0], book_id, BACKUP_GROUP_ID[0])
                )
                # If orig_chat_id was never set, set it now to the real source
                c.execute(
                    'UPDATE books SET orig_chat_id=? WHERE id=? AND (orig_chat_id IS NULL OR orig_chat_id=0)',
                    (src_chat_id, book_id)
                )
                # Also update message_id/chat_id unconditionally for retrieval
                c.execute(
                    'UPDATE books SET message_id=?, chat_id=?, is_restricted=0 WHERE id=?',
                    (bak.id, BACKUP_GROUP_ID[0], book_id)
                )
                conn.commit(); conn.close()
                log.info(f'Backed up & unmarked restricted: {file_name} (orig_src={src_chat_id})')
            except Exception as e:
                log.warning(f'Backup upload failed: {e}')

            await _update_progress('Sending to you…', 100)
            sent = None
            if bak:
                try:
                    sent = await user_client.send_file(
                        target_entity, bak.media, caption=caption,
                        reply_to=reply_to_id, parse_mode='md',
                        formatting_entities=caption_entities or None,
                    )
                except Exception as e:
                    log.warning(f'Send from backup media failed: {e}')

            if not sent:
                for client, tgt in [(user_client, target_entity), (interaction_client, target_chat_id)]:
                    try:
                        sent = await client.send_file(
                            tgt, local_path, caption=caption,
                            reply_to=reply_to_id, parse_mode='md',
                            formatting_entities=caption_entities or None,
                        )
                        break
                    except Exception as e:
                        log.warning(f'Direct send failed: {e}')

            resend_running[0] = False
            if notify[0]:
                try: await interaction_client.delete_messages(target_chat_id, notify[0].id)
                except Exception: pass

            if not sent: return False
            _is_dm = isinstance(target_chat_id, int) and target_chat_id > 0
            if _is_dm:
                _dl_delay = DM_PURGE_SECS_REF[0] or 600
            else:
                _dl_delay = group_purge_secs or 3600
            schedule_delete(sent.id, target_chat_id, _dl_delay, use_user_client=_is_dm)

        return True

    return False

# ─────────────────────────────────────────────────────────────────────────────
# Book Request system
# ─────────────────────────────────────────────────────────────────────────────
async def handle_request(event, interaction_client, user_client):
    if not REQUEST_GROUP[0]: return

    sender_id  = get_sender_id(event)
    sender     = event.sender
    first_name = getattr(sender, 'first_name', 'User') or 'User'
    username   = getattr(sender, 'username', None)
    tid        = get_event_thread_id(event)

    allowed_req, req_reason = spam_check_request(sender_id)
    if not allowed_req:
        mins = req_reason.split(':')[1] if ':' in req_reason else '10'
        sent = await interaction_client.send_message(
            event.chat_id,
            f'⏳ Too many requests. You can request again in `{mins}m`.',
            reply_to=tid, parse_mode='md'
        )
        schedule_delete(sent.id, event.chat_id, 15)
        if not event.is_private: schedule_delete(event.id, event.chat_id, 0)
        return

    text = event.text.strip()
    req_text = re.sub(r'^\.request\s*', '', text, flags=re.IGNORECASE).strip()

    if not req_text:
        sent = await interaction_client.send_message(
            event.chat_id,
            '📝 Usage: `.request <book name> by <author>`\nExample: `.request Harry Potter by J.K. Rowling`',
            reply_to=tid, parse_mode='md'
        )
        schedule_delete(sent.id, sent.chat_id, 30)
        schedule_delete(event.id, event.chat_id, 0)
        return

    by_match = re.search(r'\s+by\s+(.+)$', req_text, re.IGNORECASE)
    if by_match:
        book_title = req_text[:by_match.start()].strip()
        author     = by_match.group(1).strip()
    else:
        book_title = req_text
        author     = None

    if not event.is_private:
        schedule_delete(event.id, event.chat_id, 0)

    uref = user_mention_md(sender_id, first_name)
    uname_str  = f'@{username}' if username else f'id:{sender_id}'
    author_line = f'\n✍️ Author: **{author}**' if author else ''

    req_text_post = (
        f'#request #boi\n\n'
        f'📖 Book: **{book_title}**{author_line}\n\n'
        f'👤 Requested by: {uref} ({uname_str})\n'
        f'🕐 {ts_str()}'
    )
    try:
        req_msg = await user_client.send_message(REQUEST_GROUP[0], req_text_post, parse_mode='md')
        access_hash = getattr(sender, 'access_hash', None)
        pending_requests[req_msg.id] = {
            'requester_id':   sender_id,
            'access_hash':    access_hash,
            'book_title':     book_title,
            'first_name':     first_name,
            'username':       username,
            'origin_chat_id': event.chat_id,
            'origin_thread':  tid,
            'is_private':     event.is_private,
        }
        confirm_text = (
            f'✅ **Request sent!**\n\n📖 `{book_title}`'
            + (f'\n✍️ by `{author}`' if author else '') +
            f'\n\nWe\'ll deliver it to you when available. 📬'
        )
        if event.is_private:
            await interaction_client.send_message(event.chat_id, confirm_text, parse_mode='md')
        else:
            sent = await interaction_client.send_message(
                event.chat_id, confirm_text, reply_to=tid, parse_mode='md'
            )
            schedule_delete(sent.id, sent.chat_id, 60)
        asyncio.create_task(report(
            f'📝 **Book Request**\n👤 {uref} ({uname_str})\n'
            f'📖 `{book_title}`' + (f'\n✍️ `{author}`' if author else '') +
            f'\n🕐 {ts_str()}'
        ))
    except Exception as e:
        log.warning(f'handle_request: {e}')
        await report_error('handle_request', e)

# ─────────────────────────────────────────────────────────────────────────────
# Admin commands — v12 adds template management commands
# ─────────────────────────────────────────────────────────────────────────────
async def _do_disable_source(event, sender_id: int, chat_id: int, label: str,
                              raw_ref: str, book_count: int, keep_books: bool,
                              user_client, interaction_client):
    """
    Execute a source disable:
      1. Remove from SOURCE_GROUPS + settings.json
      2. Delete all books (unless keep_books=True)
      3. Remove from companion sources if assigned
      4. Delete scrape_progress row
      5. Rebuild FTS + clear search cache
      6. Report to analytics group
    """
    prog = await event.reply(
        f'⏳ Disabling `{label}`…',
        parse_mode='md'
    )
    deleted_books = 0
    try:
        # ── 1. Remove from SOURCE_GROUPS ──────────────────────────────────────
        removed_from_sources = False
        for ref in list(SOURCE_GROUPS):
            norm = _normalize_id_for_compare(ref)
            if isinstance(norm, int) and norm == chat_id:
                SOURCE_GROUPS.remove(ref); removed_from_sources = True; break
            elif isinstance(norm, str) and norm == raw_ref.lstrip('@').lower():
                SOURCE_GROUPS.remove(ref); removed_from_sources = True; break
        # Also try exact string match as fallback
        clean_ref = raw_ref.lstrip('@')
        if not removed_from_sources and clean_ref in SOURCE_GROUPS:
            SOURCE_GROUPS.remove(clean_ref); removed_from_sources = True
        if removed_from_sources:
            _save_settings()

        # ── 2. Remove from companion sources ──────────────────────────────────
        companion_removed = ''
        for comp in COMPANION_CLIENTS:
            for s in list(comp.sources):
                sn = _normalize_id_for_compare(s)
                if (isinstance(sn, int) and sn == chat_id) or \
                   (isinstance(sn, str) and sn == raw_ref.lstrip('@').lower()) or \
                   s == str(chat_id):
                    comp.sources.remove(s)
                    companion_removed = comp.name
            _save_companions()

        # ── 3. Delete books and cleanup ────────────────────────────────────────
        conn = db_connect(); c = conn.cursor()
        if not keep_books and book_count > 0:
            c.execute('DELETE FROM books WHERE chat_id=?', (chat_id,))
            deleted_books = c.rowcount
            # Remove dead references from user collections
            try:
                col_conn = col_connect()
                col_conn.execute(
                    'DELETE FROM collection_items WHERE book_id NOT IN '
                    '(SELECT id FROM books)'
                )
                col_conn.commit(); col_conn.close()
            except Exception:
                pass

        # ── 4. Clear scrape_progress ───────────────────────────────────────────
        c.execute('DELETE FROM scrape_progress WHERE chat_id=?', (chat_id,))
        conn.commit()

        # ── 5. Rebuild FTS if books were deleted ──────────────────────────────
        if deleted_books > 0:
            c.execute("INSERT INTO books_fts(books_fts) VALUES('rebuild')")
            conn.commit()
        conn.close()

        # ── 6. Clear search cache ──────────────────────────────────────────────
        cleared_cache = 0
        try:
            for fn in os.listdir(CACHE_DIR):
                fp = os.path.join(CACHE_DIR, fn)
                if os.path.isfile(fp):
                    try: os.remove(fp); cleared_cache += 1
                    except Exception: pass
            if _SEARCH_CACHE_LOCK:
                async with _SEARCH_CACHE_LOCK:
                    _SEARCH_CACHE.clear()
            else:
                _SEARCH_CACHE.clear()
        except Exception:
            pass

        # ── 7. Build result message ────────────────────────────────────────────
        lines = [
            f'✅ **Source Disabled: `{label}`**',
            f'━━━━━━━━━━━━━━━━━━━━',
            f'📌 Chat ID: `{chat_id}`',
            f'{"📋 Removed from SOURCE_GROUPS ✅" if removed_from_sources else "ℹ️ Was not in SOURCE_GROUPS"}',
        ]
        if companion_removed:
            lines.append(f'🤝 Removed from companion **{companion_removed}** ✅')
        if not keep_books:
            lines.append(f'🗑 Books deleted: **{deleted_books:,}**')
        else:
            lines.append(f'📚 Books kept in DB: **{book_count:,}** `(--keep)`')
        lines.append('🧹 Scrape progress cleared ✅')
        if deleted_books > 0:
            lines.append('🔍 FTS index rebuilt ✅')
        if cleared_cache > 0:
            lines.append(f'🗄 Cache cleared: {cleared_cache} entries ✅')
        lines.append(f'🕐 {ts_str()}')

        await prog.edit('\n'.join(lines), parse_mode='md')

        # ── 8. Report to analytics ─────────────────────────────────────────────
        await report(
            f'🚫 **Source disabled**\n'
            f'Source: `{label}` (`{chat_id}`)\n'
            f'Books deleted: `{deleted_books:,}`\n'
            f'By admin: `{sender_id}`\n'
            f'🕐 {ts_str()}'
        )

    except Exception as e:
        log.error(f'_do_disable_source: {e}', exc_info=True)
        try:
            await prog.edit(f'❌ Error: `{e}`\nPartial changes may have been applied.', parse_mode='md')
        except Exception:
            pass


async def handle_admin(event, user_client, interaction_client):
    if not event.text or not event.text.strip().startswith('/'): return
    sender_id = get_sender_id(event)
    if not sender_id: return

    parts = event.text.strip().split()
    cmd   = parts[0].lower()

    if cmd == '/help':
        if not is_staff(sender_id):
            await event.reply(
                '📚 **EbookManager** 📖\n\n'
                '🔍 **বই খোঁজার উপায়:**\n'
                '`.বই <বইয়ের নাম>` — বাংলায় লিখে খোঁজো\n'
                '`।বই <বইয়ের নাম>` — এভাবেও চলে\n'
                '`.boi <n>` · `/boi <n>` — English-এও কাজ করে\n\n'
                '📝 **বই রিকোয়েস্ট করতে:**\n'
                '`.request <book> by <author>`\n\n'
                '💡 _বইয়ের নাম কমপক্ষে কয়েকটা অক্ষর লিখো_'
            )
            return
        mode_str = '🌐 PUBLIC' if SEARCH_MODE[0] == 'public' else '🔒 PRIVATE'
        owner_extra = (
            '\n👑 **Owner Only:**\n'
            '`/add_admin <id>` · `/scrap_fresh [src]` · `/del_template <n>`\n'
            '`/export_db` · `/backup_db`\n'
        ) if is_owner(sender_id) else ''
        h1 = (
            '📚 **EbookManager — Staff Panel**\n'
            '━━━━━━━━━━━━━━━━━━━━\n\n'
            '🔍 **Search Triggers** (in assigned/trigger chats):\n'
            '`.বই` · `।বই` · `/বই` · `.boi` · `/boi` · `.find` · `.search`\n\n'
            '📋 **Thread Assignment:**\n'
            '`/add_thread [chat] [thread] [purge]` — assign free-text thread\n'
            '`/remove_thread [chat] [thread]` · `/list_threads`\n'
            '`/add_trigger [chat] [thread] [purge]` — assign trigger thread\n'
            '`/remove_trigger [chat] [thread]` · `/list_triggers`\n\n'
            f'🌐 **Search Mode** (now: {mode_str}):\n'
            '`/search_public` · `/search_private`\n\n'
            '📡 **Sources:**\n'
            '`/add_source <ref>` · `/remove_source <ref>` · `/list_sources`\n'
            '`/disable <ref>` — remove source + delete all its books\n'
            '`/disable <ref> --keep` — remove source, keep books in DB\n\n'
            '⚙️ **Scraping:**\n'
            '`/scrap [src]` · `/scrap_from_last [src]` · `/scrap_cancel`\n'
            '`/scrap_reset [src]` · `/scrap_status` · `/scrap_log [n]`\n'
            '`/scrap_join_check` · `/scrap_fresh [src]` _(owner)_\n'
        )
        h2 = (
            '📊 **Stats & Reports:**\n'
            '`/stats` · `/health` · `/daily_report` · `/weekly_report`\n'
            '`/search_stats` · `/hourly_heatmap [chat_id]`\n'
            '`/popular` · `/popular_today` · `/popular_this_week`\n'
            '`/trending` · `/leaderboard` · `/growth` · `/retention`\n'
            '`/top_users [n]` · `/top_books [n]` · `/top_sources`\n'
            '`/top_active_users [chat_id]` · `/top_active_sources`\n'
            '`/group_stats [chat_id]` · `/spam_stats`\n\n'
            '👤 **User & Book Info:**\n'
            '`/user_profile <id>` · `/user_history <id>`\n'
            '`/book_info <id>` · `/book_stats <id>` · `/related <id>`\n'
            '`/dead_books [n]` · `/find_dupes` · `/fix_search_names`\n\n'
            '🏷 **Book Aliases:**\n'
            '`/add_alias <id> <name>` · `/remove_alias <id> <name>`\n'
            '`/list_aliases <id>`\n\n'
            '⭐ **VIP Users:**\n'
            '`/add_vip <id> [limit]` · `/remove_vip <id>` · `/list_vip`\n'
            '`/vip_set_limit <id> <n>` — set custom daily limit for one user\n'
            '`/vip_perms [perm on|off]` — toggle VIP privileges\n'
            '`/vip_add_admins <chat_id> [limit]` — bulk VIP all group admins 🆕\n'
            '`/vip_card` — preview VIP card · `/vip_card_send <chat> [thread]` — send it\n'
            '`/vip_card_style [key val]` — customize card appearance\n'
            '_VIP perks: higher limit, no cooldowns, no flood — all configurable_\n\n'
            '🛡 **Spam & User Control:**\n'
            '`/show_spam` · `/set_spam <key> <val>`\n'
            '`/ban <id> [mins]` · `/unban <id>`\n'
            '`/notify_zero` — toggle zero-result instant alerts\n'
        )
        h3 = (
            '🔔 **Keyword Alerts:**\n'
            '`/alert_add <kw> [chat] [thread]` · `/alert_remove <kw>`\n'
            '`/alert_list` · `/alert_test <kw>`\n\n'
            '📢 **Announce:**\n'
            '`/announce <msg>` — broadcast to all assigned+trigger chats\n'
            '_Vars: `{book_count}` `{date}` `{brand}`_\n\n'
            '💌 **Feedback:**\n'
            '`/feedbacks [n]` — view received feedback\n'
            '`.feedback <msg>` — users send feedback (available to all)\n\n'
            '📖 **Book of the Day:**\n'
            '`/botd_add [chat] [thread]` · `/botd_remove [chat] [thread]`\n'
            '`/botd_list` · `/botd_test`\n\n'
            '📡 **Broadcast Reports:**\n'
            '`/broadcast_report_add [chat] [thread] [daily|weekly|both]`\n'
            '`/broadcast_report_remove [chat] [thread]`\n'
            '`/broadcast_report_list` · `/broadcast_report_test [daily|weekly]`\n\n'
            '⏰ **Report Schedule:**\n'
            f'`/set_report_time HH:MM` — now: `{REPORT_TIME_UTC[0]} UTC`\n\n'
            '🎨 **Caption Templates:**\n'
            '`/list_templates` · `/preview_template <n>`\n'
            '`/set_group_template <n> [chat] [thread]`\n'
            '`/get_group_template [chat] [thread]`\n'
            '`/add_template <n> <text>` _(owner)_ · `/del_template <n>` _(owner)_\n'
            '_Vars: `{book_name}` `{user_mention}` `{user_full_mention}` `{brand}` `{purge_time}`_\n'
        )
        await event.reply(h1, parse_mode='md')
        await asyncio.sleep(0.3)
        await event.reply(h2, parse_mode='md')
        await asyncio.sleep(0.3)
        await event.reply(h3 + owner_extra, parse_mode='md')
        return

    if not is_staff(sender_id): return

    def _parse_add(parts, event):
        """
        Parses: /cmd [chat_id] [thread_id] [purge]
        Returns (cid, tid, psec) or (None, error_msg, None) on bad input.

        Accepted forms (1-indexed after command):
          /cmd                          → chat=current, thread=current, purge=72h
          /cmd <thread_id>              → chat=current, thread=<thread_id>, purge=72h
          /cmd <chat_id> <thread_id>    → thread=<thread_id>, purge=72h
          /cmd <chat_id> <thread_id> <purge>  → full explicit
          /cmd <chat_id> 0 <purge>      → whole-chat (no thread), explicit purge
        """
        EXAMPLE = (
            '\n\n📌 **Examples:**\n'
            '`/add_thread` — assign current thread\n'
            '`/add_thread 1234` — thread 1234 in current chat\n'
            '`/add_thread -1001234567890 1234` — thread 1234 in specific chat\n'
            '`/add_thread -1001234567890 1234 24h` — with 24h purge\n'
            '`/add_thread -1001234567890 0 48h` — whole chat, 48h purge\n'
            '\n_chat\\_id is the full negative ID like `-1001234567890`_\n'
            '_thread\\_id is the topic/message-thread ID (0 = whole chat)_'
        )
        cid  = event.chat_id
        tid  = get_event_thread_id(event)
        psec = 72 * 3600

        try:
            if len(parts) == 1:
                # no args — use current chat/thread
                pass
            elif len(parts) == 2:
                # one arg — treat as thread_id in current chat
                raw_tid = parts[1].strip()
                if not raw_tid.lstrip('-').isdigit():
                    return None, f'❌ `{raw_tid}` is not a valid thread\\_id (must be a number).{EXAMPLE}', None
                tid = _tid(raw_tid)
            elif len(parts) == 3:
                # two args — chat_id thread_id
                raw_cid, raw_tid = parts[1].strip(), parts[2].strip()
                if not raw_cid.lstrip('-').isdigit() and not raw_cid.startswith('@'):
                    return None, f'❌ `{raw_cid}` is not a valid chat\\_id.{EXAMPLE}', None
                if not raw_tid.lstrip('-').isdigit():
                    return None, f'❌ `{raw_tid}` is not a valid thread\\_id.{EXAMPLE}', None
                cid = _normalize_chat_id(raw_cid)
                tid = _tid(raw_tid)
            elif len(parts) >= 4:
                # three args — chat_id thread_id purge
                raw_cid, raw_tid, raw_purge = parts[1].strip(), parts[2].strip(), parts[3].strip()
                if not raw_cid.lstrip('-').isdigit() and not raw_cid.startswith('@'):
                    return None, f'❌ `{raw_cid}` is not a valid chat\\_id.{EXAMPLE}', None
                if not raw_tid.lstrip('-').isdigit():
                    return None, f'❌ `{raw_tid}` is not a valid thread\\_id.{EXAMPLE}', None
                cid  = _normalize_chat_id(raw_cid)
                tid  = _tid(raw_tid)
                psec = _parse_purge(raw_purge)
        except Exception as ex:
            return None, f'❌ Parse error: `{ex}`{EXAMPLE}', None

        return cid, tid, psec

    def _parse_rem(parts, event):
        """
        Parses: /cmd [chat_id] [thread_id]
        Returns (cid, tid) or (None, error_msg).
        """
        EXAMPLE = (
            '\n\n📌 **Examples:**\n'
            '`/remove_thread` — remove current thread\n'
            '`/remove_thread 1234` — thread 1234 in current chat\n'
            '`/remove_thread -1001234567890 1234` — thread 1234 in specific chat\n'
            '`/remove_thread -1001234567890 0` — whole-chat entry\n'
            '\n_Use `/list_threads` or `/list_triggers` to see exact IDs stored._'
        )
        cid = event.chat_id
        tid = get_event_thread_id(event)

        try:
            if len(parts) == 1:
                pass
            elif len(parts) == 2:
                raw_tid = parts[1].strip()
                if not raw_tid.lstrip('-').isdigit():
                    return None, f'❌ `{raw_tid}` is not a valid thread\\_id.{EXAMPLE}'
                tid = _tid(raw_tid)
            elif len(parts) >= 3:
                raw_cid, raw_tid = parts[1].strip(), parts[2].strip()
                if not raw_cid.lstrip('-').isdigit() and not raw_cid.startswith('@'):
                    return None, f'❌ `{raw_cid}` is not a valid chat\\_id.{EXAMPLE}'
                if not raw_tid.lstrip('-').isdigit():
                    return None, f'❌ `{raw_tid}` is not a valid thread\\_id.{EXAMPLE}'
                cid = _normalize_chat_id(raw_cid)
                tid = _tid(raw_tid)
        except Exception as ex:
            return None, f'❌ Parse error: `{ex}`{EXAMPLE}'

        return cid, tid


    ft = lambda t: str(t) if t else 'whole chat'

    # ── Template management commands ──────────────────────────────────────────

    if cmd == '/list_templates':
        names = list_templates()
        lines = ['📋 **Caption Templates**\n━━━━━━━━━━━━━━━━━━━━']
        for name in names:
            is_custom = name in _CUSTOM_TEMPLATES
            tag = ' _(custom)_' if is_custom else ' _(built-in)_'
            lines.append(f'• `{name}`{tag}')
        lines.append(
            '\n_Use `/preview_template <name>` to see how a template looks._\n'
            '_Use `/set_group_template <name>` to assign to a group._'
        )
        await event.reply('\n'.join(lines))

    elif cmd == '/preview_template':
        if len(parts) < 2:
            await event.reply('Usage: `/preview_template <name>`\nExample: `/preview_template dm`')
            return
        name = parts[1].lower().strip()
        if get_template(name) is None:
            await event.reply(f'❌ Template `{name}` not found. Use `/list_templates` to see options.')
            return
        # Render with fake data so the preview looks realistic
        preview = render_caption(
            template_name=name,
            fname='Sample Book Title.pdf',
            user_id=sender_id,
            first_name='John',
            username='johndoe',
            purge_secs=600,
            src_chat_id=None,   # will show fallback source label
        )
        dm_tag = ' _(current DM template)_' if name == DM_TEMPLATE_REF[0] else ''
        await event.reply(
            f'👁 **Preview of template `{name}`**{dm_tag}:\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'{preview}\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'_Use `/set_group_template {name}` to assign to a group._\n'
            f'_Use `/set_dm_template {name}` to use for DM deliveries._'
        )

    elif cmd == '/set_search_purge':
        """
        /set_search_purge <seconds>   — set how long search result messages stay
        /set_search_purge 60          — 1 minute (default)
        /set_search_purge 300         — 5 minutes
        /set_search_purge 0           — use the group's file purge time instead
        /set_search_purge             — show current setting
        """
        if len(parts) < 2:
            cur = SEARCH_RESULT_PURGE_SECS[0]
            cur_str = f'`{cur}s` ({cur//60}m {cur%60}s)' if cur > 0 else '**group purge time** (0 = disabled)'
            await event.reply(
                f'⏱ **Search Result Purge Time**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'Current: {cur_str}\n\n'
                f'**Usage:** `/set_search_purge <seconds>`\n'
                f'`/set_search_purge 60`  — 1 minute _(default)_\n'
                f'`/set_search_purge 120` — 2 minutes\n'
                f'`/set_search_purge 300` — 5 minutes\n'
                f'`/set_search_purge 0`   — use group file purge time\n\n'
                f'_Applies to all search result messages in all groups._\n'
                f'_Also configurable via `settings.json` → `"search_result_purge_secs": 60`_'
            )
            return
        try:
            new_secs = max(0, int(parts[1]))
        except ValueError:
            await event.reply('❌ Invalid value. Use seconds like `60`, `120`, or `0` to disable.')
            return
        old = SEARCH_RESULT_PURGE_SECS[0]
        SEARCH_RESULT_PURGE_SECS[0] = new_secs
        _save_settings()
        if new_secs == 0:
            await event.reply(
                f'⏱ **Search result purge disabled**\n'
                f'_Search results will now use the group\'s file purge time._'
            )
        else:
            m, s = new_secs // 60, new_secs % 60
            desc = f'{m}m {s}s' if m else f'{s}s'
            await event.reply(
                f'⏱ **Search result purge set to `{desc}`**\n'
                f'_Was: `{old}s`. Saved to settings.json._'
            )
        await report(f'⏱ Search purge changed: `{old}s` → `{new_secs}s` by `{sender_id}`\n🕐 {ts_str()}')
        """
        /set_dm_template <name>   — set the template used for all DM/private deliveries
        /set_dm_template          — show current DM template
        """
        if len(parts) < 2:
            cur = DM_TEMPLATE_REF[0]
            await event.reply(
                f'📬 **DM Template Settings**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'Current DM template: `{cur}`\n\n'
                f'**Usage:** `/set_dm_template <name>`\n'
                f'**Available:** use `/list_templates` to see all options\n\n'
                f'_This template is used when books are delivered to users in private/DM chats._\n'
                f'_Supports `{{book_source}}` and `{{book_source_link}}` placeholders._'
            )
            return
        tname = parts[1].lower().strip()
        if get_template(tname) is None:
            await event.reply(
                f'❌ Template `{tname}` not found.\n'
                f'Use `/list_templates` to see available templates.'
            )
            return
        old = DM_TEMPLATE_REF[0]
        DM_TEMPLATE_REF[0] = tname
        _save_settings()
        await event.reply(
            f'✅ **DM template updated**\n'
            f'`{old}` → `{tname}`\n\n'
            f'_All future DM deliveries will use `{tname}`._\n'
            f'_Use `/preview_template {tname}` to see how it looks._'
        )
        await report(f'📬 DM template changed: `{old}` → `{tname}` by `{sender_id}`\n🕐 {ts_str()}')

    elif cmd == '/set_group_template':
        """
        /set_group_template <template_name>                  → current chat, no thread
        /set_group_template <template_name> <thread_id>      → current chat, specific thread
        /set_group_template <template_name> <chat> <thread>  → specific chat and thread
        """
        if len(parts) < 2:
            await event.reply(
                '❌ Usage:\n'
                '`/set_group_template <name>` — assign to this chat\n'
                '`/set_group_template <name> <thread_id>` — assign to thread in this chat\n'
                '`/set_group_template <name> <chat_id> <thread_id>` — assign to specific chat/thread\n\n'
                'Use `/list_templates` to see available template names.'
            )
            return
        tname = parts[1].lower().strip()
        if get_template(tname) is None:
            await event.reply(
                f'❌ Template `{tname}` not found.\n'
                f'Use `/list_templates` to see available templates.'
            )
            return

        # Parse target chat/thread
        if len(parts) == 2:
            cid = event.chat_id
            tid = None
        elif len(parts) == 3:
            cid = event.chat_id
            tid = _tid(parts[2])
        else:
            try:
                cid = _normalize_chat_id(parts[2])
                tid = _tid(parts[3])
            except Exception:
                await event.reply('❌ Could not parse chat_id / thread_id.')
                return

        GROUP_TEMPLATES[(cid, tid)] = tname
        _save_settings()

        thread_str = f'thread `{tid}`' if tid else 'whole chat'
        await event.reply(
            f'✅ **Template assigned**\n'
            f'📋 Template: `{tname}`\n'
            f'📍 Chat: `{cid}` | {thread_str}\n\n'
            f'_All PDF deliveries in this group will now use the `{tname}` caption._'
        )

    elif cmd == '/get_group_template':
        """
        /get_group_template                — check current chat
        /get_group_template <thread_id>    — check specific thread
        /get_group_template <chat> <thread>
        """
        if len(parts) == 1:
            cid = event.chat_id
            tid = get_event_thread_id(event)
        elif len(parts) == 2:
            cid = event.chat_id
            tid = _tid(parts[1])
        else:
            try:
                cid = _normalize_chat_id(parts[1])
                tid = _tid(parts[2])
            except Exception:
                await event.reply('❌ Could not parse chat_id / thread_id.')
                return

        tname = get_group_template(cid, tid)
        thread_str = f'thread `{tid}`' if tid else 'whole chat'
        is_explicit = (cid, tid) in GROUP_TEMPLATES or (cid, None) in GROUP_TEMPLATES
        default_note = ' _(default — not explicitly set)_' if not is_explicit else ''
        await event.reply(
            f'📋 **Template for chat `{cid}` / {thread_str}:**\n'
            f'`{tname}`{default_note}\n\n'
            f'_Use `/set_group_template <name>` to change it._'
        )

    elif cmd == '/add_template':
        """
        /add_template <name> <caption text>
        The entire text after the name is used as the template.
        """
        if not is_owner(sender_id):
            await event.reply('🔒 `/add_template` is owner-only.')
            return
        raw = event.text.strip()
        # Split off command and name, rest is template text
        m = re.match(r'^/add_template\s+(\S+)\s+([\s\S]+)$', raw, re.IGNORECASE)
        if not m:
            await event.reply(
                '❌ Usage: `/add_template <name> <caption text>`\n\n'
                'Example:\n'
                '`/add_template mytemplate 📚 **{book_name}**\n'
                '👤 For {user_mention}`\n\n'
                '_Variables: {book_name} {user_mention} {user_mention_link} {user_full_mention} {brand} {source} {purge_time} {separator}_'
            )
            return
        name = m.group(1).lower().strip()
        text = m.group(2).strip()

        if name in _BUILTIN_TEMPLATES:
            await event.reply(
                f'❌ `{name}` is a built-in template name and cannot be overwritten.\n'
                f'Choose a different name.'
            )
            return
        if len(name) > 32:
            await event.reply('❌ Template name must be 32 characters or fewer.')
            return
        if not re.match(r'^[a-z0-9_]+$', name):
            await event.reply('❌ Template name must contain only lowercase letters, digits, and underscores.')
            return

        _CUSTOM_TEMPLATES[name] = text
        _save_settings()
        await event.reply(
            f'✅ **Custom template `{name}` saved!**\n'
            f'Use `/preview_template {name}` to check it.\n'
            f'Use `/set_group_template {name}` to assign it to a group.'
        )

    elif cmd == '/del_template':
        if not is_owner(sender_id):
            await event.reply('🔒 `/del_template` is owner-only.')
            return
        if len(parts) < 2:
            await event.reply('Usage: `/del_template <name>`')
            return
        name = parts[1].lower().strip()
        if name in _BUILTIN_TEMPLATES:
            await event.reply(f'❌ Cannot delete built-in template `{name}`.')
            return
        if name not in _CUSTOM_TEMPLATES:
            await event.reply(f'❌ Custom template `{name}` not found.')
            return

        # Remove any group assignments using this template
        affected = [k for k, v in GROUP_TEMPLATES.items() if v == name]
        for k in affected:
            del GROUP_TEMPLATES[k]

        del _CUSTOM_TEMPLATES[name]
        _save_settings()

        note = f'\n⚠️ Also removed {len(affected)} group assignment(s) that used it.' if affected else ''
        await event.reply(f'🗑️ Custom template `{name}` deleted.{note}')

    # ── Existing admin commands (unchanged) ────────────────────────────────────

    elif cmd == '/add_thread':
        cid, tid_or_err, psec = _parse_add(parts, event)
        if cid is None:
            await event.reply(tid_or_err); return
        tid = tid_or_err
        ASSIGNED_CHATS[(cid, tid)] = psec; _save_settings()
        tid_display = f'`{tid}`' if tid else '`—` _(whole chat)_'
        await event.reply(
            f'✅ **Free-text thread assigned**\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'📍 Chat ID : `{cid}`\n'
            f'🧵 Thread  : {tid_display}\n'
            f'⏳ Purge   : `{_fmt_purge(psec)}`\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'_Verify with `/list_threads`_'
        )

    elif cmd == '/remove_thread':
        cid, tid_or_err = _parse_rem(parts, event)
        if cid is None:
            await event.reply(tid_or_err); return
        tid = tid_or_err
        key = (cid, tid)
        if key in ASSIGNED_CHATS:
            del ASSIGNED_CHATS[key]; _save_settings()
            tid_display = f'`{tid}`' if tid else '`—` _(whole chat)_'
            await event.reply(f'🗑️ **Removed free-text entry**\nChat: `{cid}` | Thread: {tid_display}')
        else:
            stored = [f'`{c}` / `{t}`' for (c, t) in ASSIGNED_CHATS]
            hint = '\n'.join(stored[:10]) or '_none_'
            await event.reply(
                f'⚠️ **Not found:** `{cid}` / `{tid}`\n\n'
                f'📋 **Currently stored:**\n{hint}\n\n'
                f'_Use exact IDs from `/list_threads`_'
            )

    elif cmd == '/list_threads':
        if not ASSIGNED_CHATS:
            await event.reply(
                '📋 No free-text threads assigned yet.\n\n'
                '📌 **To add one:**\n'
                '`/add_thread <chat_id> <thread_id> <purge>`\n'
                'Example: `/add_thread -1001234567890 1234 24h`'
            ); return
        lines = ['📋 **Free-text Threads** (`.boi` / `/boi` works here)\n━━━━━━━━━━━━━━━━━━━━']
        for (c, t), p in ASSIGNED_CHATS.items():
            tmpl = GROUP_TEMPLATES.get((c, t)) or GROUP_TEMPLATES.get((c, None)) or 'default'
            tid_display = str(t) if t else '— (whole chat)'
            lines.append(f'• Chat `{c}`\n  Thread `{tid_display}` | Purge `{_fmt_purge(p)}` | Template `{tmpl}`')
        await event.reply('\n'.join(lines))

    elif cmd == '/add_trigger':
        cid, tid_or_err, psec = _parse_add(parts, event)
        if cid is None:
            # rewrite example lines for trigger context
            await event.reply(
                tid_or_err.replace('/add_thread', '/add_trigger')
                          .replace('/remove_thread', '/remove_trigger')
            ); return
        tid = tid_or_err
        TRIGGER_CHATS[(cid, tid)] = psec; _save_settings()
        tid_display = f'`{tid}`' if tid else '`—` _(whole chat)_'
        await event.reply(
            f'✅ **Trigger chat assigned**\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'📍 Chat ID : `{cid}`\n'
            f'🧵 Thread  : {tid_display}\n'
            f'⏳ Purge   : `{_fmt_purge(psec)}`\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'_Users can now search with `.boi` / `/boi` here_\n'
            f'_Verify with `/list_triggers`_'
        )

    elif cmd == '/remove_trigger':
        cid, tid_or_err = _parse_rem(parts, event)
        if cid is None:
            await event.reply(
                tid_or_err.replace('/remove_thread', '/remove_trigger')
                          .replace('/list_threads', '/list_triggers')
            ); return
        tid = tid_or_err
        key = (cid, tid)
        if key in TRIGGER_CHATS:
            del TRIGGER_CHATS[key]; _save_settings()
            tid_display = f'`{tid}`' if tid else '`—` _(whole chat)_'
            await event.reply(f'🗑️ **Trigger removed**\nChat: `{cid}` | Thread: {tid_display}')
        else:
            stored = [f'`{c}` / `{t}`' for (c, t) in TRIGGER_CHATS]
            hint = '\n'.join(stored[:10]) or '_none_'
            await event.reply(
                f'⚠️ **Not found:** `{cid}` / `{tid}`\n\n'
                f'📋 **Currently stored:**\n{hint}\n\n'
                f'_Use exact IDs from `/list_triggers`_'
            )

    elif cmd == '/list_triggers':
        if not TRIGGER_CHATS:
            await event.reply(
                '📋 No trigger chats assigned yet.\n\n'
                '📌 **To add one:**\n'
                '`/add_trigger <chat_id> <thread_id> <purge>`\n'
                'Example: `/add_trigger -1001234567890 1234 24h`'
            ); return
        lines = ['📋 **Trigger Chats** (`.boi` · `/boi` active here)\n━━━━━━━━━━━━━━━━━━━━']
        for (c, t), p in TRIGGER_CHATS.items():
            tmpl = GROUP_TEMPLATES.get((c, t)) or GROUP_TEMPLATES.get((c, None)) or 'default'
            tid_display = str(t) if t else '— (whole chat)'
            lines.append(f'• Chat `{c}`\n  Thread `{tid_display}` | Purge `{_fmt_purge(p)}` | Template `{tmpl}`')
        await event.reply('\n'.join(lines))

    elif cmd == '/search_public':
        SEARCH_MODE[0] = 'public'; _save_settings()
        await event.reply('🌐 **PUBLIC mode** — saved.')

    elif cmd == '/search_private':
        SEARCH_MODE[0] = 'private'; _save_settings()
        await event.reply('🔒 **PRIVATE mode** — saved.')

    elif cmd == '/add_source':
        if len(parts) < 2: await event.reply('Usage: `/add_source <ref>`'); return
        ref = parts[1].lstrip('@')
        if ref not in SOURCE_GROUPS:
            SOURCE_GROUPS.append(ref); _save_settings()
        await event.reply(f'➕ `{ref}` added and saved.')

    elif cmd == '/remove_source':
        if len(parts) < 2: await event.reply('Usage: `/remove_source <ref>`'); return
        ref = parts[1].lstrip('@')
        if ref in SOURCE_GROUPS:
            SOURCE_GROUPS.remove(ref); _save_settings()
            await event.reply(f'➖ `{ref}` removed and saved.')
        else: await event.reply('⚠️ Not found.')

    elif cmd == '/disable':
        """
        /disable <chat_id or @username>         — remove source + delete ALL its books
        /disable <chat_id or @username> --keep  — remove from sources only, keep books
        """
        if len(parts) < 2:
            await event.reply(
                '**Usage:**\n'
                '`/disable <chat_id or @username>` — remove source + delete all its books\n'
                '`/disable <chat_id or @username> --keep` — remove from sources, keep books in DB\n\n'
                '**Examples:**\n'
                '`/disable -1002333091163`\n'
                '`/disable @SomeBookGroup`\n'
                '`/disable -1002333091163 --keep`',
                parse_mode='md'
            )
            return

        raw_ref    = parts[1].strip()
        keep_books = '--keep' in parts

        # ── Resolve to numeric chat_id ─────────────────────────────────────────
        resolved_chat_id = None

        # Direct numeric parse
        try:
            resolved_chat_id = int(raw_ref)
        except ValueError:
            pass

        # Look up in scrape_progress by label
        if resolved_chat_id is None:
            norm_ref = raw_ref.lstrip('@').lower()
            try:
                conn = db_connect(); c = conn.cursor()
                c.execute('SELECT chat_id FROM scrape_progress WHERE LOWER(scrape_label)=?',
                          (norm_ref,))
                row = c.fetchone(); conn.close()
                if row:
                    resolved_chat_id = row[0]
            except Exception:
                pass

        # Try Telegram API resolve
        if resolved_chat_id is None:
            try:
                entity = await _resolve_source(user_client, raw_ref)
                resolved_chat_id = entity.id
                norm = _normalize_id_for_compare(str(resolved_chat_id))
                if isinstance(norm, int):
                    resolved_chat_id = norm
            except Exception:
                pass

        if resolved_chat_id is None:
            await event.reply(
                f'❌ Could not resolve `{raw_ref}` to a known chat.\n'
                f'_Make sure it has been scraped before, or use the numeric chat ID._',
                parse_mode='md'
            )
            return

        # ── Look up book count and label ───────────────────────────────────────
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books WHERE chat_id=?', (resolved_chat_id,))
        book_count = c.fetchone()[0]
        c.execute('SELECT scrape_label FROM scrape_progress WHERE chat_id=?', (resolved_chat_id,))
        sp_row = c.fetchone(); conn.close()
        label = (sp_row[0] if sp_row else '') or raw_ref

        # Check it actually exists somewhere
        in_sources = any(
            _normalize_id_for_compare(s) == resolved_chat_id
            or _normalize_id_for_compare(s) == raw_ref.lstrip('@').lower()
            for s in SOURCE_GROUPS
        )
        if book_count == 0 and not in_sources:
            await event.reply(
                f'❌ No books found for `{raw_ref}` (resolved: `{resolved_chat_id}`) '
                f'and it is not in SOURCE_GROUPS.\n'
                f'_Nothing to disable._',
                parse_mode='md'
            )
            return

        # ── Confirm before destructive action ─────────────────────────────────
        if not keep_books and book_count > 0:
            _DISABLE_PENDING[sender_id] = {
                'chat_id': resolved_chat_id,
                'label':   label,
                'raw_ref': raw_ref,
                'books':   book_count,
                'keep':    keep_books,
                'ts':      time.time(),
            }

            btns = None
            if BOT_TOKEN:
                btns = [[
                    Button.inline(
                        f'🗑 Yes, delete {book_count:,} books',
                        f'disable_confirm_{resolved_chat_id}'.encode()
                    ),
                    Button.inline('❌ Cancel', b'disable_cancel'),
                ]]

            await event.reply(
                f'⚠️ **Confirm Disable**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'Source: `{label}`\n'
                f'Chat ID: `{resolved_chat_id}`\n'
                f'Books to delete: **{book_count:,}**\n\n'
                f'This will permanently:\n'
                f'• Remove from SOURCE_GROUPS\n'
                f'• Delete all **{book_count:,}** books from the DB\n'
                f'• Clear its scrape progress\n'
                f'• Rebuild FTS index + clear search cache\n\n'
                f'_This cannot be undone._\n\n'
                + (f'Confirm with: `/disable_confirm {resolved_chat_id}`'
                   if not BOT_TOKEN else ''),
                buttons=btns,
                parse_mode='md'
            )
            return

        # No confirmation needed (keep_books or book_count==0)
        await _do_disable_source(
            event, sender_id, resolved_chat_id, label, raw_ref,
            book_count, keep_books, user_client, interaction_client
        )

    elif cmd == '/disable_confirm':
        """Text-mode confirmation fallback (no bot token): /disable_confirm <chat_id>"""
        if len(parts) < 2:
            await event.reply('Usage: `/disable_confirm <chat_id>`'); return
        try:
            cid = int(parts[1])
        except ValueError:
            await event.reply('❌ Invalid chat_id.'); return
        pending = _DISABLE_PENDING.pop(sender_id, None)
        if not pending or pending['chat_id'] != cid:
            await event.reply('❌ No pending /disable for that ID. Run `/disable` again.')
            return
        if time.time() - pending['ts'] > 120:
            await event.reply('❌ Confirmation expired (2 min). Run `/disable` again.')
            return
        await _do_disable_source(
            event, sender_id, cid,
            pending['label'], pending['raw_ref'],
            pending['books'], pending['keep'],
            user_client, interaction_client
        )

    elif cmd == '/list_sources':
        if SOURCE_GROUPS:
            await event.reply('🔎 **Sources:**\n' + '\n'.join(f'• `{s}`' for s in SOURCE_GROUPS))
        else:
            await event.reply('No sources.')

    elif cmd == '/scrap':
        if _scrap_running[0]:
            elapsed = int(time.time() - _scrap_started[0])
            await event.reply(
                f'⚠️ A scrape job is already running!\n'
                f'📌 Current source: `{_scrap_current[0] or "starting…"}`\n'
                f'⏱ Running for: `{elapsed // 60}m {elapsed % 60}s`\n'
                f'Use `/scrap_cancel` to stop it, or `/scrap_status` to check progress.'
            )
            return
        targets = [parts[1].lstrip('@')] if len(parts) >= 2 else list(SOURCE_GROUPS)
        if not targets: await event.reply('No sources configured. Use `/add_source` first.'); return
        async def _rf(m): await event.reply(m)
        asyncio.create_task(_run_scrap_job(user_client, _rf, targets,
                                           job_label='manual', started_by=sender_id))

    elif cmd == '/scrap_cancel':
        if not _scrap_running[0]:
            await event.reply('ℹ️ No scrape job is currently running.')
            return
        _scrap_cancel[0] = True
        await event.reply(
            f'🛑 **Cancel requested.**\n'
            f'The current source (`{_scrap_current[0] or "…"}`) will finish its current message, then stop.\n'
            f'Progress so far is already saved to DB.'
        )

    elif cmd == '/set_auto_scrap':
        """
        /set_auto_scrap 72          — set interval to 72 hours (= every 3 days)
        /set_auto_scrap 0           — disable auto-scrape
        /set_auto_scrap             — show current setting
        """
        if len(parts) < 2:
            cur = AUTO_SCRAP_INTERVAL_H[0]
            status = f'`{cur}h` (every {cur//24}d {cur%24}h)' if cur > 0 else '**disabled** ⭕'
            shadow_status = '🔄 Shadow scrape in progress…' if _shadow_active[0] else '✅ Idle'
            await event.reply(
                f'⏰ **Auto-scrape Settings**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'Current interval: {status}\n'
                f'Shadow scrape: {shadow_status}\n\n'
                f'📌 **Usage:**\n'
                f'`/set_auto_scrap 72` — every 72h (3 days)\n'
                f'`/set_auto_scrap 48` — every 48h (2 days)\n'
                f'`/set_auto_scrap 0`  — disable\n\n'
                f'_Interval is also configurable in `settings.json` → `"auto_scrap_interval_h": 72`_'
            )
            return
        try:
            new_h = max(0, int(parts[1]))
        except ValueError:
            await event.reply('❌ Invalid interval. Use a number like `72` (hours) or `0` to disable.')
            return
        old_h = AUTO_SCRAP_INTERVAL_H[0]
        AUTO_SCRAP_INTERVAL_H[0] = new_h
        _save_settings()
        _log_scrap('_admin_', 'set_auto_scrap', f'{old_h}h → {new_h}h by {sender_id}')
        if new_h == 0:
            await event.reply(
                f'⭕ **Auto-scrape disabled.**\n'
                f'_Use `/set_auto_scrap <hours>` to re-enable._'
            )
        else:
            days = new_h // 24
            hrs  = new_h % 24
            desc = f'{days}d {hrs}h' if days else f'{new_h}h'
            await event.reply(
                f'⏰ **Auto-scrape set to every `{desc}`**\n'
                f'_Also saved to `settings.json` → `"auto_scrap_interval_h": {new_h}`_\n'
                f'_Uses shadow DB — live DB stays unlocked during scrape._'
            )
        await report(
            f'⏰ **Auto-scrape interval changed**\n'
            f'{old_h}h → {new_h}h by `{sender_id}`\n'
            f'🕐 {ts_str()}'
        )

    elif cmd == '/scrap_from_last':
        if _scrap_running[0]:
            elapsed = int(time.time() - _scrap_started[0])
            await event.reply(
                f'⚠️ A scrape job is already running!\n'
                f'📌 Current: `{_scrap_current[0] or "starting…"}`\n'
                f'⏱ Running for: `{elapsed // 60}m {elapsed % 60}s`\n'
                f'Use `/scrap_cancel` to stop it first.'
            )
            return
        single_src = parts[1].lstrip('@') if len(parts) >= 2 else None
        targets = [single_src] if single_src else list(SOURCE_GROUPS)
        if not targets:
            await event.reply('No sources configured. Use `/add_source` first.')
            return
        from_date_map: dict[str, int] = {}
        conn = db_connect(); c = conn.cursor()
        for src in targets:
            try:
                entity = await _resolve_source(user_client, src)
                cid    = entity.id
                c.execute('SELECT last_scraped_at FROM scrape_progress WHERE chat_id=?', (cid,))
                row = c.fetchone()
                ts  = row[0] if row and row[0] else 0
                from_date_map[src] = ts
            except Exception as e:
                log.warning(f'scrap_from_last resolve {src}: {e}')
                from_date_map[src] = 0
        conn.close()
        preview_lines = ['📅 **Scrap from last** — date windows:']
        for src, ts in from_date_map.items():
            if ts:
                dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
                preview_lines.append(f'• `{src}` → from `{dt_str}`')
            else:
                preview_lines.append(f'• `{src}` → _no prior scrape — will do full scrape_')
        await event.reply('\n'.join(preview_lines))
        async def _rf(m): await event.reply(m)
        asyncio.create_task(_run_scrap_job(
            user_client, _rf, targets,
            job_label='from-last', started_by=sender_id,
            scrape_mode='from_date', from_date_map=from_date_map,
        ))

    elif cmd == '/scrap_reset':
        if len(parts) >= 2:
            ref = parts[1].lstrip('@')
            try:
                entity = await _resolve_source(user_client, ref)
                src_chat_id = entity.id
                conn = db_connect()
                conn.execute('UPDATE scrape_progress SET last_msg_id=0, last_scraped_at=0 WHERE chat_id=?', (src_chat_id,))
                conn.commit(); conn.close()
                _log_scrap(ref, 'reset', 'manual single-source reset')
                await event.reply(
                    f'🔄 **Reset** scrape position for `{ref}`.\n'
                    f'Run `/scrap {ref}` to do a full re-scrape of just this source,\n'
                    f'or `/scrap` to re-scrape everything.'
                )
            except Exception as e:
                await event.reply(f'❌ Error: `{e}`')
        else:
            if not SOURCE_GROUPS:
                await event.reply('No sources configured. Use `/add_source` first.'); return
            if _scrap_running[0]:
                await event.reply(
                    f'⚠️ A scrape job is already running (`{_scrap_current[0]}`). '
                    f'Use `/scrap_cancel` first.'
                )
                return
            conn = db_connect(); c = conn.cursor()
            c.execute('UPDATE scrape_progress SET last_msg_id=0, last_scraped_at=0')
            reset_count = c.rowcount
            conn.commit(); conn.close()
            _log_scrap('_all_', 'reset', f'{reset_count} sources reset')
            await event.reply(
                f'🔄 **Reset ALL {len(SOURCE_GROUPS)} source(s).** Starting full re-scrape now…\n'
                f'_(Progress will appear below. Use `/scrap_cancel` to stop.)_'
            )
            async def _rf(m): await event.reply(m)
            asyncio.create_task(_run_scrap_job(
                user_client, _rf, list(SOURCE_GROUPS),
                job_label='full-reset', started_by=sender_id
            ))

    elif cmd == '/scrap_fresh':
        if not is_owner(sender_id):
            await event.reply('🔒 `/scrap_fresh` is owner-only.')
            return
        if _scrap_running[0]:
            await event.reply(
                f'⚠️ A scrape job is already running (`{_scrap_current[0]}`).\n'
                f'Use `/scrap_cancel` first.'
            )
            return
        single_src = parts[1].lstrip('@') if len(parts) >= 2 else None
        scope_key  = single_src or 'all'
        pending = _fresh_confirm.get(sender_id)
        if not pending or pending[1] != scope_key or time.time() > pending[0]:
            conn = db_connect(); c = conn.cursor()
            if single_src:
                try:
                    entity = await _resolve_source(user_client, single_src)
                    cid    = entity.id
                    c.execute('SELECT COUNT(*) FROM books WHERE chat_id=?', (cid,))
                    affected = c.fetchone()[0]
                    c.execute('SELECT scrape_label FROM scrape_progress WHERE chat_id=?', (cid,))
                    lbl_row = c.fetchone()
                    lbl = lbl_row[0] if lbl_row and lbl_row[0] else single_src
                except Exception as e:
                    await event.reply(f'❌ Cannot resolve `{single_src}`: `{e}`')
                    conn.close(); return
                scope_desc = f'source `{lbl}`\n📖 Books to delete: `{affected}`'
            else:
                c.execute('SELECT COUNT(*) FROM books')
                affected = c.fetchone()[0]
                c.execute('SELECT COUNT(*) FROM scrape_progress')
                sources  = c.fetchone()[0]
                scope_desc = (
                    f'**ALL sources** ({sources} sources)\n'
                    f'📖 Books to delete: `{affected}`\n'
                    f'🔄 Scrape progress: **fully reset**\n'
                    f'🗂 Search cache: **cleared**\n'
                    f'🔢 Book IDs: **restart from 1**'
                )
            conn.close()
            _fresh_confirm[sender_id] = (time.time() + _FRESH_CONFIRM_TTL, scope_key)
            await event.reply(
                f'⚠️ **WARNING — This cannot be undone!**\n\n'
                f'**Scope:** {scope_desc}\n\n'
                f'**Run `/scrap_fresh{" " + single_src if single_src else ""}` again within '
                f'{_FRESH_CONFIRM_TTL}s to confirm.**'
            )
            return
        del _fresh_confirm[sender_id]
        if single_src:
            try:
                entity = await _resolve_source(user_client, single_src)
                cid    = entity.id
                label  = getattr(entity, 'username', None) or getattr(entity, 'title', None) or single_src
            except Exception as e:
                await event.reply(f'❌ Cannot resolve `{single_src}`: `{e}`')
                return
            await event.reply(f'🗑 Wiping `{label}`…')
            result = await _wipe_books(scope='source', chat_id=cid)
            await event.reply(
                f'✅ **Wiped `{label}`**\n'
                f'📖 Books deleted: `{result["books_deleted"]}`\n'
                f'🗂 Cache cleared: `{result["cache_cleared"]}` files\n\n'
                f'🚀 Starting fresh scrape of `{label}`…'
            )
            async def _rf(m): await event.reply(m)
            asyncio.create_task(_run_scrap_job(
                user_client, _rf, [single_src],
                job_label='fresh-source', started_by=sender_id, scrape_mode='full'
            ))
        else:
            await event.reply('💣 **Wiping entire books DB…**')
            result = await _wipe_books(scope='all')
            if not SOURCE_GROUPS:
                await event.reply(
                    f'✅ **DB wiped.**\n'
                    f'📖 Books deleted: `{result["books_deleted"]}`\n'
                    f'⚠️ No sources configured — nothing to re-scrape.'
                )
                return
            await event.reply(
                f'✅ **DB wiped clean.**\n'
                f'📖 Books deleted: `{result["books_deleted"]}`\n'
                f'🚀 Starting full fresh re-scrape of {len(SOURCE_GROUPS)} source(s)…'
            )
            async def _rf2(m): await event.reply(m)
            asyncio.create_task(_run_scrap_job(
                user_client, _rf2, list(SOURCE_GROUPS),
                job_label='fresh-all', started_by=sender_id, scrape_mode='full'
            ))
        await report(
            f'💣 **DB Fresh Wipe** by owner\n'
            f'📖 Deleted: `{result["books_deleted"]}` books\n'
            f'🔭 Scope: `{scope_key}`\n'
            f'🕐 {ts_str()}'
        )

    elif cmd == '/scrap_status':
        conn = db_connect(); c = conn.cursor()
        try:
            c.execute('SELECT last_scrape_mode FROM scrape_progress LIMIT 1')
            has_mode_col = True
        except Exception:
            has_mode_col = False

        if has_mode_col:
            c.execute('''
                SELECT sp.chat_id,
                       COALESCE(sp.scrape_label, ''),
                       COALESCE(sp.last_msg_id, 0),
                       COALESCE(sp.last_scraped_at, 0),
                       COUNT(b.id),
                       COALESCE(sp.last_scrape_mode, 'resume')
                FROM scrape_progress sp
                LEFT JOIN books b ON b.chat_id = sp.chat_id
                GROUP BY sp.chat_id
                ORDER BY sp.last_scraped_at DESC
            ''')
        else:
            c.execute('''
                SELECT sp.chat_id,
                       COALESCE(sp.scrape_label, ''),
                       COALESCE(sp.last_msg_id, 0),
                       COALESCE(sp.last_scraped_at, 0),
                       COUNT(b.id),
                       'resume'
                FROM scrape_progress sp
                LEFT JOIN books b ON b.chat_id = sp.chat_id
                GROUP BY sp.chat_id
                ORDER BY sp.last_scraped_at DESC
            ''')
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM books')
        total_books = c.fetchone()[0]
        conn.close()

        scraped_chat_ids: set = {_normalize_id_for_compare(r[0]) for r in rows}
        scraped_labels:   set = {(r[1] or '').lower().lstrip('@') for r in rows}

        never_scraped = []
        for s in SOURCE_GROUPS:
            norm = _normalize_id_for_compare(s)
            if isinstance(norm, int):
                if norm not in scraped_chat_ids:
                    never_scraped.append(s)
            else:
                if norm not in scraped_labels:
                    never_scraped.append(s)

        header = f'📊 **Scrape Status** — 📚 `{total_books}` total books\n━━━━━━━━━━━━━━━━━━━━'
        if _scrap_running[0]:
            elapsed = int(time.time() - _scrap_started[0])
            header += (
                f'\n🟢 **Job RUNNING** | ⏱ `{elapsed//60}m {elapsed%60}s`\n'
                f'📌 Current: `{_scrap_current[0] or "…"}`'
            )
            if AUTO_SCRAP_INTERVAL_H[0]:
                header += f'\n🕐 Auto-scrape: every `{AUTO_SCRAP_INTERVAL_H[0]}h`'
        else:
            if AUTO_SCRAP_INTERVAL_H[0]:
                latest_ts = max((int(r[3]) for r in rows if r[3]), default=0)
                if latest_ts:
                    next_run = latest_ts + AUTO_SCRAP_INTERVAL_H[0] * 3600
                    diff = int(next_run - time.time())
                    if diff > 0:
                        h2, m2 = diff // 3600, (diff % 3600) // 60
                        header += f'\n⏰ Next auto-scrape in `{h2}h {m2}m`'
                    else:
                        header += '\n⏰ Auto-scrape: **due soon**'
                header += f' (every `{AUTO_SCRAP_INTERVAL_H[0]}h`)'

        lines = [header]

        if not rows and not SOURCE_GROUPS:
            lines.append('\n📭 No scrape history yet. Add sources with `/add_source`.')
            await event.reply('\n'.join(lines))
            return

        mode_icons = {'resume': '▶️', 'from_date': '📅', 'full': '🔁', 'auto': '⏰'}
        for row in rows:
            chat_id   = row[0]
            label     = row[1] or f'chat:{chat_id}'
            last_msg  = int(row[2]) if row[2] else 0
            last_ts   = int(row[3]) if row[3] else 0
            book_count= row[4]
            last_mode = row[5] or 'resume'

            label_str = f'`{label}`'
            currently = ' 🟢 _scraping now_' if _scrap_current[0] == label else ''
            mode_str  = mode_icons.get(last_mode, '▶️') + f' `{last_mode}`'

            if last_ts and last_ts > 0:
                try:
                    dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)
                    ago_secs = int(time.time()) - last_ts
                    if ago_secs < 3600:    ago = f'{ago_secs // 60}m ago'
                    elif ago_secs < 86400: ago = f'{ago_secs // 3600}h ago'
                    else:                  ago = f'{ago_secs // 86400}d ago'
                    time_str = f'`{dt.strftime("%Y-%m-%d %H:%M UTC")}` _{ago}_'
                except Exception:
                    time_str = '_invalid timestamp_'
            else:
                time_str = '_never scraped_'

            lines.append(
                f'\n📦 {label_str}{currently}\n'
                f'   📅 Last: {time_str} | {mode_str}\n'
                f'   📖 Books: `{book_count}` | 📌 Last msg: `{last_msg}`'
            )

        if never_scraped:
            lines.append(f'\n⏳ **Not yet scraped** ({len(never_scraped)}):')
            for s in never_scraped:
                lines.append(f'   • `{s}`')

        full_text = '\n'.join(lines)
        if len(full_text) > 4000:
            chunk = ''
            for line in lines:
                if len(chunk) + len(line) + 1 > 3800:
                    await event.reply(chunk)
                    chunk = line
                else:
                    chunk += '\n' + line
            if chunk:
                await event.reply(chunk)
        else:
            await event.reply(full_text)

    elif cmd == '/scrap_join_check':
        if not SOURCE_GROUPS:
            await event.reply('No sources configured.'); return
        await event.reply(f'🔍 Checking {len(SOURCE_GROUPS)} sources… (this may take a moment)')
        ok_sources    = []
        failed_sources = []
        for src in SOURCE_GROUPS:
            try:
                entity = await _resolve_source(user_client, src)
                got_msg = False
                async for _ in user_client.iter_messages(
                    getattr(entity, '_full_id', entity), limit=1
                ):
                    got_msg = True
                    break
                label = getattr(entity, 'username', None) or getattr(entity, 'title', None) or src
                if got_msg:
                    ok_sources.append((src, label))
                else:
                    failed_sources.append((src, label, 'accessible but 0 messages'))
            except Exception as e:
                failed_sources.append((src, src, str(e)[:80]))
        lines = [f'🔍 **Source Access Check** ({len(SOURCE_GROUPS)} sources)\n━━━━━━━━━━━━━━━━━━━━']
        if ok_sources:
            lines.append(f'\n✅ **Accessible** ({len(ok_sources)}):')
            for src, label in ok_sources:
                lines.append(f'   • `{label}` (`{src}`)')
        if failed_sources:
            lines.append(f'\n❌ **Cannot access** ({len(failed_sources)}):')
            for src, label, err in failed_sources:
                lines.append(f'   • `{src}` — _{err}_')
        else:
            lines.append('\n🎉 All sources are accessible!')
        full = '\n'.join(lines)
        if len(full) > 4000:
            chunk = ''
            for line in lines:
                if len(chunk) + len(line) + 1 > 3800:
                    await event.reply(chunk); chunk = line
                else:
                    chunk += '\n' + line
            if chunk: await event.reply(chunk)
        else:
            await event.reply(full)

    elif cmd == '/scrap_log':
        n = 20
        try: n = min(50, int(parts[1])) if len(parts) >= 2 else 20
        except: pass
        if not _scrap_log:
            await event.reply('📭 Scrape log is empty.')
            return
        entries = _scrap_log[-n:]
        lines = [f'📋 **Scrape Log** (last {len(entries)} entries)\n━━━━━━━━━━━━━━━━━━━━']
        icons = {'start': '▶️', 'done': '✅', 'error': '❌', 'cancelled': '🛑',
                 'reset': '🔄', 'start_job': '🚀', 'done_job': '🏁'}
        for ts, src, etype, detail in reversed(entries):
            dt  = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%m-%d %H:%M')
            ico = icons.get(etype, '•')
            detail_str = f' — `{detail}`' if detail else ''
            lines.append(f'`{dt}` {ico} **{src}** _{etype}_{detail_str}')
        await event.reply('\n'.join(lines))

    elif cmd == '/fix_search_names':
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books')
        total = c.fetchone()[0]
        conn.close()
        if total == 0:
            await event.reply('📭 No books in DB.'); return
        await event.reply(
            f'🔧 **Fixing search names** for `{total}` books…\n'
            f'_Then rebuilding FTS index. Please wait…_'
        )
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT id, file_name FROM books')
        rows = c.fetchall()
        updated = 0
        for book_id, file_name in rows:
            new_sname = normalize_name(file_name)
            c.execute(
                'UPDATE books SET search_name=?, stripped_name=? WHERE id=?',
                (new_sname, _RE_STRIP_VOWELS.sub('', new_sname), book_id)
            )
            updated += 1
            if updated % 500 == 0:
                conn.commit()
        conn.commit()
        try:
            c.execute('DROP TABLE IF EXISTS books_fts')
            c.execute('''CREATE VIRTUAL TABLE books_fts USING fts5(
                search_name, content='books', content_rowid='id', tokenize='unicode61'
            )''')
            c.execute('''CREATE TRIGGER IF NOT EXISTS books_ai AFTER INSERT ON books BEGIN
                INSERT INTO books_fts(rowid, search_name) VALUES(new.id, new.search_name);
            END;''')
            c.execute("INSERT INTO books_fts(rowid, search_name) SELECT id, search_name FROM books")
            conn.commit()
            fts_ok = True
        except Exception as fts_err:
            log.warning(f'fix_search_names FTS rebuild: {fts_err}')
            try:
                conn.execute("INSERT INTO books_fts(books_fts) VALUES('rebuild')")
                conn.commit()
            except Exception: pass
            fts_ok = False
        conn.close()
        cleared = 0
        for fn in os.listdir(CACHE_DIR):
            fp = os.path.join(CACHE_DIR, fn)
            if os.path.isfile(fp):
                try: os.remove(fp); cleared += 1
                except: pass
        mem_cleared = len(_SEARCH_CACHE)
        _SEARCH_CACHE.clear()
        _log_scrap('_fix_', 'fix_search_names', f'{updated} rows updated')
        await event.reply(
            f'✅ **Search names fixed!**\n'
            f'📚 Updated: `{updated}` books\n'
            f'🔍 FTS index: {"rebuilt clean" if fts_ok else "rebuilt (fallback)"}\n'
            f'🗂 Cache cleared: `{cleared}` files + `{mem_cleared}` memory entries'
        )

    elif cmd == '/find_dupes':
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT file_name, COUNT(*) as cnt, GROUP_CONCAT(id) as ids,
                   GROUP_CONCAT(chat_id) as chats
            FROM books
            GROUP BY file_name
            HAVING cnt > 1
            ORDER BY cnt DESC
            LIMIT 30
        ''')
        rows = c.fetchall(); conn.close()
        if not rows:
            await event.reply('✅ No duplicate books found.')
            return
        lines = [f'🔍 **Duplicate Books** (top {len(rows)})']
        for fname, cnt, ids, chats in rows:
            display = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', fname, flags=re.IGNORECASE)[:50]
            lines.append(f'\n`{display}`\n  {cnt}× | IDs: `{ids}` | Chats: `{chats}`')
        lines.append('\n_Use `/purge_book <id>` to remove unwanted duplicates._')
        await event.reply('\n'.join(lines))

    elif cmd == '/book_info':
        if len(parts) < 2:
            await event.reply('Usage: `/book_info <book_id>`'); return
        try:
            bid = int(parts[1])
        except ValueError:
            await event.reply('❌ Invalid ID.'); return
        conn = db_connect(); c = conn.cursor()
        c.execute('''SELECT id, file_name, search_name, file_size, message_id,
                            chat_id, file_ext, is_restricted
                     FROM books WHERE id=?''', (bid,))
        row = c.fetchone()
        if row:
            c.execute('SELECT scrape_label FROM scrape_progress WHERE chat_id=?', (row[5],))
            src_row = c.fetchone()
        conn.close()
        if not row:
            await event.reply(f'❌ No book with ID `{bid}`.'); return
        bid, fname, sname, fsize, msg_id, chat_id, ext, is_restr = row
        src_label = src_row[0] if src_row and src_row[0] else str(chat_id)
        size_str  = f'{fsize/(1024*1024):.2f} MB' if fsize else 'unknown'
        lock_str  = '🔒 Yes' if is_restr else '🔓 No'
        fmt       = (ext or '?').upper().replace('.', '')
        await event.reply(
            f'📖 **Book Info** — ID `{bid}`\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'**File:** `{fname}`\n'
            f'**Format:** `{fmt}` | **Size:** `{size_str}`\n'
            f'**Source:** `{src_label}` (chat `{chat_id}`)\n'
            f'**Message ID:** `{msg_id}`\n'
            f'**Restricted:** {lock_str}\n'
            f'**Search index:** `{sname}`'
        )

    elif cmd == '/stats':
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books'); total = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM books WHERE is_restricted=1'); restr = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM books WHERE file_ext=".epub"'); epubs = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM books WHERE file_ext IN ('.mobi','.azw','.azw3','.kfx')"); kindles = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM books WHERE file_ext NOT IN ('.pdf','.epub','.mobi','.azw','.azw3','.kfx')"); others = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM cleanup_queue'); pending = c.fetchone()[0]
        c.execute('SELECT COUNT(DISTINCT file_name) FROM books'); unique_names = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM download_log'); total_dls = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log'); total_searches = c.fetchone()[0]
        c.execute('SELECT COUNT(DISTINCT user_id) FROM download_log'); uniq_users = c.fetchone()[0]
        today_cut = int(time.time()) - 86400
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts>?', (today_cut,)); dls_today = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts>?', (today_cut,)); searches_today = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE result_count=0 AND ts>?', (today_cut,)); zero_today = c.fetchone()[0]
        conn.close()
        try:
            db_mb = os.path.getsize(DB_PATH) / 1048576
            wal_path = DB_PATH + '-wal'
            wal_mb = os.path.getsize(wal_path) / 1048576 if os.path.exists(wal_path) else 0
            db_str = f'`{db_mb:.1f}MB`' + (f' + WAL `{wal_mb:.1f}MB`' if wal_mb > 0.1 else '')
        except Exception:
            db_str = '_unknown_'
        uptime_secs = int(time.time() - _BOT_START_TIME)
        scrap_str = (
            f'🟢 Running (`{_scrap_current[0] or "…"}`)' if _scrap_running[0]
            else (f'⏰ Every {AUTO_SCRAP_INTERVAL_H[0]}h' if AUTO_SCRAP_INTERVAL_H[0] else '⭕ Off')
        )
        conv_rate = _pct(total_dls, total_searches)
        await event.reply(
            f'📊 **Bot Stats**\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'📚 Books: `{total}` total · `{unique_names}` unique\n'
            f'   🔒 Restricted: `{restr}` · 📕 EPUB: `{epubs}` · 📗 Kindle: `{kindles}` · 📄 Other: `{others}`\n'
            f'\n'
            f'📈 **Today**\n'
            f'   🔍 Searches: `{searches_today}` · ❌ Zero-result: `{zero_today}`\n'
            f'   📥 Downloads: `{dls_today}`\n'
            f'\n'
            f'📊 **All-time**\n'
            f'   🔍 `{total_searches}` searches · 📥 `{total_dls}` downloads\n'
            f'   👥 Unique users: `{uniq_users}` · 🔄 Conversion: `{conv_rate}`\n'
            f'\n'
            f'⚙️ **System**\n'
            f'   🗄 DB: {db_str} · ⏱ Uptime: `{_fmt_uptime(uptime_secs)}`\n'
            f'   🗑 Pending deletes: `{pending}` · ⚙️ Scrape: {scrap_str}\n'
            f'   🗂 Cache: `{len(os.listdir(CACHE_DIR))}` files · `{len(_SEARCH_CACHE)}` memory\n'
            f'\n'
            f'🔧 **Config**\n'
            f'   📋 Threads: `{len(ASSIGNED_CHATS)}` free-text · `{len(TRIGGER_CHATS)}` trigger\n'
            f'   📝 Pending requests: `{len(pending_requests)}`\n'
            f'   🌐 Mode: `{SEARCH_MODE[0]}` · {PER_PAGE}/page\n'
            f'   📋 Templates: `{len(_BUILTIN_TEMPLATES)}` built-in + `{len(_CUSTOM_TEMPLATES)}` custom'
        )

    elif cmd == '/clear_cache':
        n = 0
        for f in os.listdir(CACHE_DIR):
            try: os.remove(os.path.join(CACHE_DIR, f)); n += 1
            except: pass
        mem_n = len(_SEARCH_CACHE)
        _SEARCH_CACHE.clear()
        await event.reply(f'🧹 Cleared `{n}` files + `{mem_n}` memory cache entries.')

    elif cmd == '/purge_book':
        if len(parts) < 2: await event.reply('Usage: `/purge_book <id>`'); return
        try:
            bid = int(parts[1])
            conn = db_connect(); c = conn.cursor()
            c.execute('DELETE FROM books WHERE id=?', (bid,))
            c.execute("INSERT INTO books_fts(books_fts) VALUES('rebuild')")
            conn.commit(); conn.close()
            await event.reply(f'🗑️ Book `{bid}` removed.')
        except ValueError:
            await event.reply('Invalid ID.')

    elif cmd == '/add_admin':
        if not is_owner(sender_id): return
        if len(parts) < 2: await event.reply('Usage: `/add_admin <id>`'); return
        try:
            nid = int(parts[1]); ADMIN_IDS.add(nid); ALL_STAFF.add(nid)
            _save_settings()
            conn = db_connect(); c = conn.cursor()
            c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (nid,))
            r = c.fetchone(); conn.close()
            aname = (r[0] if r else None) or str(nid)
            ref = user_mention_md(nid, aname)
            await event.reply(f'✅ Admin {ref} (`{nid}`) added.', parse_mode='md')
        except ValueError:
            await event.reply('Invalid ID.')

    elif cmd == '/ban':
        if len(parts) < 2:
            await event.reply('Usage: `/ban <user_id> [minutes]`')
            return
        try:
            target_id = int(parts[1])
        except ValueError:
            await event.reply('❌ Invalid user ID.'); return
        if is_staff(target_id):
            await event.reply('❌ Cannot ban staff members.'); return
        try:
            mins = int(parts[2]) if len(parts) >= 3 else 60
        except ValueError:
            mins = 60
        duration = (365 * 24 * 3600) if mins == 0 else (mins * 60)
        _flood_muted[target_id] = time.time() + duration
        label = 'permanent (1 year)' if mins == 0 else f'{mins}m'
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (target_id,))
        r = c.fetchone(); conn.close()
        bname = (r[0] if r else None) or str(target_id)
        bref  = user_mention_md(target_id, bname)
        await event.reply(
            f'🚫 **Banned** {bref} (`{target_id}`) for `{label}`.\n'
            f'Use `/unban {target_id}` to lift early.',
            parse_mode='md'
        )
        _log_scrap('_spam_', 'ban', f'uid={target_id} dur={label} by={sender_id}')

    elif cmd == '/unban':
        if len(parts) < 2:
            await event.reply('Usage: `/unban <user_id>`'); return
        try:
            target_id = int(parts[1])
        except ValueError:
            await event.reply('❌ Invalid user ID.'); return
        if target_id in _flood_muted:
            del _flood_muted[target_id]
            conn = db_connect(); c = conn.cursor()
            c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (target_id,))
            r = c.fetchone(); conn.close()
            uname = (r[0] if r else None) or str(target_id)
            ref = user_mention_md(target_id, uname)
            await event.reply(f'✅ User {ref} (`{target_id}`) unbanned.', parse_mode='md')
        else:
            await event.reply(f'ℹ️ User `{target_id}` is not currently muted.')

    elif cmd == '/show_spam':
        descriptions = {
            'search_cooldown': ('Search cooldown',       's',    'min between searches per user'),
            'daily_dl_limit':  ('Daily download limit',  'books','max downloads per user per day'),
            'page_cooldown':   ('Page button cooldown',  's',    'min between Next/Prev presses'),
            'request_max':     ('Request limit',         'reqs', 'max .request per window'),
            'request_window':  ('Request window',        's',    'window for request limit'),
            'query_min_len':   ('Query min length',      'chars','shorter queries ignored'),
            'query_max_len':   ('Query max length',      'chars','longer queries truncated'),
            'chat_rate_limit': ('Chat rate limit',       'searches','per window per chat'),
            'chat_rate_window':('Chat rate window',      's',    'window for chat rate'),
            'flood_msgs':      ('Flood threshold',       'msgs', 'messages before auto-mute'),
            'flood_window':    ('Flood window',          's',    'window for flood detection'),
            'flood_mute':      ('Flood mute duration',   's',    'how long auto-mute lasts'),
            'warn_on_cooldown':('Warn on cooldown',      '',     'send warning msg on cooldown hit'),
        }
        lines = ['⚙️ **Spam Config** — `/set_spam <key> <value>` to change\n━━━━━━━━━━━━━━━━━━━━']
        for key, val_list in SPAM_CFG.items():
            val = val_list[0]
            desc, unit, note = descriptions.get(key, (key, '', ''))
            unit_str = f' {unit}' if unit else ''
            lines.append(f'`{key}` = **{val}**{unit_str}  — _{note}_')
        await event.reply('\n'.join(lines))

    elif cmd == '/set_spam':
        if len(parts) < 3:
            await event.reply('❌ Usage: `/set_spam <key> <value>`\nRun `/show_spam` to see all keys.')
            return
        key   = parts[1].lower().strip()
        raw   = parts[2].strip()
        if key not in SPAM_CFG:
            keys_list = ', '.join(f'`{k}`' for k in SPAM_CFG)
            await event.reply(f'❌ Unknown key `{key}`.\nValid keys: {keys_list}')
            return
        current = SPAM_CFG[key][0]
        try:
            if isinstance(current, bool):
                if raw.lower() in ('true', '1', 'yes', 'on'):
                    new_val = True
                elif raw.lower() in ('false', '0', 'no', 'off'):
                    new_val = False
                else:
                    raise ValueError(f'Expected true/false, got {raw!r}')
            elif isinstance(current, float):
                new_val = float(raw)
                if new_val < 0: raise ValueError('Value must be >= 0')
            else:
                new_val = int(raw)
                if new_val < 0: raise ValueError('Value must be >= 0')
        except ValueError as e:
            await event.reply(f'❌ Invalid value: `{e}`'); return
        old_val = SPAM_CFG[key][0]
        SPAM_CFG[key][0] = new_val
        _save_settings()
        units = {
            'search_cooldown': 's', 'page_cooldown': 's',
            'request_window': 's', 'chat_rate_window': 's',
            'flood_window': 's', 'flood_mute': 's',
            'daily_dl_limit': ' books/day', 'chat_rate_limit': ' searches',
            'request_max': ' requests', 'query_min_len': ' chars',
            'query_max_len': ' chars',
        }
        unit = units.get(key, '')
        await event.reply(
            f'✅ **`{key}`** updated\n'
            f'`{old_val}{unit}` → **`{new_val}{unit}`**\n'
            f'_Saved. Takes effect immediately._'
        )
        _log_scrap('_cfg_', 'set_spam', f'{key}={new_val} (was {old_val}) by={sender_id}')

    elif cmd == '/spam_stats':
        now = time.time()
        active_mutes = [(uid, ts) for uid, ts in _flood_muted.items() if ts > now]
        active_mutes.sort(key=lambda x: x[1])
        top_users = sorted(
            [(uid, len(l)) for uid, l in _flood_tracker.items() if l],
            key=lambda x: -x[1]
        )[:10]
        hot_chats = sorted(
            [(cid, len(l)) for cid, l in _chat_rate_log.items() if l],
            key=lambda x: -x[1]
        )[:5]
        top_dl = sorted(
            [(uid, len(l)) for uid, l in _daily_dl_log.items() if l],
            key=lambda x: -x[1]
        )[:10]
        lines = [
            f'🛡 **Spam Control Stats**\n━━━━━━━━━━━━━━━━━━━━',
            f'⚙️ Config: flood={FLOOD_MSGS()}msg/{FLOOD_WINDOW_SECS()}s → mute {FLOOD_MUTE_SECS()//60}m | '
            f'daily_dl={DAILY_DL_LIMIT()} | chat_rate={CHAT_RATE_LIMIT_N()}/{CHAT_RATE_LIMIT_SECS()}s',
        ]
        lines.append(f'\n🔇 **Active mutes** ({len(active_mutes)}):')
        if active_mutes:
            for uid, ts in active_mutes[:10]:
                mins_left = int((ts - now) / 60) + 1
                lines.append(f'  • `{uid}` — {mins_left}m remaining')
        else:
            lines.append('  _None_')
        lines.append(f'\n📊 **Search activity** (last {FLOOD_WINDOW_SECS()}s):')
        if top_users:
            for uid, count in top_users:
                lines.append(f'  • `{uid}` — {count} messages')
        else:
            lines.append('  _Quiet_')
        lines.append(f'\n💬 **Hot chats** (last {CHAT_RATE_LIMIT_SECS()}s):')
        if hot_chats:
            for cid, count in hot_chats:
                bar = '█' * min(count, 20) + f' {count}'
                lines.append(f'  • `{cid}` — {bar}')
        else:
            lines.append('  _All quiet_')
        lines.append(f'\n📥 **Daily downloads** (top users today):')
        if top_dl:
            for uid, count in top_dl:
                lines.append(f'  • `{uid}` — {count}/{DAILY_DL_LIMIT()} books')
        else:
            lines.append('  _None yet_')
        await event.reply('\n'.join(lines))

    # ── Analytics (unchanged from v11) ────────────────────────────────────────

    elif cmd == '/top_users':
        n = 10
        try: n = max(1, min(50, int(parts[1]))) if len(parts) >= 2 else 10
        except: pass
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT user_id, first_name, username, COUNT(*) as total
            FROM download_log GROUP BY user_id ORDER BY total DESC LIMIT ?
        ''', (n,))
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM download_log')
        grand_total = c.fetchone()[0]
        conn.close()
        medals = ['🥇', '🥈', '🥉'] + ['🏅'] * 47
        lines = [f'🏆 **Top {n} Downloaders** (all time)\n📥 Total: `{grand_total}`\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No data yet._')
        for i, (uid, fname, uname, total) in enumerate(rows):
            display = fname or uname or str(uid)
            ref = user_mention_md(uid, display)
            uname_str = f' (@{uname})' if uname else ''
            lines.append(f'{medals[i]} {ref}{uname_str} — **{total}** downloads')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/top_books':
        n = 10
        try: n = max(1, min(50, int(parts[1]))) if len(parts) >= 2 else 10
        except: pass
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT book_id, book_name, COUNT(*) as total
            FROM download_log GROUP BY book_id ORDER BY total DESC LIMIT ?
        ''', (n,))
        rows = c.fetchall(); conn.close()
        medals = ['🥇', '🥈', '🥉'] + ['🏅'] * 47
        lines = [f'📚 **Top {n} Most Downloaded Books**\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No data yet._')
        for i, (bid, bname, total) in enumerate(rows):
            display = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', bname or f'Book #{bid}', flags=re.IGNORECASE).strip()
            if len(display) > 50: display = display[:47] + '…'
            lines.append(f'{medals[i]} `{display}` — **{total}** downloads _(id:{bid})_')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/top_sources':
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT sp.chat_id, COALESCE(sp.scrape_label, '') as label,
                   COUNT(b.id) as total,
                   SUM(CASE WHEN b.file_ext='.epub' THEN 1 ELSE 0 END) as epubs,
                   COALESCE(sp.last_scraped_at, 0) as last_ts
            FROM scrape_progress sp
            LEFT JOIN books b ON b.chat_id = sp.chat_id
            GROUP BY sp.chat_id ORDER BY total DESC
        ''')
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM books')
        total_all = c.fetchone()[0]; conn.close()
        lines = [f'📦 **Sources by Book Count**\n📚 Total: `{total_all}`\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No sources scraped yet._')
        for i, (cid, label, total, epubs, last_ts) in enumerate(rows, 1):
            pdfs = total - (epubs or 0)
            lbl = label or f'chat:{cid}'
            pct = f'{total/total_all*100:.1f}%' if total_all else '—'
            ago = ''
            if last_ts:
                diff = int(time.time()) - int(last_ts)
                if diff < 3600: ago = f' _{diff//60}m ago_'
                elif diff < 86400: ago = f' _{diff//3600}h ago_'
                else: ago = f' _{diff//86400}d ago_'
            lines.append(f'**{i}.** `{lbl}` — **{total}** books ({pct})\n   📄 PDF:`{pdfs}` 📕 EPUB:`{epubs or 0}`{ago}')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/top_active_sources':
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT sp.chat_id, COALESCE(sp.scrape_label,'') as label,
                   sp.last_scraped_at, sp.last_msg_id, COUNT(b.id) as books
            FROM scrape_progress sp
            LEFT JOIN books b ON b.chat_id = sp.chat_id
            GROUP BY sp.chat_id ORDER BY sp.last_scraped_at DESC LIMIT 20
        ''')
        rows = c.fetchall(); conn.close()
        lines = [f'⚡ **Most Recently Active Sources**\n━━━━━━━━━━━━━━━━━━━━']
        now = time.time()
        for i, (cid, label, last_ts, last_msg, books) in enumerate(rows, 1):
            lbl = label or f'chat:{cid}'
            if last_ts and int(last_ts) > 0:
                diff = int(now) - int(last_ts)
                if diff < 3600: when = f'{diff//60}m ago'
                elif diff < 86400: when = f'{diff//3600}h ago'
                else: when = f'{diff//86400}d ago'
                dt_str = datetime.fromtimestamp(int(last_ts), tz=timezone.utc).strftime('%m-%d %H:%M')
                time_str = f'`{dt_str}` _{when}_'
            else:
                time_str = '_never_'
            status = '🟢' if (_scrap_current[0] and _scrap_current[0] in lbl) else '⚫'
            lines.append(f'{status} **{i}.** `{lbl}`\n   📅 {time_str} | 📚 `{books}` books | 📌 msg `{last_msg}`')
        if not rows: lines.append('_No scrape history._')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/top_active_users':
        target_chat = None
        if len(parts) >= 2:
            try: target_chat = int(parts[1])
            except: pass
        cutoff = int(time.time()) - 30 * 86400
        conn = db_connect(); c = conn.cursor()
        if target_chat:
            c.execute('''
                SELECT user_id, first_name, username, SUM(searches) as s, SUM(downloads) as d
                FROM (
                    SELECT user_id, first_name, username, COUNT(*) as searches, 0 as downloads
                    FROM search_log WHERE ts > ? AND chat_id = ? GROUP BY user_id
                    UNION ALL
                    SELECT user_id, first_name, username, 0 as searches, COUNT(*) as downloads
                    FROM download_log WHERE ts > ? AND chat_id = ? GROUP BY user_id
                ) combined
                GROUP BY user_id ORDER BY (s + d) DESC LIMIT 15
            ''', (cutoff, target_chat, cutoff, target_chat))
            header_scope = f'Chat `{target_chat}`'
        else:
            c.execute('''
                SELECT user_id, first_name, username, SUM(searches) as s, SUM(downloads) as d
                FROM (
                    SELECT user_id, first_name, username, COUNT(*) as searches, 0 as downloads
                    FROM search_log WHERE ts > ? GROUP BY user_id
                    UNION ALL
                    SELECT user_id, first_name, username, 0 as searches, COUNT(*) as downloads
                    FROM download_log WHERE ts > ? GROUP BY user_id
                ) combined
                GROUP BY user_id ORDER BY (s + d) DESC LIMIT 15
            ''', (cutoff, cutoff))
            header_scope = 'All Groups'
        rows = c.fetchall(); conn.close()
        medals = ['🥇','🥈','🥉'] + ['🏅']*12
        lines = [f'👥 **Top Active Users** — {header_scope} _(last 30 days)_\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No activity data yet._')
        for i, (uid, fname, uname, searches, downloads) in enumerate(rows):
            display = fname or uname or str(uid)
            ref = user_mention_md(uid, display)
            uname_str = f' (@{uname})' if uname else ''
            s = searches or 0; d = downloads or 0
            lines.append(f'{medals[i]} {ref}{uname_str} · 🆔`{uid}`\n   🔍 {s} searches · 📥 {d} downloads · 🔥 {s+d} total')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/group_stats':
        cutoff = int(time.time()) - 30 * 86400
        target_chat = None
        if len(parts) >= 2:
            try: target_chat = int(parts[1])
            except: pass
        conn = db_connect(); c = conn.cursor()
        if target_chat:
            c.execute('SELECT COUNT(*) FROM search_log WHERE ts>? AND chat_id=?', (cutoff, target_chat))
            searches = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM download_log WHERE ts>? AND chat_id=?', (cutoff, target_chat))
            downloads = c.fetchone()[0]
            c.execute('SELECT COUNT(DISTINCT user_id) FROM search_log WHERE ts>? AND chat_id=?', (cutoff, target_chat))
            unique_searchers = c.fetchone()[0]
            c.execute('SELECT COUNT(DISTINCT user_id) FROM download_log WHERE ts>? AND chat_id=?', (cutoff, target_chat))
            unique_dlers = c.fetchone()[0]
            c.execute('''SELECT query, COUNT(*) as n FROM search_log
                WHERE ts>? AND chat_id=? GROUP BY query ORDER BY n DESC LIMIT 5''', (cutoff, target_chat))
            top_q = c.fetchall(); conn.close()
            lines = [f'📊 **Group Stats** — Chat `{target_chat}` _(last 30 days)_\n━━━━━━━━━━━━━━━━━━━━',
                     f'🔍 Searches: `{searches}` by `{unique_searchers}` unique users',
                     f'📥 Downloads: `{downloads}` by `{unique_dlers}` unique users']
            if top_q:
                lines.append('\n🔎 **Top queries:**')
                for q, n in top_q:
                    lines.append(f'  • `{q}` — {n}×')
        else:
            c.execute('''
                SELECT chat_id, COUNT(*) as searches
                FROM search_log WHERE ts > ? GROUP BY chat_id ORDER BY searches DESC LIMIT 10
            ''', (cutoff,))
            search_rows = {r[0]: r[1] for r in c.fetchall()}
            c.execute('''
                SELECT chat_id, COUNT(*) as downloads
                FROM download_log WHERE ts > ? GROUP BY chat_id ORDER BY downloads DESC LIMIT 10
            ''', (cutoff,))
            dl_rows = {r[0]: r[1] for r in c.fetchall()}; conn.close()
            all_chats = set(list(search_rows.keys()) + list(dl_rows.keys()))
            lines = [f'📊 **Group Stats** — All Groups _(last 30 days)_\n━━━━━━━━━━━━━━━━━━━━']
            if not all_chats: lines.append('_No activity data yet._')
            else:
                sorted_chats = sorted(all_chats, key=lambda ch: -(search_rows.get(ch,0)+dl_rows.get(ch,0)))
                for cid in sorted_chats[:10]:
                    s = search_rows.get(cid, 0); d = dl_rows.get(cid, 0)
                    lines.append(f'• `{cid}` — 🔍 {s} searches · 📥 {d} downloads')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/leaderboard':
        cutoff_week  = int(time.time()) - 7 * 86400
        cutoff_month = int(time.time()) - 30 * 86400
        conn = db_connect(); c = conn.cursor()
        c.execute('''SELECT user_id, first_name, username, COUNT(*) as n
            FROM download_log WHERE ts > ? GROUP BY user_id ORDER BY n DESC LIMIT 5''', (cutoff_week,))
        top_dl_week = c.fetchall()
        c.execute('''SELECT user_id, first_name, username, COUNT(*) as n
            FROM search_log WHERE ts > ? GROUP BY user_id ORDER BY n DESC LIMIT 5''', (cutoff_week,))
        top_sr_week = c.fetchall()
        c.execute('''SELECT user_id, first_name, username, COUNT(*) as n
            FROM download_log GROUP BY user_id ORDER BY n DESC LIMIT 1''')
        all_time = c.fetchone()
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts > ?', (cutoff_week,))
        week_dls = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ?', (cutoff_week,))
        week_srs = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts > ?', (cutoff_month,))
        month_dls = c.fetchone()[0]; conn.close()
        medals = ['🥇','🥈','🥉','4️⃣','5️⃣']
        lines = [
            f'🏆 **Weekly Leaderboard**\n'
            f'📅 Week: `{week_srs}` searches · `{week_dls}` downloads\n'
            f'📆 Month: `{month_dls}` downloads\n'
            f'━━━━━━━━━━━━━━━━━━━━'
        ]
        lines.append('\n📥 **Top Downloaders** _(this week)_:')
        if top_dl_week:
            for i, (uid, fname, uname, n) in enumerate(top_dl_week):
                display = fname or uname or str(uid)
                ref = user_mention_md(uid, display)
                lines.append(f'{medals[i]} {ref} · 🆔`{uid}` — {n} books')
        else:
            lines.append('  _No downloads this week yet._')
        lines.append('\n🔍 **Top Searchers** _(this week)_:')
        if top_sr_week:
            for i, (uid, fname, uname, n) in enumerate(top_sr_week):
                display = fname or uname or str(uid)
                ref = user_mention_md(uid, display)
                lines.append(f'{medals[i]} {ref} · 🆔`{uid}` — {n} searches')
        else:
            lines.append('  _No searches this week yet._')
        if all_time:
            uid, fname, uname, n = all_time
            display = fname or uname or str(uid)
            ref = user_mention_md(uid, display)
            lines.append(f'\n👑 **All-time champion:** {ref} · 🆔`{uid}` with **{n}** downloads')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/popular':
        cutoff = int(time.time()) - 86400
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT query, COUNT(*) as n
            FROM search_log WHERE ts > ? GROUP BY query ORDER BY n DESC LIMIT 10
        ''', (cutoff,))
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ?', (cutoff,))
        total_today = c.fetchone()[0]; conn.close()
        lines = [f'🔥 **Popular Searches Today**\n📊 Total: `{total_today}`\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No searches today._')
        for i, (query, n) in enumerate(rows, 1):
            bar = '█' * min(n, 15)
            lines.append(f'**{i}.** `{query}` — {bar} {n}×')
        await event.reply('\n'.join(lines), parse_mode='md')
    elif cmd == '/popular_today':
        cutoff = int(time.time()) - 86400
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT book_name, COUNT(*) as n FROM download_log WHERE ts > ? GROUP BY book_id ORDER BY n DESC LIMIT 10', (cutoff,))
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts > ?', (cutoff,)); total = c.fetchone()[0]
        conn.close()
        medals = ['🥇','🥈','🥉'] + ['🏅']*7
        lines = [f'🔥 **Most Downloaded Today**\n📥 Total: `{total}` downloads\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No downloads today._')
        for i, (bname, n) in enumerate(rows):
            nm = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', bname or '?', flags=re.IGNORECASE).strip()
            if len(nm) > 45: nm = nm[:42] + '…'
            lines.append(f'{medals[i]} `{nm}` — {n}×')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/popular_this_week':
        cutoff = int(time.time()) - 7 * 86400
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT book_name, COUNT(*) as n FROM download_log WHERE ts > ? GROUP BY book_id ORDER BY n DESC LIMIT 10', (cutoff,))
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts > ?', (cutoff,)); total = c.fetchone()[0]
        conn.close()
        medals = ['🥇','🥈','🥉'] + ['🏅']*7
        lines = [f'📈 **Most Downloaded This Week**\n📥 Total: `{total}` downloads\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('_No downloads this week._')
        for i, (bname, n) in enumerate(rows):
            nm = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', bname or '?', flags=re.IGNORECASE).strip()
            if len(nm) > 45: nm = nm[:42] + '…'
            lines.append(f'{medals[i]} `{nm}` — {n}×')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/search_stats':
        cutoff_day   = int(time.time()) - 86400
        cutoff_week  = int(time.time()) - 7*86400
        cutoff_month = int(time.time()) - 30*86400
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM search_log');                               total_s = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ?', (cutoff_day,));   day_s   = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ?', (cutoff_week,));  week_s  = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE result_count = 0');                 zero_all  = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE result_count = 0 AND ts > ?', (cutoff_day,)); zero_day = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM download_log');                             total_d = c.fetchone()[0]
        c.execute('SELECT query, COUNT(*) as n FROM search_log WHERE result_count = 0 GROUP BY query ORDER BY n DESC LIMIT 7')
        zero_queries = c.fetchall()
        c.execute('SELECT query, COUNT(*) as n FROM search_log WHERE ts > ? GROUP BY query ORDER BY n DESC LIMIT 7', (cutoff_week,))
        top_queries = c.fetchall()
        conn.close()
        conv = _pct(total_d, total_s)
        zero_pct = _pct(zero_all, total_s)
        lines = [
            f'🔍 **Search Stats**\n━━━━━━━━━━━━━━━━━━━━',
            f'📊 Total searches: `{total_s}`',
            f'   Today: `{day_s}` · This week: `{week_s}`',
            f'📥 Total downloads: `{total_d}`',
            f'🔄 Search→Download conversion: `{conv}`',
            f'\n❌ **Zero-result queries**',
            f'   All time: `{zero_all}` ({zero_pct}) · Today: `{zero_day}`',
        ]
        if zero_queries:
            lines.append('   _Top unanswered queries:_')
            for q, n in zero_queries:
                lines.append(f'   • `{q}` — {n}×')
        if top_queries:
            lines.append(f'\n🔥 **Top queries this week:**')
            for i, (q, n) in enumerate(top_queries, 1):
                bar = _bar(n, top_queries[0][1])
                lines.append(f'  **{i}.** `{q}` {bar} {n}×')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/user_profile':
        if len(parts) < 2:
            await event.reply('Usage: `/user_profile <user_id>`'); return
        try: target_uid = int(parts[1])
        except: await event.reply('❌ Invalid user ID.'); return
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT first_name, username FROM download_log WHERE user_id=? ORDER BY ts ASC LIMIT 1', (target_uid,))
        row = c.fetchone()
        if not row:
            c.execute('SELECT first_name, username FROM search_log WHERE user_id=? ORDER BY ts ASC LIMIT 1', (target_uid,))
            row = c.fetchone()
        if not row: await event.reply(f'❌ No data found for user `{target_uid}`.'); conn.close(); return
        fname, uname = row
        c.execute('SELECT COUNT(*), MIN(ts), MAX(ts) FROM download_log WHERE user_id=?', (target_uid,))
        dl_count, dl_first, dl_last = c.fetchone()
        c.execute('SELECT COUNT(*), MIN(ts), MAX(ts) FROM search_log WHERE user_id=?', (target_uid,))
        sr_count, sr_first, sr_last = c.fetchone()
        first_seen_ts = min(t for t in [dl_first, sr_first] if t)
        last_seen_ts  = max(t for t in [dl_last, sr_last] if t)
        c.execute('SELECT book_name, COUNT(*) as n FROM download_log WHERE user_id=? GROUP BY book_id ORDER BY n DESC LIMIT 3', (target_uid,))
        fav_books = c.fetchall()
        c.execute('SELECT chat_id, COUNT(*) as n FROM download_log WHERE user_id=? GROUP BY chat_id ORDER BY n DESC LIMIT 1', (target_uid,))
        fav_chat_row = c.fetchone()
        conn.close()
        first_seen = datetime.fromtimestamp(first_seen_ts, tz=timezone.utc).strftime('%Y-%m-%d') if first_seen_ts else '?'
        last_seen  = datetime.fromtimestamp(last_seen_ts,  tz=timezone.utc).strftime('%Y-%m-%d %H:%M') if last_seen_ts else '?'
        days_active = max(1, (last_seen_ts - first_seen_ts) // 86400) if (first_seen_ts and last_seen_ts) else 1
        avg_dls = f'{dl_count/days_active:.1f}' if dl_count else '0'
        ref = user_mention_md(target_uid, fname or uname or str(target_uid))
        ustr = f' (@{uname})' if uname else ''
        lines = [
            f'👤 **User Profile**\n━━━━━━━━━━━━━━━━━━━━',
            f'{ref}{ustr}',
            f'🆔 ID: `{target_uid}`  _(tap name above to open profile)_',
            f'📅 First seen: `{first_seen}` | Last active: `{last_seen}`',
            f'\n📊 **Activity**',
            f'🔍 Searches: `{sr_count or 0}` · 📥 Downloads: `{dl_count or 0}`',
            f'📈 Daily avg downloads: `{avg_dls}`',
        ]
        if fav_books:
            lines.append('\n📚 **Favourite books:**')
            for bname, n in fav_books:
                nm = re.sub(r'\.(pdf|epub)$','',bname or '?',flags=re.IGNORECASE).strip()
                if len(nm) > 40: nm = nm[:37] + '…'
                lines.append(f'  • `{nm}` ({n}×)')
        if fav_chat_row:
            lines.append(f'\n💬 Most active chat: `{fav_chat_row[0]}`')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/book_stats':
        if len(parts) < 2:
            await event.reply('Usage: `/book_stats <book_id>`'); return
        try: bid = int(parts[1])
        except: await event.reply('❌ Invalid book ID.'); return
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT file_name, chat_id FROM books WHERE id=?', (bid,))
        brow = c.fetchone()
        if not brow: await event.reply(f'❌ Book `{bid}` not found in DB.'); conn.close(); return
        bname, bchat = brow
        c.execute('SELECT COUNT(*), COUNT(DISTINCT user_id), MIN(ts), MAX(ts) FROM download_log WHERE book_id=?', (bid,))
        dl_total, uniq_users, first_dl, last_dl = c.fetchone()
        c.execute('SELECT chat_id, COUNT(*) as n FROM download_log WHERE book_id=? GROUP BY chat_id ORDER BY n DESC LIMIT 5', (bid,))
        chats = c.fetchall()
        conn.close()
        bname_clean = re.sub(r'\.(pdf|epub)$','',bname or f'Book #{bid}',flags=re.IGNORECASE).strip()
        first_str = datetime.fromtimestamp(first_dl, tz=timezone.utc).strftime('%Y-%m-%d') if first_dl else 'never'
        last_str  = datetime.fromtimestamp(last_dl,  tz=timezone.utc).strftime('%Y-%m-%d %H:%M') if last_dl else 'never'
        lines = [
            f'📖 **Book Stats**\n━━━━━━━━━━━━━━━━━━━━',
            f'`{bname_clean}`',
            f'🆔 ID: `{bid}` | Source chat: `{bchat}`',
            f'\n📥 Downloads: `{dl_total or 0}`',
            f'👥 Unique users: `{uniq_users or 0}`',
            f'📅 First download: `{first_str}`',
            f'🕐 Last download: `{last_str}`',
        ]
        if chats:
            lines.append('\n💬 **By chat:**')
            for cid, n in chats:
                lines.append(f'  • `{cid}` — {n} downloads')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/hourly_heatmap':
        cutoff = int(time.time()) - 7 * 86400
        target_chat = None
        if len(parts) >= 2:
            try: target_chat = int(parts[1])
            except: pass
        conn = db_connect(); c = conn.cursor()
        if target_chat:
            c.execute('SELECT (ts / 3600) % 24 as hr, COUNT(*) as n FROM download_log WHERE ts > ? AND chat_id=? GROUP BY hr ORDER BY hr', (cutoff, target_chat))
        else:
            c.execute('SELECT (ts / 3600) % 24 as hr, COUNT(*) as n FROM download_log WHERE ts > ? GROUP BY hr ORDER BY hr', (cutoff,))
        rows = c.fetchall(); conn.close()
        by_hour = {hr: n for hr, n in rows}
        mx = max(by_hour.values()) if by_hour else 1
        scope = f'Chat `{target_chat}`' if target_chat else 'All Groups'
        lines = [f'🕐 **Download Heatmap** — {scope} _(last 7 days)_\n━━━━━━━━━━━━━━━━━━━━']
        for hr in range(24):
            n = by_hour.get(hr, 0)
            bar = _bar(n, mx, 14)
            peak = ' ← peak' if n == mx and mx > 0 else ''
            lines.append(f'`{hr:02d}:00` {bar} {n}{peak}')
        await event.reply('\n'.join(lines))

    elif cmd == '/retention':
        now = int(time.time())
        w1s, w1e = now - 7*86400, now
        w2s, w2e = now - 14*86400, now - 7*86400
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT DISTINCT user_id FROM search_log WHERE ts > ? AND ts <= ?', (w2s, w2e))
        prev_users = {r[0] for r in c.fetchall()}
        c.execute('SELECT DISTINCT user_id FROM search_log WHERE ts > ? AND ts <= ?', (w1s, w1e))
        this_users = {r[0] for r in c.fetchall()}
        c.execute('SELECT COUNT(DISTINCT user_id) FROM search_log'); total_ever = c.fetchone()[0]
        conn.close()
        returned  = len(prev_users & this_users)
        new_users = len(this_users - prev_users)
        lost      = len(prev_users - this_users)
        retention_rate = _pct(returned, len(prev_users)) if prev_users else '—'
        lines = [
            f'🔄 **User Retention** _(week-over-week)_\n━━━━━━━━━━━━━━━━━━━━',
            f'👥 Users last week: `{len(prev_users)}` | This week: `{len(this_users)}`',
            f'↩️ Returned: `{returned}` ({retention_rate})',
            f'🆕 New this week: `{new_users}`',
            f'👋 Lost (inactive): `{lost}`',
            f'👤 Total unique users ever: `{total_ever}`',
        ]
        await event.reply('\n'.join(lines))

    elif cmd == '/growth':
        conn = db_connect(); c = conn.cursor()
        now = int(time.time())
        lines = [f'📈 **Monthly Growth** _(last 6 months)_\n━━━━━━━━━━━━━━━━━━━━']
        prev_dls = prev_srs = prev_users = 0
        for m in range(5, -1, -1):
            ms = now - (m+1)*30*86400
            me = now - m*30*86400
            c.execute('SELECT COUNT(*) FROM download_log WHERE ts>? AND ts<=?', (ms, me));            dls  = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM search_log WHERE ts>? AND ts<=?', (ms, me));              srs  = c.fetchone()[0]
            c.execute('SELECT COUNT(DISTINCT user_id) FROM download_log WHERE ts>? AND ts<=?', (ms, me)); users = c.fetchone()[0]
            dt_label = datetime.fromtimestamp(me, tz=timezone.utc).strftime('%b %Y')
            t_dls = _trend(dls, prev_dls) if prev_dls else ''
            t_srs = _trend(srs, prev_srs) if prev_srs else ''
            lines.append(f'📅 **{dt_label}**  🔍`{srs}`{t_srs}  📥`{dls}`{t_dls}  👥`{users}`')
            prev_dls = dls; prev_srs = srs; prev_users = users
        conn.close()
        await event.reply('\n'.join(lines))

    elif cmd == '/dead_books':
        n = 20
        try: n = max(5, min(100, int(parts[1]))) if len(parts) >= 2 else 20
        except: pass
        conn = db_connect(); c = conn.cursor()
        c.execute('''
            SELECT b.id, b.file_name, b.file_ext, b.file_size
            FROM books b
            LEFT JOIN download_log d ON d.book_id = b.id
            WHERE d.id IS NULL
            ORDER BY b.id DESC
            LIMIT ?
        ''', (n,))
        rows = c.fetchall()
        c.execute('''SELECT COUNT(*) FROM books b LEFT JOIN download_log d ON d.book_id=b.id WHERE d.id IS NULL''')
        total_dead = c.fetchone()[0]
        conn.close()
        lines = [f'💀 **Never-Downloaded Books** ({total_dead} total)\n_Showing {n} most recent_\n━━━━━━━━━━━━━━━━━━━━']
        if not rows: lines.append('✨ Every book has been downloaded at least once!')
        for bid, fname, ext, fsize in rows:
            nm = re.sub(r'\.(pdf|epub)$','',fname or f'#{bid}',flags=re.IGNORECASE).strip()
            if len(nm) > 45: nm = nm[:42] + '…'
            sz = f'{fsize//1024}KB' if fsize else '?'
            lines.append(f'• `{bid}` {ext or ""} `{nm}` _{sz}_')
        await event.reply('\n'.join(lines))

    elif cmd == '/daily_report':
        await event.reply('⏳ Generating daily report…')
        txt = await _generate_daily_report()
        await event.reply(txt)

    elif cmd == '/weekly_report':
        await event.reply('⏳ Generating weekly report…')
        txt = await _generate_weekly_report()
        await event.reply(txt)

    elif cmd == '/alert_add':
        # /alert_add <keyword> [chat_id] [thread_id]
        if len(parts) < 2:
            await event.reply(
                '📌 **Usage:** `/alert_add <keyword> [chat_id] [thread_id]`\n\n'
                '**Examples:**\n'
                '`/alert_add হুমায়ূন` — alert in current chat\n'
                '`/alert_add rumi -1001234567890 1234` — alert in specific thread\n\n'
                '_When a new book matching this keyword is scraped, the bot posts an alert._'
            ); return
        kw = parts[1].lower().strip()
        if len(parts) >= 4:
            try: cid = int(parts[2]); tid = _tid(parts[3])
            except: await event.reply('❌ Invalid chat_id or thread_id.'); return
        elif len(parts) == 3:
            try: cid = int(parts[2]); tid = None
            except: await event.reply('❌ Invalid chat_id.'); return
        else:
            cid = event.chat_id; tid = get_event_thread_id(event)
        if kw not in KEYWORD_ALERTS:
            KEYWORD_ALERTS[kw] = []
        target = (cid, tid)
        if target not in KEYWORD_ALERTS[kw]:
            KEYWORD_ALERTS[kw].append(target)
        _save_settings()
        await event.reply(f'🔔 **Alert added**\nKeyword: `{kw}`\nChat: `{cid}` | Thread: `{tid or "—"}`')

    elif cmd == '/alert_remove':
        if len(parts) < 2:
            await event.reply('Usage: `/alert_remove <keyword>`'); return
        kw = parts[1].lower().strip()
        if kw in KEYWORD_ALERTS:
            del KEYWORD_ALERTS[kw]; _save_settings()
            await event.reply(f'🗑 Alert removed for keyword `{kw}`')
        else:
            await event.reply(f'⚠️ No alert found for `{kw}`')

    elif cmd == '/alert_list':
        if not KEYWORD_ALERTS:
            await event.reply('📋 No keyword alerts set.\n\nAdd one with `/alert_add <keyword>`'); return
        lines = ['🔔 **Keyword Alerts**\n━━━━━━━━━━━━━━━━━━━━']
        for kw, targets in KEYWORD_ALERTS.items():
            t_str = ', '.join(f'`{c}`/`{t or "—"}`' for c, t in targets)
            lines.append(f'• `{kw}` → {t_str}')
        await event.reply('\n'.join(lines))

    elif cmd == '/alert_test':
        if len(parts) < 2:
            await event.reply('Usage: `/alert_test <keyword>`'); return
        kw = parts[1].lower().strip()
        if kw not in KEYWORD_ALERTS:
            await event.reply(f'⚠️ No alert configured for `{kw}`'); return
        await _check_keyword_alerts(f'Test Book — {kw} Sample Title.pdf', 0)
        await event.reply(f'✅ Test alert fired for `{kw}` — check the alert chats.')

    elif cmd == '/announce':
        if len(parts) < 2:
            await event.reply(
                '📌 **Usage:** `/announce <message>`\n\n'
                'Broadcasts your message to ALL assigned + trigger chats.\n'
                'Supports variables: `{book_count}` `{date}` `{brand}`\n\n'
                '**Tip:** Write multi-line messages — newlines are preserved!\n\n'
                '**Example:**\n'
                '`/announce 📚 আজ নতুন বই যোগ হয়েছে!\n{book_count}টি বই আছে।`'
            ); return
        # Use event.text directly to preserve newlines, tabs, and formatting.
        # Strip only the command prefix (e.g. "/announce ") from the front.
        raw_msg = event.text
        # Remove the command word from the start (handles /announce, /announce@botname, etc.)
        raw_msg = re.sub(r'^/announce\S*\s*', '', raw_msg, count=1)
        if not raw_msg.strip():
            await event.reply('❌ Message is empty after the command.'); return
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM books'); book_count = c.fetchone()[0]; conn.close()
        try:
            msg_text = raw_msg.format(
                book_count=book_count,
                date=datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                brand=BRAND_CHANNEL[0],
            )
        except Exception as fmt_e:
            await event.reply(f'❌ Template error: `{fmt_e}`'); return
        all_chats = set(ASSIGNED_CHATS.keys()) | set(TRIGGER_CHATS.keys())
        if not all_chats:
            await event.reply('⚠️ No assigned or trigger chats to broadcast to.'); return
        sent_count = 0; fail_count = 0
        client = _analytics_client_ref[0] if _analytics_client_ref else None
        if not client:
            await event.reply('❌ No analytics client available.'); return
        prog = await event.reply(f'📡 Broadcasting to {len(all_chats)} chats…')
        for (cid, tid) in all_chats:
            try:
                await client.send_message(cid, msg_text, reply_to=tid, parse_mode='md')
                sent_count += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                log.debug(f'announce {cid}: {e}'); fail_count += 1
        await prog.delete()
        await event.reply(f'✅ **Announced!**\n📨 Sent: `{sent_count}` | ❌ Failed: `{fail_count}`')

    elif cmd == '/notify_zero':
        # Toggle zero-result notifications
        # Store in spam_cfg as a flag
        current = SPAM_CFG.get('notify_zero_results', [False])[0]
        if 'notify_zero_results' not in SPAM_CFG:
            SPAM_CFG['notify_zero_results'] = [False]
        SPAM_CFG['notify_zero_results'][0] = not current
        _save_settings()
        state = 'ON ✅' if not current else 'OFF ⭕'
        await event.reply(f'🔔 Zero-result notifications: **{state}**\n_When a user search returns 0 books, analytics group gets notified._')

    elif cmd == '/add_alias':
        # /add_alias <book_id> <alias>
        if len(parts) < 3:
            await event.reply(
                '📌 **Usage:** `/add_alias <book_id> <alias name>`\n\n'
                '**Example:**\n'
                '`/add_alias 1234 Shotru Ontore` — book #1234 also found by this name\n'
                '`/add_alias 1234 শত্রু অন্তরে` — Bengali alias\n\n'
                '_Use `/book_info <id>` to find book IDs._'
            ); return
        try: bid = int(parts[1])
        except: await event.reply('❌ Invalid book ID.'); return
        alias = ' '.join(parts[2:]).strip()
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT file_name FROM books WHERE id=?', (bid,))
        row = c.fetchone()
        if not row: conn.close(); await event.reply(f'❌ Book `{bid}` not found.'); return
        try:
            c.execute('INSERT OR IGNORE INTO book_aliases(book_id, alias) VALUES(?,?)', (bid, alias.lower()))
            conn.commit()
        except Exception as e:
            conn.close(); await event.reply(f'❌ DB error: `{e}`'); return
        conn.close()
        if bid not in BOOK_ALIASES: BOOK_ALIASES[bid] = []
        if alias.lower() not in BOOK_ALIASES[bid]:
            BOOK_ALIASES[bid].append(alias.lower())
        _save_settings()
        bname = re.sub(r'\.(pdf|epub)$','',row[0],flags=re.IGNORECASE).strip()
        await event.reply(f'✅ **Alias added**\n📖 Book: `{bname}`\n🏷 Alias: `{alias}`\n_Searchers can now find this book by typing the alias._')

    elif cmd == '/remove_alias':
        if len(parts) < 3:
            await event.reply('Usage: `/remove_alias <book_id> <alias>`'); return
        try: bid = int(parts[1])
        except: await event.reply('❌ Invalid book ID.'); return
        alias = ' '.join(parts[2:]).strip().lower()
        conn = db_connect(); c = conn.cursor()
        c.execute('DELETE FROM book_aliases WHERE book_id=? AND alias=?', (bid, alias))
        conn.commit(); conn.close()
        if bid in BOOK_ALIASES and alias in BOOK_ALIASES[bid]:
            BOOK_ALIASES[bid].remove(alias)
        _save_settings()
        await event.reply(f'🗑 Alias `{alias}` removed from book `{bid}`.')

    elif cmd == '/list_aliases':
        if len(parts) < 2:
            await event.reply('Usage: `/list_aliases <book_id>`'); return
        try: bid = int(parts[1])
        except: await event.reply('❌ Invalid book ID.'); return
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT alias FROM book_aliases WHERE book_id=?', (bid,))
        aliases = [r[0] for r in c.fetchall()]
        c.execute('SELECT file_name FROM books WHERE id=?', (bid,))
        brow = c.fetchone(); conn.close()
        if not brow: await event.reply(f'❌ Book `{bid}` not found.'); return
        bname = re.sub(r'\.(pdf|epub)$','',brow[0],flags=re.IGNORECASE).strip()
        if not aliases:
            await event.reply(f'📖 `{bname}`\n_No aliases set. Add with `/add_alias {bid} <name>`_'); return
        await event.reply(f'📖 **{bname}** (id:`{bid}`)\n🏷 **Aliases:**\n' + '\n'.join(f'  • `{a}`' for a in aliases))

    elif cmd == '/add_vip':
        # /add_vip <user_id> [daily_limit]
        if len(parts) < 2:
            await event.reply(
                '⭐ **Add VIP User**\n'
                'Usage: `/add_vip <user_id> [daily_limit]`\n\n'
                '**Examples:**\n'
                '`/add_vip 123456` — VIP with default limit (3×)\n'
                '`/add_vip 123456 100` — VIP with custom 100 books/day\n\n'
                '_VIP perks: higher limit, no cooldowns, no flood checks_'
            ); return
        try: uid = int(parts[1])
        except: await event.reply('❌ Invalid user ID.'); return
        VIP_USERS.add(uid)
        if len(parts) >= 3:
            try:
                custom_lim = int(parts[2])
                if custom_lim > 0:
                    VIP_CUSTOM_LIMITS[uid] = custom_lim
            except: pass
        _save_settings()
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
        r = c.fetchone(); conn.close()
        name = r[0] if r else str(uid)
        uname_str = f' (@{r[1]})' if r and r[1] else ''
        ref = user_mention_md(uid, name)
        custom_note = f' _(custom)_' if uid in VIP_CUSTOM_LIMITS else ''
        await event.reply(
            f'⭐ **VIP Granted!**\n'
            f'👤 {ref}{uname_str}\n'
            f'🆔 ID: `{uid}`\n'
            f'📥 Daily limit: **{_get_daily_limit(uid)}** books/day{custom_note}\n'
            f'✅ All VIP perks applied.\n\n'
            f'_Use `/vip_card` to generate a card to send to the group._',
            parse_mode='md'
        )

    elif cmd == '/remove_vip':
        if len(parts) < 2:
            await event.reply('Usage: `/remove_vip <user_id>`'); return
        try: uid = int(parts[1])
        except: await event.reply('❌ Invalid user ID.'); return
        VIP_USERS.discard(uid)
        VIP_CUSTOM_LIMITS.pop(uid, None)
        _save_settings()
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
        r = c.fetchone(); conn.close()
        name = r[0] if r else str(uid)
        ref = user_mention_md(uid, name)
        await event.reply(f'✅ VIP removed for {ref} (`{uid}`).', parse_mode='md')

    elif cmd == '/list_vip':
        if not VIP_USERS:
            await event.reply('⭐ No VIP users.\n\nAdd with `/add_vip <user_id>` or `/vip_add_admins <chat_id>`'); return
        conn = db_connect(); c = conn.cursor()
        lines = [f'⭐ **VIP Users** ({len(VIP_USERS)} total)\n━━━━━━━━━━━━━━━━━━━━']
        for uid in sorted(VIP_USERS):
            c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
            r = c.fetchone()
            name  = (r[0] if r else None) or str(uid)
            uname = f' (@{r[1]})' if r and r[1] else ''
            ref   = user_mention_md(uid, name)
            lim   = _get_daily_limit(uid)
            custom_tag = ' ⚙️' if uid in VIP_CUSTOM_LIMITS else ''
            perms_off = [k for k, v in VIP_PERMS.items() if not v]
            perm_tag = f' ⚠️_{len(perms_off)} perm(s) off_' if perms_off else ''
            lines.append(f'• {ref}{uname} — `{lim}`/day{custom_tag}{perm_tag}\n  🆔 `{uid}`')
        conn.close()
        lines.append(f'\n_Active perks: {sum(v for v in VIP_PERMS.values())}/{len(VIP_PERMS)}_')
        lines.append('_Use `/vip_perms` to configure · `/vip_card` to generate a card_')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/vip_set_limit':
        # /vip_set_limit <user_id> <limit>
        if len(parts) < 3:
            await event.reply('Usage: `/vip_set_limit <user_id> <daily_books>`\n\nExample: `/vip_set_limit 123456 50`'); return
        try: uid = int(parts[1]); lim = int(parts[2])
        except: await event.reply('❌ Invalid args.'); return
        if lim <= 0:
            await event.reply('❌ Limit must be > 0.'); return
        if uid not in VIP_USERS:
            await event.reply(f'⚠️ `{uid}` is not a VIP user. Add them first with `/add_vip {uid}`.'); return
        VIP_CUSTOM_LIMITS[uid] = lim
        _save_settings()
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
        r = c.fetchone(); conn.close()
        name = (r[0] if r else None) or str(uid)
        ref = user_mention_md(uid, name)
        await event.reply(f'✅ Custom limit set for {ref}\n📥 **{lim}** books/day', parse_mode='md')

    elif cmd == '/vip_perms':
        # /vip_perms                  — show current
        # /vip_perms <perm> on|off    — toggle
        if len(parts) == 1:
            lines = ['⚙️ **VIP Permissions** — `/vip_perms <perm> on|off` to change\n━━━━━━━━━━━━━━━━━━━━']
            for perm, val in VIP_PERMS.items():
                icon = '✅' if val else '❌'
                lines.append(f'{icon} `{perm}`')
            lines.append('\n_All these apply to every VIP user._')
            await event.reply('\n'.join(lines))
            return
        if len(parts) < 3:
            await event.reply(
                'Usage: `/vip_perms <perm> on|off`\n\n'
                'Available perms:\n' +
                '\n'.join(f'• `{k}`' for k in VIP_PERMS)
            ); return
        perm_key = parts[1].lower().strip()
        val_str  = parts[2].lower().strip()
        if perm_key not in VIP_PERMS:
            await event.reply(f'❌ Unknown permission `{perm_key}`.\nValid: ' + ', '.join(f'`{k}`' for k in VIP_PERMS)); return
        if val_str in ('on', 'true', '1', 'yes'):
            new_val = True
        elif val_str in ('off', 'false', '0', 'no'):
            new_val = False
        else:
            await event.reply('❌ Value must be `on` or `off`.'); return
        VIP_PERMS[perm_key] = new_val
        _save_settings()
        icon = '✅' if new_val else '❌'
        await event.reply(f'{icon} Permission `{perm_key}` set to **{"ON" if new_val else "OFF"}** for all VIP users.')

    elif cmd == '/vip_add_admins':
        # /vip_add_admins <chat_id> [daily_limit]
        # Fetches all admins of a group and makes them VIP in one shot
        if len(parts) < 2:
            await event.reply(
                '⭐ **Bulk VIP — Group Admins**\n'
                'Usage: `/vip_add_admins <chat_id> [daily_limit]`\n\n'
                '**Example:**\n'
                '`/vip_add_admins -1001234567890` — adds all admins as VIP\n'
                '`/vip_add_admins -1001234567890 50` — adds with 50 books/day limit\n\n'
                '_Fetches the admin list from the group and grants VIP to all._'
            ); return
        try: target_chat = int(parts[1])
        except: await event.reply('❌ Invalid chat ID.'); return
        custom_lim = None
        if len(parts) >= 3:
            try: custom_lim = int(parts[2])
            except: pass

        await event.reply(f'⏳ Fetching admins from chat `{target_chat}`…')
        try:
            participants = await user_client.get_participants(
                target_chat, filter=types.ChannelParticipantsAdmins()
            )
        except Exception as e:
            await event.reply(f'❌ Could not fetch admins: `{e}`\n_Make sure the user account is a member of that chat._')
            return

        added = []
        skipped = []
        conn = db_connect(); c = conn.cursor()
        for p in participants:
            uid = p.id
            if is_owner(uid):
                skipped.append((uid, getattr(p, 'first_name', 'Owner') or 'Owner', 'already owner'))
                continue
            VIP_USERS.add(uid)
            if custom_lim and custom_lim > 0:
                VIP_CUSTOM_LIMITS[uid] = custom_lim
            c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
            r = c.fetchone()
            fname  = (r[0] if r else None) or getattr(p, 'first_name', None) or str(uid)
            uname  = (r[1] if r and r[1] else None) or getattr(p, 'username', None)
            added.append((uid, fname, uname, 'Group Admin'))
        conn.close()
        _save_settings()

        lim_str = f'{custom_lim}' if custom_lim else f'{DAILY_DL_LIMIT() * VIP_DL_LIMIT_MULTIPLIER} (default)'
        summary_lines = [
            f'✅ **VIP Bulk Grant Complete**\n━━━━━━━━━━━━━━━━━━━━',
            f'📍 Chat: `{target_chat}`',
            f'⭐ Added: **{len(added)}** admins as VIP',
            f'📥 Daily limit: **{lim_str}** books/day',
            f'⏭ Skipped: {len(skipped)} (owner/self)',
            f'━━━━━━━━━━━━━━━━━━━━',
        ]
        for uid, fname, uname, note in added:
            ref = user_mention_md(uid, fname)
            ustr = f' (@{uname})' if uname else ''
            summary_lines.append(f'• {ref}{ustr}')
        if skipped:
            summary_lines.append(f'\n_Skipped: {", ".join(str(u) for u,_,_ in [(x[0],x[1],x[2]) for x in skipped])}_')
        summary_lines.append('\n_Use `/vip_card` to generate a beautiful card for the group._')
        await event.reply('\n'.join(summary_lines), parse_mode='md')

    elif cmd == '/vip_card':
        # /vip_card [chat_id]   — generate a card listing all VIPs (or just those in a chat)
        # Sends a beautiful formatted card you can forward to the group
        if not VIP_USERS:
            await event.reply('⭐ No VIP users yet. Use `/add_vip` or `/vip_add_admins` first.'); return

        conn = db_connect(); c = conn.cursor()
        entries = []
        for uid in sorted(VIP_USERS):
            c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
            r = c.fetchone()
            fname = (r[0] if r else None) or str(uid)
            uname = (r[1] if r and r[1] else None)
            note  = 'Custom limit' if uid in VIP_CUSTOM_LIMITS else ''
            entries.append((uid, fname, uname, note))
        conn.close()

        card_text = _build_vip_card(entries)
        await event.reply(card_text, parse_mode='md')

    elif cmd == '/vip_card_send':
        # /vip_card_send <chat_id> [thread_id]  — send card directly to the group
        if len(parts) < 2:
            await event.reply('Usage: `/vip_card_send <chat_id> [thread_id]`'); return
        if not VIP_USERS:
            await event.reply('⭐ No VIP users yet.'); return
        try: target_chat = int(parts[1])
        except: await event.reply('❌ Invalid chat ID.'); return
        tid_send = None
        if len(parts) >= 3:
            try: tid_send = int(parts[2])
            except: pass

        conn = db_connect(); c = conn.cursor()
        entries = []
        for uid in sorted(VIP_USERS):
            c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (uid,))
            r = c.fetchone()
            fname = (r[0] if r else None) or str(uid)
            uname = (r[1] if r and r[1] else None)
            note  = 'Custom limit' if uid in VIP_CUSTOM_LIMITS else ''
            entries.append((uid, fname, uname, note))
        conn.close()

        card_text = _build_vip_card(entries)
        client_to_use = _analytics_client_ref[0] if _analytics_client_ref else interaction_client
        try:
            await client_to_use.send_message(target_chat, card_text, reply_to=tid_send, parse_mode='md')
            await event.reply(f'✅ VIP card sent to chat `{target_chat}`!')
        except Exception as e:
            await event.reply(f'❌ Failed to send: `{e}`')

    elif cmd == '/vip_card_style':
        # /vip_card_style header|border|emoji|footer <value>
        if len(parts) < 3:
            lines = ['🎨 **VIP Card Style** — `/vip_card_style <key> <value>` to change\n━━━━━━━━━━━━━━━━━━━━']
            for k, v in VIP_CARD_STYLE.items():
                lines.append(f'`{k}` = `{v}`')
            lines.append('\n_Preview with `/vip_card`_')
            await event.reply('\n'.join(lines)); return
        key = parts[1].lower().strip()
        val = ' '.join(parts[2:]).strip()
        if key not in VIP_CARD_STYLE:
            await event.reply(f'❌ Unknown key `{key}`. Valid: ' + ', '.join(f'`{k}`' for k in VIP_CARD_STYLE)); return
        VIP_CARD_STYLE[key] = val
        _save_settings()
        await event.reply(f'✅ Card style updated: `{key}` = `{val}`\n_Preview with `/vip_card`_')

    elif cmd == '/user_history':
        if len(parts) < 2:
            await event.reply('Usage: `/user_history <user_id>`\n_Shows last 30 days of downloads._'); return
        try: target_uid = int(parts[1])
        except: await event.reply('❌ Invalid user ID.'); return
        conn = db_connect(); c = conn.cursor()
        cutoff = int(time.time()) - 30 * 86400
        c.execute('''SELECT book_name, ts FROM download_log
            WHERE user_id=? AND ts>? ORDER BY ts DESC LIMIT 50''', (target_uid, cutoff))
        rows = c.fetchall()
        c.execute('SELECT first_name, username FROM download_log WHERE user_id=? LIMIT 1', (target_uid,))
        info = c.fetchone(); conn.close()
        if not rows:
            await event.reply(f'📋 No downloads in last 30 days for `{target_uid}`.'); return
        name = info[0] if info else str(target_uid)
        ref = user_mention_md(target_uid, name)
        lines = [f'📋 **Download History** — {ref} _(last 30 days)_\n━━━━━━━━━━━━━━━━━━━━']
        for bname, ts in rows:
            nm = re.sub(r'\.(pdf|epub)$','',bname or '?',flags=re.IGNORECASE).strip()
            if len(nm) > 40: nm = nm[:37] + '…'
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%m-%d %H:%M')
            lines.append(f'`{dt}` — {nm}')
        await event.reply('\n'.join(lines), parse_mode='md')

    elif cmd == '/botd_add':
        # /botd_add [chat_id] [thread_id]
        if len(parts) >= 3:
            try: cid = int(parts[1]); tid = _tid(parts[2])
            except: await event.reply('❌ Invalid chat_id/thread_id.\nUsage: `/botd_add [chat_id] [thread_id]`'); return
        elif len(parts) == 2:
            try: cid = int(parts[1]); tid = None
            except: await event.reply('❌ Invalid chat_id.\nUsage: `/botd_add [chat_id] [thread_id]`'); return
        else:
            cid = event.chat_id; tid = get_event_thread_id(event)
        BOTD_CHATS[(cid, tid)] = True; _save_settings()
        await event.reply(
            f'📖 **Book of the Day** enabled\n'
            f'📍 Chat: `{cid}` | Thread: `{tid or "—"}`\n'
            f'🕐 Posts daily at `{REPORT_TIME_UTC[0]} UTC`\n\n'
            f'_Test it now with `/botd_test`_'
        )

    elif cmd == '/botd_remove':
        if len(parts) >= 3:
            try: cid = int(parts[1]); tid = _tid(parts[2])
            except: await event.reply('❌ Invalid args.'); return
        elif len(parts) == 2:
            try: cid = int(parts[1]); tid = None
            except: await event.reply('❌ Invalid args.'); return
        else:
            cid = event.chat_id; tid = get_event_thread_id(event)
        key = (cid, tid)
        if key in BOTD_CHATS:
            del BOTD_CHATS[key]; _save_settings()
            await event.reply(f'✅ BOTD removed for chat `{cid}`.')
        else:
            await event.reply('⚠️ That chat is not in BOTD list.')

    elif cmd == '/botd_list':
        if not BOTD_CHATS:
            await event.reply('📋 No BOTD chats configured.\n\nAdd with `/botd_add [chat_id] [thread_id]`'); return
        lines = ['📖 **Book of the Day Chats**\n━━━━━━━━━━━━━━━━━━━━']
        for (cid, tid) in BOTD_CHATS:
            lines.append(f'• Chat `{cid}` | Thread `{tid or "—"}`')
        lines.append(f'\n🕐 Posts daily at `{REPORT_TIME_UTC[0]} UTC`')
        await event.reply('\n'.join(lines))

    elif cmd == '/botd_test':
        await event.reply('📖 Generating book of the day…')
        await _post_book_of_the_day()
        if not BOTD_CHATS:
            await event.reply('⚠️ No BOTD chats configured. Add one with `/botd_add`')

    elif cmd == '/export_db':
        await event.reply('⏳ Generating book list CSV…')
        def _export():
            conn = db_connect(); c = conn.cursor()
            c.execute('SELECT id, file_name, file_ext, file_size, chat_id, is_restricted FROM books ORDER BY id')
            rows = c.fetchall(); conn.close()
            import io, csv
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(['id','file_name','ext','size_bytes','source_chat','is_restricted'])
            w.writerows(rows)
            return buf.getvalue().encode('utf-8')
        import io
        loop = asyncio.get_event_loop()
        csv_bytes = await loop.run_in_executor(_SEARCH_EXECUTOR, _export)
        conn2 = db_connect(); c2 = conn2.cursor()
        c2.execute('SELECT COUNT(*) FROM books'); total = c2.fetchone()[0]; conn2.close()
        fname = f'books_export_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")}.csv'
        import tempfile, os as _os
        tmp = tempfile.mktemp(suffix='.csv')
        with open(tmp, 'wb') as f:
            f.write(csv_bytes)
        await event.reply_document(tmp, caption=f'📚 Book DB Export\n`{total}` books · {datetime.now(timezone.utc).strftime("%Y-%m-%d")}')
        _os.unlink(tmp)

    elif cmd == '/health':
        txt = await _health_check()
        await event.reply(txt)

    elif cmd == '/backup_db':
        await event.reply('⏳ Creating DB backup…')
        def _do_backup():
            import sqlite3 as _sq
            ts_str2 = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            bak_path = os.path.join(_BASE_DIR, f'ebooks_backup_{ts_str2}.db')
            src = _sq.connect(DB_PATH)
            dst = _sq.connect(bak_path)
            src.backup(dst)
            src.close(); dst.close()
            size = os.path.getsize(bak_path)
            return bak_path, size
        loop = asyncio.get_event_loop()
        try:
            bak_path, bak_size = await loop.run_in_executor(_SEARCH_EXECUTOR, _do_backup)
            await event.reply_document(bak_path,
                caption=f'🗄 **DB Backup**\n📦 Size: `{bak_size//1048576:.1f}MB`\n🕐 {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}')
            os.unlink(bak_path)
        except Exception as e:
            await event.reply(f'❌ Backup failed: `{e}`')

    elif cmd == '/broadcast_report_add':
        # /broadcast_report_add [chat_id] [thread_id] [daily|weekly|both]
        rtype = 'both'
        if len(parts) >= 4:
            try: cid = int(parts[1]); tid = _tid(parts[2]); rtype = parts[3].lower()
            except: await event.reply('❌ Invalid args.\nUsage: `/broadcast_report_add [chat_id] [thread_id] [daily|weekly|both]`'); return
        elif len(parts) == 3:
            try: cid = int(parts[1]); tid = _tid(parts[2])
            except: await event.reply('❌ Invalid args.'); return
        elif len(parts) == 2:
            try: cid = int(parts[1]); tid = None
            except: await event.reply('❌ Invalid chat_id.'); return
        else:
            cid = event.chat_id; tid = get_event_thread_id(event)
        cfg = {'daily': rtype in ('daily','both'), 'weekly': rtype in ('weekly','both')}
        BROADCAST_REPORT_CHATS[(cid, tid)] = cfg; _save_settings()
        await event.reply(
            f'📡 **Broadcast report assigned**\n'
            f'📍 Chat: `{cid}` | Thread: `{tid or "—"}`\n'
            f'📅 Daily: `{"ON" if cfg["daily"] else "OFF"}` | 📆 Weekly: `{"ON" if cfg["weekly"] else "OFF"}`\n\n'
            f'_Test with `/broadcast_report_test daily` or `/broadcast_report_test weekly`_'
        )

    elif cmd == '/broadcast_report_remove':
        if len(parts) >= 3:
            try: cid = int(parts[1]); tid = _tid(parts[2])
            except: await event.reply('❌ Invalid args.'); return
        elif len(parts) == 2:
            try: cid = int(parts[1]); tid = None
            except: await event.reply('❌ Invalid args.'); return
        else:
            cid = event.chat_id; tid = get_event_thread_id(event)
        key = (cid, tid)
        if key in BROADCAST_REPORT_CHATS:
            del BROADCAST_REPORT_CHATS[key]; _save_settings()
            await event.reply(f'✅ Broadcast report removed for chat `{cid}`.')
        else:
            await event.reply('⚠️ Not found in broadcast list.')

    elif cmd == '/broadcast_report_list':
        if not BROADCAST_REPORT_CHATS:
            await event.reply('📡 No broadcast report chats.\n\nAdd with `/broadcast_report_add [chat_id] [thread_id] [daily|weekly|both]`'); return
        lines = ['📡 **Broadcast Report Chats**\n━━━━━━━━━━━━━━━━━━━━']
        for (cid, tid), cfg in BROADCAST_REPORT_CHATS.items():
            d = '✅' if cfg.get('daily') else '⭕'
            w = '✅' if cfg.get('weekly') else '⭕'
            lines.append(f'• `{cid}` / `{tid or "—"}` — 📅 Daily:{d} 📆 Weekly:{w}')
        await event.reply('\n'.join(lines))

    elif cmd == '/broadcast_report_test':
        rtype = parts[1].lower() if len(parts) >= 2 else 'daily'
        if rtype not in ('daily', 'weekly'):
            await event.reply('Usage: `/broadcast_report_test daily` or `/broadcast_report_test weekly`'); return
        await event.reply(f'⏳ Sending test {rtype} broadcast…')
        await _broadcast_report(rtype)
        await event.reply(f'✅ Test {rtype} broadcast sent to `{len(BROADCAST_REPORT_CHATS)}` chats.')

    elif cmd == '/set_report_time':
        if len(parts) < 2:
            await event.reply(
                f'📌 **Usage:** `/set_report_time HH:MM`\n\n'
                f'Current: `{REPORT_TIME_UTC[0]} UTC` ({REPORT_TZ_NAME[0]})\n\n'
                f'**Examples:**\n'
                f'`/set_report_time 00:00` — midnight UTC\n'
                f'`/set_report_time 18:00` — 6 PM UTC\n'
                f'`/set_report_time 21:00` — 9 PM UTC (midnight BDT)\n\n'
                f'_Time is always stored and applied in UTC._'
            ); return
        raw_time = parts[1].strip()
        try:
            h, m = _parse_report_time(raw_time)
            REPORT_TIME_UTC[0] = f'{h:02d}:{m:02d}'
            _save_settings()
            await event.reply(
                f'✅ **Report time set to `{REPORT_TIME_UTC[0]} UTC`**\n'
                f'_Daily reports, BOTD, and broadcasts will fire at this time._\n'
                f'_(BDT = UTC+6, so `18:00 UTC` = midnight BDT)_'
            )
        except Exception as e:
            await event.reply(f'❌ Invalid time format: `{e}`\nUse HH:MM (e.g. `00:00` or `21:30`)')

    elif cmd == '/related':
        if len(parts) < 2:
            await event.reply('Usage: `/related <book_id>`'); return
        try: bid = int(parts[1])
        except: await event.reply('❌ Invalid book ID.'); return
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT file_name FROM books WHERE id=?', (bid,))
        brow = c.fetchone(); conn.close()
        if not brow: await event.reply(f'❌ Book `{bid}` not found.'); return
        bname = brow[0]
        loop = asyncio.get_event_loop()
        related = await loop.run_in_executor(_SEARCH_EXECUTOR, _related_books, bid, bname)
        if not related:
            await event.reply('_No related books found._'); return
        name_clean = re.sub(r'\.(pdf|epub)$','',bname,flags=re.IGNORECASE).strip()
        lines = [f'🔗 **Related to:** `{name_clean}`\n━━━━━━━━━━━━━━━━━━━━']
        for row in related:
            rid, rfname = row[0], row[1]
            rname = re.sub(r'\.(pdf|epub)$','',rfname or f'#{rid}',flags=re.IGNORECASE).strip()
            if len(rname) > 45: rname = rname[:42] + '…'
            lines.append(f'• `#{rid}` — {rname}')
        lines.append('\n_Use `/book_info <id>` to download._')
        await event.reply('\n'.join(lines))

    elif cmd == '/trending':
        await event.reply('⏳ Calculating trending books…')
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(_SEARCH_EXECUTOR, _trending_books, 10)
        if not rows:
            await event.reply('📊 No trending books right now.\n_A book trends when its downloads this week are >30% above its weekly average._'); return
        lines = ['📈 **Trending Books** _(rising downloads this week)_\n━━━━━━━━━━━━━━━━━━━━']
        for i, (bid, bname, week_n, avg, velocity) in enumerate(rows, 1):
            nm = re.sub(r'\.(pdf|epub)$','',bname or f'#{bid}',flags=re.IGNORECASE).strip()
            if len(nm) > 42: nm = nm[:39] + '…'
            arrow = '🔥' if velocity > 3 else '📈' if velocity > 1.8 else '↗️'
            lines.append(f'{arrow} **{i}.** `{nm}`\n   _This week: {week_n}× · avg: {avg:.1f}×/wk · {velocity:.1f}× velocity_')
        await event.reply('\n'.join(lines))

    elif cmd == '/companion_status':
        if not COMPANION_CLIENTS:
            await event.reply(
                '🤝 **No companions configured.**\n\n'
                'Add companions in `settings.json`:\n'
                '```json\n"companions": [\n  {\n'
                '    "name": "Companion A",\n'
                '    "api_id": 12345678,\n'
                '    "api_hash": "abc123...",\n'
                '    "session": "companion_a",\n'
                '    "sources": ["@BlockedGroup1"]\n'
                '  }\n]\n```\n'
                '_Session file must already exist (log in once manually)._',
                parse_mode='md'
            ); return
        lines = ['🤝 **Companion Status**\n━━━━━━━━━━━━━━━━━━━━']
        for i, comp in enumerate(COMPANION_CLIENTS, 1):
            status = '🟢 Online' if comp.running else '🔴 Offline'
            uname  = f'@{comp.me.username}' if comp.me else '_not logged in_'
            err    = f'\n   ⚠️ `{comp.error}`' if comp.error else ''
            srcs   = ', '.join(f'`{s}`' for s in comp.sources) or '_none_'
            lines.append(
                f'**{i}. {comp.name}** — {status}\n'
                f'   👤 {uname}{err}\n'
                f'   📡 Sources ({len(comp.sources)}): {srcs}'
            )
        await event.reply('\n\n'.join(lines), parse_mode='md')

    elif cmd == '/companion_add_source':
        if len(parts) < 3:
            await event.reply(
                '**Usage:** `/companion_add_source <name_or_index> <source>`\n\n'
                'Examples:\n'
                '`/companion_add_source 1 @SomeGroup`\n'
                '`/companion_add_source "Companion A" -100123456789`',
                parse_mode='md'
            ); return
        comp_ref = parts[1].strip('"\''); source = parts[2].strip()
        comp = None
        try:
            idx = int(comp_ref) - 1
            comp = COMPANION_CLIENTS[idx] if 0 <= idx < len(COMPANION_CLIENTS) else None
        except ValueError:
            for c in COMPANION_CLIENTS:
                if c.name.lower() == comp_ref.lower(): comp = c; break
        if not comp:
            await event.reply(f'❌ Companion `{comp_ref}` not found. Use `/companion_status` to list.'); return
        if source in comp.sources:
            await event.reply(f'ℹ️ `{source}` is already assigned to **{comp.name}**.'); return
        comp.sources.append(source)
        if source not in SOURCE_GROUPS:
            SOURCE_GROUPS.append(source)
        _save_companions(); _save_settings()
        await event.reply(
            f'✅ Added `{source}` → **{comp.name}**\n'
            f'_Scraping and real-time indexing for this source will use {comp.name}._',
            parse_mode='md'
        )

    elif cmd == '/companion_remove_source':
        if len(parts) < 3:
            await event.reply('**Usage:** `/companion_remove_source <name_or_index> <source>`'); return
        comp_ref = parts[1].strip('"\''); source = parts[2].strip()
        comp = None
        try:
            idx = int(comp_ref) - 1
            comp = COMPANION_CLIENTS[idx] if 0 <= idx < len(COMPANION_CLIENTS) else None
        except ValueError:
            for c in COMPANION_CLIENTS:
                if c.name.lower() == comp_ref.lower(): comp = c; break
        if not comp:
            await event.reply(f'❌ Companion `{comp_ref}` not found.'); return
        if source not in comp.sources:
            await event.reply(f'ℹ️ `{source}` is not assigned to **{comp.name}**.'); return
        comp.sources.remove(source)
        _save_companions()
        await event.reply(f'✅ Removed `{source}` from **{comp.name}**.')

    elif cmd == '/companion_restart':
        if len(parts) < 2:
            await event.reply('**Usage:** `/companion_restart <name_or_index>`'); return
        comp_ref = parts[1].strip('"\'')
        comp = None
        try:
            idx = int(comp_ref) - 1
            comp = COMPANION_CLIENTS[idx] if 0 <= idx < len(COMPANION_CLIENTS) else None
        except ValueError:
            for c in COMPANION_CLIENTS:
                if c.name.lower() == comp_ref.lower(): comp = c; break
        if not comp:
            await event.reply(f'❌ Companion `{comp_ref}` not found.'); return
        prog = await event.reply(f'🔄 Restarting **{comp.name}**…', parse_mode='md')
        try:
            if comp.client:
                try: await comp.client.disconnect()
                except Exception: pass
            comp.running = False; comp.client = None; comp.me = None; comp.error = ''
            async def _do_restart(c=comp):
                await asyncio.sleep(2)
                session_path = os.path.join(_BASE_DIR, c.session)
                c.client = TelegramClient(session_path, c.api_id, c.api_hash)
                await c.client.start()
                c.me = await c.client.get_me(); c.running = True
                log.info(f'Companion "{c.name}" restarted as @{c.me.username}')
                await report(f'🔄 **Companion "{c.name}" restarted** as @{c.me.username}\n🕐 {ts_str()}')
            asyncio.create_task(_do_restart())
            await prog.edit(f'✅ **{comp.name}** restart initiated. Check `/companion_status` in ~5s.')
        except Exception as e:
            await prog.edit(f'❌ Restart failed: `{e}`')

    elif cmd in ('/mycollections', '.mycollections', '/col', '.col', '/collections'):
        # ── User collections — full button UI ─────────────────────────────────
        uid = get_sender_id(event)
        if not BOT_TOKEN:
            cols = col_user_list(uid)
            if not cols:
                await event.reply('📚 You have no collections. Use `.col new <name>` to create one.')
                return
            lines = ['📚 **My Collections**\n━━━━━━━━━━━━━━━━━━━━']
            for c in cols:
                pub = '🌐' if c['is_public'] else '🔒'
                code = col_share_code(uid, c['id'])
                lines.append(f'{c["emoji"]} **{c["name"]}** {pub} — `{c["count"]} books`\nShare: `{code}`')
            await event.reply('\n\n'.join(lines)); return
        text, btns = _col_list_text(uid)
        await event.reply(text, buttons=btns, parse_mode='md')

    elif cmd in ('/col_new', '.col_new') or (cmd in ('/col', '.col') and len(parts) >= 2 and parts[1].lower() == 'new'):
        uid = get_sender_id(event)
        # .col new <name>
        name_parts = parts[2:] if len(parts) >= 3 else []
        if not name_parts:
            await event.reply(
                '📚 **Create Collection**\n\n'
                'Usage: `.col new <name>`\n'
                'Example: `.col new My Favourites`\n\n'
                '_Emojis allowed: `.col new 🌟 Must Read`_'
            ); return
        name_raw = ' '.join(name_parts).strip()
        # Extract leading emoji if present
        emoji = '📚'
        m_emoji = re.match(r'^([\U00010000-\U0010FFFF\u2600-\u27BF])\s+(.+)$', name_raw)
        if m_emoji:
            emoji    = m_emoji.group(1)
            name_raw = m_emoji.group(2)
        if len(col_user_list(uid)) >= MAX_COLLECTIONS_PER_USER:
            await event.reply(f'❌ Max {MAX_COLLECTIONS_PER_USER} collections per user.'); return
        cid  = col_create(uid, name_raw, emoji)
        code = col_share_code(uid, cid)
        btns = [[Button.inline(f'📂 Open {emoji} {name_raw[:20]}', f'col_open_{cid}'.encode())]] if BOT_TOKEN else None
        await event.reply(
            f'✅ **Collection created!**\n'
            f'{emoji} **{name_raw}**\n'
            f'🔗 Share code: `{code}`\n\n'
            f'_Add books from search results using the_ **📁 Save** _button._',
            buttons=btns, parse_mode='md'
        )

    elif re.match(r'#col\d+_\d+', event.text.strip()):
        # ── Share code lookup — anyone can paste a #col code ─────────────────
        parsed = col_parse_code(event.text.strip())
        if not parsed:
            await event.reply(
                '❌ Invalid collection code.\n\n'
                '📌 **Collection codes look like:** `#col123456789_42`\n'
                '_Ask the collection owner to share their code._'
            ); return
        uid_owner, cid = parsed
        col = col_get(cid)
        if not col:
            await event.reply('❌ Collection not found. It may have been deleted.'); return
        requester_id = get_sender_id(event)
        if not col['is_public'] and col['user_id'] != requester_id:
            await event.reply(
                '🔒 This collection is **private**.\n'
                '_Only the owner can view it._'
            ); return
        items = col_items(cid)
        text, btns = _col_detail_text(col, items)
        await event.reply(text, buttons=btns if BOT_TOKEN else None, parse_mode='md')


        # Works in DMs AND groups — no need to go private
        raw = event.text.strip()
        msg_text = re.sub(r'^[/\.](feedback)\s*', '', raw, flags=re.IGNORECASE).strip()
        if not msg_text:
            btn = [[Button.inline('📝 Write Feedback', b'feedback_prompt')]]
            await event.reply(
                '💌 **Send Feedback**\n\n'
                'Usage: `.feedback <your message>`\n'
                'Or tap the button below:\n\n'
                '_Your message goes straight to the admin team._',
                buttons=btn if BOT_TOKEN else None
            )
            return
        if len(msg_text) < 5:
            await event.reply('❌ Too short — write at least a few words.'); return
        sender_id = get_sender_id(event)
        sender    = event.sender
        fname     = getattr(sender, 'first_name', 'User') or 'User'
        uname     = getattr(sender, 'username', None)
        chat_info = ''
        if not event.is_private:
            chat_title = getattr(event.chat, 'title', '') or ''
            chat_info  = f' in **{chat_title}**' if chat_title else ''
        try:
            conn = db_connect(); c = conn.cursor()
            c.execute('INSERT INTO feedback(user_id,username,first_name,message,ts) VALUES(?,?,?,?,?)',
                      (sender_id, uname or '', fname, msg_text, int(time.time())))
            conn.commit(); conn.close()
        except Exception as fe:
            log.debug(f'feedback insert: {fe}')
        ref  = user_mention_md(sender_id, fname)
        ustr = f' (@{uname})' if uname else ''
        await report(
            f'💌 **User Feedback**{chat_info}\n'
            f'👤 {ref}{ustr}\n'
            f'💬 {msg_text}\n'
            f'🕐 {ts_str()}'
        )
        reply = await event.reply(
            '💌 **Thank you for your feedback!**\n'
            '_Your message has been sent to the admin team. ✅_'
        )
        schedule_delete(reply.id, reply.chat_id, 30)


    elif cmd == '/ping':
        t0 = time.monotonic()
        def _db_ping():
            conn = db_connect(); c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM books'); result = c.fetchone()[0]
            conn.close(); return result
        loop = asyncio.get_event_loop()
        db_t0 = time.monotonic()
        book_count = await loop.run_in_executor(_SEARCH_EXECUTOR, _db_ping)
        db_ms = int((time.monotonic() - db_t0) * 1000)
        tg_ms = int((time.monotonic() - t0) * 1000)
        uptime = _fmt_uptime(int(time.time() - _BOT_START_TIME))
        await event.reply(
            f'🏓 **Pong!**\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'⚡ Response: `{tg_ms}ms`\n'
            f'🗄 DB query: `{db_ms}ms` ({book_count} books)\n'
            f'⏱ Uptime: `{uptime}`\n'
            f'🕐 {datetime.now(timezone.utc).strftime("%H:%M:%S UTC")}'
        )

    elif cmd == '/feedbacks':
        if not is_staff(get_sender_id(event)):
            return
        n = 20
        try: n = max(5, min(100, int(parts[1]))) if len(parts) >= 2 else 20
        except: pass
        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT user_id, first_name, username, message, ts FROM feedback ORDER BY ts DESC LIMIT ?', (n,))
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM feedback'); total = c.fetchone()[0]
        conn.close()
        if not rows:
            await event.reply('💌 No feedback received yet.'); return
        lines = [f'💌 **User Feedbacks** ({total} total, showing {len(rows)})\n━━━━━━━━━━━━━━━━━━━━']
        for uid, fname, uname, msg_txt, ts_val in rows:
            dt = datetime.fromtimestamp(ts_val, tz=timezone.utc).strftime('%m-%d %H:%M')
            ref = user_mention_md(uid, fname or uname or str(uid))
            ustr = f' (@{uname})' if uname else ''
            lines.append(f'[{dt}] {ref}{ustr}:\n_{msg_txt}_\n')
        await event.reply('\n'.join(lines), parse_mode='md')




# ─────────────────────────────────────────────────────────────────────────────
# Feature helpers: keyword alerts, VIP, aliases, BOTD, broadcast, health,
# backup, related, trending, feedback, auto-report time
# ─────────────────────────────────────────────────────────────────────────────

def _is_vip(user_id: int) -> bool:
    return user_id in VIP_USERS or user_id == OWNER_ID

def _get_daily_limit(user_id: int) -> int:
    if _is_vip(user_id):
        # Per-user custom limit takes priority over the multiplier
        if user_id in VIP_CUSTOM_LIMITS:
            return VIP_CUSTOM_LIMITS[user_id]
        return DAILY_DL_LIMIT() * VIP_DL_LIMIT_MULTIPLIER
    return DAILY_DL_LIMIT()

def _build_vip_card(vip_entries: list[tuple]) -> str:
    """
    Build a beautiful VIP announcement card for sending to a group.

    vip_entries: list of (user_id, first_name, username, note)
      where note is optional extra text (e.g. 'Group Admin').
    Returns a Markdown string.
    """
    border_char = VIP_CARD_STYLE.get('border', '━')
    header      = VIP_CARD_STYLE.get('header', '🌟 VIP Members 🌟')
    star        = VIP_CARD_STYLE.get('emoji', '⭐')
    footer      = VIP_CARD_STYLE.get('footer', 'Powered by the bot 🤖')
    sep         = border_char * 28

    lines = [
        f'**{header}**',
        sep,
        '',
    ]
    for uid, fname, uname, note in vip_entries:
        ref   = f'[{fname or "User"}](tg://user?id={uid})'
        ustr  = f' (@{uname})' if uname else ''
        limit = _get_daily_limit(uid)
        note_str = f' · _{note}_' if note else ''
        lines.append(f'{star} {ref}{ustr}{note_str}')
        lines.append(f'   📥 Daily limit: **{limit}** books/day')
        lines.append('')

    # Active VIP permissions
    active_perms = [k.replace('_', ' ').title() for k, v in VIP_PERMS.items() if v]
    if active_perms:
        lines.append(sep)
        lines.append('✨ **VIP Perks:**')
        for perm in active_perms:
            lines.append(f'   ✅ {perm}')
        lines.append('')

    lines += [sep, f'_{footer}_']
    return '\n'.join(lines)


async def _handle_request_status(event, interaction_client):
    """User-facing: check whether their book requests were fulfilled."""
    sender_id  = get_sender_id(event)
    sender     = event.sender
    first_name = getattr(sender, 'first_name', 'User') or 'User'
    tid        = get_event_thread_id(event)

    # Look up pending_requests for this user
    my_requests = [(q, ts) for (uid, q), ts in pending_requests.items() if uid == sender_id]

    # Check if any of their requests were fulfilled (book now exists in DB)
    fulfilled = []
    still_pending = []
    conn = db_connect(); c = conn.cursor()
    for q, req_ts in my_requests:
        q_norm = normalize_name(q)
        c.execute('SELECT id, file_name FROM books WHERE search_name LIKE ? LIMIT 1', (f'%{q_norm[:20]}%',))
        found = c.fetchone()
        if found:
            bname = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', found[1], flags=re.IGNORECASE).strip()
            fulfilled.append((q, bname, found[0]))
        else:
            still_pending.append(q)
    conn.close()

    # Also check downloads in last 30 days
    cutoff = int(time.time()) - 30 * 86400
    conn2 = db_connect(); c2 = conn2.cursor()
    c2.execute('SELECT book_name, COUNT(*) FROM download_log WHERE user_id=? AND ts>? GROUP BY book_id ORDER BY ts DESC LIMIT 5', (sender_id, cutoff))
    recent_dls = c2.fetchall()
    conn2.close()

    if not my_requests and not recent_dls:
        sent = await interaction_client.send_message(
            event.chat_id,
            f'📋 {first_name}, তোমার কোনো পেন্ডিং রিকোয়েস্ট নেই।\n'
            f'_বই চাইতে লিখো:_ `.request <বইয়ের নাম> by <লেখক>`',
            reply_to=tid, parse_mode='md'
        )
        schedule_delete(sent.id, sent.chat_id, 60)
        schedule_delete(event.id, event.chat_id, 0)
        return

    lines = [f'📋 **{first_name}, তোমার রিকোয়েস্ট স্ট্যাটাস:**\n━━━━━━━━━━━━━━━━━━━━']
    if fulfilled:
        lines.append('✅ **পাওয়া গেছে:**')
        for q, bname, bid in fulfilled:
            lines.append(f'  • `{bname}` — `/get_{bid}` দিয়ে নামাও')
    if still_pending:
        lines.append('⏳ **এখনো পাওয়া যায়নি:**')
        for q in still_pending:
            lines.append(f'  • `{q}`')
    if recent_dls:
        lines.append('📥 **সম্প্রতি ডাউনলোড করেছো:**')
        for bname, n in recent_dls[:3]:
            nm = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', bname or '', flags=re.IGNORECASE).strip()
            lines.append(f'  • `{nm}` ({n}×)')

    sent = await interaction_client.send_message(
        event.chat_id, '\n'.join(lines), reply_to=tid, parse_mode='md'
    )
    schedule_delete(sent.id, sent.chat_id, 120)
    schedule_delete(event.id, event.chat_id, 0)


async def _check_keyword_alerts(book_name: str, book_id: int):
    """Called after a new book is scraped. Fires alerts for matching keywords."""
    if not KEYWORD_ALERTS:
        return
    bname_lower = book_name.lower()
    fired = set()
    for kw, targets in KEYWORD_ALERTS.items():
        if kw in bname_lower and kw not in fired:
            fired.add(kw)
            for (cid, tid) in targets:
                try:
                    client = _analytics_client_ref[0] if _analytics_client_ref else None
                    if not client:
                        continue
                    await client.send_message(
                        cid,
                        f'🔔 **Keyword Alert: `{kw}`**\n'
                        f'📖 New book scraped matching your alert!\n'
                        f'`{book_name}`\n'
                        f'🆔 Book ID: `{book_id}`\n'
                        f'🕐 {datetime.now(timezone.utc).strftime("%H:%M UTC")}',
                        reply_to=tid,
                        parse_mode='md',
                    )
                except Exception as e:
                    log.debug(f'keyword_alert send: {e}')

async def _pick_book_of_the_day() -> tuple:
    """Pick a random well-downloaded book for book of the day. Returns (book_id, book_name, dl_count)."""
    def _q():
        conn = db_connect(); c = conn.cursor()
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        c.execute('SELECT book_id FROM botd_history ORDER BY date DESC LIMIT 30')
        recent_ids = {r[0] for r in c.fetchall()}
        # Pick from top 200 books by downloads, excluding recent BOTD
        c.execute('''SELECT b.id, b.file_name, COUNT(d.id) as n
            FROM books b
            LEFT JOIN download_log d ON d.book_id = b.id
            WHERE b.is_restricted = 0
            GROUP BY b.id ORDER BY n DESC LIMIT 200''')
        rows = [r for r in c.fetchall() if r[0] not in recent_ids]
        conn.close()
        if not rows:
            return None
        import random
        # Weighted random from top 50
        pool = rows[:50]
        weights = [max(1, r[2]) for r in pool]
        total_w = sum(weights)
        import random
        r_val = random.uniform(0, total_w)
        cum = 0
        for row, w in zip(pool, weights):
            cum += w
            if r_val <= cum:
                return row
        return pool[0]
    return await _run_stats_query(_q)

async def _post_book_of_the_day():
    """Post book of the day to all BOTD_CHATS."""
    if not BOTD_CHATS:
        return
    result = await _pick_book_of_the_day()
    if not result:
        return
    book_id, book_name, dl_count = result
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    name_clean = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', book_name or '', flags=re.IGNORECASE).strip()

    def _save_botd():
        conn = db_connect(); c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO botd_history(date,book_id,book_name) VALUES(?,?,?)',
                      (today, book_id, book_name))
            conn.commit()
        except Exception: pass
        conn.close()
    await _run_stats_query(_save_botd)
    _BOTD_LAST_DATE[0] = today

    msg = (
        f'📖 **Book of the Day** 🌟\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'**{name_clean}**\n'
        f'📥 Downloaded `{dl_count}` times\n'
        f'🆔 Book ID: `{book_id}`\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'🔍 Search: `.বই {name_clean[:30]}` to download\n'
        f'🕐 {today}'
    )
    client = _analytics_client_ref[0] if _analytics_client_ref else None
    if not client:
        return
    for (cid, tid) in BOTD_CHATS:
        try:
            await client.send_message(cid, msg, reply_to=tid, parse_mode='md')
        except Exception as e:
            log.debug(f'botd send {cid}: {e}')

async def _broadcast_report(report_type: str):
    """Broadcast daily/weekly report to all BROADCAST_REPORT_CHATS with that type enabled."""
    if not BROADCAST_REPORT_CHATS:
        return
    if report_type == 'daily':
        txt = await _generate_daily_report()
    else:
        txt = await _generate_weekly_report()
    client = _analytics_client_ref[0] if _analytics_client_ref else None
    if not client:
        return
    for (cid, tid), cfg in BROADCAST_REPORT_CHATS.items():
        if not cfg.get(report_type, True):
            continue
        try:
            await client.send_message(cid, txt, reply_to=tid, parse_mode='md')
            await asyncio.sleep(1)
        except Exception as e:
            log.debug(f'broadcast_report {cid}: {e}')

def _parse_report_time(time_str: str) -> tuple[int, int]:
    """Parse 'HH:MM' → (hour, minute). Returns (0,0) on error."""
    try:
        h, m = time_str.strip().split(':')
        return int(h) % 24, int(m) % 60
    except Exception:
        return 0, 0

def _search_with_aliases(query: str) -> list:
    """Run smart_search, then also check book_aliases for matches."""
    results = smart_search(query)
    # additionally check aliases
    try:
        conn = db_connect(); c = conn.cursor()
        q_low = query.lower().strip()
        c.execute('''SELECT ba.book_id, b.file_name, b.file_size, b.is_restricted, b.file_ext, b.search_name
            FROM book_aliases ba JOIN books b ON b.id = ba.book_id
            WHERE LOWER(ba.alias) LIKE ?
            LIMIT 20''', (f'%{q_low}%',))
        alias_rows = c.fetchall()
        conn.close()
        existing_ids = {r[0] for r in results}
        for row in alias_rows:
            if row[0] not in existing_ids:
                results.append(row)
    except Exception as e:
        log.debug(f'alias search: {e}')
    return results

def _related_books(book_id: int, book_name: str, limit: int = 8) -> list:
    """Find books with similar names (same words) or same inferred author."""
    try:
        conn = db_connect(); c = conn.cursor()
        norm = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', book_name or '', flags=re.IGNORECASE).strip().lower()
        words = [w for w in norm.split() if len(w) > 2]
        if not words:
            conn.close(); return []
        conditions = ' OR '.join('search_name LIKE ?' for _ in words[:5])
        params = [f'%{w}%' for w in words[:5]] + [book_id, limit * 3]
        c.execute(f'''SELECT id, file_name, file_size, is_restricted, file_ext, search_name
            FROM books WHERE ({conditions}) AND id != ? LIMIT ?''', params)
        rows = c.fetchall()
        conn.close()
        # score by number of matching words
        def score(row):
            sn = (row[5] or row[1] or '').lower()
            return sum(1 for w in words if w in sn)
        rows = sorted(rows, key=score, reverse=True)
        return rows[:limit]
    except Exception as e:
        log.debug(f'related_books: {e}')
        return []

def _trending_books(limit: int = 10) -> list:
    """Books whose download velocity this week exceeds their historical average."""
    try:
        conn = db_connect(); c = conn.cursor()
        now = int(time.time())
        week_start = now - 7 * 86400
        prev_start = now - 30 * 86400
        # Downloads this week per book
        c.execute('''SELECT book_id, book_name, COUNT(*) as w
            FROM download_log WHERE ts > ?
            GROUP BY book_id''', (week_start,))
        week_map = {r[0]: (r[1], r[2]) for r in c.fetchall()}
        if not week_map:
            conn.close(); return []
        # Downloads in prior 3 weeks per book (historical baseline)
        ids = list(week_map.keys())
        ph = ','.join('?' * len(ids))
        c.execute(f'''SELECT book_id, COUNT(*) as prev
            FROM download_log WHERE ts > ? AND ts <= ? AND book_id IN ({ph})
            GROUP BY book_id''', [prev_start, week_start] + ids)
        prev_map = {r[0]: r[1] for r in c.fetchall()}
        conn.close()
        # Velocity score: this_week / (prev/3 + 0.5) — higher means trending up
        scored = []
        for bid, (bname, week_n) in week_map.items():
            prev_n = prev_map.get(bid, 0)
            weekly_avg = prev_n / 3.0
            velocity = week_n / (weekly_avg + 0.5)
            if velocity >= 1.3:  # at least 30% above average
                scored.append((bid, bname, week_n, weekly_avg, velocity))
        scored.sort(key=lambda r: -r[4])
        return scored[:limit]
    except Exception as e:
        log.debug(f'trending_books: {e}')
        return []

async def _health_check() -> str:
    """Run DB integrity check and return health report string."""
    lines = ['🏥 **System Health Check**\n━━━━━━━━━━━━━━━━━━━━']
    try:
        conn = db_connect(); c = conn.cursor()
        c.execute('PRAGMA integrity_check')
        ic = c.fetchone()[0]
        lines.append(f'🗄 DB integrity: `{ic}`' + (' ✅' if ic == 'ok' else ' ❌'))
        c.execute('PRAGMA wal_checkpoint(PASSIVE)')
        ckpt = c.fetchone()
        lines.append(f'📝 WAL checkpoint: pages=`{ckpt[1]}` moved=`{ckpt[2]}`')
        c.execute('SELECT COUNT(*) FROM cleanup_queue'); pending = c.fetchone()[0]
        lines.append(f'🗑 Pending deletes: `{pending}`' + (' ⚠️ backlog!' if pending > 500 else ' ✅'))
        c.execute('SELECT COUNT(*) FROM books'); total_books = c.fetchone()[0]
        lines.append(f'📚 Books in DB: `{total_books}`')
        conn.close()
    except Exception as e:
        lines.append(f'❌ DB error: `{e}`')
    try:
        db_mb = os.path.getsize(DB_PATH) / 1048576
        wal_path = DB_PATH + '-wal'
        wal_mb = os.path.getsize(wal_path) / 1048576 if os.path.exists(wal_path) else 0
        lines.append(f'📦 DB size: `{db_mb:.1f}MB`' + (f' + WAL `{wal_mb:.1f}MB`' + (' ⚠️ large!' if wal_mb > 50 else '') if wal_mb > 0.1 else '') + ' ✅')
    except Exception as e:
        lines.append(f'❌ Disk check failed: `{e}`')
    uptime = int(time.time() - _BOT_START_TIME)
    lines.append(f'⏱ Uptime: `{_fmt_uptime(uptime)}`')
    lines.append(f'🔄 Downloads in progress: `{len(_active_downloads)}`')
    lines.append(f'🔍 Cache entries: `{len(_SEARCH_CACHE)}`')
    lines.append(f'🕐 {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}')
    return '\n'.join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Extended analytics helpers
# ─────────────────────────────────────────────────────────────────────────────

def _run_stats_query(fn):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_SEARCH_EXECUTOR, fn)

def _fmt_uptime(seconds: int) -> str:
    d = seconds // 86400; h = (seconds % 86400) // 3600; m = (seconds % 3600) // 60
    parts = []
    if d: parts.append(f'{d}d')
    if h: parts.append(f'{h}h')
    if m: parts.append(f'{m}m')
    return ' '.join(parts) if parts else '<1m'

def _bar(n: int, mx: int, width: int = 12) -> str:
    filled = round(width * n / mx) if mx else 0
    return '█' * filled + '░' * (width - filled)

def _pct(a, b) -> str:
    return f'{a/b*100:.1f}%' if b else '—'

def _trend(now_v: int, prev_v: int) -> str:
    if prev_v == 0: return '🆕'
    diff = now_v - prev_v
    if diff > 0:  return f'↑{diff}'
    if diff < 0:  return f'↓{abs(diff)}'
    return '→'

async def _generate_daily_report() -> str:
    def _q():
        conn = db_connect(); c = conn.cursor()
        now = int(time.time()); today = now - 86400; yday = now - 2 * 86400
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ?', (today,));    searches_today = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ? AND ts <= ?', (yday, today)); searches_yday = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts > ?', (today,));  dls_today = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM download_log WHERE ts > ? AND ts <= ?', (yday, today)); dls_yday = c.fetchone()[0]
        c.execute('SELECT COUNT(DISTINCT user_id) FROM search_log WHERE ts > ?', (today,));   uniq_s = c.fetchone()[0]
        c.execute('SELECT COUNT(DISTINCT user_id) FROM download_log WHERE ts > ?', (today,)); uniq_d = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM search_log WHERE ts > ? AND result_count = 0', (today,)); zero_r = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM books'); total_books = c.fetchone()[0]
        c.execute('SELECT book_name, COUNT(*) as n FROM download_log WHERE ts > ? GROUP BY book_id ORDER BY n DESC LIMIT 3', (today,))
        top_books = c.fetchall()
        c.execute('SELECT first_name, username, COUNT(*) as n FROM download_log WHERE ts > ? GROUP BY user_id ORDER BY n DESC LIMIT 3', (today,))
        top_users = c.fetchall()
        conn.close()
        return searches_today, searches_yday, dls_today, dls_yday, uniq_s, uniq_d, zero_r, total_books, top_books, top_users
    (searches_today, searches_yday, dls_today, dls_yday,
     uniq_s, uniq_d, zero_r, total_books, top_books, top_users) = await _run_stats_query(_q)
    dt = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = [
        f'📅 **Daily Report — {dt}**',
        f'━━━━━━━━━━━━━━━━━━━━',
        f'🔍 Searches:  `{searches_today}` {_trend(searches_today, searches_yday)} _(yesterday: {searches_yday})_',
        f'📥 Downloads: `{dls_today}` {_trend(dls_today, dls_yday)} _(yesterday: {dls_yday})_',
        f'👥 Unique searchers: `{uniq_s}` | Downloaders: `{uniq_d}`',
        f'❌ Zero-result queries: `{zero_r}`',
        f'📚 Total books in DB: `{total_books}`',
    ]
    medals = ['🥇', '🥈', '🥉']
    if top_books:
        lines.append('\n🔥 **Top books today:**')
        for i, (bname, n) in enumerate(top_books):
            nm = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2|lit|cbz|cbr|rtf|docx?)$', '', bname or '?', flags=re.IGNORECASE).strip()
            if len(nm) > 40: nm = nm[:37] + '…'
            lines.append(f'  {medals[i]} `{nm}` — {n}×')
    if top_users:
        lines.append('\n👑 **Top downloaders today:**')
        for i, (fname, uname, n) in enumerate(top_users):
            ustr = f' (@{uname})' if uname else ''
            lines.append(f'  {medals[i]} {fname or uname or "?"}{ustr} — {n} books')
    lines.append(f'\n🕐 {datetime.now(timezone.utc).strftime("%H:%M UTC")}')
    return '\n'.join(lines)

async def _generate_weekly_report() -> str:
    def _q():
        conn = db_connect(); c = conn.cursor()
        now = int(time.time())
        w1s, w1e = now - 7*86400, now
        w2s, w2e = now - 14*86400, now - 7*86400
        def wk(ws, we):
            c.execute('SELECT COUNT(*) FROM search_log WHERE ts>? AND ts<=?', (ws, we));   sr = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM download_log WHERE ts>? AND ts<=?', (ws, we)); dl = c.fetchone()[0]
            c.execute('SELECT COUNT(DISTINCT user_id) FROM search_log WHERE ts>? AND ts<=?', (ws, we));   uqs = c.fetchone()[0]
            c.execute('SELECT COUNT(DISTINCT user_id) FROM download_log WHERE ts>? AND ts<=?', (ws, we)); uqd = c.fetchone()[0]
            c.execute('SELECT book_name, COUNT(*) as n FROM download_log WHERE ts>? AND ts<=? GROUP BY book_id ORDER BY n DESC LIMIT 5', (ws, we))
            books = c.fetchall()
            return sr, dl, uqs, uqd, books
        w1 = wk(w1s, w1e); w2 = wk(w2s, w2e)
        c.execute('SELECT COUNT(*) FROM search_log WHERE result_count=0 AND ts>?', (w1s,)); zero_r = c.fetchone()[0]
        conn.close()
        return w1, w2, zero_r
    (sr1,dl1,uqs1,uqd1,books1), (sr2,dl2,uqs2,uqd2,_), zero_r = await _run_stats_query(_q)
    lines = [
        f'📆 **Weekly Report**',
        f'━━━━━━━━━━━━━━━━━━━━',
        f'🔍 Searches:  `{sr1}` {_trend(sr1,sr2)} _(prev week: {sr2})_',
        f'📥 Downloads: `{dl1}` {_trend(dl1,dl2)} _(prev week: {dl2})_',
        f'👥 Unique searchers: `{uqs1}` {_trend(uqs1,uqs2)} | Downloaders: `{uqd1}` {_trend(uqd1,uqd2)}',
        f'❌ Zero-result searches: `{zero_r}`',
    ]
    if books1:
        lines.append('\n🏆 **Top 5 books this week:**')
        medals = ['🥇','🥈','🥉','4️⃣','5️⃣']
        for i, (bname, n) in enumerate(books1):
            nm = re.sub(r'\.(pdf|epub)$','',bname or '?',flags=re.IGNORECASE).strip()
            if len(nm) > 45: nm = nm[:42] + '…'
            lines.append(f'  {medals[i]} `{nm}` — {n}×')
    lines.append(f'\n🕐 {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}')
    return '\n'.join(lines)

_LAST_VACUUM_DATE: list[str] = ['']  # YYYY-Www

async def _db_maintenance_loop():
    """Weekly DB vacuum + optimize in executor — keeps DB lean and fast."""
    await asyncio.sleep(120)  # let startup settle
    log.info('DB maintenance loop started')
    while True:
        await asyncio.sleep(6 * 3600)  # check every 6 hours
        week_key = datetime.now(timezone.utc).strftime('%Y-W%W')
        if week_key == _LAST_VACUUM_DATE[0]:
            continue
        _LAST_VACUUM_DATE[0] = week_key
        def _do_maintenance():
            try:
                conn = db_connect(); c = conn.cursor()
                c.execute('PRAGMA optimize')
                c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                conn.close()
                # vacuum needs its own connection (can't run inside transaction)
                conn2 = db_connect()
                conn2.execute('VACUUM')
                conn2.close()
                db_mb = os.path.getsize(DB_PATH) / 1048576
                return f'✅ DB maintenance done. Size: {db_mb:.1f}MB'
            except Exception as e:
                return f'❌ DB maintenance error: {e}'
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_SEARCH_EXECUTOR, _do_maintenance)
        log.info(result)
        try:
            await report(f'🗄 **Weekly DB Maintenance**\n{result}\n🕐 {ts_str()}')
        except Exception:
            pass

async def _auto_report_loop():
    await asyncio.sleep(30)
    log.info('Auto-report loop started (report_time=%s UTC)', REPORT_TIME_UTC[0])
    while True:
        try:
            rh, rm = _parse_report_time(REPORT_TIME_UTC[0])
            now = datetime.now(timezone.utc)
            target = now.replace(hour=rh, minute=rm, second=0, microsecond=0)
            if target <= now:
                target = target.replace(day=target.day + 1) if target.day < 28 else target + timedelta(days=1)
            secs = (target - now).total_seconds()
            await asyncio.sleep(secs)
        except Exception:
            await asyncio.sleep(3600)
            continue
        try:
            # Daily report → analytics group
            txt = await _generate_daily_report()
            await report(txt)
            # Broadcast daily report to assigned chats
            await _broadcast_report('daily')
            # Book of the day
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            if BOTD_CHATS and _BOTD_LAST_DATE[0] != today_str:
                await asyncio.sleep(3)
                await _post_book_of_the_day()
            # Weekly report on Monday
            if datetime.now(timezone.utc).weekday() == 0:
                await asyncio.sleep(5)
                txt = await _generate_weekly_report()
                await report(txt)
                await _broadcast_report('weekly')
        except Exception as e:
            log.warning(f'auto_report_loop: {e}')

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
async def start_clients():
    setup_db()
    warmup_db()
    warmup_search_cache()
    _parse_env_chats('ASSIGNED_CHATS', ASSIGNED_CHATS)
    _parse_env_chats('TRIGGER_CHATS', TRIGGER_CHATS)
    _apply_settings(_load_settings())

    cfg = {'connection_retries': None, 'retry_delay': 5, 'auto_reconnect': True}
    user_client = TelegramClient('session_user', API_ID, API_HASH, **cfg)
    bot_client: TelegramClient | None = None
    if BOT_TOKEN:
        bot_client = TelegramClient('session_bot', API_ID, API_HASH, **cfg)

    await user_client.start()
    me = await user_client.get_me()
    OWN_IDS.add(me.id)
    log.info(f'User: @{me.username} ({me.id})')

    global BOT_USERNAME
    if bot_client:
        await bot_client.start(bot_token=BOT_TOKEN)
        bme = await bot_client.get_me()
        BOT_USERNAME = bme.username
        _BOT_USERNAME[0] = bme.username or ''
        OWN_IDS.add(bme.id)
        interaction_client = bot_client
        log.info(f'Bot: @{BOT_USERNAME}')
    else:
        interaction_client = user_client
        BOT_USERNAME = me.username
        _BOT_USERNAME[0] = me.username or ''

    _analytics_client_ref.append(interaction_client)
    global _DL_SEMAPHORE, _scrap_lock, _shadow_lock, _SEARCH_EXECUTOR, _SEARCH_CACHE_LOCK
    _DL_SEMAPHORE       = asyncio.Semaphore(_DL_MAX)
    _scrap_lock         = asyncio.Lock()
    _shadow_lock        = asyncio.Lock()
    _SEARCH_CACHE_LOCK  = asyncio.Lock()
    _SEARCH_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix='search')

    conn = db_connect(); c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM books')
    if c.fetchone()[0] == 0:
        conn.close()
        if not SOURCE_GROUPS:
            log.warning('DB empty but SOURCE_GROUPS is also empty — check that settings.json exists at: %s', SETTINGS_FILE)
        else:
            log.info('DB empty — running initial scrape…')
            async def _noop(m): log.info(f'[init-scrape] {m}')
            await _run_scrap_job(user_client, _noop, list(SOURCE_GROUPS),
                                 job_label='init', started_by=OWNER_ID)
    else:
        conn.close()

    await report(f'🚀 **Bot started** v12\nUser: @{me.username}\nMode: `{SEARCH_MODE[0]}`\n{ts_str()}')
    log.info('🚀 Ready.')

    @user_client.on(events.NewMessage())
    async def book_listener(event):
        if not event.file or not event.chat: return
        if event.sender_id in OWN_IDS: return

        raw_chat_id = event.chat_id
        norm = _normalize_id_for_compare(raw_chat_id)
        chat_id = norm if isinstance(norm, int) else raw_chat_id

        conn = db_connect(); c = conn.cursor()
        c.execute('SELECT 1 FROM scrape_progress WHERE chat_id=?', (chat_id,))
        known = c.fetchone(); conn.close()

        if not known:
            uname = (getattr(event.chat, 'username', '') or '').lower().lstrip('@')
            is_source = False
            for s in SOURCE_GROUPS:
                s_norm = _normalize_id_for_compare(s)
                if isinstance(s_norm, int):
                    if s_norm == chat_id:
                        is_source = True; break
                else:
                    if s_norm == uname and uname:
                        is_source = True; break
            if not is_source:
                return
            conn = db_connect()
            conn.execute(
                'INSERT OR IGNORE INTO scrape_progress(chat_id,last_msg_id,last_scraped_at) VALUES(?,0,0)',
                (chat_id,)
            )
            conn.commit(); conn.close()

        file_info = _extract_file_info(event)
        if not file_info:
            return

        fname, ext, fsize = file_info
        is_restr = 1 if getattr(event.chat, 'noforwards', False) else 0

        conn = db_connect(); c = conn.cursor()
        try:
            c.execute(
                'SELECT id, is_restricted FROM books WHERE file_name=? AND file_size=?',
                (fname, fsize)
            )
            existing = c.fetchone()
            if not existing:
                sname = normalize_name(fname)
                c.execute(
                    'INSERT INTO books(file_name,search_name,stripped_name,file_size,'
                    'message_id,chat_id,orig_chat_id,file_ext,is_restricted) VALUES(?,?,?,?,?,?,?,?,?)',
                    (fname, sname, _RE_STRIP_VOWELS.sub('', sname),
                     fsize, event.id, chat_id, chat_id, ext, is_restr)
                )
                conn.commit()
            elif existing[1] == 1 and is_restr == 0:
                c.execute(
                    'UPDATE books SET message_id=?, chat_id=?, is_restricted=0 WHERE id=?',
                    (event.id, chat_id, existing[0])
                )
                conn.commit()
        except Exception as db_err:
            log.warning(f'book_listener DB error for {fname}: {db_err}')
        finally:
            conn.close()

    @user_client.on(events.NewMessage(chats=REQUEST_GROUP[0] if REQUEST_GROUP[0] else None))
    async def request_fulfillment_listener(event):
        if not REQUEST_GROUP[0] or not event.file or not event.reply_to: return
        replied_to_id = getattr(event.reply_to, 'reply_to_msg_id', None)
        if not replied_to_id or replied_to_id not in pending_requests: return
        req = pending_requests[replied_to_id]
        file_info = _extract_file_info(event)
        if not file_info:
            return

        fname, ext, fsize = file_info
        if fname.startswith('unknown_'):
            req_title = req.get('book_title', '')
            if req_title:
                fname = f"{req_title}{ext}"

        requester_id   = req['requester_id']
        access_hash    = req['access_hash']
        book_title     = req['book_title']
        requester_name = req['first_name']
        requester_uname= req.get('username')
        origin_chat    = req['origin_chat_id']
        origin_thread  = req['origin_thread']
        is_dm          = req['is_private']

        try:
            is_restr = 1 if getattr(event.chat, 'noforwards', False) else 0
            conn = db_connect(); c = conn.cursor()
            _fulfill_book_id = None
            try:
                c.execute('SELECT id FROM books WHERE file_name=? AND file_size=?', (fname, fsize))
                existing_row = c.fetchone()
                if not existing_row:
                    sname2 = normalize_name(fname)
                    c.execute(
                        'INSERT INTO books(file_name,search_name,stripped_name,file_size,'
                        'message_id,chat_id,orig_chat_id,file_ext,is_restricted) VALUES(?,?,?,?,?,?,?,?,?)',
                        (fname, sname2, _RE_STRIP_VOWELS.sub('', sname2),
                         fsize, event.id, REQUEST_GROUP[0], REQUEST_GROUP[0], ext, is_restr)
                    )
                    conn.commit()
                    _fulfill_book_id = c.lastrowid
                else:
                    _fulfill_book_id = existing_row[0]
            except Exception as db_err:
                log.warning(f'request_fulfillment DB error for {fname}: {db_err}')
            finally:
                conn.close()

            # Use DM template for DMs, group template for group fulfillments
            _fulfill_src_chat = REQUEST_GROUP[0]
            if is_dm:
                tmpl_name = DM_TEMPLATE_REF[0]
            else:
                tmpl_name = get_group_template(origin_chat, origin_thread)
            sep = '─' * 30
            safe_mention = _safe_mention(requester_id, requester_name, requester_uname)

            if is_dm:
                target_entity = requester_id
                if access_hash:
                    try:
                        target_entity = await user_client.get_entity(
                            types.InputPeerUser(user_id=requester_id, access_hash=access_hash)
                        )
                    except Exception: pass
                cap = render_caption(
                    template_name=tmpl_name,
                    fname=fname,
                    user_id=requester_id,
                    first_name=requester_name,
                    username=requester_uname,
                    purge_secs=DM_PURGE_SECS_REF[0] or 600,
                    src_chat_id=_fulfill_src_chat,
                    book_id=_fulfill_book_id,
                )
                sent = await user_client.send_file(target_entity, event.media, caption=cap, parse_mode='md')
                schedule_delete(sent.id, requester_id, DM_PURGE_SECS_REF[0] or 600, use_user_client=True)
                dest_str = 'DM'
            else:
                purge = (get_assignment(origin_chat, origin_thread, TRIGGER_CHATS)
                         or get_assignment(origin_chat, origin_thread, ASSIGNED_CHATS) or 3600)
                cap = render_caption(
                    template_name=tmpl_name,
                    fname=fname,
                    user_id=requester_id,
                    first_name=requester_name,
                    username=requester_uname,
                    purge_secs=purge,
                    src_chat_id=_fulfill_src_chat,
                    book_id=_fulfill_book_id,
                )
                sent = None
                for client in [interaction_client, user_client]:
                    try:
                        sent = await client.send_file(origin_chat, event.media, caption=cap,
                                                       reply_to=origin_thread, parse_mode='md')
                        schedule_delete(sent.id, origin_chat, purge); break
                    except Exception as ex:
                        log.warning(f'group fulfil: {ex}')
                dest_str = f'chat `{origin_chat}`' + (f' thread `{origin_thread}`' if origin_thread else '')

            uref = user_mention_md(requester_id, requester_name)
            del pending_requests[replied_to_id]
            await user_client.send_message(REQUEST_GROUP[0], f'✅ Fulfilled → {uref} ({dest_str})',
                                            reply_to=replied_to_id, parse_mode='md')
            asyncio.create_task(report(
                f'✅ **Request Fulfilled**\n📖 `{book_title}`\n👤 {uref}\n📍 {dest_str}\n🕐 {ts_str()}'
            ))
        except Exception as e:
            log.warning(f'request fulfillment: {e}')
            await report_error('request_fulfillment_listener', e)

    @interaction_client.on(events.NewMessage())
    async def message_listener(event):
        if not event.text: return
        sender_id = get_sender_id(event)
        if not sender_id or sender_id in OWN_IDS: return
        if _is_channel_post(event): return

        text = event.text.strip()
        # /boi /find /search /kitab /বই must go to search, not admin
        _SLASH_SEARCH_RE = re.compile(r'^/(boi|find|search|kitab|বই)(\s|$)', re.IGNORECASE)
        if text.startswith('/') and not _SLASH_SEARCH_RE.match(text):
            await handle_admin(event, user_client, interaction_client); return
        if re.match(r'^[/\.]suggest(\s|$)', text, re.IGNORECASE): return
        # .feedback / /feedback — available to all users in groups and DMs
        if re.match(r'^[/\.](feedback)(\s|$)', text, re.IGNORECASE):
            await handle_admin(event, user_client, interaction_client); return
        # .mycollections / .col / /collections — user collection UI
        if re.match(r'^[/\.]((my)?collections?|col(\s|$)|col_new)', text, re.IGNORECASE):
            await handle_admin(event, user_client, interaction_client); return
        # #col share codes — anyone can paste them
        if re.match(r'^#col\d+_\d+', text):
            await handle_admin(event, user_client, interaction_client); return
        if re.match(r'^\.request(\s|$)', text, re.IGNORECASE):
            allowed, mode = check_access(event)
            if allowed or event.is_private:
                await handle_request(event, interaction_client, user_client)
            return
        # .mystatus — user checks their own request status
        if re.match(r'^\.(mystatus|request_status)(\s|$)', text, re.IGNORECASE):
            await _handle_request_status(event, interaction_client)
            return
        # ── Reply-resend: user replies to a bot-delivered book with trigger phrase
        _RESEND_TRIGGERS = re.compile(
            r'^(আবার\s*দাও|send\s*again|resend|আবার|again)$',
            re.IGNORECASE
        )
        if _RESEND_TRIGGERS.match(text.strip()) and event.reply_to:
            try:
                replied_msg = await interaction_client.get_messages(
                    event.chat_id, ids=event.reply_to.reply_to_msg_id
                )
                if replied_msg and replied_msg.file:
                    # Only resend if that message was sent by the bot itself
                    replied_sender = getattr(replied_msg, 'sender_id', None)
                    if replied_sender in OWN_IDS:
                        # Extract book_id from message text if present
                        _bid_match = re.search(r'🆔.*?`(\d+)`', replied_msg.text or '')
                        if not _bid_match:
                            # Try to find by filename match
                            _fname_hint = getattr(replied_msg.file, 'name', None)
                            if _fname_hint:
                                _conn_rs = db_connect(); _c_rs = _conn_rs.cursor()
                                _c_rs.execute('SELECT id FROM books WHERE file_name=? LIMIT 1', (_fname_hint,))
                                _br = _c_rs.fetchone(); _conn_rs.close()
                                _bid_resend = _br[0] if _br else None
                            else:
                                _bid_resend = None
                        else:
                            _bid_resend = int(_bid_match.group(1))
                        if _bid_resend:
                            tid_rs = get_event_thread_id(event)
                            tmpl_rs = get_group_template(event.chat_id, tid_rs)
                            purge_rs = get_assignment(event.chat_id, tid_rs, ASSIGNED_CHATS) or \
                                       get_assignment(event.chat_id, tid_rs, TRIGGER_CHATS) or 600
                            sender_rs = event.sender
                            fn_rs = getattr(sender_rs, 'first_name', 'User') or 'User'
                            ln_rs = getattr(sender_rs, 'last_name', None)
                            un_rs = getattr(sender_rs, 'username', None)
                            # Fetch src_chat_id for book_source placeholder
                            _conn_src = db_connect(); _c_src = _conn_src.cursor()
                            _c_src.execute('SELECT chat_id FROM books WHERE id=?', (_bid_resend,))
                            _src_row = _c_src.fetchone(); _conn_src.close()
                            _src_cid_rs = _src_row[0] if _src_row else None
                            cap_rs = build_caption(
                                fname=getattr(replied_msg.file, 'name', 'book'),
                                user_id=sender_id, first_name=fn_rs, last_name=ln_rs,
                                username=un_rs, template_name=tmpl_rs, purge_secs=purge_rs,
                                src_chat_id=_src_cid_rs,
                                book_id=_bid_resend,
                            )
                            asyncio.create_task(deliver_book(
                                event.chat_id, _bid_resend, user_client, interaction_client,
                                cap_rs, event.id,
                                requester_id=sender_id, requester_name=fn_rs,
                                requester_username=un_rs or fn_rs,
                                request_chat_id=event.chat_id, thread_id=tid_rs,
                                group_purge_secs=purge_rs
                            ))
                            schedule_delete(event.id, event.chat_id, 5)
                            return
            except Exception as _e:
                log.debug(f'resend handler: {_e}')

        _SEARCH_ALIAS_RE = re.compile(r'^([/\\.](boi|find|search|kitab)|[/\\.]বই|।বই)(\s|$)', re.IGNORECASE)
        allowed, mode = check_access(event)
        if not allowed: return
        if mode == 'trigger':
            if not _SEARCH_ALIAS_RE.match(text): return
        else:
            if _SEARCH_ALIAS_RE.match(text):
                mode = 'trigger'
            else:
                lower = text.lower()
                if '.pdf' in lower or '.epub' in lower: return
        await handle_search(event, interaction_client, user_client, mode)

    if bot_client:
        @user_client.on(events.NewMessage())
        async def user_admin_listener(event):
            if not event.text or not event.text.strip().startswith('/'): return
            sid = get_sender_id(event)
            if sid and is_staff(sid):
                await handle_admin(event, user_client, interaction_client)

    @interaction_client.on(events.NewMessage())
    async def file_delete_listener(event):
        # ── IMPORTANT ────────────────────────────────────────────────────────
        # This listener must NEVER delete files uploaded by regular users.
        # It only deletes files sent BY THE BOT ITSELF (OWN_IDS) as search
        # results — and only after the configured purge delay.
        # Deleting user-uploaded files (e.g. branded PDFs like @boi_mohol)
        # caused by this listener firing on non-bot senders has been removed.
        # ─────────────────────────────────────────────────────────────────────
        if not event.file or event.is_private: return
        sender_id = get_sender_id(event)

        # Only act on files sent by the bot itself
        if sender_id not in OWN_IDS:
            return

        if _is_channel_post(event): return

        # Don't touch forwarded channel posts
        msg = event.message
        fwd = getattr(msg, 'fwd_from', None)
        if fwd is not None:
            if getattr(fwd, 'channel_post', None):
                return
            from telethon.tl.types import PeerChannel as _PC
            if isinstance(getattr(fwd, 'from_id', None), _PC):
                return

        tid = get_event_thread_id(event)
        purge_secs = get_assignment(event.chat_id, tid, ASSIGNED_CHATS)
        if purge_secs is None:
            purge_secs = get_assignment(event.chat_id, tid, TRIGGER_CHATS)
        if purge_secs is not None:
            # Use the configured purge delay — never 0 (instant wipe)
            schedule_delete(event.id, event.chat_id, max(purge_secs, 60))

    if bot_client:
        @bot_client.on(events.CallbackQuery(data=re.compile(rb'disable_confirm_(-?\d+)')))
        async def disable_confirm_cb(event):
            admin_id = event.sender_id
            if not is_staff(admin_id):
                await event.answer('❌ Not authorised.', alert=True); return
            chat_id = int(event.data_match.group(1))
            pending = _DISABLE_PENDING.pop(admin_id, None)
            if not pending or pending['chat_id'] != chat_id:
                await event.answer('❌ Session expired. Run /disable again.', alert=True)
                return
            if time.time() - pending['ts'] > 120:
                await event.answer('❌ Confirmation expired (2 min). Run /disable again.', alert=True)
                return
            await event.answer('🗑 Disabling source…')
            try: await event.delete()
            except Exception: pass
            await _do_disable_source(
                event, admin_id, chat_id,
                pending['label'], pending['raw_ref'],
                pending['books'], pending['keep'],
                user_client, interaction_client
            )

        @bot_client.on(events.CallbackQuery(data=b'disable_cancel'))
        async def disable_cancel_cb(event):
            _DISABLE_PENDING.pop(event.sender_id, None)
            await event.answer('Cancelled.')
            try: await event.delete()
            except Exception: pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'get_(\d+)')))
        async def get_callback(event):
            try:
                sid     = event.sender_id
                book_id = int(event.data_match.group(1))
                allowed_dl, dl_reason = spam_check_download(sid, book_id)
                if not allowed_dl:
                    if dl_reason.startswith('book_cooldown:'):
                        remaining = int(dl_reason.split(':')[1])
                        await event.answer(
                            f'⏳ Wait {remaining//60}m {remaining%60}s before requesting this book again.',
                            alert=True
                        )
                    elif dl_reason.startswith('daily_limit:'):
                        lim = dl_reason.split(':')[1]
                        await event.answer(
                            f'🚫 Daily download limit reached ({lim} books/day). Try again tomorrow.',
                            alert=True
                        )
                    else:
                        await event.answer('⏳ Please wait before downloading again.', alert=True)
                    return

                thread_id_cb = None; reply_to = None
                try:
                    mo = await event.get_message()
                    if mo and mo.reply_to:
                        rt = mo.reply_to
                        top = getattr(rt, 'reply_to_top_id', None)
                        mid = getattr(rt, 'reply_to_msg_id', None)
                        is_topic = getattr(rt, 'forum_topic', False)
                        if top: thread_id_cb = top; reply_to = top
                        elif mid and is_topic: thread_id_cb = mid; reply_to = mid
                        else: reply_to = mid
                    for (cid, tid), _ in ASSIGNED_CHATS.items():
                        if cid == event.chat_id and tid is not None:
                            if thread_id_cb is None or thread_id_cb == tid:
                                thread_id_cb = tid; reply_to = tid; break
                except Exception: pass

                sender = await event.get_sender()
                ah = getattr(sender, 'access_hash', None)
                fn = getattr(sender, 'first_name', 'User') or 'User'
                un = getattr(sender, 'username', None)

                conn = db_connect(); c = conn.cursor()
                c.execute('SELECT file_name, chat_id FROM books WHERE id=?', (book_id,))
                r = c.fetchone(); conn.close()
                fname       = r[0] if r else 'Book'
                src_chat_id = r[1] if r else None

                # ── Determine template for this group/DM ──────────────────────
                is_dm_cb = isinstance(event.chat_id, int) and event.chat_id > 0 and getattr(event, 'is_private', False)
                if is_dm_cb:
                    tmpl_name  = DM_TEMPLATE_REF[0]
                    purge_secs = DM_PURGE_SECS_REF[0] or 600
                else:
                    tmpl_name  = get_group_template(event.chat_id, thread_id_cb)
                    purge_secs = (
                        get_assignment(event.chat_id, thread_id_cb, ASSIGNED_CHATS)
                        or get_assignment(event.chat_id, thread_id_cb, TRIGGER_CHATS)
                        or 600
                    )

                # ── Fetch query_hash from the message data ────────────────────
                # We need it to look up mentioned_user in the registry.
                # The book buttons don't carry query_hash directly, so we
                # match by chat_id+msg_id against _SEARCH_MSG_REGISTRY.
                _msg_id_cb  = event.message_id
                _qhash_cb   = None
                _mentioned  = None
                for _qh, _reg in list(_SEARCH_MSG_REGISTRY.items()):
                    # Registry entry is (msg_id, chat_id, mentioned_user)
                    # Support old entries that are just (msg_id, chat_id)
                    if isinstance(_reg, tuple) and len(_reg) >= 2:
                        if _reg[1] == event.chat_id and _reg[0] == _msg_id_cb:
                            _qhash_cb = _qh
                            _mentioned = _reg[2] if len(_reg) > 2 else None
                            break

                cap = build_caption(
                    fname=fname,
                    user_id=sid,
                    first_name=fn,
                    username=un,
                    template_name=tmpl_name,
                    purge_secs=purge_secs,
                    src_chat_id=src_chat_id,
                    book_id=book_id,
                )

                # ── Inject mentioned_user into caption (first download only) ──
                # Uses InputMessageEntityMentionName which works for ALL users,
                # including those with no @username — guaranteed real notification.
                _mention_entities: list = []
                if _mentioned and not is_dm_cb:
                    _muid  = _mentioned['user_id']
                    _mfn   = _mentioned.get('first_name') or 'User'
                    _mln   = _mentioned.get('last_name')  or ''
                    _full  = f'{_mfn} {_mln}'.strip()
                    _prefix = f'👤 {_full}\n'
                    cap     = _prefix + cap
                    try:
                        from telethon.tl.types import InputMessageEntityMentionName, MessageEntityBold
                        _peer = await interaction_client.get_input_entity(_muid)
                        _off  = len('👤 ')
                        _mention_entities = [
                            InputMessageEntityMentionName(
                                offset=_off, length=len(_full), user_id=_peer
                            ),
                            MessageEntityBold(offset=_off, length=len(_full)),
                        ]
                    except Exception:
                        pass  # plain text fallback — name still visible
                    # Clear so only fires once
                    if _qhash_cb and _qhash_cb in _SEARCH_MSG_REGISTRY:
                        _old = _SEARCH_MSG_REGISTRY[_qhash_cb]
                        _SEARCH_MSG_REGISTRY[_qhash_cb] = (_old[0], _old[1], None)

                await event.answer('🚀 Sending your book…')
                asyncio.create_task(deliver_book(
                    event.chat_id, book_id, user_client, interaction_client,
                    cap, reply_to, access_hash=ah,
                    requester_id=sid, requester_name=fn, requester_username=un or fn,
                    request_chat_id=event.chat_id, thread_id=thread_id_cb,
                    group_purge_secs=purge_secs,
                    caption_entities=_mention_entities if _mention_entities else None,
                ))
            except Exception as e:
                log.warning(f'get_callback: {e}')
                await report_error('get_callback', e)
                try: await event.answer('❌ Error occurred', alert=True)
                except Exception: pass

        # ════════════════════════════════════════════════════════════════════════
        # COLLECTION CALLBACKS
        # ════════════════════════════════════════════════════════════════════════

        @bot_client.on(events.CallbackQuery(data=b'col_list'))
        async def col_list_cb(event):
            uid = event.sender_id
            text, btns = _col_list_text(uid)
            try: await event.edit(text, buttons=btns, parse_mode='md')
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=b'col_close'))
        async def col_close_cb(event):
            try: await event.delete()
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=b'col_new'))
        async def col_new_cb(event):
            uid = event.sender_id
            if len(col_user_list(uid)) >= MAX_COLLECTIONS_PER_USER:
                await event.answer(f'❌ Max {MAX_COLLECTIONS_PER_USER} collections reached.', alert=True)
                return
            await event.answer()
            await interaction_client.send_message(
                event.sender_id,
                '📚 **Create a Collection**\n\n'
                'Reply with the name of your new collection.\n'
                'You can start with an emoji: `🌟 Must Read`\n\n'
                '_Type `/cancel` to cancel._',
                parse_mode='md'
            )
            # Store pending state
            _COL_PENDING_CREATE[uid] = True

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_open_(\d+)')))
        async def col_open_cb(event):
            cid = int(event.data_match.group(1))
            uid = event.sender_id
            col = col_get(cid)
            if not col:
                await event.answer('❌ Not found.', alert=True); return
            if col['user_id'] != uid and not col['is_public']:
                await event.answer('🔒 Private collection.', alert=True); return
            items = col_items(cid)
            text, btns = _col_detail_text(col, items)
            try: await event.edit(text, buttons=btns, parse_mode='md')
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_toggle_(\d+)')))
        async def col_toggle_cb(event):
            cid = int(event.data_match.group(1))
            uid = event.sender_id
            result = col_toggle_public(cid, uid)
            if result is None:
                await event.answer('❌ Not found.', alert=True); return
            status = '🌐 Public' if result else '🔒 Private'
            await event.answer(f'Changed to {status}')
            col = col_get(cid); items = col_items(cid)
            if col:
                text, btns = _col_detail_text(col, items)
                try: await event.edit(text, buttons=btns, parse_mode='md')
                except Exception: pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_del_(\d+)')))
        async def col_del_cb(event):
            cid = int(event.data_match.group(1))
            uid = event.sender_id
            col = col_get(cid)
            if not col or col['user_id'] != uid:
                await event.answer('❌ Not found.', alert=True); return
            btns = [[
                Button.inline('✅ Yes, delete', f'col_del_confirm_{cid}'.encode()),
                Button.inline('❌ Cancel', f'col_open_{cid}'.encode()),
            ]]
            try: await event.edit(
                f'🗑 **Delete "{col["name"]}"?**\n'
                f'This will remove all {len(col_items(cid))} books from the collection.\n'
                f'_This cannot be undone._',
                buttons=btns, parse_mode='md'
            )
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_del_confirm_(\d+)')))
        async def col_del_confirm_cb(event):
            cid = int(event.data_match.group(1))
            uid = event.sender_id
            col = col_get(cid)
            if col and col['user_id'] == uid:
                name = col['name']
                col_delete(cid, uid)
                text, btns = _col_list_text(uid)
                try: await event.edit(f'🗑 **Deleted "{name}".**\n\n' + text, buttons=btns, parse_mode='md')
                except Exception: await event.answer('Deleted.')
            else:
                await event.answer('❌ Not found.', alert=True)

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_rename_(\d+)')))
        async def col_rename_cb(event):
            cid = int(event.data_match.group(1))
            uid = event.sender_id
            col = col_get(cid)
            if not col or col['user_id'] != uid:
                await event.answer('❌ Not found.', alert=True); return
            await event.answer()
            await interaction_client.send_message(
                uid,
                f'✏️ **Rename "{col["name"]}"**\n\nReply with the new name.\n_Type `/cancel` to cancel._',
                parse_mode='md'
            )
            _COL_PENDING_RENAME[uid] = cid

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_rm_sel_(\d+)')))
        async def col_rm_sel_cb(event):
            cid = int(event.data_match.group(1))
            uid = event.sender_id
            col = col_get(cid)
            if not col or col['user_id'] != uid:
                await event.answer('❌ Not found.', alert=True); return
            items = col_items(cid)
            if not items:
                await event.answer('No books to remove.', alert=True); return
            btns = []
            for it in items[:10]:
                bname = re.sub(r'\.(pdf|epub|mobi|azw3?|djvu)$', '', it['book_name'], flags=re.IGNORECASE)
                btns.append([Button.inline(
                    f'🗑 {bname[:35]}',
                    f'col_rm_{cid}_{it["book_id"]}'.encode()
                )])
            btns.append([Button.inline('◀️ Back', f'col_open_{cid}'.encode())])
            try: await event.edit(
                f'🗑 **Remove book from "{col["name"]}"**\nTap a book to remove it:',
                buttons=btns, parse_mode='md'
            )
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_rm_(\d+)_(\d+)')))
        async def col_rm_cb(event):
            cid     = int(event.data_match.group(1))
            book_id = int(event.data_match.group(2))
            uid     = event.sender_id
            col     = col_get(cid)
            if not col or col['user_id'] != uid:
                await event.answer('❌ Not found.', alert=True); return
            col_remove_book(cid, book_id)
            await event.answer('✅ Removed.')
            items = col_items(cid); col = col_get(cid)
            text, btns = _col_detail_text(col, items)
            try: await event.edit(text, buttons=btns, parse_mode='md')
            except Exception: pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_add_(\d+)_(\d+)')))
        async def col_add_cb(event):
            """Add a book to a specific collection."""
            col_id  = int(event.data_match.group(1))
            book_id = int(event.data_match.group(2))
            uid     = event.sender_id
            col     = col_get(col_id)
            if not col or col['user_id'] != uid:
                await event.answer('❌ Not your collection.', alert=True); return
            if len(col_items(col_id)) >= MAX_ITEMS_PER_COLLECTION:
                await event.answer(f'❌ Max {MAX_ITEMS_PER_COLLECTION} books per collection.', alert=True); return
            conn = db_connect(); c = conn.cursor()
            c.execute('SELECT file_name FROM books WHERE id=?', (book_id,))
            r = c.fetchone(); conn.close()
            bname = r[0] if r else f'Book #{book_id}'
            added = col_add_book(col_id, book_id, bname)
            code  = col_share_code(uid, col_id)
            if added:
                await event.answer(f'✅ Saved to "{col["name"]}"!')
            else:
                await event.answer(f'Already in "{col["name"]}".')
            # Delete the picker message so the chat stays clean
            try: await event.delete()
            except Exception: pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_dlall_(\d+)')))
        async def col_dlall_cb(event):
            """Show all books in a collection with download buttons."""
            cid   = int(event.data_match.group(1))
            uid   = event.sender_id
            col   = col_get(cid)
            if not col or (col['user_id'] != uid and not col['is_public']):
                await event.answer('❌ Not found.', alert=True); return
            items = col_items(cid)
            btns  = []
            for it in items:
                bname = re.sub(r'\.(pdf|epub|mobi|azw3?|djvu)$', '', it['book_name'],
                               flags=re.IGNORECASE).strip()[:32]
                btns.append([Button.inline(f'📥 {bname}', f'get_{it["book_id"]}_0'.encode())])
            btns.append([Button.inline('◀ Back', f'col_open_{cid}'.encode())])
            await event.answer()
            try: await event.edit(
                f'📚 **All books in "{col["name"]}"** ({len(items)} total):',
                buttons=btns, parse_mode='md'
            )
            except Exception: pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_picker_(\d+)')))
        async def col_picker_cb(event):
            """
            ➕ tapped on a book.

            In GROUPS: redirect to bot DM — the picker appears there, not in the group.
              This avoids polluting the group chat and keeps things private.
            In DM: show picker inline immediately.
            """
            book_id  = int(event.data_match.group(1))
            uid      = event.sender_id
            bot_un   = _BOT_USERNAME[0]
            is_group = not getattr(event, 'is_private', True)

            if is_group:
                # Always redirect to DM for groups — cleaner UX, no group noise
                await event.answer()
                if bot_un:
                    start_param = f'savebook_{book_id}'
                    link = f'https://t.me/{bot_un}?start={start_param}'
                    try:
                        # Send user a DM with the picker directly
                        cols = col_user_list(uid)
                        if not cols:
                            await interaction_client.send_message(
                                uid,
                                '📚 **Save to collection**\n\n'
                                'You have no collections yet. Create one first!\n\n'
                                '_Use the button below or type_ `.col new <name>`_._',
                                buttons=[[Button.inline('➕ Create Collection',
                                                        f'col_new_for_{book_id}'.encode())]],
                                parse_mode='md'
                            )
                        else:
                            btns = []
                            for c in cols[:8]:
                                btns.append([Button.inline(
                                    f'{c["emoji"]} {c["name"][:28]}  ({c["count"]}📚)',
                                    f'col_add_{c["id"]}_{book_id}'.encode()
                                )])
                            btns.append([
                                Button.inline('➕ New Collection',
                                              f'col_new_for_{book_id}'.encode()),
                                Button.inline('✖ Cancel', b'col_picker_cancel_0'),
                            ])
                            await interaction_client.send_message(
                                uid,
                                '📁 **Save to collection**\nChoose a collection:',
                                buttons=btns, parse_mode='md'
                            )
                    except Exception:
                        # Can't DM — show alert with link
                        await event.answer(
                            f'Open my DM to save books to collections!',
                            alert=True
                        )
                else:
                    await event.answer(
                        '📚 Open the bot in DM to manage collections.',
                        alert=True
                    )
                return

            # ── DM: show picker inline ─────────────────────────────────────────
            cols = col_user_list(uid)
            if not cols:
                await event.answer(
                    '📚 No collections yet! Create one first.',
                    alert=True
                )
                await interaction_client.send_message(
                    uid,
                    '📚 **Create your first collection!**\n'
                    'Then tap ➕ again to save books.',
                    buttons=[[Button.inline('➕ Create Collection',
                                           f'col_new_for_{book_id}'.encode())]],
                    parse_mode='md'
                )
                return

            btns = []
            for c in cols[:8]:
                btns.append([Button.inline(
                    f'{c["emoji"]} {c["name"][:28]}  ({c["count"]}📚)',
                    f'col_add_{c["id"]}_{book_id}'.encode()
                )])
            btns.append([
                Button.inline('➕ New Collection', f'col_new_for_{book_id}'.encode()),
                Button.inline('✖ Cancel', f'col_picker_cancel_{event.message_id}'.encode()),
            ])
            await event.answer()
            try:
                await event.edit(
                    '📁 **Save to collection**\nChoose a collection:',
                    buttons=btns, parse_mode='md'
                )
            except Exception:
                try:
                    await interaction_client.send_message(
                        uid,
                        '📁 **Save to collection**\nChoose a collection:',
                        buttons=btns, parse_mode='md'
                    )
                except Exception:
                    pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_picker_cancel_(\d+)')))
        async def col_picker_cancel_cb(event):
            """Cancel picker — try to restore original search results view or just delete."""
            msg_id = int(event.data_match.group(1))
            await event.answer('Cancelled.')
            try: await event.delete()
            except Exception: pass

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'col_new_for_(\d+)')))
        async def col_new_for_cb(event):
            """Create new collection then immediately queue adding the book."""
            book_id = int(event.data_match.group(1))
            uid     = event.sender_id
            bot_un  = _BOT_USERNAME[0]
            if len(col_user_list(uid)) >= MAX_COLLECTIONS_PER_USER:
                await event.answer(f'❌ Max {MAX_COLLECTIONS_PER_USER} collections.', alert=True)
                return
            await event.answer()
            _COL_PENDING_CREATE[uid] = book_id   # will auto-add book after name is given
            try:
                await interaction_client.send_message(
                    uid,
                    '📚 **New Collection**\n\n'
                    'Send me the name for your collection.\n'
                    'Start with an emoji: `🌟 My Favourites`\n\n'
                    '_Type /cancel to cancel._',
                    parse_mode='md'
                )
                try: await event.delete()
                except Exception: pass
            except Exception:
                await event.answer(
                    f'Open @{bot_un} and type: .col new <name>',
                    alert=True
                )

        # ── Feedback prompt button ────────────────────────────────────────────
        @bot_client.on(events.CallbackQuery(data=b'feedback_prompt'))
        async def feedback_prompt_cb(event):
            await event.answer()
            await interaction_client.send_message(
                event.sender_id,
                '💌 **Send Feedback**\n\n'
                'Just reply here with your message.\n'
                '_Your feedback goes straight to the admin team._',
                parse_mode='md'
            )
            _COL_PENDING_FEEDBACK[event.sender_id] = True

        # ════════════════════════════════════════════════════════════════════════
        # INLINE QUERY HANDLER — @botname <query>
        # Works in any chat without the bot being a member.
        # ════════════════════════════════════════════════════════════════════════
        @bot_client.on(events.InlineQuery())
        async def inline_handler(event):
            try:
                query = (event.text or '').strip()
                builder = event.builder

                # ── Collection share code inline ──────────────────────────────
                if query.startswith('#col'):
                    parsed = col_parse_code(query)
                    if parsed:
                        uid_owner, cid = parsed
                        col = col_get(cid)
                        if col and col['is_public']:
                            items = col_items(cid)
                            desc  = f'{col["emoji"]} {len(items)} books • Public collection'
                            text  = f'**{col["emoji"]} {col["name"]}**\n{col_share_code(uid_owner, cid)}'
                            results = [builder.article(
                                title=f'{col["emoji"]} {col["name"]}',
                                description=desc,
                                text=text,
                                parse_mode='md',
                            )]
                            await event.answer(results, cache_time=30)
                            return
                    await event.answer([], switch_pm='Collection not found or private', switch_pm_param='start')
                    return

                # ── Book search inline ────────────────────────────────────────
                if len(query) < 2:
                    # Show user's public collections as default results
                    uid = event.sender_id
                    cols = [c for c in col_user_list(uid) if c['is_public']]
                    results = []
                    for c in cols[:5]:
                        code = col_share_code(uid, c['id'])
                        results.append(builder.article(
                            title=f'{c["emoji"]} {c["name"]}',
                            description=f'{c["count"]} books • Share: {code}',
                            text=f'**{c["emoji"]} {c["name"]}**\n`{code}`',
                            parse_mode='md',
                        ))
                if not results:
                    results.append(builder.article(
                        title='🔍 Search for books',
                        description='Type a book name to search…',
                        text=(
                            '🔍 **Search for books:**\n'
                            f'Type `@{_BOT_USERNAME[0]} <book name>` to search\n\n'
                            '📁 **Share a collection:**\n'
                            f'Type `@{_BOT_USERNAME[0]} #col<code>` to share\n\n'
                            '📚 **Collection share codes look like:**\n'
                            '`#col123456789_42` — paste in any chat!'
                        ),
                        parse_mode='md',
                    ))
                    await event.answer(results, cache_time=10)
                    return

                loop    = asyncio.get_event_loop()
                results_raw = await loop.run_in_executor(_SEARCH_EXECUTOR, smart_search, query)
                if not results_raw:
                    await event.answer([], switch_pm=f'No results for "{query[:20]}"', switch_pm_param='start')
                    return

                inline_results = []
                for row in results_raw[:20]:
                    book_id  = row[0]
                    fname    = row[1]
                    fsize    = row[2]
                    is_restr = row[3]
                    ext      = (row[4] if len(row) > 4 else 'pdf') or 'pdf'
                    bname    = re.sub(r'\.(pdf|epub|mobi|azw3?|kfx|djvu|fb2)$', '', fname, flags=re.IGNORECASE).strip()
                    size_mb  = f'{fsize/1024/1024:.1f} MB' if fsize else ''
                    lock_tag = '🔒 ' if is_restr else ''
                    ext_tag  = ext.upper()
                    desc     = f'{lock_tag}{ext_tag}  {size_mb}'.strip()
                    # Deep link: /start get_{book_id}
                    bot_uname = _BOT_USERNAME[0] or 'bot'
                    link = f'https://t.me/{bot_uname}?start=get_{book_id}'
                    text_msg = (
                        f'📖 **{bname}**\n'
                        f'{desc}\n\n'
                        f'[Download from bot]({link})'
                    )
                    inline_results.append(builder.article(
                        title=bname[:80],
                        description=desc,
                        text=text_msg,
                        parse_mode='md',
                        link_preview=False,
                        buttons=Button.url('📥 Download', link),
                    ))
                await event.answer(inline_results, cache_time=30)
            except Exception as e:
                log.warning(f'inline_handler: {e}')
                try: await event.answer([], cache_time=5)
                except Exception: pass

        # ── Pending state dicts (for multi-step DM flows) ─────────────────────
        _COL_PENDING_CREATE:   dict[int, bool] = {}
        _COL_PENDING_RENAME:   dict[int, int]  = {}
        _COL_PENDING_FEEDBACK: dict[int, bool] = {}

        @bot_client.on(events.NewMessage(func=lambda e: e.is_private and not e.text.startswith('/')))
        async def dm_text_handler(event):
            """
            Handle multi-step DM flows: collection create/rename, feedback.
            MUST return early for all pending flows so the message is NOT
            forwarded to the search system.
            """
            uid  = event.sender_id
            text = (event.text or '').strip()

            # Cancel any pending flow
            if text.lower() in ('/cancel', '.cancel', 'cancel'):
                _COL_PENDING_CREATE.pop(uid, None)
                _COL_PENDING_RENAME.pop(uid, None)
                _COL_PENDING_FEEDBACK.pop(uid, None)
                await event.reply('✅ Cancelled.')
                return

            # ── Feedback flow ──────────────────────────────────────────────────
            if _COL_PENDING_FEEDBACK.pop(uid, False):
                if len(text) < 5:
                    await event.reply('❌ Too short. Try again or type /cancel.'); return
                sender = event.sender
                fname  = getattr(sender, 'first_name', 'User') or 'User'
                uname  = getattr(sender, 'username', None)
                try:
                    conn = db_connect(); c = conn.cursor()
                    c.execute('INSERT INTO feedback(user_id,username,first_name,message,ts) VALUES(?,?,?,?,?)',
                              (uid, uname or '', fname, text, int(time.time())))
                    conn.commit(); conn.close()
                except Exception: pass
                await report(f'💌 **Feedback (DM)**\n👤 {fname} ({uid})\n💬 {text}\n🕐 {ts_str()}')
                await event.reply('💌 **Thank you!** Your feedback has been sent. ✅')
                return   # ← CRITICAL: stop here, don't fall through to search

            # ── Collection create flow ─────────────────────────────────────────
            pending_create = _COL_PENDING_CREATE.pop(uid, None)
            if pending_create is not None:
                name_raw = text[:60]
                emoji    = '📚'
                m_emoji  = re.match(r'^([\U00010000-\U0010FFFF\u2600-\u27BF])\s+(.+)$', name_raw)
                if m_emoji:
                    emoji = m_emoji.group(1); name_raw = m_emoji.group(2)
                if not name_raw.strip():
                    await event.reply('❌ Name can\'t be empty. Try again or type /cancel.'); return
                cid  = col_create(uid, name_raw, emoji)
                code = col_share_code(uid, cid)
                # Auto-add the book if triggered from a ➕ button
                book_id_to_add = pending_create if isinstance(pending_create, int) else None
                added_msg = ''
                if book_id_to_add:
                    conn = db_connect(); c2 = conn.cursor()
                    c2.execute('SELECT file_name FROM books WHERE id=?', (book_id_to_add,))
                    rb = c2.fetchone(); conn.close()
                    bname = rb[0] if rb else f'Book #{book_id_to_add}'
                    col_add_book(cid, book_id_to_add, bname)
                    bn_clean = re.sub(r'\.(pdf|epub|mobi|azw3?|djvu)$', '', bname, flags=re.IGNORECASE)
                    added_msg = f'\n✅ **Added:** _{bn_clean}_'
                text_r, btns = _col_list_text(uid)
                await event.reply(
                    f'✅ **Collection created:** {emoji} **{name_raw}**\n'
                    f'🔗 Share: `{code}`'
                    + added_msg + '\n\n' + text_r,
                    buttons=btns, parse_mode='md'
                )
                return   # ← stop here

            # ── Collection rename flow ─────────────────────────────────────────
            if uid in _COL_PENDING_RENAME:
                cid = _COL_PENDING_RENAME.pop(uid)
                if not text.strip():
                    await event.reply('❌ Name can\'t be empty.'); return
                col_rename(cid, uid, text[:60])
                col = col_get(cid); items = col_items(cid)
                if col:
                    t2, b2 = _col_detail_text(col, items)
                    await event.reply(f'✅ Renamed!\n\n' + t2, buttons=b2, parse_mode='md')
                else:
                    await event.reply('✅ Renamed.')
                return   # ← stop here

            # No pending flow — let the message fall through to normal DM handling
            # (search, /start, etc.) by NOT returning here

        @bot_client.on(events.CallbackQuery(data=re.compile(rb'page_(.+)_(\d+)')))
        async def page_callback(event):
            try:
                if not spam_check_page(event.sender_id):
                    await event.answer()
                    return
                qh = event.data_match.group(1).decode()
                pg = int(event.data_match.group(2))

                results = None
                entry = _SEARCH_CACHE.get(qh)
                if entry and (time.time() - entry[1]) < _SEARCH_CACHE_TTL:
                    results = entry[0]
                else:
                    cp = os.path.join(CACHE_DIR, f'{qh}.json')
                    if not os.path.exists(cp):
                        await event.answer('⚠️ Session expired — search again', alert=True)
                        return
                    try:
                        with open(cp) as f:
                            results = json.load(f)
                        _SEARCH_CACHE[qh] = (results, time.time())
                    except Exception:
                        await event.answer('⚠️ Session expired — search again', alert=True)
                        return

                tid = None
                try:
                    msg = await event.get_message()
                    if msg and msg.reply_to:
                        tid = (getattr(msg.reply_to, 'reply_to_top_id', None)
                               or getattr(msg.reply_to, 'reply_to_msg_id', None))
                except Exception: pass
                mode = 'trigger' if get_assignment(event.chat_id, tid, TRIGGER_CHATS) else 'assigned'
                await send_page(event, interaction_client, qh, results, pg, mode, tid_override=tid)
            except Exception as e:
                log.warning(f'page_callback: {e}')
                await report_error('page_callback', e)

        @bot_client.on(events.NewMessage(pattern=r'/start(.*)'))
        async def start_handler(event):
            try:
                param = (event.pattern_match.group(1) or '').strip()
                uid   = get_sender_id(event)
                sender = event.sender
                fn    = getattr(sender, 'first_name', 'User') or 'User'
                un    = getattr(sender, 'username', None)
                bot_un = _BOT_USERNAME[0]

                # ── /start get_<book_id> — deep link book download ────────────
                m_get = re.match(r'get_(\d+)', param)
                if m_get:
                    book_id = int(m_get.group(1))
                    allowed_dl, dl_reason = spam_check_download(uid, book_id)
                    if not allowed_dl:
                        if dl_reason.startswith('book_cooldown:'):
                            remaining = int(dl_reason.split(':')[1])
                            await event.reply(f'⏳ Wait {remaining//60}m {remaining%60}s before downloading again.')
                        elif dl_reason.startswith('daily_limit:'):
                            lim = dl_reason.split(':')[1]
                            await event.reply(f'🚫 Daily download limit reached ({lim} books/day).')
                        else:
                            await event.reply('⏳ Please wait before downloading again.')
                        return
                    conn = db_connect(); c = conn.cursor()
                    c.execute('SELECT file_name, chat_id FROM books WHERE id=?', (book_id,))
                    r = c.fetchone(); conn.close()
                    fname       = r[0] if r else 'Book'
                    src_chat_id = r[1] if r else None
                    cap = build_caption(fname, uid, fn, username=un,
                                        template_name=DM_TEMPLATE_REF[0],
                                        purge_secs=DM_PURGE_SECS_REF[0] or 600,
                                        src_chat_id=src_chat_id, book_id=book_id)
                    reply_to    = getattr(event.reply_to, 'reply_to_top_id', None) if event.reply_to else None
                    thread_id_sg = (getattr(event.reply_to, 'reply_to_top_id', None)
                                    or getattr(event.reply_to, 'reply_to_msg_id', None)) if event.reply_to else None
                    asyncio.create_task(deliver_book(
                        event.chat_id, book_id, user_client, interaction_client,
                        cap, reply_to, access_hash=getattr(sender, 'access_hash', None),
                        requester_id=uid, requester_name=fn, requester_username=un or fn,
                        request_chat_id=event.chat_id, thread_id=thread_id_sg
                    ))
                    return

                # ── /start dmopen — user tapped our "say hello" button ────────
                if param == 'dmopen':
                    sender = event.sender
                    fname  = getattr(sender, 'first_name', '') or ''
                    uname  = getattr(sender, 'username', '') or ''
                    dm_mark_unlocked(uid, uname, fname, method='start_dmopen')
                    await event.reply(
                        f'✅ **All set, {fname or "there"}!**\n\n'
                        f'You can now receive books directly in this chat.\n'
                        f'Go back and tap the download button again.',
                        parse_mode='md'
                    )
                    return

                # ── /start mycollections — open collection UI ─────────────────
                if param in ('mycollections', 'collections'):
                    text, btns = _col_list_text(uid)
                    await event.reply(text, buttons=btns, parse_mode='md')
                    return

                # ── /start savebook_<id> — collection picker for a book ───────
                m_savebook = re.match(r'savebook_(\d+)', param)
                if m_savebook:
                    book_id = int(m_savebook.group(1))
                    cols    = col_user_list(uid)
                    if not cols:
                        await event.reply(
                            '📚 **Save to collection**\n\nNo collections yet!',
                            buttons=[[Button.inline('➕ Create Collection',
                                                    f'col_new_for_{book_id}'.encode())]],
                            parse_mode='md'
                        )
                    else:
                        btns = []
                        for c in cols[:8]:
                            btns.append([Button.inline(
                                f'{c["emoji"]} {c["name"][:28]}  ({c["count"]}📚)',
                                f'col_add_{c["id"]}_{book_id}'.encode()
                            )])
                        btns.append([Button.inline('➕ New Collection',
                                                   f'col_new_for_{book_id}'.encode())])
                        await event.reply(
                            '📁 **Save to collection**\nChoose a collection:',
                            buttons=btns, parse_mode='md'
                        )
                    return

                # ── /start newcol_<book_id> — create collection then add book ─
                m_newcol = re.match(r'newcol_(\d+)', param)
                if m_newcol:
                    book_id = int(m_newcol.group(1))
                    if len(col_user_list(uid)) >= MAX_COLLECTIONS_PER_USER:
                        await event.reply(f'❌ Max {MAX_COLLECTIONS_PER_USER} collections reached.')
                        return
                    _COL_PENDING_CREATE[uid] = book_id
                    await event.reply(
                        '📚 **New Collection**\n\n'
                        'Send me the name for your collection.\n'
                        'You can start with an emoji: `🌟 My Favourites`\n\n'
                        '_Type /cancel to cancel._',
                        parse_mode='md'
                    )
                    return

                # ── /start (plain) — full welcome screen ──────────────────────
                conn = db_connect(); c = conn.cursor()
                c.execute('SELECT COUNT(*) FROM books'); total_books = c.fetchone()[0]; conn.close()

                cols       = col_user_list(uid)
                col_count  = len(cols)
                col_status = f'📚 {col_count} collection{"s" if col_count != 1 else ""}' if cols else '📚 No collections yet'

                welcome = (
                    f'👋 **Hello, {fn}!**\n'
                    f'━━━━━━━━━━━━━━━━━━━━\n'
                    f'Welcome to **{BRAND_CHANNEL[0] or "CCR Library"}** 📖\n\n'
                    f'📊 **{total_books:,}** books available\n'
                    f'{col_status}\n\n'
                    f'**🔍 Search:**\n'
                    f'• In groups: `.বই <name>` or `.boi <name>`\n'
                    f'• Inline anywhere: `@{bot_un} <name>`\n\n'
                    f'**📁 Collections:**\n'
                    f'• Save books with the **➕** button on results\n'
                    f'• Share collections with a `#col` code\n'
                    f'• Paste a `#col` code anywhere to view it\n\n'
                    f'**💬 Commands:**\n'
                    f'• `.feedback <msg>` — send feedback to admins\n'
                    f'• `.request <book>` — request a book\n'
                )

                btns = [
                    [Button.inline('🔍 Search Books', b'help_search'),
                     Button.inline('📚 My Collections', b'col_list')],
                    [Button.inline('💌 Send Feedback', b'feedback_prompt'),
                     Button.inline('📖 How to Use', b'help_main')],
                ]
                if BRAND_CHANNEL[0]:
                    btns.append([Button.url('📢 Our Channel', f'https://t.me/{BRAND_CHANNEL[0].lstrip("@")}')])

                await event.reply(welcome, buttons=btns, parse_mode='md')

            except Exception as e:
                log.warning(f'start_handler: {e}')

        # ── Help/info button callbacks ────────────────────────────────────────
        @bot_client.on(events.CallbackQuery(data=b'help_main'))
        async def help_main_cb(event):
            bot_un = _BOT_USERNAME[0]
            text = (
                '📖 **How to Use the Bot**\n'
                '━━━━━━━━━━━━━━━━━━━━\n\n'
                '**In Groups:**\n'
                '`.বই হুমায়ূন` — search in Bengali\n'
                '`.boi humayun` — search in English\n'
                '`.request <name>` — request a missing book\n'
                '`.feedback <msg>` — send feedback\n\n'
                '**Inline (any chat):**\n'
                f'`@{bot_un} <book name>` — search anywhere\n'
                f'`@{bot_un} #col<code>` — share a collection\n\n'
                '**Collections:**\n'
                '1. Search for a book\n'
                '2. Tap **➕** to save it to a collection\n'
                '3. Share with `#col<your_id>_<col_id>`\n'
                '4. Anyone can paste the code to view it\n\n'
                '**Share Codes:**\n'
                '`#col123456_42` — paste in any group or DM\n'
                'to view and browse that collection!'
            )
            btns = [[Button.inline('◀ Back', b'help_back')]]
            try: await event.edit(text, buttons=btns, parse_mode='md')
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=b'help_search'))
        async def help_search_cb(event):
            text = (
                '🔍 **Searching for Books**\n'
                '━━━━━━━━━━━━━━━━━━━━\n\n'
                '**Bengali search:**\n'
                '`.বই রবীন্দ্রনাথ` — by author\n'
                '`.বই গল্পগুচ্ছ` — by title\n'
                '`.বই হুমায়ূন হিমু` — author + title\n\n'
                '**English search:**\n'
                '`.boi humayun` or `.boi 48 laws`\n\n'
                '**Result buttons:**\n'
                '📄 PDF 2.3MB — tap to download\n'
                '📕 EPUB — tap for EPUB version\n'
                '➕ — save to your collection\n\n'
                '**Tips:**\n'
                '• Try shorter keywords if nothing found\n'
                '• Bengali ি/ী confusion is auto-handled\n'
                '• Type English names like "rabindranath"\n'
                '  to find Bengali books'
            )
            btns = [[Button.inline('◀ Back', b'help_back')]]
            try: await event.edit(text, buttons=btns, parse_mode='md')
            except Exception: await event.answer()

        @bot_client.on(events.CallbackQuery(data=b'help_back'))
        async def help_back_cb(event):
            uid    = event.sender_id
            sender = await event.get_sender()
            fn     = getattr(sender, 'first_name', 'User') or 'User'
            bot_un = _BOT_USERNAME[0]
            conn   = db_connect(); c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM books'); total_books = c.fetchone()[0]; conn.close()
            cols      = col_user_list(uid)
            col_count = len(cols)
            col_status = f'📚 {col_count} collection{"s" if col_count != 1 else ""}' if cols else '📚 No collections yet'
            welcome = (
                f'👋 **Hello, {fn}!**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'📊 **{total_books:,}** books available  •  {col_status}\n\n'
                f'Search: `.বই <name>` or `@{bot_un} <name>`'
            )
            btns = [
                [Button.inline('🔍 Search Help', b'help_search'),
                 Button.inline('📚 My Collections', b'col_list')],
                [Button.inline('💌 Send Feedback', b'feedback_prompt'),
                 Button.inline('📖 How to Use', b'help_main')],
            ]
            try: await event.edit(welcome, buttons=btns, parse_mode='md')
            except Exception: await event.answer()

    async def cleanup_loop():
        while True:
            try:
                now = int(time.time())
                # Step 1: read rows then CLOSE DB before any Telegram awaits
                conn = db_connect(); c = conn.cursor()
                c.execute('SELECT id,message_id,chat_id,use_user_client FROM cleanup_queue WHERE delete_at<=?', (now,))
                rows = c.fetchall()
                conn.close()

                # Step 2: do all Telegram deletes with no DB connection open
                done = []
                for row_id, mid, cid, use_uc in rows:
                    # Safety net: never delete from source channels or backup group
                    if _is_protected_chat(cid):
                        log.warning(f'cleanup_loop: blocked deletion from protected chat {cid} msg {mid} — removing from queue')
                        done.append(row_id)
                        continue
                    clients_to_try = [user_client] if use_uc else [interaction_client, user_client]
                    for client in clients_to_try:
                        try: await client.delete_messages(cid, mid); break
                        except errors.ChatAdminRequiredError: continue
                        except errors.MessageDeleteForbiddenError: break
                        except Exception: continue
                    done.append(row_id)

                # Step 3: delete processed rows in a fresh, short-lived connection
                if done:
                    conn = db_connect(); c = conn.cursor()
                    for rid in done:
                        c.execute('DELETE FROM cleanup_queue WHERE id=?', (rid,))
                    conn.commit(); conn.close()

                # Flush pending schedule_delete() calls in one transaction
                _flush_delete_queue()

                for fn in os.listdir(CACHE_DIR):
                    fp = os.path.join(CACHE_DIR, fn)
                    if os.path.isfile(fp) and time.time() - os.path.getmtime(fp) > 1800:
                        try: os.remove(fp)
                        except: pass

                cutoff = time.time() - 660
                stale = [k for k, v in download_cooldowns.items() if v < cutoff]
                for k in stale: del download_cooldowns[k]

                cleanup_spam_state()

                now_ts = time.time()
                stale_keys = [k for k, (_, ts) in _SEARCH_CACHE.items()
                              if now_ts - ts > _SEARCH_CACHE_TTL]
                for k in stale_keys:
                    _SEARCH_CACHE.pop(k, None)
                    # Delete the search result window message from chat
                    reg = _SEARCH_MSG_REGISTRY.pop(k, None)
                    if reg:
                        _msg_id = reg[0]; _chat_id = reg[1]
                        if not _is_protected_chat(_chat_id):
                            try:
                                await interaction_client.delete_messages(_chat_id, _msg_id)
                            except Exception:
                                pass

            except Exception as e:
                log.warning(f'cleanup_loop: {e}')
                await report_error('cleanup_loop', e)
            await asyncio.sleep(20)

    asyncio.create_task(cleanup_loop())

    # ── Start companion clients ───────────────────────────────────────────────
    async def _start_companion(comp: CompanionClient):
        """Connect a companion client and register its book listener."""
        try:
            session_path = os.path.join(_BASE_DIR, comp.session)
            comp.client  = TelegramClient(session_path, comp.api_id, comp.api_hash)
            await comp.client.start()
            comp.me      = await comp.client.get_me()
            comp.running = True
            comp.error   = ''
            OWN_IDS.add(comp.me.id)
            log.info(f'Companion "{comp.name}" started as @{comp.me.username} '
                     f'({len(comp.sources)} sources: {comp.sources})')

            # Register book_listener for companion's sources
            @comp.client.on(events.NewMessage())
            async def _companion_book_listener(event):
                """Real-time indexing from companion-owned sources."""
                if not event.file or not event.chat:
                    return
                if event.sender_id in OWN_IDS:
                    return
                raw_chat_id = event.chat_id
                norm        = _normalize_id_for_compare(raw_chat_id)
                chat_id     = norm if isinstance(norm, int) else raw_chat_id
                # Only index from sources this companion owns
                uname       = (getattr(event.chat, 'username', '') or '').lower().lstrip('@')
                is_source   = False
                for s in comp.sources:
                    sn = _normalize_id_for_compare(s)
                    if isinstance(sn, int):
                        if sn == chat_id: is_source = True; break
                    elif sn and uname and sn == uname:
                        is_source = True; break
                if not is_source:
                    return
                file_info = _extract_file_info(event)
                if not file_info:
                    return
                fname, ext, fsize = file_info
                is_restr  = 1 if getattr(event.chat, 'noforwards', False) else 0
                sname     = normalize_name(fname)
                sstripped = _RE_STRIP_VOWELS.sub('', sname)
                conn = db_connect(); c = conn.cursor()
                try:
                    c.execute('SELECT id, is_restricted FROM books WHERE file_name=? AND file_size=?',
                              (fname, fsize))
                    existing = c.fetchone()
                    if not existing:
                        c.execute(
                            'INSERT INTO books(file_name,search_name,stripped_name,file_size,'
                            'message_id,chat_id,orig_chat_id,file_ext,is_restricted) VALUES(?,?,?,?,?,?,?,?,?)',
                            (fname, sname, sstripped, fsize, event.id, chat_id, chat_id, ext, is_restr)
                        )
                        conn.commit()
                        conn.execute(
                            'INSERT OR IGNORE INTO scrape_progress(chat_id,last_msg_id,last_scraped_at) VALUES(?,?,?)',
                            (chat_id, event.id, int(time.time()))
                        )
                        conn.execute(
                            'UPDATE scrape_progress SET last_msg_id=MAX(last_msg_id,?) WHERE chat_id=?',
                            (event.id, chat_id)
                        )
                        conn.commit()
                        log.debug(f'[{comp.name}] indexed "{fname}" from {chat_id}')
                    elif existing[1] == 1 and is_restr == 0:
                        c.execute('UPDATE books SET message_id=?,chat_id=?,is_restricted=0 WHERE id=?',
                                  (event.id, chat_id, existing[0]))
                        conn.commit()
                except Exception as db_err:
                    log.warning(f'[{comp.name}] DB error for {fname}: {db_err}')
                finally:
                    conn.close()

            await report(
                f'🤝 **Companion "{comp.name}" online**\n'
                f'👤 @{comp.me.username} (ID: `{comp.me.id}`)\n'
                f'📡 Sources: `{len(comp.sources)}`\n'
                f'🕐 {ts_str()}'
            )
        except Exception as e:
            comp.running = False
            comp.error   = str(e)
            log.error(f'Companion "{comp.name}" failed to start: {e}')
            await report(
                f'❌ **Companion "{comp.name}" FAILED**\n'
                f'Error: `{e}`\n'
                f'_Check session file and credentials._\n'
                f'🕐 {ts_str()}',
                is_error=True
            )

    for comp in COMPANION_CLIENTS:
        asyncio.create_task(_start_companion(comp))

    # ── Auto-scrape loop — uses shadow DB to avoid locking the live DB ──────────
    async def auto_scrap_loop():
        """
        Periodically runs a FULL fresh scrape into the shadow scanbook.db,
        then atomically promotes it to ebooks.db once complete.

        This means the live ebooks.db is NEVER wiped or locked while users are
        searching — they keep getting results from the old DB the whole time.
        Only after the shadow scrape finishes do we atomically swap the DB files.
        """
        log.info(f'Auto-scrape loop started (interval={AUTO_SCRAP_INTERVAL_H[0]}h)')
        while True:
            interval = AUTO_SCRAP_INTERVAL_H[0]
            if interval <= 0:
                await asyncio.sleep(60)   # idle — check again in 1 min
                continue

            await asyncio.sleep(interval * 3600)

            interval = AUTO_SCRAP_INTERVAL_H[0]  # re-read in case hot-reloaded
            if interval <= 0:
                continue
            if not SOURCE_GROUPS:
                log.info('Auto-scrape: no sources configured, skipping')
                continue
            if _scrap_running[0]:
                log.info('Auto-scrape: manual job running, skipping this cycle')
                continue
            if _shadow_active[0]:
                log.info('Auto-scrape: shadow scrape already in progress, skipping')
                continue

            log.info(f'Auto-scrape triggered (interval={interval}h) — using shadow DB')
            _log_scrap('_auto_', 'start', f'interval={interval}h shadow=True')

            # ── Step 1: Build a fresh shadow DB ──────────────────────────────
            _shadow_active[0] = True
            try:
                # Remove any leftover shadow DB from a previous failed run
                for leftover in [DB_SCAN, DB_SCAN + '-wal', DB_SCAN + '-shm']:
                    if os.path.exists(leftover):
                        try: os.remove(leftover)
                        except Exception: pass

                # Set up schema in shadow DB — mirrors setup_db() but on DB_SCAN
                conn_shadow = db_scan_connect()
                _setup_schema(conn_shadow)
                conn_shadow.close()

                # Run the full scrape into DB_SCAN (monkey-patch DB_PATH temporarily)
                # We do this by passing db_path=DB_SCAN into index_books.
                await report(
                    f'⏰ **Auto-scrape started** (shadow mode)\n'
                    f'Sources: `{len(SOURCE_GROUPS)}`\n'
                    f'_Live DB untouched until scrape completes._\n'
                    f'🕐 {ts_str()}'
                )
                async def _noop(m): pass
                await _run_scrap_job(user_client, _noop, list(SOURCE_GROUPS),
                                     job_label='auto-shadow', started_by=0,
                                     scrape_mode='full', target_db=DB_SCAN)

                # ── Step 2: Promote shadow DB → live DB ──────────────────────
                loop = asyncio.get_event_loop()
                ok = await loop.run_in_executor(_SEARCH_EXECUTOR, _promote_shadow_db)

                if ok:
                    # Clear search cache so next query hits the fresh DB
                    _SEARCH_CACHE.clear()
                    for fn in os.listdir(CACHE_DIR):
                        fp = os.path.join(CACHE_DIR, fn)
                        if os.path.isfile(fp):
                            try: os.remove(fp)
                            except Exception: pass

                    conn = db_connect(); c = conn.cursor()
                    c.execute('SELECT COUNT(*) FROM books')
                    new_count = c.fetchone()[0]; conn.close()

                    await report(
                        f'⏰ **Auto-scrape completed** ✅\n'
                        f'📚 Books in new DB: `{new_count}`\n'
                        f'Sources: `{len(SOURCE_GROUPS)}`\n'
                        f'🕐 {ts_str()}'
                    )
                    _log_scrap('_auto_', 'promoted', f'new_count={new_count}')
                else:
                    await report(
                        f'⚠️ **Auto-scrape shadow promotion FAILED**\n'
                        f'Live DB unchanged. Check logs for details.\n'
                        f'🕐 {ts_str()}',
                        is_error=True
                    )
            except Exception as auto_err:
                log.error(f'auto_scrap_loop: {auto_err}', exc_info=True)
                await report_error('auto_scrap_loop', auto_err)
            finally:
                _shadow_active[0] = False

    asyncio.create_task(auto_scrap_loop())

    asyncio.create_task(_settings_watchdog())
    asyncio.create_task(_scrape_watchdog())
    asyncio.create_task(_auto_report_loop())
    asyncio.create_task(_db_maintenance_loop())

    tasks = [user_client.run_until_disconnected()]
    if bot_client:
        tasks.append(bot_client.run_until_disconnected())
    await asyncio.gather(*tasks)


async def main():
    while True:
        try:
            await start_clients()
        except Exception as e:
            log.error(f'Crash: {e}')
            try:
                if _analytics_client_ref:
                    await report_error('main() crash', e)
            except Exception: pass
            await asyncio.sleep(10)


if __name__ == '__main__':
    asyncio.run(main())