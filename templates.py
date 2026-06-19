"""
templates.py — Caption Template Configuration
==============================================
Edit this file to customise all delivery captions.

Template variables:
  {book_name}        — filename without extension
  {user_mention}     — plain "Name (@username)" — safe for ALL groups (no hyperlink)
  {user_mention_link}— clickable [Name](tg://user?id=...) — default template only
  {user_full_mention}— clickable [First Last](tg://user?id=...) — boi_mohol style
  {brand}            — BRAND_CHANNEL value
  {source}           — SOURCE_CREDIT global
  {book_source}      — actual source group label (plain text)
  {book_source_link} — source group as t.me link when username known
  {purge_time}       — e.g. "10m", "2h"
  {separator}        — ────────────────────────────── (30 dashes)
"""

# ── Separator used in all templates ──────────────────────────────────────────
SEP = '─' * 30

# ── Built-in templates ────────────────────────────────────────────────────────
# Edit any value below.  Keys are the template names used in /set_group_template.
BUILTIN_TEMPLATES: dict[str, str] = {

    # ── default ───────────────────────────────────────────────────────────────
    # Full branding + hyperlink mention.  Safe for open/public groups.
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
    # Private / DM deliveries.  Set via /set_dm_template or settings.json.
    'dm': (
        '📚 **{book_name}**\n'
        '{separator}\n'
        '📦 Source: {book_source_link}\n'
        '🌐 {brand}\n'
        '{separator}\n'
        '⏳ _Auto-deletes in {purge_time}_\n'
        '💡 _আরও বই পেতে `.বই <নাম>` লিখো_'
    ),

    # ── minimal ───────────────────────────────────────────────────────────────
    # Book name + plain mention only.  Best for sensitive groups.
    'minimal': (
        '📚 **{book_name}**\n'
        '👤 Requested by: {user_mention}'
    ),

    # ── branded ───────────────────────────────────────────────────────────────
    # Brand + source visible, plain mention (no hyperlink).
    'branded': (
        '📚 **{book_name}**\n'
        '{separator}\n'
        '👤 For: {user_mention}\n'
        '🌐 {brand} • 📦 {source}\n'
        '{separator}\n'
        '⏳ _Deletes in {purge_time}._'
    ),

    # ── silent ────────────────────────────────────────────────────────────────
    # Bare minimum: just book name.  Ultra-sensitive groups.
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
    # "Fulfilled request" look.
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
    # No branding, plain mention, no links anywhere.
    'no_brand': (
        '📚 **{book_name}**\n'
        '👤 For: {user_mention}\n'
        '⏳ _Deletes in {purge_time}_'
    ),

    # ── boi_mohol ─────────────────────────────────────────────────────────────
    # Aesthetic style for Boi Mohol group 🌸
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

# ── Inline query settings ─────────────────────────────────────────────────────
# Minimum characters user must type before book search fires.
# Keep >= 3 to avoid hammering the DB on every keystroke.
INLINE_MIN_CHARS: int = 3

# Debounce delay (seconds) — bot waits this long after last keystroke before
# actually hitting the DB.  Telegram throttles inline anyway, but this adds a
# server-side guard.  5s is safe for low–medium traffic bots.
INLINE_DEBOUNCE_SECS: float = 5.0

# How many inline results to return (max 50 per Telegram limits).
INLINE_MAX_RESULTS: int = 20

# Inline results cache time (seconds) — how long Telegram caches per query.
INLINE_CACHE_TIME: int = 30
