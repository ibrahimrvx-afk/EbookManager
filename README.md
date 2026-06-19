<p align="center">
  <img src="ccr.jpg" alt="Curious Crew Return" width="160" style="border-radius: 50%;" />
</p>

<h1 align="center">📚 EbookManager Bot</h1>

<p align="center">
  A powerful Telegram bot for searching, delivering, and managing Bengali ebooks across Telegram groups.<br/>
  Built for the <strong>Free the Library / Free the Books</strong> movement.
</p>

<p align="center">
  <a href="https://t.me/CuriousCrewReturn"><img src="https://img.shields.io/badge/Telegram-Curious%20Crew%20Return-2CA5E0?logo=telegram&logoColor=white" /></a>
  <a href="https://t.me/FuckYouHasina"><img src="https://img.shields.io/badge/Owner-%40FuckYouHasina-blue?logo=telegram&logoColor=white" /></a>
  <img src="https://img.shields.io/badge/License-MIT-green" />
  <img src="https://img.shields.io/badge/Python-3.11%2B-blue?logo=python&logoColor=white" />
</p>

<p align="center">
  <em>বই মুক্ত হোক। জ্ঞান মুক্ত হোক।</em><br/>
  <em>Let books be free. Let knowledge be free.</em>
</p>

---

## 👤 Author & Owner

| | |
|---|---|
| **Owner** | [@FuckYouHasina](https://t.me/FuckYouHasina) |
| **Community** | [Curious Crew Return](https://t.me/CuriousCrewReturn) |

This bot was created and is maintained by **[@FuckYouHasina](https://t.me/FuckYouHasina)** as part of the [Curious Crew Return](https://t.me/CuriousCrewReturn) community — a Telegram hub for ebooks, news, geopolitics, APKs, Magisk modules, internet archiving, and much more.

---

## 🌐 Curious Crew Return

<p align="center">
  <a href="https://t.me/CuriousCrewReturn">
    <img src="ccr.jpg" alt="Curious Crew Return Logo" width="100"/>
  </a>
</p>

**[t.me/CuriousCrewReturn](https://t.me/CuriousCrewReturn)** is more than just a book group. It's a full internet archive and community covering:

- 📚 **Ebooks** — Bengali & international books, PDFs, EPUBs
- 📰 **News & Geopolitics** — Independent news, analysis, and commentary
- 📱 **APKs** — Modified and premium Android apps
- 🔧 **Magisk Modules** — Root tools and system tweaks
- 🗄️ **Internet Archiving** — Preserving content before it disappears
- 🌍 **And much more** — A growing archive for the curious mind

Join us: **[t.me/CuriousCrewReturn](https://t.me/CuriousCrewReturn)**

---

## ✨ Features

- **Book Search** — Search a large catalogue of Bengali ebooks by title or author using `.বই <name>` or `.boi <name>`
- **Instant Delivery** — Sends PDFs directly to groups or DMs with customisable captions
- **Multi-Source Scraping** — Indexes books from dozens of Telegram channels automatically
- **Caption Templates** — Multiple named templates (`default`, `minimal`, `branded`, `silent`, `boi_mohol`, …) assignable per group
- **Companion Clients** — Run multiple Telegram user-bot clients each owning different source channels
- **Spam Protection** — Per-user cooldowns, daily download limits, flood muting, and chat rate limiting
- **VIP System** — Bypass limits for trusted users; customisable VIP permissions
- **Inline Search** — Works as an inline bot (`@BotUsername query`) in any chat
- **Book of the Day** — Scheduled daily/weekly book recommendations
- **Analytics** — Broadcasts daily/weekly download reports to admin groups
- **Request System** — Users can request books that aren't in the catalogue

---

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.11+
- A Telegram account to use as the scraper userbot
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- API credentials from [my.telegram.org/apps](https://my.telegram.org/apps)

### 2. Clone & Install

```bash
git clone https://github.com/your-username/ebook-manager-bot.git
cd ebook-manager-bot
pip install -r requirements.txt
```

### 3. Configure Secrets

```bash
cp .env.example .env
# Edit .env with your real credentials
nano .env
```

### 4. Configure the Bot

```bash
cp settings.example.json settings.json
# Edit settings.json to set your group IDs, sources, templates, etc.
nano settings.json
```

### 5. (Optional) Add Group Logo

Place your group logo as `ccr.jpg` in the project root — it will appear in captions and the README.

### 6. Run

```bash
python ebookmanager.py
```

On first run, the userbot will ask you to log in with your phone number (one-time).

---

## ⚙️ Configuration Reference

### `.env` — Secrets

| Variable | Description |
|---|---|
| `API_ID` | Telegram API ID from my.telegram.org |
| `API_HASH` | Telegram API hash from my.telegram.org |
| `PHONE_NUMBER` | Phone number for the scraper userbot |
| `OWNER_ID` | Your Telegram user ID (has full admin access) |
| `ADMIN_IDS` | Comma-separated extra admin user IDs |
| `BOT_TOKEN` | Bot token from @BotFather |
| `BOT_USERNAME` | Bot's username (without @) |
| `GROUP_ID` | Your main group username or ID |
| `THREAD_ID` | Thread ID inside the group (0 if none) |
| `AUTO_SCRAP_INTERVAL_H` | How often to auto-scrape sources (hours) |

### `settings.json` — Runtime Config

| Key | Description |
|---|---|
| `sources` | List of Telegram channel usernames or IDs to index |
| `backup_group_id` | Group where bot backs up delivered files |
| `analytics_group` | Group for analytics reports |
| `request_group` | Group for book requests |
| `brand_channel` | Your channel shown in captions |
| `source_credit` | Attribution shown in captions |
| `dm_template` | Default caption template for DM deliveries |
| `dm_purge_secs` | How long DM files stay before auto-delete |
| `search_result_purge_secs` | How long search result messages stay |
| `spam_cfg` | Cooldowns, rate limits, flood protection settings |
| `assigned_chats` | Groups the bot actively responds in |
| `trigger_chats` | Groups the bot watches for trigger words |
| `group_templates` | Per-group caption template assignments |
| `vip_users` | User IDs with elevated permissions |
| `vip_perms` | What VIP users can bypass |
| `extra_admins` | Additional admin user IDs |
| `botd_chats` | Groups to receive Book of the Day |
| `broadcast_report_chats` | Groups to receive daily/weekly reports |

---

## 🎨 Caption Templates

Templates control how delivered books are captioned. Edit `templates.py` to customise them.

**Built-in templates:**

| Name | Use case |
|---|---|
| `default` | Full branding, hyperlink mention — public groups |
| `dm` | Private/DM deliveries |
| `minimal` | Book name + plain mention — sensitive groups |
| `branded` | Brand + source, no hyperlink |
| `silent` | Just the book name |
| `clean` | Formatted, brand visible, plain mention |
| `request_style` | "Fulfilled request" look |
| `no_brand` | No branding at all |
| `boi_mohol` | Aesthetic Bengali style 🌸 |

**Template variables:**

| Variable | Value |
|---|---|
| `{book_name}` | Filename without extension |
| `{user_mention}` | Plain "Name (@username)" |
| `{user_mention_link}` | Clickable hyperlink mention |
| `{brand}` | Your brand channel |
| `{source}` | Global source credit |
| `{book_source}` | Actual source group label |
| `{book_source_link}` | Source group as t.me link |
| `{purge_time}` | e.g. "10m", "2h" |

---

## 🤖 Bot Commands

### User Commands
| Command | Description |
|---|---|
| `.বই <name>` / `.boi <name>` | Search for a book |
| `/request <book name>` | Request a book not in the catalogue |

### Admin Commands
| Command | Description |
|---|---|
| `/list_templates` | Show all available caption templates |
| `/preview_template <name>` | Preview a template |
| `/set_group_template <name> [chat] [thread]` | Assign template to a group |
| `/get_group_template [chat] [thread]` | Check a group's current template |
| `/set_dm_template <name>` | Set DM delivery template |
| `/set_search_purge <seconds>` | Set search result auto-delete time |
| `/add_template <name> <text>` | Add a custom template |
| `/del_template <name>` | Delete a custom template |
| `/disable <ref>` | Remove source and all its books from DB |
| `/disable <ref> --keep` | Remove source only, keep books |

### Companion Client Commands
| Command | Description |
|---|---|
| `/companion_status` | Show all companion client statuses |
| `/companion_add_source <n> <src>` | Assign source to a companion |
| `/companion_remove_source <n> <src>` | Remove source from a companion |
| `/companion_restart <n>` | Reconnect a companion client |

---

## 🗃️ Database

The bot uses SQLite databases (auto-created on first run, not included in this repo):

- `ebooks.db` — Main book catalogue (title, source, file_id, …)
- `collection.db` — User collection tracking
- `dm.db` — DM delivery log

---

## 🛡️ Security Notes

- **Never commit your `.env` file.** It contains your bot token, API credentials, and phone number.
- Rotate your bot token via @BotFather if it is ever exposed.
- The `.gitignore` already excludes `.env`, `*.db`, `*.session`, and `*.log`.

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repo
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes
4. Open a pull request

All contributors should join [Curious Crew Return](https://t.me/CuriousCrewReturn) to stay in touch with the project.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE) — free to use, modify, and distribute.

---

## 🙏 Acknowledgements

Built with love by **[@FuckYouHasina](https://t.me/FuckYouHasina)** for the Bengali reading community and the broader [Curious Crew Return](https://t.me/CuriousCrewReturn) family.

This bot exists to make books and knowledge accessible to everyone, everywhere — free and open, always.

> *বই পড়ুন, জ্ঞান বাড়ান।*
> *Read books. Grow your mind.*
