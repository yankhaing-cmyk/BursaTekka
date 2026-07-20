# Bursa Malaysia Quant Screener

Regime-aware daily screener for the full Bursa universe (~1000 stocks),
filtered to names with **20-day average daily traded value ≥ RM 1 million**.

- **Data**: TradingView (`tvDatafeed`, exchange `MYX`) primary → `yfinance` (`.KL`) fallback
- **Signals**: regime detection (KLCI trend + vol + breadth), 6-month momentum,
  low-volatility defensive screen, 5-day oversold mean-reversion
- **Delivery**: daily Telegram message after Bursa close
- **Scheduling**: GitHub Actions cron (17:30 MYT, Mon–Fri)
- **On-demand**: Cloudflare Worker `/run` endpoint (URL or `/run` in Telegram chat)

> ⚠️ Educational framework only — not investment advice. Backtest before trusting anything.

---

## Architecture

```
Telegram "/run"  ──►  Cloudflare Worker  ──►  GitHub repository_dispatch
                                                     │
GitHub cron (17:30 MYT weekdays) ────────────────────┤
                                                     ▼
                                          GitHub Actions runs screener.py
                                          TradingView ──fallback──► yfinance
                                                     │
                                                     ▼
                                          Telegram daily message + CSV snapshot
```

## 1. GitHub setup

1. Create a repo and push these files.
2. Repo → **Settings → Secrets and variables → Actions**, add:
   | Secret | Value |
   |---|---|
   | `TELEGRAM_BOT_TOKEN` | from @BotFather |
   | `TELEGRAM_CHAT_ID` | your chat/group ID (get via @userinfobot) |
   | `TV_USERNAME` | *(optional)* TradingView login — raises rate limits |
   | `TV_PASSWORD` | *(optional)* |
3. Repo → **Settings → Actions → General → Workflow permissions** →
   enable **Read and write** (needed for the snapshot commit step).
4. Test: **Actions → Bursa Daily Screener → Run workflow**.

The cron `30 9 * * 1-5` = 17:30 MYT. GitHub cron can drift 5–15 min; that's fine post-close.

## 2. Universe (~1000 stocks)

On first run, if `data/universe.csv` has only the 50-name starter list, the
screener calls TradingView's public scanner API to pull the **entire MYX
exchange (~1000 listed stocks)** and saves it back to `data/universe.csv`,
which then gets committed by the workflow. To force a rebuild, delete the file.
You can also hand-edit the CSV (columns: `code,name,tv_symbol,yf_symbol,sector`).

The RM 1m ADV filter is applied at scan time, so the message only ever shows
tradeable names — typically 200–350 of the ~1000 pass on a normal day.

## 3. Telegram bot

1. Message **@BotFather** → `/newbot` → copy the token.
2. Start a chat with your bot (or add it to a group), send any message.
3. Get the chat ID: `https://api.telegram.org/bot<TOKEN>/getUpdates`
   → `"chat":{"id": ...}`.

## 4. Cloudflare Worker (the `/run` trigger)

```bash
npm i -g wrangler
wrangler login
wrangler deploy                       # deploys cloudflare-worker.js

wrangler secret put GH_TOKEN          # GitHub PAT (repo scope / Actions RW)
wrangler secret put GH_OWNER          # e.g. yourname
wrangler secret put GH_REPO           # e.g. bursa-screener
wrangler secret put RUN_KEY           # any long random string
wrangler secret put TG_TOKEN          # optional: bot token for /run replies
```

**Trigger by URL:**
```
https://bursa-screener-trigger.<you>.workers.dev/run?key=<RUN_KEY>
```

**Trigger by Telegram `/run`:** point the bot's webhook at the worker:
```
https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://bursa-screener-trigger.<you>.workers.dev/telegram
```
Then typing `/run` in the bot chat dispatches the workflow and the bot confirms.

## 5. Run locally

```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...
python screener.py
```
Without Telegram credentials it prints the report to stdout instead.

## Tuning

All knobs live in the `CONFIG` dict at the top of `screener.py`:
`MIN_ADV_RM`, momentum lookback/skip, reversion z-threshold, vol window,
`TOP_N` per bucket, and per-symbol throttle (`BATCH_PAUSE_SEC` — raise it if
TradingView anonymous rate limits bite; supplying `TV_USERNAME/PASSWORD` helps).

## Known constraints

- `tvDatafeed` is an **unofficial** TradingView client; anonymous access is
  rate-limited and can break if TradingView changes endpoints — hence the
  yfinance fallback and the throttle.
- ~1000 symbols with throttling takes 30–90 minutes; the workflow timeout is
  set to 120 min. GitHub free tier allows this comfortably on public repos.
- yfinance coverage of Bursa small-caps is patchy; names missing on both
  feeds are silently skipped (they'd fail the liquidity filter anyway).
- Snapshots in `data/snapshots/` give you a growing dataset for backtesting
  the signals later.
