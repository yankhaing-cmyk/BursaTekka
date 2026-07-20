"""
Bursa Malaysia Quant Screener
=============================
Regime-aware daily screener covering the full Bursa main-market universe
(~1000 stocks), filtered to names with average daily traded value > RM 1m.

Data sources : TradingView (tvDatafeed, exchange=MYX)  -> primary
               yfinance (.KL suffix)                    -> fallback
Output       : Telegram daily message (chunked to 4096-char limit)
Scheduler    : GitHub Actions cron + repository_dispatch (Cloudflare /run)

DISCLAIMER: Educational framework only. Not investment advice.
"""

import os
import sys
import time
import json
import math
import logging
import datetime as dt
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
log = logging.getLogger("bursa-screener")

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
CONFIG = {
    "MIN_ADV_RM": 1_000_000,        # min 20-day average daily traded VALUE (RM)
    "ADV_WINDOW": 20,               # days for ADV calc
    "HISTORY_BARS": 260,            # ~1 trading year of daily bars
    # Momentum windows: list of (label, lookback_days, skip_days).
    # 6-month is the proven core; the shorter ones are experimental/fast.
    # Trim or extend this list freely — everything downstream adapts.
    "MOMENTUM_WINDOWS": [
        ("6m", 126, 21),   # PRIMARY (classic 12-1 style skip)
        ("1m", 21, 0),     # 1 month
        ("2w", 10, 0),     # 2 weeks
        ("1w", 5, 0),      # 1 week
    ],
    "REGIME_MOMENTUM_KEY": "6m",   # which window the BULL headline uses
    "SHORT_REVERSION_WINDOW": 5,    # 5-day z-score for mean reversion
    "REVERSION_ZSCORE": -2.0,       # oversold threshold
    "LOW_VOL_WINDOW": 60,           # realized vol window for defensive screen
    "TOP_N": 30,                    # names shown per bucket in Telegram
    "BATCH_PAUSE_SEC": 0.35,        # throttle between symbol fetches
    "MAX_WORKERS_YF": 8,            # yfinance concurrent threads
    "UNIVERSE_FILE": "data/universe.csv",
    "CACHE_DIR": "data/cache",
    "KLCI_TV": "FBMKLCI",           # TradingView symbol for the index
    "KLCI_YF": "^KLSE",             # yfinance symbol for the index
}

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TV_USERNAME = os.environ.get("TV_USERNAME", "")   # optional, raises TV limits
TV_PASSWORD = os.environ.get("TV_PASSWORD", "")


# ----------------------------------------------------------------------------
# Universe: load ~1000 Bursa symbols
# ----------------------------------------------------------------------------
def load_universe() -> pd.DataFrame:
    """
    Load the Bursa universe from data/universe.csv.
    Expected columns: code, name, tv_symbol, yf_symbol, sector
      code      : Bursa stock code, e.g. 1155
      tv_symbol : TradingView symbol on MYX, e.g. MAYBANK
      yf_symbol : Yahoo symbol, e.g. 1155.KL
    If the file is missing, attempt to build it from Bursa's public
    equity list via the fallback builder.
    """
    path = CONFIG["UNIVERSE_FILE"]
    if os.path.exists(path):
        df = pd.read_csv(path, dtype={"code": str})
        log.info("Universe loaded: %d symbols", len(df))
        return df

    log.warning("universe.csv not found — building from remote source")
    df = build_universe_remote()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    return df


def build_universe_remote() -> pd.DataFrame:
    """
    Best-effort universe builder. Pulls the Malaysian equity list from
    TradingView's public scanner API (no login needed), which covers the
    whole MYX exchange. Falls back to a minimal hardcoded core list.
    """
    try:
        url = "https://scanner.tradingview.com/malaysia/scan"
        payload = {
            "filter": [{"left": "type", "operation": "equal", "right": "stock"}],
            "options": {"lang": "en"},
            "columns": ["name", "description", "close", "volume", "sector"],
            "range": [0, 1500],
        }
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        rows = r.json().get("data", [])
        recs = []
        for row in rows:
            full = row.get("s", "")            # e.g. "MYX:MAYBANK"
            d = row.get("d", [])
            if not full.startswith("MYX:"):
                continue
            tv_sym = full.split(":", 1)[1]
            name = d[1] if len(d) > 1 else tv_sym
            sector = d[4] if len(d) > 4 else ""
            recs.append({
                "code": "",                    # filled manually if needed
                "name": name,
                "tv_symbol": tv_sym,
                "yf_symbol": "",               # resolved lazily via search
                "sector": sector,
            })
        df = pd.DataFrame(recs).drop_duplicates("tv_symbol")
        log.info("Universe built from TradingView scanner: %d symbols", len(df))
        return df
    except Exception as e:
        log.error("Remote universe build failed (%s); using core fallback", e)
        core = [
            ("1155", "MAYBANK", "MAYBANK", "1155.KL", "Financials"),
            ("1023", "CIMB", "CIMB", "1023.KL", "Financials"),
            ("1295", "PBBANK", "PBBANK", "1295.KL", "Financials"),
            ("5225", "IHH", "IHH", "5225.KL", "Healthcare"),
            ("6033", "PETGAS", "PETGAS", "6033.KL", "Energy"),
            ("5347", "TENAGA", "TENAGA", "5347.KL", "Utilities"),
            ("4863", "TM", "TM", "4863.KL", "Telco"),
            ("4197", "SIME", "SIME", "4197.KL", "Industrials"),
            ("2445", "KLK", "KLK", "2445.KL", "Plantation"),
            ("8869", "PMETAL", "PMETAL", "8869.KL", "Materials"),
        ]
        return pd.DataFrame(core, columns=["code", "name", "tv_symbol", "yf_symbol", "sector"])


# ----------------------------------------------------------------------------
# Data layer: TradingView primary, yfinance fallback
# ----------------------------------------------------------------------------
class DataFeed:
    def __init__(self):
        self.tv = None
        self._init_tv()

    def _init_tv(self):
        try:
            from tvDatafeed import TvDatafeed
            if TV_USERNAME and TV_PASSWORD:
                self.tv = TvDatafeed(TV_USERNAME, TV_PASSWORD)
            else:
                self.tv = TvDatafeed()  # anonymous, lower rate limits
            log.info("TradingView feed initialised")
        except Exception as e:
            log.warning("tvDatafeed unavailable (%s) — yfinance only", e)
            self.tv = None

    # ---- primary --------------------------------------------------------
    def fetch_tv(self, tv_symbol: str, bars: int) -> pd.DataFrame | None:
        if self.tv is None or not tv_symbol:
            return None
        try:
            from tvDatafeed import Interval
            df = self.tv.get_hist(
                symbol=tv_symbol, exchange="MYX",
                interval=Interval.in_daily, n_bars=bars,
            )
            if df is None or df.empty:
                return None
            df = df.rename(columns=str.lower)[["open", "high", "low", "close", "volume"]]
            df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
            return df
        except Exception as e:
            log.debug("TV fetch failed %s: %s", tv_symbol, e)
            return None

    # ---- fallback -------------------------------------------------------
    def fetch_yf(self, yf_symbol: str, bars: int) -> pd.DataFrame | None:
        if not yf_symbol:
            return None
        try:
            import yfinance as yf
            period_days = int(bars * 1.6)  # calendar padding for weekends/holidays
            df = yf.Ticker(yf_symbol).history(
                period=f"{period_days}d", interval="1d", auto_adjust=False,
            )
            if df is None or df.empty:
                return None
            df = df.rename(columns=str.lower)[["open", "high", "low", "close", "volume"]]
            df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
            return df.tail(bars)
        except Exception as e:
            log.debug("YF fetch failed %s: %s", yf_symbol, e)
            return None

    def fetch(self, row: pd.Series, bars: int) -> pd.DataFrame | None:
        df = self.fetch_tv(row.get("tv_symbol", ""), bars)
        if df is not None and len(df) >= 60:
            return df
        return self.fetch_yf(row.get("yf_symbol", ""), bars)


# ----------------------------------------------------------------------------
# Regime detection
# ----------------------------------------------------------------------------
@dataclass
class Regime:
    label: str
    score: float
    details: dict = field(default_factory=dict)


def detect_regime(feed: DataFeed, breadth_pct_above_ma: float | None) -> Regime:
    """
    Simple, parsimonious regime score in [-2, +2]:
      +1 if KLCI 20d MA > 60d MA (trend up), -1 if below
      +0.5 / -0.5 for 20d realized vol below/above its 1y median
      +0.5 / -0.5 for market breadth (>55% / <45% of names above 50d MA)
    Labels: BULL (>=1), BEAR (<=-1), SIDEWAYS otherwise.
    """
    idx = feed.fetch_tv(CONFIG["KLCI_TV"], CONFIG["HISTORY_BARS"])
    if idx is None:
        idx = feed.fetch_yf(CONFIG["KLCI_YF"], CONFIG["HISTORY_BARS"])
    if idx is None or len(idx) < 120:
        return Regime("UNKNOWN", 0.0, {"error": "no index data"})

    close = idx["close"]
    ma20, ma60 = close.rolling(20).mean().iloc[-1], close.rolling(60).mean().iloc[-1]
    ret = close.pct_change()
    vol20 = ret.rolling(20).std().iloc[-1] * math.sqrt(252)
    vol_med = (ret.rolling(20).std() * math.sqrt(252)).median()

    score = 0.0
    score += 1.0 if ma20 > ma60 else -1.0
    score += 0.5 if vol20 < vol_med else -0.5
    if breadth_pct_above_ma is not None:
        if breadth_pct_above_ma > 55:
            score += 0.5
        elif breadth_pct_above_ma < 45:
            score -= 0.5

    label = "BULL" if score >= 1 else "BEAR" if score <= -1 else "SIDEWAYS"
    return Regime(label, score, {
        "klci_close": round(float(close.iloc[-1]), 2),
        "ma20_gt_ma60": bool(ma20 > ma60),
        "vol20_ann": round(float(vol20) * 100, 1),
        "breadth_pct": None if breadth_pct_above_ma is None else round(breadth_pct_above_ma, 1),
    })


# ----------------------------------------------------------------------------
# Per-stock metrics
# ----------------------------------------------------------------------------
def compute_metrics(df: pd.DataFrame) -> dict | None:
    c = CONFIG
    if df is None or len(df) < 130:
        return None
    close, vol = df["close"], df["volume"]

    adv_rm = float((close * vol).tail(c["ADV_WINDOW"]).mean())
    if not np.isfinite(adv_rm):
        return None

    # Momentum for each configured window -> keys like "momentum_6m"
    mom_vals = {}
    for label, lb, skip in c["MOMENTUM_WINDOWS"]:
        if len(close) > lb + skip + 1:
            mom_vals[f"momentum_{label}"] = float(
                close.iloc[-1 - skip] / close.iloc[-1 - skip - lb] - 1
            )
        else:
            mom_vals[f"momentum_{label}"] = np.nan

    # 5-day z-score vs 60-day distribution (mean reversion)
    r5 = close.pct_change(c["SHORT_REVERSION_WINDOW"])
    mu, sd = r5.rolling(60).mean().iloc[-1], r5.rolling(60).std().iloc[-1]
    z5 = float((r5.iloc[-1] - mu) / sd) if sd and np.isfinite(sd) and sd > 0 else np.nan

    # Realized vol (defensive screen)
    rv = float(close.pct_change().tail(c["LOW_VOL_WINDOW"]).std() * math.sqrt(252))

    # Trend participation for breadth
    ma50 = close.rolling(50).mean().iloc[-1]
    above_ma50 = bool(close.iloc[-1] > ma50) if np.isfinite(ma50) else False

    ret_1d = float(close.iloc[-1] / close.iloc[-2] - 1) if len(close) > 1 else np.nan

    metrics = {
        "price": round(float(close.iloc[-1]), 3),
        "ret_1d": ret_1d,
        "adv_rm": adv_rm,
        "zscore_5d": z5,
        "realized_vol": rv,
        "above_ma50": above_ma50,
    }
    metrics.update(mom_vals)
    return metrics


# ----------------------------------------------------------------------------
# Scan
# ----------------------------------------------------------------------------
def scan(universe: pd.DataFrame, feed: DataFeed) -> pd.DataFrame:
    rows, n = [], len(universe)
    for i, (_, stk) in enumerate(universe.iterrows(), 1):
        df = feed.fetch(stk, CONFIG["HISTORY_BARS"])
        m = compute_metrics(df)
        if m:
            m["name"] = stk["name"]
            m["tv_symbol"] = stk.get("tv_symbol", "")
            m["sector"] = stk.get("sector", "")
            rows.append(m)
        if i % 50 == 0:
            log.info("Scanned %d/%d (%d valid)", i, n, len(rows))
        time.sleep(CONFIG["BATCH_PAUSE_SEC"])

    res = pd.DataFrame(rows)
    if res.empty:
        return res
    liquid = res[res["adv_rm"] >= CONFIG["MIN_ADV_RM"]].copy()
    log.info("Liquidity filter: %d/%d pass ADV >= RM%,.0f",
             len(liquid), len(res), CONFIG["MIN_ADV_RM"])
    return liquid


# ----------------------------------------------------------------------------
# Signal buckets
# ----------------------------------------------------------------------------
def build_buckets(liquid: pd.DataFrame, regime: Regime) -> dict:
    n = CONFIG["TOP_N"]
    out = {}

    # One ranked bucket per momentum window, keyed "mom_<label>"
    for label, _lb, _skip in CONFIG["MOMENTUM_WINDOWS"]:
        col = f"momentum_{label}"
        out[f"mom_{label}"] = (
            liquid.dropna(subset=[col]).sort_values(col, ascending=False).head(n)
        )

    defense = liquid.dropna(subset=["realized_vol"]).sort_values("realized_vol").head(n)
    out["defensive"] = defense

    mr = liquid.dropna(subset=["zscore_5d"])
    out["oversold"] = mr[mr["zscore_5d"] <= CONFIG["REVERSION_ZSCORE"]] \
        .sort_values("zscore_5d").head(n)

    # Regime-weighted headline picks
    reg_key = f"mom_{CONFIG['REGIME_MOMENTUM_KEY']}"
    if regime.label == "BULL":
        out["primary"], out["primary_label"] = out[reg_key], "Momentum (bull regime)"
    elif regime.label == "BEAR":
        out["primary"], out["primary_label"] = out["defensive"], "Low-vol defensive (bear regime)"
    else:
        out["primary"], out["primary_label"] = out["oversold"], "Oversold reversion (sideways regime)"
    return out


# ----------------------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------------------
def fmt_rm(x: float) -> str:
    return f"RM{x/1e6:.1f}m" if x >= 1e6 else f"RM{x/1e3:.0f}k"


def build_message(regime: Regime, buckets: dict, universe_size: int, liquid_size: int) -> str:
    today = dt.date.today().strftime("%a %d %b %Y")
    emoji = {"BULL": "🟢", "BEAR": "🔴", "SIDEWAYS": "🟡", "UNKNOWN": "⚪"}[regime.label]
    d = regime.details

    lines = [
        f"📊 *Bursa Quant Screener* — {today}",
        f"{emoji} Regime: *{regime.label}* (score {regime.score:+.1f})",
        f"KLCI {d.get('klci_close','?')} | 20d vol {d.get('vol20_ann','?')}% | "
        f"breadth {d.get('breadth_pct','?')}%",
        f"Universe {universe_size} → liquid (ADV≥RM1m): {liquid_size}",
        "",
        f"⭐ *{buckets['primary_label']}*",
    ]

    def block(df: pd.DataFrame, metric: str, fmt) -> list[str]:
        if df.empty:
            return ["  _none today_"]
        return [
            f"  `{r['tv_symbol']:<9}` {fmt(r[metric])}  ADV {fmt_rm(r['adv_rm'])}"
            for _, r in df.iterrows()
        ]

    reg_mom_col = f"momentum_{CONFIG['REGIME_MOMENTUM_KEY']}"
    lines += block(buckets["primary"],
                   reg_mom_col if regime.label == "BULL"
                   else "realized_vol" if regime.label == "BEAR" else "zscore_5d",
                   (lambda v: f"{v:+.1%}") if regime.label == "BULL"
                   else (lambda v: f"vol {v:.0%}") if regime.label == "BEAR"
                   else (lambda v: f"z {v:.1f}"))

    # One momentum block per configured window
    core_key = CONFIG["REGIME_MOMENTUM_KEY"]
    window_names = {"6m": "6 months", "3m": "3 months", "1m": "1 month",
                    "2w": "2 weeks", "1w": "1 week"}
    for label, _lb, _skip in CONFIG["MOMENTUM_WINDOWS"]:
        tag = "core" if label == core_key else "fast/experimental"
        icon = "🚀" if label == core_key else "⚡"
        pretty = window_names.get(label, label)
        lines += ["", f"{icon} *Top momentum ({pretty} — {tag})*"]
        lines += block(buckets[f"mom_{label}"], f"momentum_{label}", lambda v: f"{v:+.1%}")

    lines += ["", "🛡 *Lowest realized vol*"]
    lines += block(buckets["defensive"], "realized_vol", lambda v: f"{v:.0%}")
    lines += ["", "↩️ *Oversold (5d z ≤ -2)*"]
    lines += block(buckets["oversold"], "zscore_5d", lambda v: f"z {v:.1f}")
    lines += ["", "_Educational screener — not investment advice._"]
    return "\n".join(lines)


def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing — printing instead\n%s", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # chunk to Telegram's 4096-char limit, splitting on newlines
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > 3900:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    chunks.append(cur)
    for chunk in chunks:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "Markdown",
        }, timeout=30)
        if not r.ok:
            log.error("Telegram send failed: %s", r.text)
        time.sleep(1)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    log.info("=== Bursa Quant Screener starting ===")
    universe = load_universe()
    feed = DataFeed()

    liquid = scan(universe, feed)
    if liquid.empty:
        send_telegram("⚠️ Bursa screener: no data retrieved today (both feeds failed).")
        sys.exit(1)

    breadth = 100.0 * liquid["above_ma50"].mean()
    regime = detect_regime(feed, breadth)
    log.info("Regime: %s (%.1f) %s", regime.label, regime.score, regime.details)

    buckets = build_buckets(liquid, regime)
    msg = build_message(regime, buckets, len(universe), len(liquid))
    send_telegram(msg)

    # persist snapshot for audit/backtest
    os.makedirs("data/snapshots", exist_ok=True)
    liquid.to_csv(f"data/snapshots/{dt.date.today().isoformat()}.csv", index=False)
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
