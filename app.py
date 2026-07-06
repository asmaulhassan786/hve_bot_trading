"""
HVE Action — Flask web app
Connects to Alpaca paper trading and provides a dashboard for managing
positions, stop orders, and the daily ticker scan list.
"""

import json
import math
import os
import time
import threading
from collections import deque
from datetime import datetime, timedelta

import certifi
os.environ["SSL_CERT_FILE"]      = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["CURL_CA_BUNDLE"]     = certifi.where()

import pytz
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

ET = pytz.timezone("America/New_York")

ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_DATA_URL = os.environ.get("ALPACA_DATA_URL", "https://data.alpaca.markets")
ALPACA_API_KEY  = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET   = os.environ.get("ALPACA_SECRET", "")
CRON_SECRET     = os.environ.get("CRON_SECRET", "")   # set this in Render env vars
FMP_API_KEY     = os.environ.get("FMP_API_KEY", "")    # financialmodelingprep.com key — market cap lookups

DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
TICKERS_FILE = os.path.join(DATA_DIR, "tickers.json")
LOG_FILE     = os.path.join(DATA_DIR, "activity.json")
POSITION_META_FILE  = os.path.join(DATA_DIR, "position_meta.json")
STOP_FILLS_FILE     = os.path.join(DATA_DIR, "stop_fills_seen.json")
WATCHLIST_FILE      = os.path.join(DATA_DIR, "watchlist.json")
WATCHLIST_HIST_FILE = os.path.join(DATA_DIR, "watchlist_history.json")

os.makedirs(DATA_DIR, exist_ok=True)

DEFAULT_BUY_AMOUNT = 500   # used when no buy_amount is saved yet; user-configurable via UI
MOVERS_MIN_PRICE    = 7    # minimum price for an Alpaca mover to be HVE-eligible
MOVERS_MIN_CHANGE_PCT = 10 # minimum intraday % change for an Alpaca mover to be HVE-eligible
SNAPSHOT_BATCH_SIZE  = 200  # symbols per /v2/stocks/snapshots request
MIN_TODAY_VOLUME_FOR_MOVER = 5000  # sanity floor so a single stale/thin extended-hours print can't fake a "10% gainer"
MIN_IPO_AGE_DAYS    = 28    # stock must have been trading at least this many days (4 weeks)
ATR_STOP_MULT       = 1.2  # initial stop = entry - 1.2x ATR(14)
ATR_BREAKEVEN_MULT  = 1.2  # move stop to breakeven once price is +1.2x ATR(14) above entry
BREAKEVEN_POLL_MIN   = 2   # how often to check for the breakeven trigger during market hours
LOG_RETENTION_DAYS   = 15  # keep this many calendar days of activity log (covers ~10 trading days)
LOG_MAX_ENTRIES      = 5000  # hard safety cap in case of runaway logging within the window

activity_log = deque(maxlen=LOG_MAX_ENTRIES)
_log_lock = threading.Lock()

scheduler = BackgroundScheduler(timezone=ET)


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str, level: str = "info"):
    now = datetime.now(ET)
    entry = {"date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S ET"), "msg": msg, "level": level}
    with _log_lock:
        activity_log.appendleft(entry)
    try:
        existing = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE) as f:
                existing = json.load(f)
        existing.insert(0, entry)
        # Retain by calendar day (covers ~10 trading days), not a fixed count,
        # so the timeline survives restarts/redeploys instead of resetting.
        cutoff = (now - timedelta(days=LOG_RETENTION_DAYS)).strftime("%Y-%m-%d")
        existing = [e for e in existing if e.get("date", "") >= cutoff][:LOG_MAX_ENTRIES]
        with open(LOG_FILE, "w") as f:
            json.dump(existing, f)
    except Exception:
        pass


def load_log():
    try:
        with open(LOG_FILE) as f:
            return json.load(f)
    except Exception:
        return list(activity_log)


# ── Alpaca helpers ────────────────────────────────────────────────────────────

def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }


def alpaca_get(path):
    r = requests.get(f"{ALPACA_BASE_URL}{path}", headers=alpaca_headers(), timeout=10)
    return r.json()


def alpaca_post(path, payload):
    r = requests.post(
        f"{ALPACA_BASE_URL}{path}", json=payload,
        headers=alpaca_headers(), timeout=10,
    )
    return r.json()


def alpaca_delete(path):
    r = requests.delete(f"{ALPACA_BASE_URL}{path}", headers=alpaca_headers(), timeout=10)
    return r.status_code


# ── Ticker storage ────────────────────────────────────────────────────────────

def load_tickers() -> dict:
    try:
        with open(TICKERS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"tickers": [], "schedule": "15:50", "mode": "automated", "buy_amount": DEFAULT_BUY_AMOUNT}


def save_tickers(data: dict):
    with open(TICKERS_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Position metadata (entry ATR, breakeven state) ──────────────────────────────
# Alpaca's own /positions doesn't know the ATR a stop was sized from, so this
# tracks per-symbol {"entry_atr": ..., "breakeven_triggered": ...} locally.

def load_position_meta() -> dict:
    try:
        with open(POSITION_META_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_position_meta(meta: dict):
    with open(POSITION_META_FILE, "w") as f:
        json.dump(meta, f, indent=2)


# ── Position + stop merge ─────────────────────────────────────────────────────

def fetch_positions_with_stops():
    positions = alpaca_get("/positions")
    open_orders = alpaca_get("/orders?status=open&limit=100")

    stops = {}
    if isinstance(open_orders, list):
        for o in open_orders:
            if o.get("type") in ("stop", "stop_limit") and o.get("side") == "sell":
                stops[o["symbol"]] = {
                    "id": o["id"],
                    "stop_price": o.get("stop_price"),
                    "status": o.get("status"),
                    "order_type": o.get("type"),
                }

    result = []
    if isinstance(positions, list):
        for p in positions:
            sym    = p["symbol"]
            entry  = float(p["avg_entry_price"])
            current = float(p["current_price"])
            pnl_pct = (current - entry) / entry * 100
            stop   = stops.get(sym)

            stop_label = None
            if stop and stop.get("stop_price"):
                sp = float(stop["stop_price"])
                if abs(sp - entry) / entry < 0.005:
                    stop_label = "Break-even"
                elif sp < entry:
                    pct_below = (entry - sp) / entry * 100
                    stop_label = f"{round(pct_below, 1)}% below entry"
                else:
                    stop_label = "Custom"

            result.append({
                "symbol":          sym,
                "qty":             float(p["qty"]),
                "entry_price":     round(entry, 4),
                "current_price":   round(current, 4),
                "market_value":    round(float(p["market_value"]), 2),
                "unrealized_pl":   round(float(p["unrealized_pl"]), 2),
                "unrealized_plpc": round(pnl_pct, 2),
                "side":            p["side"],
                "stop":            stop,
                "stop_label":      stop_label,
            })

    synced_at = datetime.now(ET).strftime("%b %d, %Y  %I:%M:%S %p ET")
    return {"positions": result, "synced_at": synced_at}


# ── Scan logic ────────────────────────────────────────────────────────────────

def compute_atr14(daily) -> float:
    """
    14-period ATR from a daily OHLC DataFrame (needs >=15 completed bars).
    Excludes the last row, which is today's still-forming bar during market
    hours — same convention as the 2-year volume comparison below.
    """
    d = daily.iloc[:-1]
    highs, lows, closes = d["High"].tolist(), d["Low"].tolist(), d["Close"].tolist()
    if len(closes) < 15:
        return None
    trs = [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, len(closes))
    ]
    return sum(trs[-14:]) / 14


def get_quote_and_volume(ticker: str):
    """
    Fetch intraday range, 2-year max volume, and 14-day ATR using yfinance
    (full SIP market volume). SSL env vars are set at module load so this
    works on both Linux (Render) and Windows.
    Returns None if data is unavailable or insufficient.
    """
    try:
        t        = yf.Ticker(ticker)
        intraday = t.history(period="1d", interval="1m")
        if intraday.empty:
            return None

        day_low   = float(intraday["Low"].min())
        day_high  = float(intraday["High"].max())
        price     = float(intraday["Close"].iloc[-1])
        today_vol = int(intraday["Volume"].sum())

        daily = t.history(period="2y", interval="1d")
        if len(daily) < 2:
            return None

        # Exclude today so we compare against historical max only
        max_2y_vol = int(daily["Volume"].iloc[:-1].max())

        atr14 = compute_atr14(daily)
        if atr14 is None:
            return None

        # Earliest bar in the 2y window doubles as an IPO-age proxy: a
        # recent IPO simply has no data before its first trading day, so
        # this reflects true listing age (not just capped by the 2y window)
        # for any stock younger than 2 years.
        first_bar_date = daily.index[0]
        if hasattr(first_bar_date, "date"):
            first_bar_date = first_bar_date.date()
        age_days = (datetime.now(ET).date() - first_bar_date).days

        return {
            "ticker":    ticker,
            "price":     round(price, 2),
            "low":       round(day_low, 2),
            "high":      round(day_high, 2),
            "today_vol": today_vol,
            "max_2y_vol": max_2y_vol,
            "atr14":     atr14,
            "age_days":  age_days,
        }
    except Exception:
        return None


def position_in_range(price, low, high):
    rng = high - low
    return (price - low) / rng if rng > 0 else 0.0


def place_stop_order_internal(ticker, qty, stop_price):
    payload = {
        "symbol":        ticker,
        "qty":           str(int(qty)),
        "side":          "sell",
        "type":          "stop",
        "stop_price":    str(round(stop_price, 2)),
        "time_in_force": "gtc",
    }
    return alpaca_post("/orders", payload)


def place_buy_order(ticker, qty):
    payload = {
        "symbol":        ticker,
        "qty":           str(int(qty)),
        "side":          "buy",
        "type":          "market",
        "time_in_force": "day",
    }
    return alpaca_post("/orders", payload)


def wait_for_fill(order_id, timeout_sec=120):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        order = alpaca_get(f"/orders/{order_id}")
        status = order.get("status", "")
        if status == "filled":
            return order
        if status in ("cancelled", "expired", "rejected"):
            return None
        time.sleep(5)
    return None


def is_trading_day() -> bool:
    """Check Alpaca's market calendar to confirm today is a trading day."""
    try:
        today = datetime.now(ET).strftime("%Y-%m-%d")
        r = requests.get(
            f"{ALPACA_BASE_URL}/calendar",
            params={"start": today, "end": today},
            headers=alpaca_headers(),
            timeout=10,
        )
        calendar = r.json()
        if isinstance(calendar, list) and len(calendar) > 0:
            return calendar[0].get("date") == today
        return False
    except Exception as e:
        log(f"Calendar check failed: {e} — assuming trading day", "warn")
        return True  # fail open so scan isn't silently skipped


def is_market_open() -> bool:
    now = datetime.now(ET)
    return now.weekday() < 5 and (9, 30) <= (now.hour, now.minute) < (16, 0)


def get_latest_trade_price(ticker: str) -> float:
    try:
        r = requests.get(
            f"{ALPACA_DATA_URL}/v2/stocks/{ticker}/trades/latest",
            headers=alpaca_headers(), timeout=10,
        )
        return float(r.json()["trade"]["p"])
    except Exception:
        try:
            hist = yf.Ticker(ticker).history(period="1d", interval="1m")
            return float(hist["Close"].iloc[-1]) if not hist.empty else 0.0
        except Exception:
            return 0.0


def check_breakeven_stops():
    """
    Runs every BREAKEVEN_POLL_MIN minutes during market hours. For each open
    position that hasn't hit breakeven yet, checks whether price has risen
    ATR_BREAKEVEN_MULT x ATR(14) above entry — if so, cancels the existing
    stop and replaces it with one at entry price (breakeven).
    """
    if not is_market_open():
        return

    meta = load_position_meta()
    if not meta:
        return

    positions_raw = alpaca_get("/positions")
    if not isinstance(positions_raw, list):
        log(f"Breakeven check: failed to fetch positions — {positions_raw}", "error")
        return
    alpaca_positions = {p["symbol"]: p for p in positions_raw}
    open_orders = alpaca_get("/orders?status=open&limit=100")
    stop_orders = {}
    if isinstance(open_orders, list):
        for o in open_orders:
            if o.get("type") == "stop" and o.get("side") == "sell":
                stop_orders[o["symbol"]] = o

    changed = False
    for sym in list(meta.keys()):
        m = meta[sym]
        if sym not in alpaca_positions:
            # position closed (stop hit, manual sell, etc.) — drop stale metadata
            del meta[sym]
            changed = True
            continue
        if m.get("breakeven_triggered"):
            continue

        atr = m.get("entry_atr")
        if not atr:
            continue

        entry   = float(alpaca_positions[sym]["avg_entry_price"])
        qty     = float(alpaca_positions[sym]["qty"])
        trigger = entry + ATR_BREAKEVEN_MULT * atr

        current = get_latest_trade_price(sym)
        if current <= 0 or current < trigger:
            continue

        existing_stop = stop_orders.get(sym)
        if existing_stop:
            alpaca_delete(f"/orders/{existing_stop['id']}")

        result = place_stop_order_internal(sym, qty, entry)
        if result.get("id"):
            log(
                f"{sym}: price ${current:.2f} reached breakeven trigger "
                f"${trigger:.2f} (entry ${entry:.2f} + {ATR_BREAKEVEN_MULT}x ATR) "
                f"— stop moved to breakeven @ ${entry:.2f}",
                "ok",
            )
            m["breakeven_triggered"] = True
            changed = True
        else:
            log(f"{sym}: breakeven stop replacement failed — {result.get('message', 'unknown error')}", "error")

    if changed:
        save_position_meta(meta)


def load_stop_fills_seen() -> set:
    try:
        with open(STOP_FILLS_FILE) as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()


def save_stop_fills_seen(seen: set):
    with open(STOP_FILLS_FILE, "w") as f:
        json.dump(list(seen), f)


def load_watchlist() -> dict:
    """
    Returns {symbol: {day0_high, day0_date, atr14, price}} for stocks
    that qualified on Day 0 and are pending a next-day breakout buy.
    """
    try:
        with open(WATCHLIST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_watchlist(wl: dict):
    with open(WATCHLIST_FILE, "w") as f:
        json.dump(wl, f, indent=2)


def load_watchlist_history() -> dict:
    """Returns {date_str: {symbol: {day0_high, day0_price, atr14, outcome, fill_price}}}"""
    try:
        with open(WATCHLIST_HIST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_watchlist_history(hist: dict):
    with open(WATCHLIST_HIST_FILE, "w") as f:
        json.dump(hist, f, indent=2)


def record_watchlist_history(date: str, entries: list):
    """
    Called from run_scan() when qualified tickers are added to the watchlist.
    entries = list of scan result dicts (same as qualified list).
    Each ticker starts with outcome='pending'.
    Prunes dates older than 7 trading days.
    """
    hist = load_watchlist_history()
    if date not in hist:
        hist[date] = {}
    for s in entries:
        tkr = s["ticker"]
        hist[date][tkr] = {
            "day0_high":  round(s["high"], 4),
            "day0_price": round(s["price"], 4),
            "atr14":      round(s["atr14"], 4),
            "outcome":    "pending",
            "fill_price": None,
        }
    # Prune to 7 most-recent dates
    sorted_dates = sorted(hist.keys(), reverse=True)
    for old in sorted_dates[7:]:
        del hist[old]
    save_watchlist_history(hist)


def update_watchlist_outcome(date: str, symbol: str, outcome: str, fill_price: float = None):
    """outcome: 'bought' | 'expired' | 'skipped'"""
    hist = load_watchlist_history()
    if date in hist and symbol in hist[date]:
        hist[date][symbol]["outcome"] = outcome
        if fill_price is not None:
            hist[date][symbol]["fill_price"] = round(fill_price, 4)
        save_watchlist_history(hist)


def get_prev_trading_day(from_date_str: str) -> str:
    """Return the most recent trading day before from_date_str (YYYY-MM-DD)."""
    try:
        from_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
        look_back = (from_dt - timedelta(days=7)).strftime("%Y-%m-%d")
        r = requests.get(
            f"{ALPACA_BASE_URL}/calendar",
            params={"start": look_back, "end": from_date_str},
            headers=alpaca_headers(), timeout=10,
        )
        calendar = r.json()
        if isinstance(calendar, list):
            trading_days = [c["date"] for c in calendar if c["date"] < from_date_str]
            if trading_days:
                return trading_days[-1]
    except Exception:
        pass
    # fallback: last weekday
    from_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
    delta = 1 if from_dt.weekday() != 0 else 3
    return (from_dt - timedelta(days=delta)).strftime("%Y-%m-%d")


def check_stop_fills():
    """
    Polls Alpaca's recently closed orders every 2 minutes for filled stop/stop_limit
    sell orders. Any new fill not yet logged is written to the activity log with
    symbol, qty, fill price, and estimated P&L vs entry.
    """
    try:
        # Fetch orders closed in last 24 hours
        since = (datetime.now(ET) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        orders = alpaca_get(f"/orders?status=closed&limit=100&after={since}")
        if not isinstance(orders, list):
            return

        seen = load_stop_fills_seen()
        changed = False

        for o in orders:
            if o.get("id") in seen:
                continue
            if o.get("type") not in ("stop", "stop_limit"):
                continue
            if o.get("side") != "sell":
                continue
            if o.get("status") != "filled":
                continue

            sym        = o.get("symbol", "?")
            qty        = float(o.get("filled_qty") or o.get("qty") or 0)
            fill_price = float(o.get("filled_avg_price") or o.get("stop_price") or 0)
            stop_price = float(o.get("stop_price") or 0)

            # Estimate P&L from position meta if available; otherwise omit
            meta  = load_position_meta()
            entry = float(meta.get(sym, {}).get("entry_price", 0))
            pl_str = ""
            if entry and fill_price:
                pl     = (fill_price - entry) * qty
                pl_sign = "+" if pl >= 0 else ""
                pl_str  = f" | P&L: {pl_sign}${abs(pl):.2f}"

            log(
                f"STOP HIT (auto) — {sym}: {qty} share(s) filled @ ${fill_price:.2f}"
                f" (stop was ${stop_price:.2f}){pl_str} | Trigger: stop order executed by Alpaca",
                "warn",
            )

            seen.add(o["id"])
            changed = True

        if changed:
            save_stop_fills_seen(seen)

    except Exception as e:
        log(f"check_stop_fills error: {e}", "error")


def check_breakout_entries():
    """
    Runs every minute from 9:30–11:00 AM ET on trading days.
    For each watchlist entry whose day0_date was the previous trading day,
    checks if current price has broken above the Day 0 high.
    Buys on breakout; expires any remaining entries at 11:00 AM.
    """
    now = datetime.now(ET)

    # Only run on trading days, market hours up to 11:00 AM
    if now.weekday() >= 5:
        return
    if not ((9, 30) <= (now.hour, now.minute) <= (11, 0)):
        return

    watchlist = load_watchlist()
    if not watchlist:
        return

    today     = now.strftime("%Y-%m-%d")
    prev_day  = get_prev_trading_day(today)

    # Separate entries that belong to yesterday (actionable) vs older (stale)
    actionable = {sym: v for sym, v in watchlist.items() if v.get("day0_date") == prev_day}
    stale      = {sym: v for sym, v in watchlist.items() if v.get("day0_date") != prev_day}

    if stale:
        for sym in stale:
            d0 = stale[sym].get("day0_date", "")
            log(f"  {sym}: watchlist entry from {d0} expired (>1 day old) — removed", "warn")
            update_watchlist_outcome(d0, sym, "expired")

    if not actionable:
        if stale:
            save_watchlist({})
        return

    # At or after 11:00 AM — expire everything that hasn't triggered
    if now.hour >= 11:
        for sym in actionable:
            log(
                f"  {sym}: 11:00 AM ET passed — no breakout above Day 0 high "
                f"${actionable[sym]['day0_high']:.2f} — watchlist entry expired",
                "warn",
            )
            update_watchlist_outcome(prev_day, sym, "expired")
        save_watchlist({})
        return

    data      = load_tickers()
    buy_amount = data.get("buy_amount", DEFAULT_BUY_AMOUNT)
    existing_positions = {p["symbol"] for p in (alpaca_get("/positions") or [])}
    position_meta = load_position_meta()
    to_remove = list(stale.keys())

    for sym, entry in actionable.items():
        if sym in existing_positions:
            log(f"  {sym}: already in positions — removing from watchlist", "info")
            update_watchlist_outcome(prev_day, sym, "skipped")
            to_remove.append(sym)
            continue

        day0_high = entry["day0_high"]
        current   = get_latest_trade_price(sym)
        if current <= 0:
            log(f"  {sym}: price unavailable, skipping this check", "warn")
            continue

        if current <= day0_high:
            continue  # no breakout yet — check again next minute

        # Breakout confirmed
        log(
            f"  {sym}: BREAKOUT — current ${current:.2f} > Day 0 high ${day0_high:.2f} "
            f"at {now.strftime('%H:%M')} ET — placing buy order",
            "ok",
        )

        qty = math.ceil(buy_amount / current)
        buy = place_buy_order(sym, qty)
        order_id = buy.get("id")
        if not order_id:
            log(f"  {sym}: buy order failed — {buy.get('message', 'unknown error')}", "error")
            to_remove.append(sym)
            continue

        log(f"  {sym}: buy order placed — waiting for fill…", "info")
        filled = wait_for_fill(order_id)
        if filled is None:
            log(f"  {sym}: fill timeout or rejected", "error")
            to_remove.append(sym)
            continue

        fill_price = float(filled["filled_avg_price"])
        fill_qty   = float(filled["filled_qty"])
        atr        = entry["atr14"]
        stop_price = round(fill_price - ATR_STOP_MULT * atr, 2)

        log(f"  {sym}: filled @ ${fill_price:.2f}  qty={int(fill_qty)}  ATR14=${atr:.2f}", "ok")

        stop = place_stop_order_internal(sym, fill_qty, stop_price)
        if stop.get("id"):
            log(f"  {sym}: stop-loss placed @ ${stop_price:.2f} ({ATR_STOP_MULT}x ATR below entry)", "ok")
            position_meta[sym] = {"entry_atr": atr, "entry_price": fill_price, "breakeven_triggered": False}
            save_position_meta(position_meta)
        else:
            log(f"  {sym}: stop placement failed — {stop.get('message', '')}", "error")

        update_watchlist_outcome(prev_day, sym, "bought", fill_price)
        to_remove.append(sym)

    # Prune triggered / stale entries
    if to_remove:
        for sym in to_remove:
            watchlist.pop(sym, None)
        save_watchlist(watchlist)


def fetch_fmp_quotes(symbols: list) -> dict:
    """
    Company profile lookup via Financial Modeling Prep. Returns
    {symbol: {"market_cap": ..., "name": ..., "is_etf": ...}}.
    FMP's real-time "quote" endpoints are paid-plan-only (confirmed via 402
    Restricted Endpoint with a real key); "profile" is fundamentals data
    (updated periodically, not real-time — fine for a >$300M cap check) and
    has historically been free-tier accessible. One request per symbol since
    the bulk-profile endpoint's symbol-list semantics aren't confirmed.
    (yfinance's market-cap lookup depends on Yahoo's cookie/crumb handshake,
    which silently returns None for every symbol when run from Render —
    same class of problem as the original Finviz block, just from Yahoo.)
    """
    if not FMP_API_KEY:
        log("FMP_API_KEY not set — cannot look up market cap", "error")
        return {}
    results = {}
    for sym in symbols:
        try:
            r = requests.get(
                "https://financialmodelingprep.com/stable/profile",
                params={"symbol": sym, "apikey": FMP_API_KEY}, timeout=10,
            )
            if not r.ok:
                log(f"FMP profile lookup failed for {sym}: {r.status_code} {r.text[:200]}", "error")
                continue
            data = r.json()
            if isinstance(data, list):
                data = data[0] if data else None
            if not data:
                continue
            results[sym] = {
                "market_cap": data.get("marketCap") or data.get("mktCap"),
                "name": data.get("companyName") or data.get("name", ""),
                "is_etf": bool(data.get("isEtf") or data.get("isFund")),
            }
        except Exception as e:
            log(f"FMP profile lookup failed for {sym}: {e}", "error")
    return results


_ETF_NAME_HINTS = ("etf", "ishares", "spdr", "proshares", "direxion", "vaneck")


def fetch_top_gainers(min_change_pct=MOVERS_MIN_CHANGE_PCT):
    """
    Find every gainer at or above min_change_pct across the whole tradable
    US equity universe, computed from Alpaca's own snapshot data. Alpaca's
    /v1beta1/screener movers endpoint hard-caps at 50 results server-side
    (no pagination), which silently drops legitimate gainers ranked lower —
    this pulls every active/tradable us_equity asset, snapshots them in
    batches, and computes % change ourselves so nothing above the threshold
    is missed regardless of how many stocks qualify.
    Returns a list shaped like [{"symbol":, "price":, "percent_change":}, ...].
    """
    try:
        r = requests.get(
            f"{ALPACA_BASE_URL}/assets",
            headers=alpaca_headers(),
            params={"status": "active", "asset_class": "us_equity"},
            timeout=30,
        )
        r.raise_for_status()
        assets = r.json()
    except Exception as e:
        log(f"Alpaca assets fetch failed: {e}", "error")
        return []

    if not isinstance(assets, list):
        log(f"Alpaca assets returned unexpected response: {assets}", "error")
        return []

    symbols = [
        a["symbol"] for a in assets
        if a.get("tradable") and a.get("exchange") != "OTC" and a.get("symbol")
    ]
    log(f"Alpaca assets: {len(assets)} total, {len(symbols)} tradable non-OTC equities to snapshot", "info")
    if not symbols:
        return []

    movers = []
    batches_failed = 0
    for i in range(0, len(symbols), SNAPSHOT_BATCH_SIZE):
        batch = symbols[i:i + SNAPSHOT_BATCH_SIZE]
        try:
            r = requests.get(
                f"{ALPACA_DATA_URL}/v2/stocks/snapshots",
                headers=alpaca_headers(),
                params={"symbols": ",".join(batch)},
                timeout=15,
            )
            r.raise_for_status()
            snapshots = r.json()
        except Exception as e:
            batches_failed += 1
            log(f"Snapshot batch {i // SNAPSHOT_BATCH_SIZE + 1} failed ({len(batch)} symbols): {e}", "warn")
            continue

        if not isinstance(snapshots, dict):
            batches_failed += 1
            continue

        for sym, snap in snapshots.items():
            if not isinstance(snap, dict):
                continue
            daily_bar = snap.get("dailyBar") or {}
            # Prefer the regular-session daily bar's close over the latest
            # trade: latestTrade can drift to a stray after-hours print once
            # the session ends, which silently understates (or fakes) the
            # day's real % change depending on when the scan happens to run.
            # dailyBar.c stays pinned to the actual regular-session close.
            price = daily_bar.get("c") or (snap.get("latestTrade") or {}).get("p")
            prev_close = (snap.get("prevDailyBar") or {}).get("c")
            today_volume = daily_bar.get("v", 0)
            if not price or not prev_close:
                continue
            # A single stale/thin extended-hours trade print can make an
            # illiquid ticker look like a double-digit gainer with zero real
            # trading behind it. Require actual volume today before trusting
            # the computed % change — this is a sanity floor, not the real
            # HVE volume check (that compares against 2yr history later).
            if not today_volume or today_volume < MIN_TODAY_VOLUME_FOR_MOVER:
                continue
            pct_change = (price - prev_close) / prev_close * 100
            if pct_change >= min_change_pct:
                movers.append({"symbol": sym, "price": price, "percent_change": pct_change})

    if batches_failed:
        log(f"{batches_failed} snapshot batch(es) failed and were skipped", "warn")
    if not movers:
        log(f"No gainers found at or above {min_change_pct}% with real trading volume today", "warn")
        return []

    movers.sort(key=lambda m: m["percent_change"], reverse=True)
    return movers


def fetch_alpaca_movers():
    """
    Pull today's top gainers (see fetch_top_gainers), strip out
    warrants/rights and ETFs/funds first, then apply the same coarse filter
    Finviz used to: market cap >$300M, price >$MOVERS_MIN_PRICE,
    change >MOVERS_MIN_CHANGE_PCT%.
    """
    today = datetime.now(ET).strftime("%Y-%m-%d")
    log(
        f"Fetching movers from Alpaca for {today} "
        f"(mktcap >$300M, price >${MOVERS_MIN_PRICE}, change >{MOVERS_MIN_CHANGE_PCT}%)…",
        "info",
    )

    gainers = fetch_top_gainers()
    if not gainers:
        log("No gainers computed from Alpaca snapshots", "warn")
        return []

    log(
        f"{len(gainers)} gainer(s) ≥{MOVERS_MIN_CHANGE_PCT}% found across full market snapshot: "
        + ", ".join(f"{g.get('symbol')}(${g.get('price')}, {g.get('percent_change'):.2f}%)" for g in gainers[:20])
        + ("…" if len(gainers) > 20 else ""),
        "info",
    )

    symbols = [g["symbol"] for g in gainers if g.get("symbol")]
    if not symbols:
        log("Alpaca movers returned no symbols", "warn")
        return []

    # ── Cheap filters first: suffix-based warrants, then price/change ───────
    # Both use data already in hand (ticker string / the snapshot itself) —
    # no API calls. Doing these before the FMP-dependent checks below matters
    # because FMP's free tier has a real per-scan rate limit: with the full
    # market snapshot now returning hundreds of >=10% gainers on a volatile
    # day, looking up all of them before narrowing by price burns through
    # that quota on symbols that were always going to fail the $7 floor
    # anyway (this is exactly what caused a wave of "429 Limit Reach" errors).
    non_suffix_warrants = [s for s in symbols if s.split(".")[-1] not in ("WS", "WT")]
    dropped_suffix = [s for s in symbols if s not in non_suffix_warrants]
    if dropped_suffix:
        log(f"Dropped warrant/rights ticker(s) by suffix: {', '.join(dropped_suffix)}", "info")
    if not non_suffix_warrants:
        log("Nothing left after warrant-suffix filter", "warn")
        return []

    gainers_by_symbol = {g["symbol"]: g for g in gainers}
    price_change_ok = [
        s for s in non_suffix_warrants
        if gainers_by_symbol[s].get("price", 0) > MOVERS_MIN_PRICE
        and gainers_by_symbol[s].get("percent_change", 0) > MOVERS_MIN_CHANGE_PCT
    ]
    if not price_change_ok:
        log("No candidates matching price/change criteria", "warn")
        return []
    log(f"{len(price_change_ok)} candidate(s) passed price/change filter: {', '.join(price_change_ok)}", "info")

    # No-separator warrants (e.g. "RAAQW") and ETFs both need a name/metadata
    # lookup — this reuses the same FMP profile call the market-cap filter
    # needs, so it's not an extra round of requests, and now only runs on the
    # (much smaller) price/change-filtered set. A failed lookup can't be
    # checked by name, so it's neither a confirmed warrant/ETF nor excluded
    # here — it passes through with an unknown market cap, resolved in the
    # market-cap filter below (assumed qualifying, not excluded).
    quotes = fetch_fmp_quotes(price_change_ok)

    candidates, dropped_warrant_name, dropped_etf, lookup_failed = [], [], [], []
    for sym in price_change_ok:
        q = quotes.get(sym)
        if q is None:
            lookup_failed.append(sym)
            candidates.append(sym)
            continue
        name_lower = (q.get("name") or "").lower()
        if "warrant" in name_lower:
            dropped_warrant_name.append(sym)
        elif q.get("is_etf") or any(h in name_lower for h in _ETF_NAME_HINTS):
            dropped_etf.append(sym)
        else:
            candidates.append(sym)

    if dropped_warrant_name:
        log(f"Dropped warrant ticker(s) by name: {', '.join(dropped_warrant_name)}", "info")
    if dropped_etf:
        log(f"Dropped ETF/fund ticker(s): {', '.join(dropped_etf)}", "info")
    if lookup_failed:
        log(f"{len(lookup_failed)} ticker(s) have no FMP profile data — market cap assumed >$300M: {', '.join(lookup_failed)}", "warn")
    if not candidates:
        log("Nothing left after warrant/ETF filter", "warn")
        return []

    # ── Market cap filter, reusing the FMP data already fetched above ───────
    # Missing market cap (lookup failed entirely, or the field came back
    # empty) is treated as qualifying rather than excluded — the FMP free
    # tier has enough data gaps that failing closed here was dropping
    # otherwise-legitimate candidates before they ever reached the real
    # HVE range/volume check.
    tickers = []
    for sym in candidates:
        cap = (quotes.get(sym) or {}).get("market_cap")
        if cap is None:
            log(f"  {sym}: market cap unavailable — assumed qualifying", "info")
            tickers.append(sym)
        else:
            log(f"  {sym}: market cap = {cap}", "info")
            if cap > 300_000_000:
                tickers.append(sym)

    if not tickers:
        log("No Alpaca movers passed the market cap filter", "warn")
        return []

    log(f"Alpaca movers returned {len(tickers)} ticker(s) for {today}: {', '.join(tickers[:20])}{'…' if len(tickers) > 20 else ''}", "info")
    return tickers


def run_scan(source: str = None):
    """
    source='alpaca' — pull from Alpaca's Market Data movers endpoint (used in automated mode)
    source='manual' — use the user's ticker list (used in manual mode)
    source=None     — auto-detect based on saved mode setting
    """
    data = load_tickers()
    if source is None:
        source = "alpaca" if data.get("mode") == "automated" else "manual"

    today = datetime.now(ET).strftime("%Y-%m-%d")
    log(f"── Scan started for {today} (source: {source}) ──", "info")

    if not is_trading_day():
        log(f"{today} is not a trading day (holiday or weekend) — scan skipped", "warn")
        return

    # ── Get ticker list ───────────────────────────────────────────────────────
    if source == "alpaca":
        tickers = fetch_alpaca_movers()
        if not tickers:
            log("Scan aborted — no tickers from Alpaca movers", "warn")
            return
    else:
        tickers = data.get("tickers", [])
        if not tickers:
            log("Scan aborted — manual ticker list is empty", "warn")
            return
        log(f"Manual list: {', '.join(tickers)}", "info")

    # ── Apply HVE criteria to every ticker ───────────────────────────────────
    log(f"Applying HVE criteria to all {len(tickers)} ticker(s)…", "info")
    log(
        "HVE criteria: (1) price ≥70% of day high-low range  (2) today's SIP vol ≥ 2-year daily high"
        f"  (3) stock ≥{MIN_IPO_AGE_DAYS} days old",
        "info",
    )

    qualified  = []
    eliminated = []
    no_data    = []

    for tkr in tickers:
        time.sleep(0.3)  # avoid rate-limiting yfinance
        q = get_quote_and_volume(tkr)

        if q is None:
            no_data.append(tkr)
            log(f"  {tkr}: — data unavailable, skipped", "warn")
            continue

        pos     = position_in_range(q["price"], q["low"], q["high"])
        pos_pct = round(pos * 100, 1)
        rng_ok  = pos >= 0.70
        vol_ok  = q["today_vol"] >= q["max_2y_vol"]
        age_ok  = q["age_days"] >= MIN_IPO_AGE_DAYS
        vol_ratio = round(q["today_vol"] / q["max_2y_vol"], 2) if q["max_2y_vol"] else 0

        criteria_1 = f"range {pos_pct}% {'✓' if rng_ok else '✗ (<70%)'}"
        criteria_2 = f"vol {q['today_vol']:,} vs 2Y high {q['max_2y_vol']:,} (ratio {vol_ratio}x) {'✓' if vol_ok else '✗'}"
        criteria_3 = f"age {q['age_days']}d {'✓' if age_ok else f'✗ (<{MIN_IPO_AGE_DAYS}d)'}"

        if rng_ok and vol_ok and age_ok:
            qualified.append({**q, "range_pos": pos_pct})
            log(f"  {tkr}: QUALIFIED — {criteria_1}  |  {criteria_2}  |  {criteria_3}", "ok")
        else:
            eliminated.append(tkr)
            log(f"  {tkr}: eliminated — {criteria_1}  |  {criteria_2}  |  {criteria_3}", "info")

    log(
        f"── HVE result: {len(qualified)} qualified  {len(eliminated)} eliminated  {len(no_data)} no data ──",
        "ok" if qualified else "warn",
    )

    if not qualified:
        log("No stocks met all HVE criteria — watchlist unchanged", "warn")
        return

    # ── Save to watchlist for next-day breakout entry ─────────────────────────
    # Strategy: buy on Day 1 only if price breaks above Day 0 high before 11 AM ET.
    existing_positions = {p["symbol"] for p in (alpaca_get("/positions") or [])}
    watchlist = load_watchlist()

    added = []
    for s in qualified:
        tkr = s["ticker"]
        if tkr in existing_positions:
            log(f"  {tkr}: already in positions — not added to watchlist", "info")
            continue
        watchlist[tkr] = {
            "day0_date":  today,
            "day0_high":  round(s["high"], 4),
            "atr14":      round(s["atr14"], 4),
            "day0_price": round(s["price"], 4),
        }
        added.append(tkr)
        log(
            f"  {tkr}: added to next-day watchlist — Day 0 high ${s['high']:.2f}  ATR14=${s['atr14']:.2f}",
            "ok",
        )

    save_watchlist(watchlist)
    added_data = [s for s in qualified if s["ticker"] in added]
    if added_data:
        record_watchlist_history(today, added_data)
    log(
        f"── Scan complete — {len(added)} ticker(s) on watchlist: {', '.join(added) if added else 'none'} ──\n"
        "   Buy triggers if price breaks above Day 0 high before 11:00 AM ET tomorrow.",
        "ok",
    )


# ── Scheduler setup ───────────────────────────────────────────────────────────

def reschedule(time_str: str, mode: str):
    """
    Always schedules the Alpaca movers scan at the given time on weekdays.
    mode only controls whether the dashboard 'Run scan now' button uses
    Alpaca movers or the manual list — the scheduled job always uses Alpaca.
    """
    if scheduler.get_job("daily_scan"):
        scheduler.remove_job("daily_scan")
    if time_str:
        try:
            hour, minute = map(int, time_str.split(":"))
            scheduler.add_job(
                run_scan, "cron",
                args=("alpaca",),
                hour=hour, minute=minute,
                id="daily_scan",
                day_of_week="mon-fri",
                timezone=ET,
                replace_existing=True,
            )
            log(f"Alpaca movers scan scheduled at {time_str} ET every weekday", "info")
        except Exception as e:
            log(f"Schedule error: {e}", "error")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/account")
def get_account():
    try:
        data = alpaca_get("/account")
        return jsonify({
            "portfolio_value": round(float(data.get("portfolio_value", 0)), 2),
            "cash":            round(float(data.get("cash", 0)), 2),
            "buying_power":    round(float(data.get("buying_power", 0)), 2),
            "equity":          round(float(data.get("equity", 0)), 2),
            "account_number":  data.get("account_number"),
            "status":          data.get("status"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/positions")
def get_positions():
    try:
        data = fetch_positions_with_stops()
        return jsonify(data)
    except Exception as e:
        log(f"Sync error: {e}", "error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/tickers", methods=["GET"])
def get_tickers():
    return jsonify(load_tickers())


@app.route("/api/tickers", methods=["POST"])
def save_tickers_route():
    data = request.get_json()
    save_tickers(data)
    mode = data.get("mode", "automated")
    schedule = data.get("schedule", "15:50")
    reschedule(schedule, mode)
    return jsonify({"ok": True})


@app.route("/api/scan", methods=["POST"])
def trigger_scan():
    body   = request.get_json(silent=True) or {}
    source = body.get("source")  # "alpaca" | "manual" | None (auto-detect)
    thread = threading.Thread(target=run_scan, args=(source,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "msg": "Scan started"})


@app.route("/api/cron/scan", methods=["POST", "GET"])
def cron_scan():
    """
    External cron trigger endpoint — called by cron-job.org at 3:50 PM ET daily.
    Protected by CRON_SECRET env var. Always runs the Alpaca movers scan.
    """
    secret = request.args.get("secret") or (request.get_json(silent=True) or {}).get("secret", "")
    if CRON_SECRET and secret != CRON_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    now_et = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    log(f"Cron trigger received at {now_et} — starting Alpaca movers scan", "info")
    thread = threading.Thread(target=run_scan, args=("alpaca",), daemon=True)
    thread.start()
    return jsonify({"ok": True, "msg": f"Alpaca movers scan started at {now_et}"})


@app.route("/api/watchlist/history")
def get_watchlist_history():
    hist = load_watchlist_history()
    # Merge current active watchlist entries as 'pending' for today's date
    wl = load_watchlist()
    if wl:
        for sym, v in wl.items():
            d = v.get("day0_date", "")
            if d:
                hist.setdefault(d, {}).setdefault(sym, {
                    "day0_high":  v.get("day0_high"),
                    "day0_price": v.get("day0_price"),
                    "atr14":      v.get("atr14"),
                    "outcome":    "pending",
                    "fill_price": None,
                })
    # Return sorted newest-first
    sorted_hist = dict(sorted(hist.items(), reverse=True))
    return jsonify(sorted_hist)


@app.route("/api/orders/stop", methods=["POST"])
def place_stop():
    data = request.get_json()
    ticker     = data["symbol"]
    qty        = data["qty"]
    stop_price = float(data["stop_price"])
    result = place_stop_order_internal(ticker, qty, stop_price)
    if result.get("id"):
        log(f"{ticker}: stop order placed @ ${stop_price:.2f}", "ok")
        # persist entry price so check_stop_fills can compute P&L later
        positions_raw = alpaca_get("/positions")
        if isinstance(positions_raw, list):
            for p in positions_raw:
                if p["symbol"] == ticker:
                    meta = load_position_meta()
                    meta.setdefault(ticker, {})["entry_price"] = float(p["avg_entry_price"])
                    save_position_meta(meta)
                    break
        return jsonify({"ok": True, "order": result})
    else:
        msg = result.get("message", "unknown error")
        log(f"{ticker}: stop order failed — {msg}", "error")
        return jsonify({"ok": False, "error": msg}), 400


@app.route("/api/orders/sell", methods=["POST"])
def market_sell():
    data   = request.get_json()
    ticker = data["symbol"]
    qty    = data["qty"]
    payload = {
        "symbol":        ticker,
        "qty":           str(round(float(qty), 4)),
        "side":          "sell",
        "type":          "market",
        "time_in_force": "day",
    }
    result = alpaca_post("/orders", payload)
    if result.get("id"):
        log(f"{ticker}: market sell placed for {qty} shares", "ok")
        return jsonify({"ok": True, "order": result})
    else:
        msg = result.get("message", "unknown error")
        log(f"{ticker}: market sell failed — {msg}", "error")
        return jsonify({"ok": False, "error": msg}), 400


@app.route("/api/orders/<order_id>", methods=["DELETE"])
def cancel_order(order_id):
    status = alpaca_delete(f"/orders/{order_id}")
    ok = status in (200, 204)
    return jsonify({"ok": ok})


@app.route("/api/log")
def get_log():
    return jsonify(load_log())


# ── Boot ──────────────────────────────────────────────────────────────────────

def boot():
    data = load_tickers()
    reschedule(data.get("schedule", "15:50"), data.get("mode", "automated"))
    scheduler.add_job(
        check_breakeven_stops, "interval",
        minutes=BREAKEVEN_POLL_MIN,
        id="breakeven_check",
        replace_existing=True,
    )
    scheduler.add_job(
        check_stop_fills, "interval",
        minutes=2,
        id="stop_fill_check",
        replace_existing=True,
    )
    scheduler.add_job(
        check_breakout_entries, "interval",
        minutes=1,
        id="breakout_entry_check",
        replace_existing=True,
    )
    if not scheduler.running:
        scheduler.start()
    log(f"Breakeven check scheduled every {BREAKEVEN_POLL_MIN} min during market hours", "info")
    log("HVE Action started", "info")


boot()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
