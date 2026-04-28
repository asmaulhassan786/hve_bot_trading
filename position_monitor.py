"""
Position Monitor — run continuously during market hours.
Manages open positions saved in positions.json:

  Rule 1 — Initial stop: 3% below entry (placed by finviz_screener.py).
            Sell all shares if stop is hit.
  Rule 2 — Break-even upgrade: when price rises 5% above entry, cancel the
            3% stop and replace it with a stop at entry price (break-even).
  Rule 3 — 10-day MA exit: once break-even is active, check each day at
            3:55 PM ET whether today's close is below the 10-day MA.
            If so, cancel the stop order and market-sell all shares.
"""

import json
import time
import requests
import yfinance as yf
from datetime import datetime
import pytz
from config import ALPACA_BASE_URL, ALPACA_API_KEY, ALPACA_SECRET

ET = pytz.timezone("America/New_York")
POSITIONS_FILE = "positions.json"

ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
}

BREAKEVEN_TRIGGER_PCT = 0.05   # +5% from entry → move stop to break-even
POLL_INTERVAL_SEC     = 60     # check every 60 seconds


# ── Persistence ───────────────────────────────────────────────────────────────

def load_positions() -> dict:
    try:
        with open(POSITIONS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_positions(positions: dict):
    with open(POSITIONS_FILE, "w") as f:
        json.dump(positions, f, indent=2)


# ── Alpaca API helpers ────────────────────────────────────────────────────────

def get_order(order_id: str) -> dict:
    r = requests.get(f"{ALPACA_BASE_URL}/orders/{order_id}",
                     headers=ALPACA_HEADERS, timeout=10)
    return r.json()


def cancel_order(order_id: str):
    requests.delete(f"{ALPACA_BASE_URL}/orders/{order_id}",
                    headers=ALPACA_HEADERS, timeout=10)


def place_stop_order(ticker: str, qty: float, stop_price: float) -> dict:
    payload = {
        "symbol": ticker,
        "qty": str(round(qty, 6)),
        "side": "sell",
        "type": "stop",
        "stop_price": str(round(stop_price, 2)),
        "time_in_force": "gtc",
    }
    r = requests.post(f"{ALPACA_BASE_URL}/orders", json=payload,
                      headers=ALPACA_HEADERS, timeout=10)
    return r.json()


def place_market_sell(ticker: str, qty: float) -> dict:
    payload = {
        "symbol": ticker,
        "qty": str(round(qty, 6)),
        "side": "sell",
        "type": "market",
        "time_in_force": "day",
    }
    r = requests.post(f"{ALPACA_BASE_URL}/orders", json=payload,
                      headers=ALPACA_HEADERS, timeout=10)
    return r.json()


def get_current_price(ticker: str) -> float:
    """Latest trade price from Alpaca data API."""
    try:
        r = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{ticker}/trades/latest",
            headers=ALPACA_HEADERS, timeout=10,
        )
        return float(r.json()["trade"]["p"])
    except Exception:
        # Fallback to yfinance
        try:
            hist = yf.Ticker(ticker).history(period="1d", interval="1m")
            return float(hist["Close"].iloc[-1]) if not hist.empty else 0.0
        except Exception:
            return 0.0


# ── Market-time helpers ───────────────────────────────────────────────────────

def is_market_open() -> bool:
    now = datetime.now(ET)
    return now.weekday() < 5 and (9, 30) <= (now.hour, now.minute) < (16, 0)


def is_eod_check_window() -> bool:
    """3:55–3:59 PM ET — end-of-day MA check window."""
    now = datetime.now(ET)
    return now.weekday() < 5 and now.hour == 15 and 55 <= now.minute <= 59


# ── Analysis helpers ──────────────────────────────────────────────────────────

def get_close_and_ma10(ticker: str):
    """Return (today_close, 10-day MA) using yfinance daily data."""
    try:
        hist = yf.Ticker(ticker).history(period="20d", interval="1d")
        if len(hist) < 11:
            return None, None
        close  = float(hist["Close"].iloc[-1])
        ma10   = float(hist["Close"].iloc[-10:].mean())
        return close, ma10
    except Exception:
        return None, None


# ── Core monitor loop ─────────────────────────────────────────────────────────

def log(msg: str):
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    print(f"[{ts}] {msg}", flush=True)


def check_positions():
    positions = load_positions()
    if not positions:
        return

    changed = False

    for ticker, pos in list(positions.items()):
        entry          = pos["entry_price"]
        qty            = pos["qty"]
        stop_order_id  = pos.get("stop_order_id")
        breakeven_done = pos.get("breakeven_triggered", False)

        # ── Rule 1: Check if the existing stop order was hit ─────────────────
        if stop_order_id:
            order = get_order(stop_order_id)
            status = order.get("status", "")
            if status == "filled":
                filled_at = order.get("filled_avg_price", "?")
                log(f"{ticker}: Stop triggered — sold @ {filled_at}. Removing position.")
                del positions[ticker]
                changed = True
                continue
            if status in ("cancelled", "expired", "rejected"):
                log(f"{ticker}: Stop order {stop_order_id[:8]} is {status} — clearing reference.")
                pos["stop_order_id"] = None
                changed = True

        # ── Rule 2: +5% → move stop to break-even ───────────────────────────
        if not breakeven_done:
            current = get_current_price(ticker)
            if current <= 0:
                continue

            if current >= entry * (1 + BREAKEVEN_TRIGGER_PCT):
                log(
                    f"{ticker}: Price {current:.2f} hit +5% above entry {entry:.2f}. "
                    f"Moving stop to break-even ({entry:.2f})."
                )
                if stop_order_id:
                    cancel_order(stop_order_id)

                result   = place_stop_order(ticker, qty, entry)
                new_stop_id = result.get("id", "")
                pos["stop_order_id"]       = new_stop_id
                pos["stop_price"]          = entry
                pos["breakeven_triggered"] = True
                changed = True
                log(f"{ticker}: Break-even stop placed ({new_stop_id[:8] if new_stop_id else 'N/A'}).")

        # ── Rule 3: End-of-day 10-day MA check ──────────────────────────────
        if breakeven_done and is_eod_check_window():
            last_eod_check = pos.get("last_eod_check")
            today_str      = datetime.now(ET).strftime("%Y-%m-%d")

            if last_eod_check == today_str:
                continue  # already checked today

            close, ma10 = get_close_and_ma10(ticker)
            pos["last_eod_check"] = today_str
            changed = True

            if close is None:
                log(f"{ticker}: Could not fetch close/MA data.")
                continue

            log(f"{ticker}: EOD check — close={close:.2f}  10-day MA={ma10:.2f}")

            if close < ma10:
                log(f"{ticker}: Close {close:.2f} < 10-day MA {ma10:.2f} — selling all shares.")
                if stop_order_id:
                    cancel_order(stop_order_id)
                result = place_market_sell(ticker, qty)
                log(f"{ticker}: Market sell order placed — {result.get('status', result.get('message'))}.")
                del positions[ticker]
            else:
                log(f"{ticker}: Close above 10-day MA — holding.")

    if changed:
        save_positions(positions)


def main():
    log("Position monitor started.")
    log(f"Watching: {POSITIONS_FILE}")
    log(f"Rules: stop=-3%, break-even at +5%, sell-on-close-below-10MA after break-even.")
    print("-" * 60, flush=True)

    while True:
        try:
            if is_market_open():
                check_positions()
            else:
                now = datetime.now(ET)
                if now.weekday() < 5:
                    log("Market closed. Sleeping 5 min...")
                else:
                    log("Weekend. Sleeping 30 min...")
                time.sleep(300 if now.weekday() < 5 else 1800)
                continue
        except Exception as e:
            log(f"Error in check loop: {e}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
