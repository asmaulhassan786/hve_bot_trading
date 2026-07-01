"""
HVE Screener — runs at 3:50 PM ET
Tickers are loaded from tickers.txt (one per line), updated manually each day
from your tracked website. Filters stocks with ALL three conditions:
  1. Ticker is in tickers.txt (manually curated daily)
  2. Current price >= 70% of today's high-low range above the low
  3. Today's volume is the highest in the past 2 years

Qualified stocks are bought via Alpaca ($500 notional each).
After fill: places a 3% stop-loss GTC order and saves position to positions.json.
"""

import json
import time
import requests
import yfinance as yf
from datetime import datetime
import pytz
from config import ALPACA_BASE_URL, ALPACA_API_KEY, ALPACA_SECRET

TICKERS_FILE = "tickers.txt"

ET = pytz.timezone("America/New_York")

ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
}
BUY_AMOUNT      = 500   # USD notional per stock
STOP_LOSS_PCT   = 0.03  # 3% initial stop loss
POSITIONS_FILE  = "positions.json"


# ── Ticker list helpers ───────────────────────────────────────────────────────

def fetch_tickers() -> list:
    """Read tickers from tickers.txt — one ticker per line, # lines are comments."""
    try:
        with open(TICKERS_FILE) as f:
            tickers = [
                line.strip().upper()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
        if not tickers:
            print(f"No tickers found in {TICKERS_FILE}.")
        return tickers
    except FileNotFoundError:
        print(f"{TICKERS_FILE} not found. Create it with one ticker per line.")
        return []


def get_quote_and_volume(ticker: str):
    try:
        t = yf.Ticker(ticker)
        intraday = t.history(period="1d", interval="1m")
        if intraday.empty:
            return None
        day_low    = float(intraday["Low"].min())
        day_high   = float(intraday["High"].max())
        price      = float(intraday["Close"].iloc[-1])
        open_price = float(intraday["Open"].iloc[0])
        today_vol  = int(intraday["Volume"].sum())
        daily      = t.history(period="2y", interval="1d")
        if len(daily) < 2:
            return None
        max_2y_vol = int(daily["Volume"].iloc[:-1].max())
        return {
            "ticker": ticker, "price": price, "open": open_price,
            "low": day_low, "high": day_high,
            "today_vol": today_vol, "max_2y_vol": max_2y_vol,
        }
    except Exception:
        return None


def position_in_range(price, low, high):
    rng = high - low
    return (price - low) / rng if rng > 0 else 0.0


# ── Alpaca helpers ────────────────────────────────────────────────────────────

def load_positions() -> dict:
    try:
        with open(POSITIONS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_positions(positions: dict):
    with open(POSITIONS_FILE, "w") as f:
        json.dump(positions, f, indent=2)


def place_buy_order(ticker: str, notional: float) -> dict:
    payload = {
        "symbol": ticker,
        "notional": str(round(notional, 2)),
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
    }
    r = requests.post(f"{ALPACA_BASE_URL}/orders", json=payload,
                      headers=ALPACA_HEADERS, timeout=10)
    return r.json()


def get_order(order_id: str) -> dict:
    r = requests.get(f"{ALPACA_BASE_URL}/orders/{order_id}",
                     headers=ALPACA_HEADERS, timeout=10)
    return r.json()


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


def wait_for_fill(order_id: str, timeout_sec: int = 300):
    """Poll until order is filled or timeout. Returns filled order dict or None."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        order = get_order(order_id)
        status = order.get("status", "")
        if status == "filled":
            return order
        if status in ("cancelled", "expired", "rejected"):
            return None
        time.sleep(5)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def screen():
    now_et = datetime.now(ET)
    print(f"\nFinviz Unusual Volume Screener  —  {now_et.strftime('%Y-%m-%d %H:%M ET')}")
    print("=" * 78)

    print(f"Loading tickers from {TICKERS_FILE}...")
    tickers = fetch_tickers()
    if not tickers:
        return

    print(f"Found {len(tickers)} tickers. Checking intraday + 2-year volume history...\n")

    qualified = []
    for tkr in tickers:
        q = get_quote_and_volume(tkr)
        if q is None:
            continue
        pct_change = (q["price"] - q["open"]) / q["open"] * 100
        pos        = position_in_range(q["price"], q["low"], q["high"])
        if pos >= 0.70 and q["today_vol"] >= q["max_2y_vol"]:
            qualified.append({**q, "change_pct": pct_change, "range_pos": pos * 100})

    if not qualified:
        print("No stocks met all three criteria today.")
        return

    qualified.sort(key=lambda x: x["change_pct"], reverse=True)

    header = (
        f"{'Ticker':<8} {'Price':>7} {'Change%':>9} "
        f"{'Low':>8} {'High':>8} {'Range%':>8} "
        f"{'Today Vol':>12} {'2Y Max Vol':>12}"
    )
    print(header)
    print("-" * len(header))
    for s in qualified:
        print(
            f"{s['ticker']:<8} {s['price']:>7.2f} {s['change_pct']:>8.1f}% "
            f"{s['low']:>8.2f} {s['high']:>8.2f} {s['range_pos']:>7.1f}% "
            f"{s['today_vol']:>12,} {s['max_2y_vol']:>12,}"
        )

    print(f"\n{len(qualified)} stock(s) qualified.")

    # ── Buy & set stop-loss ───────────────────────────────────────────────────
    print(f"\nPlacing ${BUY_AMOUNT} market buy orders...")
    print("-" * 60)

    positions = load_positions()

    for s in qualified:
        tkr = s["ticker"]

        # Skip if already holding
        if tkr in positions:
            print(f"  {tkr:<8}  already in positions — skipping")
            continue

        buy = place_buy_order(tkr, BUY_AMOUNT)
        order_id = buy.get("id")
        if not order_id:
            print(f"  {tkr:<8}  buy order failed: {buy.get('message')}")
            continue

        print(f"  {tkr:<8}  buy order placed ({order_id[:8]}) — waiting for fill...", flush=True)
        filled = wait_for_fill(order_id)

        if filled is None:
            print(f"  {tkr:<8}  fill timeout or rejected — skipping stop placement")
            continue

        entry_price = float(filled["filled_avg_price"])
        qty         = float(filled["filled_qty"])
        stop_price  = round(entry_price * (1 - STOP_LOSS_PCT), 2)

        stop = place_stop_order(tkr, qty, stop_price)
        stop_id = stop.get("id", "")

        positions[tkr] = {
            "entry_price":          entry_price,
            "qty":                  qty,
            "stop_order_id":        stop_id,
            "stop_price":           stop_price,
            "breakeven_triggered":  False,
            "buy_date":             now_et.strftime("%Y-%m-%d"),
        }
        save_positions(positions)

        print(
            f"  {tkr:<8}  filled @ {entry_price:.2f}  qty={qty:.4f}  "
            f"stop={stop_price:.2f} ({stop_id[:8] if stop_id else 'N/A'})"
        )

    print("\nDone. Run position_monitor.py to manage trailing stops.")


if __name__ == "__main__":
    screen()
