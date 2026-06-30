"""
HVE Action — Flask web app
Connects to Alpaca paper trading and provides a dashboard for managing
positions, stop orders, and the daily ticker scan list.
"""

import json
import os
import time
import threading
from collections import deque
from datetime import datetime

import pytz
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

ET = pytz.timezone("America/New_York")

ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_API_KEY  = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET   = os.environ.get("ALPACA_SECRET", "")

DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
TICKERS_FILE = os.path.join(DATA_DIR, "tickers.json")
LOG_FILE     = os.path.join(DATA_DIR, "activity.json")

os.makedirs(DATA_DIR, exist_ok=True)

BUY_AMOUNT    = 500
STOP_LOSS_PCT = 0.03

activity_log = deque(maxlen=200)
_log_lock = threading.Lock()

scheduler = BackgroundScheduler(timezone=ET)


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str, level: str = "info"):
    ts = datetime.now(ET).strftime("%H:%M:%S ET")
    entry = {"time": ts, "msg": msg, "level": level}
    with _log_lock:
        activity_log.appendleft(entry)
    try:
        existing = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE) as f:
                existing = json.load(f)
        existing.insert(0, entry)
        with open(LOG_FILE, "w") as f:
            json.dump(existing[:200], f)
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
        return {"tickers": [], "schedule": "15:50", "mode": "manual"}


def save_tickers(data: dict):
    with open(TICKERS_FILE, "w") as f:
        json.dump(data, f, indent=2)


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

def get_quote_and_volume(ticker: str):
    try:
        t = yf.Ticker(ticker)
        intraday = t.history(period="1d", interval="1m")
        if intraday.empty:
            return None
        day_low   = float(intraday["Low"].min())
        day_high  = float(intraday["High"].max())
        price     = float(intraday["Close"].iloc[-1])
        today_vol = int(intraday["Volume"].sum())
        daily     = t.history(period="2y", interval="1d")
        if len(daily) < 2:
            return None
        max_2y_vol = int(daily["Volume"].iloc[:-1].max())
        return {
            "ticker": ticker, "price": price,
            "low": day_low, "high": day_high,
            "today_vol": today_vol, "max_2y_vol": max_2y_vol,
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


def place_buy_order(ticker, notional):
    payload = {
        "symbol":        ticker,
        "notional":      str(round(notional, 2)),
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


def run_scan():
    log("Scan started", "info")
    data = load_tickers()
    tickers = data.get("tickers", [])
    if not tickers:
        log("No tickers in scan list — scan aborted", "warn")
        return

    log(f"Scanning {len(tickers)} ticker(s): {', '.join(tickers)}", "info")
    qualified = []

    for tkr in tickers:
        q = get_quote_and_volume(tkr)
        if q is None:
            log(f"{tkr}: data unavailable — skipped", "warn")
            continue
        pos = position_in_range(q["price"], q["low"], q["high"])
        if pos >= 0.70 and q["today_vol"] >= q["max_2y_vol"]:
            qualified.append({**q, "range_pos": round(pos * 100, 1)})
            log(f"{tkr}: passed — range {round(pos*100,1)}%, vol {q['today_vol']:,}", "ok")
        else:
            log(f"{tkr}: did not qualify — range {round(pos*100,1)}%, vol {q['today_vol']:,}", "info")

    if not qualified:
        log("No stocks qualified today", "warn")
        return

    log(f"{len(qualified)} stock(s) qualified — placing buy orders", "ok")

    existing_positions = {p["symbol"] for p in (alpaca_get("/positions") or [])}

    for s in qualified:
        tkr = s["ticker"]
        if tkr in existing_positions:
            log(f"{tkr}: already in positions — skipping buy", "info")
            continue

        buy = place_buy_order(tkr, BUY_AMOUNT)
        order_id = buy.get("id")
        if not order_id:
            log(f"{tkr}: buy order failed — {buy.get('message', 'unknown error')}", "error")
            continue

        log(f"{tkr}: buy order placed, waiting for fill...", "info")
        filled = wait_for_fill(order_id)

        if filled is None:
            log(f"{tkr}: fill timeout or rejected", "error")
            continue

        entry_price = float(filled["filled_avg_price"])
        qty         = float(filled["filled_qty"])
        stop_price  = round(entry_price * (1 - STOP_LOSS_PCT), 2)

        log(f"{tkr}: filled @ ${entry_price:.2f}  qty={qty:.4f}", "ok")

        stop = place_stop_order_internal(tkr, qty, stop_price)
        if stop.get("id"):
            log(f"{tkr}: stop placed @ ${stop_price:.2f}", "ok")
        else:
            log(f"{tkr}: stop placement failed — {stop.get('message', '')}", "error")

    log("Scan complete", "ok")


# ── Scheduler setup ───────────────────────────────────────────────────────────

def reschedule(time_str: str, mode: str):
    scheduler.remove_all_jobs()
    if mode == "automated" and time_str:
        try:
            hour, minute = map(int, time_str.split(":"))
            scheduler.add_job(run_scan, "cron", hour=hour, minute=minute,
                              id="daily_scan", day_of_week="mon-fri")
            log(f"Automated scan scheduled at {time_str} ET (weekdays)", "info")
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
        log(f"Synced {len(data['positions'])} position(s) from Alpaca", "info")
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
    mode = data.get("mode", "manual")
    schedule = data.get("schedule", "15:50")
    reschedule(schedule, mode)
    return jsonify({"ok": True})


@app.route("/api/scan", methods=["POST"])
def trigger_scan():
    thread = threading.Thread(target=run_scan, daemon=True)
    thread.start()
    return jsonify({"ok": True, "msg": "Scan started"})


@app.route("/api/orders/stop", methods=["POST"])
def place_stop():
    data = request.get_json()
    ticker     = data["symbol"]
    qty        = data["qty"]
    stop_price = float(data["stop_price"])
    result = place_stop_order_internal(ticker, qty, stop_price)
    if result.get("id"):
        log(f"{ticker}: stop order placed @ ${stop_price:.2f}", "ok")
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
        "qty":           str(int(float(qty))),
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
    reschedule(data.get("schedule", "15:50"), data.get("mode", "manual"))
    if not scheduler.running:
        scheduler.start()
    log("HVE Action started", "info")


boot()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
