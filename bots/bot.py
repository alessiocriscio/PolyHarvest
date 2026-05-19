import sys
import time
import socket
import requests
import pandas as pd
import json
import subprocess
import atexit
import os
import re
import sqlite3
import numpy as np
from obi_engine import calculate_obi

tor_process = None


def init_db():
    conn = sqlite3.connect('market_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS spread_log
                 (timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                  ticker TEXT,
                  binance_obi_raw REAL,
                  binance_ema REAL,
                  polymarket_obi REAL,
                  spread REAL,
                  z_score REAL)''')
    # Migrate schema: add columns if missing (safe for existing DBs)
    for col_def in ['pm_best_bid REAL', 'pm_best_ask REAL', 'order_id TEXT', 'fill_status TEXT']:
        try:
            c.execute(f'ALTER TABLE spread_log ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn

def cleanup():
    global tor_process
    print("\n[SYSTEM] Shutting down Tor tunnel...")
    if tor_process:
        tor_process.terminate()
    os.system("killall tor 2>/dev/null")

atexit.register(cleanup)

def start_tor():
    global tor_process
    print("[SYSTEM] Starting Tor tunnel in background...")
    os.system("killall tor 2>/dev/null")
    tor_process = subprocess.Popen(['tor'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("[SYSTEM] Waiting 10 seconds for secure connection...")
    time.sleep(10)

def get_binance_obi(symbol):
    if '_' in symbol:
        url = f"https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit=10"
    else:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=10"
    try:
        resp = requests.get(url, timeout=3)
        data = resp.json()
        bids = pd.DataFrame(data.get('bids', []), columns=['price', 'bid_size']).astype(float)
        asks = pd.DataFrame(data.get('asks', []), columns=['price', 'ask_size']).astype(float)
        
        # SYMMETRIC EXTRACTION: Cut to the top 5 levels of real liquidity
        vol_bids = bids.head(5)['bid_size'].sum() if not bids.empty else 0
        vol_asks = asks.head(5)['ask_size'].sum() if not asks.empty else 0
        
        df = pd.DataFrame({"bid_size": [vol_bids], "ask_size": [vol_asks]})
        return calculate_obi(df) if (vol_bids + vol_asks) > 0 else 0
    except Exception:
        return 0

def get_binance_price(symbol):
    if '_' in symbol:
        url = f"https://dapi.binance.com/dapi/v1/ticker/price?symbol={symbol}"
    else:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    try:
        resp = requests.get(url, timeout=3)
        return float(resp.json()[0]['price']) if '_' in symbol else float(resp.json()['price'])
    except Exception:
        return None

def extract_strike_price(text):
    matches = re.findall(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', text)
    if not matches: return None
    numbers = [float(m.replace(',', '')) for m in matches]
    return max(numbers)

def fetch_active_token(slug, url, proxies, headers, current_spot_price):
    """Fetch the best active market token. Returns (question, token_yes, token_no) or (None, None, None)."""
    try:
        resp = requests.get(
            f"https://gamma-api.polymarket.com/events?slug={slug}" if "/event/" in url
            else f"https://gamma-api.polymarket.com/markets?slug={slug}",
            proxies=proxies, headers=headers, timeout=10
        )
        markets = resp.json()[0].get('markets', []) if "/event/" in url else resp.json()

        market_list = []
        for m in markets:
            if m.get('closed', False) or not m.get('active', True):
                continue
            clobs = m.get('clobTokenIds')
            if not clobs:
                continue
            parsed = json.loads(clobs) if isinstance(clobs, str) else clobs
            if len(parsed) >= 2:
                market_list.append((m.get('question'), parsed[0], parsed[1]))

        if not market_list:
            return None, None, None

        best_idx, min_distance = 0, float('inf')
        for i, m in enumerate(market_list):
            strike = extract_strike_price(m[0])
            if strike and abs(strike - current_spot_price) < min_distance:
                min_distance, best_idx = abs(strike - current_spot_price), i

        question, token_yes, token_no = market_list[best_idx]
        if min_distance < float('inf'):
            print(f"\n[ATM TARGET] {question} | Distance: {min_distance:.2f}")
        else:
            print(f"\n[FALLBACK] No strike prices found — using first active market: {question}")

        return question, token_yes, token_no
    except Exception as e:
        print(f"[ERROR] fetch_active_token: {e}")
        return None, None, None


def send_executor_signal(token_id, side, price, size):
    """Send a trade signal to the C++ executor via TCP. Returns (order_id, status) or (None, None)."""
    try:
        sig = json.dumps({"token_id": token_id, "side": side, "price": price, "size": size}) + "\n"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect(("127.0.0.1", 9999))
        s.sendall(sig.encode())
        resp = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            resp += chunk
            if b"\n" in resp:
                break
        s.close()
        result = json.loads(resp.decode().strip())
        return result.get("order_id", ""), result.get("status", "error")
    except Exception as e:
        print(f"\n[EXECUTOR] Connection failed: {e}")
        return None, None


def main():
    print(f"[SYSTEM] Kernel: {sys.version.split()[0]}")
    start_tor()
    log_var = input("Start data logging ? Y(Yes)/N(No)\n")
    if log_var == "Y" or log_var == "y":
        print("Data logging enabled.")
        db_conn = init_db()
        db_cursor = db_conn.cursor()
    else:
        db_conn = None
        db_cursor = None

    proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'}
    headers = {'User-Agent': 'Mozilla/5.0'}

    url = input("\nEnter Polymarket URL: ").strip()
    ticker = input("Enter Binance Ticker (e.g., BTCUSDT): ").strip().upper()
    try:
        z_threshold = float(input("Enter Z-Score threshold for alerts (e.g., 2.0): "))
    except ValueError:
        z_threshold = 2.0

    current_spot_price = get_binance_price(ticker)
    if current_spot_price is None: return

    slug = url.rstrip('/').split('/')[-1].split('?')[0]

    question, token_yes, token_no = fetch_active_token(slug, url, proxies, headers, current_spot_price)
    if token_yes is None:
        print("[ERROR] No active market found.")
        return

    alpha, ema_binance, spread_history = 0.125, None, []
    empty_book_streak = 0
    in_trade = False

    if db_cursor is not None:
        print(f"[DATA LOGGER ACTIVE] Recording data to market_data.db...")

    while True:
        try:
            resp_pm = requests.get(f"https://clob.polymarket.com/book?token_id={token_yes}", proxies=proxies, headers=headers, timeout=5)
            book_pm = resp_pm.json()
            bids_pm, asks_pm = pd.DataFrame(book_pm.get('bids', [])), pd.DataFrame(book_pm.get('asks', []))
            
            # SYMMETRIC EXTRACTION: Cut to the top 5 levels for Polymarket too
            v_b_pm = bids_pm.head(5)['size'].astype(float).sum() if not bids_pm.empty else 0
            v_a_pm = asks_pm.head(5)['size'].astype(float).sum() if not asks_pm.empty else 0

            # Market rotation detection for expiring markets (e.g. BTC Up/Down 5m)
            if v_b_pm == 0 and v_a_pm == 0:
                empty_book_streak += 1
                if empty_book_streak >= 2:
                    current_spot_price = get_binance_price(ticker) or current_spot_price
                    new_q, new_yes, new_no = fetch_active_token(slug, url, proxies, headers, current_spot_price)
                    if new_yes is not None:
                        question, token_yes, token_no = new_q, new_yes, new_no
                        print(f"\n[MARKET ROTATION] New target: {question}")
                    empty_book_streak = 0
                    time.sleep(0.5)
                    continue
            else:
                empty_book_streak = 0

            obi_pm = calculate_obi(pd.DataFrame({"bid_size": [v_b_pm], "ask_size": [v_a_pm]})) if (v_b_pm + v_a_pm) > 0 else 0

            # Extract best bid/ask for logging and order pricing
            best_bid = float(bids_pm.iloc[0]['price']) if not bids_pm.empty else 0
            best_ask = float(asks_pm.iloc[0]['price']) if not asks_pm.empty else 0

            obi_trad_raw = get_binance_obi(symbol=ticker)
            ema_binance = obi_trad_raw if ema_binance is None else (obi_trad_raw * alpha) + (ema_binance * (1 - alpha))

            # DIRECTIONAL SPREAD: No abs() to maintain signal direction
            divergence = ema_binance - obi_pm
            
            # ROLLING WINDOW: 80 samples
            spread_history.append(divergence)
            if len(spread_history) > 80: 
                spread_history.pop(0)

            z_score = 0
            order_id = None
            fill_status = None

            if len(spread_history) == 80:
                s_series = pd.Series(spread_history)
                mean, std = s_series.mean(), s_series.std()
                if std > 0: 
                    z_score = (divergence - mean) / std
                
                # SIGNAL TO EXECUTOR IF THRESHOLD IS BREACHED
                if abs(z_score) > z_threshold and not in_trade:
                    in_trade = True
                    if z_score > 0:
                        direction = "BUY (PM Underpriced)"
                        sig_price = round(best_ask - 0.01, 2)
                        order_id, fill_status = send_executor_signal(token_yes, "BUY", sig_price, 10)
                    else:
                        direction = "SELL (PM Overpriced)"
                        # Fetch token_no book for its best ask
                        try:
                            resp_no = requests.get(f"https://clob.polymarket.com/book?token_id={token_no}", proxies=proxies, headers=headers, timeout=5)
                            book_no = resp_no.json()
                            asks_no = pd.DataFrame(book_no.get('asks', []))
                            best_ask_no = float(asks_no.iloc[0]['price']) if not asks_no.empty else 0
                        except Exception:
                            best_ask_no = 0
                        sig_price = round(best_ask_no - 0.01, 2)
                        order_id, fill_status = send_executor_signal(token_no, "BUY", sig_price, 10)
                    print(f"\n TRIGGER! Z-Score: {z_score:.2f} | {direction} | Spread: {divergence:.4f} | Order: {fill_status}")

                elif in_trade and abs(z_score) < 0.5:
                    in_trade = False
                    print(f"\n[EXIT] Z-Score back to {z_score:.2f} | Position closed")

            if db_cursor is not None:
                db_cursor.execute(
                    "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, pm_best_bid, pm_best_ask, order_id, fill_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, order_id, fill_status))
                db_conn.commit()

            print(f"Logging... Z-Score: {z_score:.2f} | Spread: {divergence:.4f}      ", end="\r", flush=True)
            time.sleep(0.5)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[ERROR] Engine Failure: {e}")
            time.sleep(0.5)
            continue

if __name__ == "__main__":
    main()
