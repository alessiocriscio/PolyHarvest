import sys
import time
import requests
import pandas as pd
import json
import subprocess
import atexit
import os
import re
import sqlite3
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
        url = f"https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit=100"
    else:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=100"
    try:
        resp = requests.get(url, timeout=3)
        data = resp.json()
        bids = pd.DataFrame(data.get('bids', []), columns=['price', 'bid_size']).astype(float)
        asks = pd.DataFrame(data.get('asks', []), columns=['price', 'ask_size']).astype(float)
        vol_bids = bids['bid_size'].sum() if not bids.empty else 0
        vol_asks = asks['ask_size'].sum() if not asks.empty else 0
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

def main():
    print(f"[SYSTEM] Kernel: {sys.version.split()[0]}")
    start_tor()
    db_conn = init_db()
    db_cursor = db_conn.cursor()

    proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'}
    headers = {'User-Agent': 'Mozilla/5.0'}

    url = input("\nEnter Polymarket URL: ").strip()
    ticker = input("Enter Binance Ticker (e.g., BTCUSDT): ").strip().upper()
    try:
        z_threshold = float(input("Enter Z-Score threshold for alerts: "))
    except ValueError:
        z_threshold = 2.0

    current_spot_price = get_binance_price(ticker)
    if current_spot_price is None: return

    slug = url.rstrip('/').split('/')[-1].split('?')[0]
    
    try:
        resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}" if "/event/" in url else f"https://gamma-api.polymarket.com/markets?slug={slug}", proxies=proxies, headers=headers, timeout=10)
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
                market_list.append((m.get('question'), parsed[0]))
        
        best_idx, min_distance = 0, float('inf')
        for i, m in enumerate(market_list):
            strike = extract_strike_price(m[0])
            if strike and abs(strike - current_spot_price) < min_distance:
                min_distance, best_idx = abs(strike - current_spot_price), i

        if not market_list:
            print("[ERROR] Nessun mercato attivo trovato.")
            return

        question, token_yes = market_list[best_idx]
        print(f"\n[ATM TARGET] {question} | Distance: {min_distance:.2f}")

    except Exception as e:
        print(f"Error: {e}")
        return

    alpha, ema_binance, spread_history = 0.125, None, []
    
    print(f"[DATA LOGGER ACTIVE] Recording data to market_data.db...")

    while True:
        try:
            resp_pm = requests.get(f"https://clob.polymarket.com/book?token_id={token_yes}", proxies=proxies, headers=headers, timeout=5)
            book_pm = resp_pm.json()
            bids_pm, asks_pm = pd.DataFrame(book_pm.get('bids', [])), pd.DataFrame(book_pm.get('asks', []))
            v_b_pm = bids_pm['size'].astype(float).sum() if not bids_pm.empty else 0
            v_a_pm = asks_pm['size'].astype(float).sum() if not asks_pm.empty else 0
            obi_pm = calculate_obi(pd.DataFrame({"bid_size": [v_b_pm], "ask_size": [v_a_pm]})) if (v_b_pm + v_a_pm) > 0 else 0

            obi_trad_raw = get_binance_obi(symbol=ticker)
            ema_binance = obi_trad_raw if ema_binance is None else (obi_trad_raw * alpha) + (ema_binance * (1 - alpha))

            divergence = abs(ema_binance - obi_pm)
            spread_history.append(divergence)
            if len(spread_history) > 20: spread_history.pop(0)

            z_score = 0
            if len(spread_history) == 20:
                s_series = pd.Series(spread_history)
                mean, std = s_series.mean(), s_series.std()
                if std > 0: z_score = (divergence - mean) / std

            db_cursor.execute("INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score) VALUES (?, ?, ?, ?, ?, ?)", (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score))
            db_conn.commit()

            print(f"Logging... Z-Score: {z_score:.2f} | Spread: {divergence:.4f}      ", end="\r", flush=True)
            time.sleep(2)

        except KeyboardInterrupt:
            break
        except Exception:
            time.sleep(2)
            continue

if __name__ == "__main__":
    main()