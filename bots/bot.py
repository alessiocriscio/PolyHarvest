import sys
import time
import socket
import aiohttp
import asyncio
import pandas as pd
import json
import os
import re
import sqlite3
import numpy as np
import datetime
from obi_engine import calculate_obi


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
                  z_score REAL,
                  pm_ask_no REAL)''')
    # Migrate schema: add columns if missing (safe for existing DBs)
    for col_def in ['pm_best_bid REAL', 'pm_best_ask REAL', 'order_id TEXT', 'fill_status TEXT', 'pm_ask_no REAL', 'pm_bid_no REAL']:
        try:
            c.execute(f'ALTER TABLE spread_log ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn


async def get_binance_obi(session, symbol):
    # USDM perp futures: più volume, lead spot, più istituzionale
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=5"
    try:
        async with session.get(url, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                bids = pd.DataFrame(data.get('bids', []), columns=['price', 'bid_size']).astype(float)
                asks = pd.DataFrame(data.get('asks', []), columns=['price', 'ask_size']).astype(float)
                vol_bids = bids.head(5)['bid_size'].sum() if not bids.empty else 0
                vol_asks = asks.head(5)['ask_size'].sum() if not asks.empty else 0
                df = pd.DataFrame({"bid_size": [vol_bids], "ask_size": [vol_asks]})
                return calculate_obi(df) if (vol_bids + vol_asks) > 0 else 0
    except Exception:
        pass
    return 0


async def get_binance_price(session, symbol):
    if '_' in symbol:
        url = f"https://dapi.binance.com/dapi/v1/ticker/price?symbol={symbol}"
    else:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    try:
        async with session.get(url, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data[0]['price']) if '_' in symbol else float(data['price'])
    except Exception:
        pass
    return None


def extract_strike_price(text):
    matches = re.findall(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', text)
    if not matches: return None
    numbers = [float(m.replace(',', '')) for m in matches]
    return max(numbers)


async def get_market_end_time(session, slug, headers):
    """Fetch endDate of the first active market for the given event slug."""
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list) and len(data) > 0:
                    markets = data[0].get('markets', [])
                    active_markets = [m for m in markets if not m.get('closed', False) and m.get('active', True)]
                    if active_markets:
                        end_date_str = active_markets[0].get('endDate')
                        if end_date_str:
                            clean_str = end_date_str.replace('Z', '').split('.')[0]
                            return datetime.datetime.fromisoformat(clean_str)
    except Exception:
        pass
    return None


async def get_market_volume(session, slug, headers):
    """Fetch volume24hr of the first active market for the given event slug."""
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list) and len(data) > 0:
                    markets = data[0].get('markets', [])
                    active_markets = [m for m in markets if not m.get('closed', False) and m.get('active', True)]
                    if active_markets:
                        volume_str = active_markets[0].get('volume24hr')
                        if volume_str is not None:
                            return float(volume_str)
    except Exception:
        pass
    return 0.0





async def get_pm_book(session, token_id, headers):
    """Fetch CLOB orderbook for a token."""
    try:
        url = f"https://clob.polymarket.com/book?token_id={token_id}"
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception:
        pass
    return {}


async def get_pm_midpoint(session, token_id, headers):
    """Fetch real mid price from CLOB /midpoint endpoint."""
    try:
        url = f"https://clob.polymarket.com/midpoint?token_id={token_id}"
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                mid = float(data['mid'])
                if 0.02 < mid < 0.98:
                    return mid
    except Exception:
        pass
    return None



async def fetch_live_5m_market(session, ticker, headers, *args, **kwargs):
    """
    Calculates the exact 5-minute window timestamp and directly fetches the specific market JSON.
    """
    try:
        now_ts = int(time.time())
        current_slot_start = (now_ts // 300) * 300
        
        prefix = "btc" if "BTC" in ticker.upper() else "eth"
        target_slug = f"{prefix}-updown-5m-{current_slot_start}"
        url = f"https://gamma-api.polymarket.com/markets/slug/{target_slug}"
        
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status != 200:
                return None
            m = await resp.json()
            
            if m.get('closed', False) or not m.get('active', True):
                return None
                
            clobs = m.get('clobTokenIds')
            if not clobs: 
                return None
            parsed = json.loads(clobs) if isinstance(clobs, str) else clobs
            if len(parsed) < 2: 
                return None
                
            end_date_str = m.get('endDate', '').replace('Z', '').split('.')[0]
            if not end_date_str: 
                return None
            end_dt = datetime.datetime.fromisoformat(end_date_str)
            
            # Core Filter: Keep only if it has MORE than 30 seconds of life remaining
            if end_dt > datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(seconds=30):
                return (end_dt, m.get('question'), parsed[0], parsed[1], target_slug)
            else:
                return None
    except Exception as e:
        return None


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
        return None, None


async def main():
    MIN_MARKET_VOLUME = 0.0
    MAX_Z_CIRCUIT_BREAKER = 7.0

    print(f"[SYSTEM] Kernel: {sys.version.split()[0]}")
    
    # Hardcoded configuration for autonomous execution
    print("Data logging enabled by default.")
    db_conn = init_db()
    db_cursor = db_conn.cursor()

    headers = {'User-Agent': 'Mozilla/5.0'}

    url = "https://polymarket.com/event/btc-up-or-down-5m"
    ticker = "BTCUSDT"

    async with aiohttp.ClientSession() as session:
        current_spot_price = await get_binance_price(session, ticker)
        if current_spot_price is None: return

        res = await fetch_live_5m_market(session, ticker, headers)
        if res:
            market_end_time, question, token_yes, token_no, slug = res
        else:
            print("[ERROR] No active market found.")
            return

        alpha, ema_binance, spread_history = 0.125, None, []
        obi_trad_raw = 0
        obi_pm = 0
        divergence = 0
        z_score = 0
        best_bid = 0
        best_ask = 0
        ask_no = 0.0
        bid_no = 0.0

        if db_cursor is not None:
            print(f"[DATA LOGGER ACTIVE] Recording data to market_data.db...")

        while True:
            try:
                def is_book_real(book, mid=None):
                    if mid is None:
                        return False
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    if not bids or not asks:
                        return False
                    return 0.05 < mid < 0.95

                # Proactive market rotation based on expiry timer
                if datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) >= market_end_time - datetime.timedelta(seconds=30):
                    if db_cursor is not None:
                        db_cursor.execute(
                            "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, pm_best_bid, pm_best_ask, pm_ask_no, pm_bid_no, order_id, fill_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, bid_no, None, 'N/A'))
                        db_conn.commit()
                    
                    res = await fetch_live_5m_market(session, ticker, headers)
                    if res:
                        market_end_time, question, token_yes, token_no, slug = res
                        print(f"\n[PROACTIVE ROTATION] Switched to next active slot: {question}")
                        await asyncio.sleep(0.1)
                        continue
                    else:
                        # If rotation fetch returns None, back off and retry on the next iteration
                        await asyncio.sleep(1.0)
                        continue

                # Fetch Binance OBI
                obi_trad_raw = await get_binance_obi(session, ticker)

                # 1. Get the book for the YES token
                async with session.get(f"https://clob.polymarket.com/book?token_id={token_yes}", headers=headers, timeout=5) as resp_yes:
                    book_yes = await resp_yes.json()
                    bids_yes = book_yes.get('bids', [])
                    asks_yes = book_yes.get('asks', [])

                # 2. Get the book for the NO token
                async with session.get(f"https://clob.polymarket.com/book?token_id={token_no}", headers=headers, timeout=5) as resp_no:
                    book_no = await resp_no.json()
                    bids_no = book_no.get('bids', [])
                    asks_no = book_no.get('asks', [])

                mid_yes = await get_pm_midpoint(session, token_yes, headers)
                if mid_yes is None:
                    await asyncio.sleep(0.5)
                    continue

                best_bid = round(mid_yes - 0.005, 4)
                best_ask = round(mid_yes + 0.005, 4)
                bid_no   = round(1.0 - mid_yes - 0.005, 4)
                ask_no   = round(1.0 - mid_yes + 0.005, 4)

                if not is_book_real(book_yes, mid_yes) or not is_book_real(book_no, 1.0 - mid_yes):
                    await asyncio.sleep(0.5)
                    continue

                bids_near = [b for b in bids_yes if abs(float(b['price']) - mid_yes) <= 0.15]
                asks_near = [a for a in asks_yes if abs(float(a['price']) - mid_yes) <= 0.15]
                bids_pm = pd.DataFrame(bids_near) if bids_near else pd.DataFrame()
                asks_pm = pd.DataFrame(asks_near) if asks_near else pd.DataFrame()

                size_col     = 'size' if not bids_pm.empty and 'size' in bids_pm.columns else 1
                size_col_ask = 'size' if not asks_pm.empty and 'size' in asks_pm.columns else 1

                # SYMMETRIC EXTRACTION: Cut to the top 5 levels for Polymarket too
                v_b_pm = bids_pm.head(5)[size_col].astype(float).sum() if not bids_pm.empty else 0
                v_a_pm = asks_pm.head(5)[size_col_ask].astype(float).sum() if not asks_pm.empty else 0



                obi_pm = calculate_obi(pd.DataFrame({"bid_size": [v_b_pm], "ask_size": [v_a_pm]})) if (v_b_pm + v_a_pm) > 0 else 0



                ema_binance = obi_trad_raw if ema_binance is None else (obi_trad_raw * alpha) + (ema_binance * (1 - alpha))

                # DIRECTIONAL SPREAD: No abs() to maintain signal direction
                divergence = ema_binance - obi_pm
                
                # ROLLING WINDOW: 80 samples
                spread_history.append(divergence)
                if len(spread_history) > 80: 
                    spread_history.pop(0)

                z_score = 0
                if len(spread_history) == 80:
                    s_series = pd.Series(spread_history)
                    mean, std = s_series.mean(), s_series.std()
                    if std > 0: 
                        z_score = (divergence - mean) / std

                if db_cursor is not None:
                    db_cursor.execute(
                        "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, pm_best_bid, pm_best_ask, pm_ask_no, pm_bid_no, order_id, fill_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, bid_no, None, 'N/A'))
                    db_conn.commit()

                print(f"Logging... Z-Score: {z_score:.2f} | Spread: {divergence:.4f}      ", end="\r", flush=True)
                await asyncio.sleep(0.1)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[ERROR] Engine Failure: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(0.1)
                continue

if __name__ == "__main__":
    asyncio.run(main())
