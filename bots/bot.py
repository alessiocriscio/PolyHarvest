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
    for col_def in ['pm_best_bid REAL', 'pm_best_ask REAL', 'order_id TEXT', 'fill_status TEXT', 'pm_ask_no REAL']:
        try:
            c.execute(f'ALTER TABLE spread_log ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn


async def get_binance_obi(session, symbol):
    if '_' in symbol:
        url = f"https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit=10"
    else:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=10"
    try:
        async with session.get(url, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                bids = pd.DataFrame(data.get('bids', []), columns=['price', 'bid_size']).astype(float)
                asks = pd.DataFrame(data.get('asks', []), columns=['price', 'ask_size']).astype(float)
                
                # SYMMETRIC EXTRACTION: Cut to the top 5 levels of real liquidity
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


async def find_current_event_slug(session, base_slug, headers=None):
    """Find the current active event slug by checking the current time slot and up to 3 previous slots."""
    current_time = int(time.time())
    current_slot = (current_time // 300) * 300
    for offset in [0, -300, -600, -900]:
        slot = current_slot + offset
        slug = f"{base_slug}-{slot}"
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        markets = data[0].get('markets', [])
                        active_markets = [m for m in markets if not m.get('closed', False) and m.get('active', True)]
                        if active_markets:
                            return slug
        except Exception:
            pass
    return None


async def fetch_active_token(session, slug, url, headers, current_spot_price):
    """Fetch the best active market token. Returns (question, token_yes, token_no) or (None, None, None)."""
    try:
        req_url = (
            f"https://gamma-api.polymarket.com/events?slug={slug}" if "/event/" in url
            else f"https://gamma-api.polymarket.com/markets?slug={slug}"
        )
        async with session.get(req_url, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                markets = data[0].get('markets', []) if "/event/" in url else data
            else:
                markets = []
        
        market_list = []
        for m in markets:
            if m.get('closed', False) or not m.get('active', True):
                continue
            clobs = m.get('clobTokenIds')
            if not clobs:
                continue
            parsed = json.loads(clobs) if isinstance(clobs, str) else clobs
            if len(parsed) >= 2:
                market_list.append((m.get('question'), parsed[1], parsed[0]))

        if not market_list:
            return None, None, None

        best_idx, min_distance = 0, float('inf')
        for i, m in enumerate(market_list):
            strike = extract_strike_price(m[0])
            if strike and abs(strike - current_spot_price) < min_distance:
                min_distance, best_idx = abs(strike - current_spot_price), i

        if min_distance == float('inf'):
            import datetime
            past_markets = []
            future_markets = []
            now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            pattern = r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})[,\s]+(\d{1,2}):(\d{2})\s*(AM|PM)'
            months = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            for i, m in enumerate(market_list):
                match = re.search(pattern, m[0], re.IGNORECASE)
                if match:
                    month_str, day_str, hour_str, minute_str, am_pm = match.groups()
                    try:
                        month = months[month_str.lower()[:3]]
                        day = int(day_str)
                        hour = int(hour_str)
                        minute = int(minute_str)
                        if am_pm.upper() == 'PM' and hour < 12:
                            hour += 12
                        elif am_pm.upper() == 'AM' and hour == 12:
                            hour = 0
                        dt_est = datetime.datetime(now_utc.year, month, day, hour, minute)
                        # ET = UTC - 4 hours => UTC = ET + 4 hours
                        dt_utc = dt_est + datetime.timedelta(hours=4)
                        if dt_utc <= now_utc:
                            past_markets.append((dt_utc, i))
                        else:
                            future_markets.append((dt_utc, i))
                    except (ValueError, KeyError):
                        pass
            if past_markets:
                best_idx = max(past_markets, key=lambda x: x[0])[1]
            elif future_markets:
                best_idx = min(future_markets, key=lambda x: x[0])[1]
            else:
                best_idx = len(market_list) - 1

        question, token_yes, token_no = market_list[best_idx]
        if min_distance < float('inf'):
            print(f"\n[ATM TARGET] {question} | Distance: {min_distance:.2f}")
        else:
            print(f"\n[FALLBACK] No strike prices found — using parsed/latest active market: {question}")

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


async def main():
    MIN_MARKET_VOLUME = 1000.0
    MAX_Z_CIRCUIT_BREAKER = 7.0

    print(f"[SYSTEM] Kernel: {sys.version.split()[0]}")
    log_var = input("Start data logging ? Y(Yes)/N(No)\n")
    if log_var == "Y" or log_var == "y":
        print("Data logging enabled.")
        db_conn = init_db()
        db_cursor = db_conn.cursor()
    else:
        db_conn = None
        db_cursor = None

    headers = {'User-Agent': 'Mozilla/5.0'}

    url = input("\nEnter Polymarket URL: ").strip()
    ticker = input("Enter Binance Ticker (e.g., BTCUSDT): ").strip().upper()
    try:
        z_threshold = float(input("Enter Z-Score threshold for alerts (e.g., 2.0): "))
    except ValueError:
        z_threshold = 2.0

    try:
        capital = float(input("Enter total capital in USD (e.g. 25.0): "))
    except ValueError:
        capital = 25.0

    try:
        trade_size_pct = float(input("Enter trade size % of capital (e.g. 5 for 5%): "))
    except ValueError:
        trade_size_pct = 5.0

    try:
        max_daily_loss_pct = float(input("Enter max daily loss % of capital (e.g. 20 for 20%): "))
    except ValueError:
        max_daily_loss_pct = 20.0

    TRADE_SIZE = capital * trade_size_pct / 100
    MAX_DAILY_LOSS = capital * max_daily_loss_pct / 100

    async with aiohttp.ClientSession() as session:
        current_spot_price = await get_binance_price(session, ticker)
        if current_spot_price is None: return

        slug = url.rstrip('/').split('/')[-1].split('?')[0]

        is_updown = '-updown-5m-' in slug
        question, token_yes, token_no = None, None, None

        if not is_updown:
            question, token_yes, token_no = await fetch_active_token(session, slug, url, headers, current_spot_price)

        if is_updown or token_yes is None:
            base_slug = re.sub(r'-\d+$', '', slug)
            active_slug = await find_current_event_slug(session, base_slug, headers)
            if active_slug:
                slug = active_slug
                url = f"https://polymarket.com/event/{slug}" if "/event/" in url else f"https://polymarket.com/market/{slug}"
                question, token_yes, token_no = await fetch_active_token(session, slug, url, headers, current_spot_price)

        if token_yes is None:
            print("[ERROR] No active market found.")
            return

        market_end_time = await get_market_end_time(session, slug, headers) or datetime.datetime.max

        alpha, ema_binance, spread_history = 0.125, None, []
        empty_book_streak = 0
        in_trade = False
        obi_trad_raw = 0
        obi_pm = 0
        divergence = 0
        z_score = 0
        best_bid = 0
        best_ask = 0
        ask_no = 0.0

        daily_pnl = 0.0
        daily_reset_date = datetime.date.today()
        trade_direction = None
        entry_price = 0.0

        if db_cursor is not None:
            print(f"[DATA LOGGER ACTIVE] Recording data to market_data.db...")

        while True:
            try:
                # Daily reset check at start of each iteration
                if datetime.date.today() != daily_reset_date:
                    daily_pnl = 0.0
                    daily_reset_date = datetime.date.today()

                # Proactive market rotation based on expiry timer
                if datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) >= market_end_time - datetime.timedelta(seconds=30):
                    if in_trade:
                        in_trade = False
                        if db_cursor is not None:
                            db_cursor.execute(
                                "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, pm_best_bid, pm_best_ask, pm_ask_no, order_id, fill_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, None, 'expired'))
                            db_conn.commit()
                        print(f"\n[MARKET EXPIRED] Position force-closed on proactive rotation")
                    
                    current_spot_price = await get_binance_price(session, ticker) or current_spot_price
                    is_updown = '-updown-5m-' in slug
                    new_q, new_yes, new_no = None, None, None
                    if not is_updown:
                        new_q, new_yes, new_no = await fetch_active_token(session, slug, url, headers, current_spot_price)
                    if is_updown or new_yes is None:
                        base_slug = re.sub(r'-\d+$', '', slug)
                        active_slug = await find_current_event_slug(session, base_slug, headers)
                        if active_slug:
                            slug = active_slug
                            url = f"https://polymarket.com/event/{slug}" if "/event/" in url else f"https://polymarket.com/market/{slug}"
                            new_q, new_yes, new_no = await fetch_active_token(session, slug, url, headers, current_spot_price)
                    if new_yes is not None:
                        question, token_yes, token_no = new_q, new_yes, new_no
                        print(f"\n[PROACTIVE ROTATION] Market expires in <30s, switching now. New target: {question}")
                        market_end_time = await get_market_end_time(session, slug, headers) or datetime.datetime.max
                    empty_book_streak = 0
                    await asyncio.sleep(0.1)
                    continue

                # Fetch Binance OBI and Polymarket book concurrently
                binance_obi_task = get_binance_obi(session, ticker)
                pm_book_task = get_pm_book(session, token_yes, headers)
                
                obi_trad_raw, book_pm = await asyncio.gather(
                    binance_obi_task,
                    pm_book_task
                )
                
                bids_pm = pd.DataFrame(book_pm.get('bids', []))
                asks_pm = pd.DataFrame(book_pm.get('asks', []))
                
                best_bid = float(bids_pm.iloc[0]['price']) if not bids_pm.empty else 0.0
                best_ask = float(asks_pm.iloc[0]['price']) if not asks_pm.empty else 0.0
                
                bid_yes = best_bid
                ask_yes = best_ask
                ask_no = round(1.0 - best_bid, 2)
                
                size_col = 'size' if not bids_pm.empty and 'size' in bids_pm.columns else 1
                size_col_ask = 'size' if not asks_pm.empty and 'size' in asks_pm.columns else 1
                
                # SYMMETRIC EXTRACTION: Cut to the top 5 levels for Polymarket too
                v_b_pm = bids_pm.head(5)[size_col].astype(float).sum() if not bids_pm.empty else 0
                v_a_pm = asks_pm.head(5)[size_col_ask].astype(float).sum() if not asks_pm.empty else 0

                # Market rotation detection for expiring markets (e.g. BTC Up/Down 5m)
                if v_b_pm == 0 and v_a_pm == 0:
                    empty_book_streak += 1
                    if empty_book_streak >= 2:
                        if in_trade:
                            in_trade = False
                            if db_cursor is not None:
                                db_cursor.execute(
                                    "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, pm_best_bid, pm_best_ask, pm_ask_no, order_id, fill_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, None, 'expired'))
                                db_conn.commit()
                            print(f"\n[MARKET EXPIRED] Position force-closed on market rotation")
                        current_spot_price = await get_binance_price(session, ticker) or current_spot_price
                        is_updown = '-updown-5m-' in slug
                        new_q, new_yes, new_no = None, None, None
                        if not is_updown:
                            new_q, new_yes, new_no = await fetch_active_token(session, slug, url, headers, current_spot_price)
                        if is_updown or new_yes is None:
                            base_slug = re.sub(r'-\d+$', '', slug)
                            active_slug = await find_current_event_slug(session, base_slug, headers)
                            if active_slug:
                                slug = active_slug
                                url = f"https://polymarket.com/event/{slug}" if "/event/" in url else f"https://polymarket.com/market/{slug}"
                                new_q, new_yes, new_no = await fetch_active_token(session, slug, url, headers, current_spot_price)
                        if new_yes is not None:
                            question, token_yes, token_no = new_q, new_yes, new_no
                            print(f"\n[MARKET ROTATION] New target: {question}")
                            market_end_time = await get_market_end_time(session, slug, headers) or datetime.datetime.max
                        empty_book_streak = 0
                        await asyncio.sleep(0.1)
                        continue
                else:
                    empty_book_streak = 0

                obi_pm = calculate_obi(pd.DataFrame({"bid_size": [v_b_pm], "ask_size": [v_a_pm]})) if (v_b_pm + v_a_pm) > 0 else 0



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
                        # Risk validation checks
                        if daily_pnl <= -MAX_DAILY_LOSS:
                            print("\n[RISK] Daily loss limit reached, bot paused")
                        elif abs(z_score) > MAX_Z_CIRCUIT_BREAKER:
                            print("\n[RISK] Z-score anomaly, skipping")
                        else:
                            # Fetch market volume
                            vol = await get_market_volume(session, slug, headers)
                            if vol < MIN_MARKET_VOLUME:
                                print(f"\n[RISK] Insufficient liquidity (volume: {vol:.2f} < {MIN_MARKET_VOLUME:.2f})")
                            else:
                                in_trade = True
                                if z_score > 0:
                                    direction = "BUY (PM Underpriced)"
                                    sig_price = round(ask_yes - 0.01, 2)
                                    trade_direction = "Yes"
                                    entry_price = sig_price
                                    order_id, fill_status = await asyncio.to_thread(send_executor_signal, token_yes, "BUY", sig_price, TRADE_SIZE)
                                else:
                                    direction = "SELL (PM Overpriced)"
                                    sig_price = round(ask_no - 0.01, 2)
                                    trade_direction = "No"
                                    entry_price = sig_price
                                    order_id, fill_status = await asyncio.to_thread(send_executor_signal, token_no, "BUY", sig_price, TRADE_SIZE)
                                print(f"\n TRIGGER! Z-Score: {z_score:.2f} | {direction} | Spread: {divergence:.4f} | Order: {fill_status}")

                    elif in_trade and abs(z_score) < 0.5:
                        in_trade = False
                        if trade_direction == "Yes":
                            estimated_pnl = (best_bid - entry_price) * TRADE_SIZE
                        else:
                            estimated_pnl = (ask_no - entry_price) * TRADE_SIZE
                        daily_pnl += estimated_pnl
                        print(f"\n[EXIT] Z-Score back to {z_score:.2f} | Position closed | Estimated P&L: {estimated_pnl:+.4f} | Daily P&L: {daily_pnl:+.4f}")
                        if abs(estimated_pnl) >= 0.01:
                            capital += estimated_pnl
                            TRADE_SIZE = capital * trade_size_pct / 100
                            MAX_DAILY_LOSS = capital * max_daily_loss_pct / 100
                            print(f"[CAPITAL] Updated capital: {capital:.4f} | New trade size: {TRADE_SIZE:.4f}")

                if db_cursor is not None:
                    db_cursor.execute(
                        "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, pm_best_bid, pm_best_ask, pm_ask_no, order_id, fill_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, order_id, fill_status))
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
