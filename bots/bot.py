import os
import sys
import time
import socket
import aiohttp
import asyncio
import pandas as pd
import json
import re
import sqlite3
import datetime
from obi_engine import calculate_obi
from pm_ws import PMBookFeed
from binance_ws import BinanceDepthFeed

ASSET_CONFIG = {
    "BTC": {"pm_prefix": "btc", "binance_symbol": "BTCUSDT"},
    "ETH": {"pm_prefix": "eth", "binance_symbol": "ETHUSDT"},
    "SOL": {"pm_prefix": "sol", "binance_symbol": "SOLUSDT"},
    "XRP": {"pm_prefix": "xrp", "binance_symbol": "XRPUSDT"},
    "DOGE": {"pm_prefix": "doge", "binance_symbol": "DOGEUSDT"},
    "HYPE": {"pm_prefix": "hype", "binance_symbol": "HYPEUSDT"},
    "BNB": {"pm_prefix": "bnb", "binance_symbol": "BNBUSDT"},
}

SLOT_SECONDS = 300          # 5-minute Up/Down markets
ROTATE_LEAD_SECONDS = 1     # switch to the next slot this long before expiry
PREFETCH_LEAD_SECONDS = 30  # look up the next slot's market this long before expiry
OBI_BAND = 0.15             # only levels within this distance from the mid
OBI_LEVELS = 5              # top-of-book levels used for the Polymarket OBI

INTERACTIVE = sys.stdout.isatty()
_last_status_ts = 0.0


def status(msg):
    """
    Transient status line: overwritten in place on a terminal, throttled to one
    line per minute when running unattended (systemd journal).
    """
    global _last_status_ts
    if INTERACTIVE:
        print(msg + "      ", end="\r", flush=True)
    elif time.time() - _last_status_ts >= 60:
        _last_status_ts = time.time()
        print(msg, flush=True)


INSERT_SQL = (
    "INSERT INTO spread_log (ticker, binance_obi_raw, binance_ema, polymarket_obi, spread, z_score, "
    "pm_best_bid, pm_best_ask, pm_ask_no, pm_bid_no, order_id, fill_status, market_slug, ts_unix) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


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
    for col_def in ['pm_best_bid REAL', 'pm_best_ask REAL', 'order_id TEXT', 'fill_status TEXT', 'pm_ask_no REAL', 'pm_bid_no REAL', 'market_slug TEXT',
                    'ts_unix REAL']:  # unix seconds with sub-second precision ("timestamp" has whole seconds only)
        try:
            c.execute(f'ALTER TABLE spread_log ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn


def binance_obi(bids, asks):
    """OBI over the top 5 levels of a Binance book side pair ([[price, qty], ...], best first)."""
    vol_bids = sum(float(q) for _, q in bids[:5])
    vol_asks = sum(float(q) for _, q in asks[:5])
    df = pd.DataFrame({"bid_size": [vol_bids], "ask_size": [vol_asks]})
    return calculate_obi(df) if (vol_bids + vol_asks) > 0 else 0


async def get_binance_obi(session, symbol):
    """REST fallback for the depth stream. Returns None on failure, never a fake 0."""
    # USD-M perpetual futures: more volume than spot, tends to lead it, more institutional flow
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=5"
    try:
        async with session.get(url, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                return binance_obi(data.get('bids', []), data.get('asks', []))
    except Exception:
        pass
    return None


async def get_binance_price(session, symbol):
    url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
    try:
        async with session.get(url, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data['price'])
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
    """
    Fetch CLOB orderbook for a token.
    Returns (book, None) on success or (None, reason) on failure, so that a
    failed request is never mistaken for an empty book.
    """
    try:
        url = f"https://clob.polymarket.com/book?token_id={token_id}"
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status != 200:
                return None, f"HTTP {resp.status}"
            book = await resp.json()
            if not isinstance(book, dict) or 'bids' not in book or 'asks' not in book:
                return None, "malformed book"
            return book, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def parse_book_side(levels, best_first_descending):
    """
    Turn a raw CLOB book side into [(price, size), ...] with the BEST level first.
    The CLOB returns bids ascending and asks descending by price (best level is
    the LAST element), so the side is always re-sorted here instead of trusting
    the wire order.
    """
    parsed = []
    for lvl in levels or []:
        try:
            price, size = float(lvl['price']), float(lvl['size'])
        except (KeyError, TypeError, ValueError):
            continue
        if size > 0:
            parsed.append((price, size))
    parsed.sort(key=lambda x: x[0], reverse=best_first_descending)
    return parsed


def parse_book(book):
    """Returns (bids, asks): bids highest price first, asks lowest price first."""
    return (parse_book_side(book.get('bids'), True),
            parse_book_side(book.get('asks'), False))


def current_slot_start(now_ts=None):
    now_ts = time.time() if now_ts is None else now_ts
    return (int(now_ts) // SLOT_SECONDS) * SLOT_SECONDS


def next_target_slot(current_slot, now_ts=None):
    """
    Slot to rotate into. Normally current_slot + 300, but if the bot was stalled
    (sleep, network outage) for longer than one slot it jumps straight to the
    slot that is live on the wall clock instead of retrying a closed one forever.
    """
    now_ts = time.time() if now_ts is None else now_ts
    target = max(current_slot + SLOT_SECONDS, current_slot_start(now_ts))
    if now_ts >= target + SLOT_SECONDS - ROTATE_LEAD_SECONDS:
        target += SLOT_SECONDS
    return target


async def fetch_5m_market(session, ticker, headers, slot_start):
    """
    Fetch the 5-minute Up/Down market that starts at slot_start (unix seconds,
    multiple of 300). The slug is deterministic: {prefix}-updown-5m-{slot_start}.
    Returns ((slot_start, question, token_yes, token_no, slug), None) on success
    or (None, reason) on failure.
    """
    asset_key = ticker.replace("USDT", "").upper()
    if asset_key not in ASSET_CONFIG:
        return None, f"unsupported asset {ticker}"
    prefix = ASSET_CONFIG[asset_key]["pm_prefix"]
    target_slug = f"{prefix}-updown-5m-{slot_start}"
    url = f"https://gamma-api.polymarket.com/markets/slug/{target_slug}"
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status != 200:
                return None, f"{target_slug}: HTTP {resp.status}"
            m = await resp.json()
        # /markets/slug/{slug} returns the bare object, /markets?slug= a list: accept both
        if isinstance(m, list):
            m = m[0] if m else {}
        if m.get('closed', False) or not m.get('active', True):
            return None, f"{target_slug}: market closed/inactive"
        clobs = m.get('clobTokenIds')
        parsed = json.loads(clobs) if isinstance(clobs, str) else clobs
        if not parsed or len(parsed) < 2:
            return None, f"{target_slug}: missing clobTokenIds"
        return (slot_start, m.get('question'), str(parsed[0]), str(parsed[1]), target_slug), None
    except Exception as e:
        return None, f"{target_slug}: {type(e).__name__}: {e}"


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
    except Exception:
        return None, None


async def main():
    print(f"[SYSTEM] Kernel: {sys.version.split()[0]}")

    # PM_LOGGING / PM_ASSET skip the prompts, so the bot can run unattended
    log_choice = os.environ.get("PM_LOGGING")
    if log_choice is None:
        log_choice = input("Enable data logging to market_data.db? [Y/n]: ")
    log_choice = log_choice.strip().lower()
    if log_choice in ('', 'y', 'yes'):
        db_conn = init_db()
        db_cursor = db_conn.cursor()
    else:
        db_conn = None
        db_cursor = None
        print("[SYSTEM] Data logging disabled for this session.")

    # Identify honestly: Polymarket's Cloudflare WAF intermittently blocks (HTTP 403)
    # non-browser clients that present a browser User-Agent.
    headers = {'User-Agent': 'PolyHarvest/1.0 (+https://github.com/alessiocriscio/PolyHarvest)'}

    asset_choice = os.environ.get("PM_ASSET")
    if asset_choice is None:
        asset_choice = input(f"Select asset {list(ASSET_CONFIG.keys())} [default: BTC]: ")
    asset_choice = asset_choice.strip().upper()
    ASSET = asset_choice if asset_choice in ASSET_CONFIG else "BTC"
    print(f"[SYSTEM] Using asset: {ASSET}")
    ticker = ASSET_CONFIG[ASSET]["binance_symbol"]

    async with aiohttp.ClientSession() as session:
        current_spot_price = await get_binance_price(session, ticker)
        if current_spot_price is None:
            print(f"[ERROR] Binance price unavailable for {ticker}.")
            return

        res, err = await fetch_5m_market(
            session, ticker, headers, next_target_slot(current_slot_start() - SLOT_SECONDS))
        if not res:
            print(f"[ERROR] No active market found ({err}).")
            return
        slot_start, question, token_yes, token_no, slug = res
        market_end_ts = slot_start + SLOT_SECONDS
        print(f"[SYSTEM] Tracking: {question} ({slug})")

        # Books stream over the CLOB WebSocket; REST /book is only the fallback
        # while the socket is down or has not sent a snapshot yet.
        feed = PMBookFeed(session, headers)
        await feed.track([token_yes, token_no])
        feed.start()
        # Same for Binance: depth5@100ms stream, REST /depth only as fallback.
        bfeed = BinanceDepthFeed(session, ticker)
        bfeed.start()
        feed_live = {"Polymarket": False, "Binance": False}

        def report_feed(name, live, err):
            if live != feed_live[name]:
                feed_live[name] = live
                if live:
                    print(f"\n[BOOK FEED] {name}: WebSocket live")
                else:
                    print(f"\n[BOOK FEED] {name}: REST fallback ({err or 'waiting for first update'})")

        alpha, ema_binance, spread_history = 0.125, None, []
        last_row = None  # last tick logged for the current market
        next_market = None  # next slot's market, pre-fetched before expiry
        next_prefetch_ts = 0
        rotation_retry_count = 0
        stall_retry_count = 0

        def fmt_px(p):
            return "  - " if p is None else f"{p:.2f}"

        if db_cursor is not None:
            print("[DATA LOGGER ACTIVE] Recording data to market_data.db...")

        while True:
            try:
                # Proactive market rotation based on expiry timer
                now_ts = time.time()
                if now_ts >= market_end_ts - ROTATE_LEAD_SECONDS:
                    if last_row is not None:
                        if db_cursor is not None:
                            db_cursor.execute(INSERT_SQL, last_row)
                            db_conn.commit()
                        last_row = None

                    target_slot = next_target_slot(slot_start, now_ts)
                    if next_market is not None and next_market[0] == target_slot:
                        res, err = next_market, None
                    else:
                        res, err = await fetch_5m_market(session, ticker, headers, target_slot)
                    next_market = None
                    if res:
                        slot_start, question, token_yes, token_no, slug = res
                        market_end_ts = slot_start + SLOT_SECONDS
                        rotation_retry_count = 0
                        stall_retry_count = 0
                        spread_history = []
                        await feed.track([token_yes, token_no])
                        print(f"\n[PROACTIVE ROTATION] Switched to next active slot: {question} ({slug})")
                        await asyncio.sleep(0.1)
                        continue
                    else:
                        rotation_retry_count += 1
                        status(f"[WAITING] Next market not live yet ({err}), retry #{rotation_retry_count}...")
                        await asyncio.sleep(1.0)
                        continue

                # Pre-fetch the next slot's market while the current one is still live,
                # so the switch at expiry does not depend on a Gamma round trip.
                if (next_market is None and now_ts >= next_prefetch_ts
                        and now_ts >= market_end_ts - PREFETCH_LEAD_SECONDS):
                    next_market, _ = await fetch_5m_market(session, ticker, headers, slot_start + SLOT_SECONDS)
                    next_prefetch_ts = now_ts + 5
                    if next_market is not None:
                        # subscribe early so the next market's books are live at the switch
                        await feed.track([token_yes, token_no, next_market[2], next_market[3]])

                # Both books normally come from memory (WebSocket feeds): no network wait.
                book_yes, book_no = feed.get_book(token_yes), feed.get_book(token_no)
                pm_live = book_yes is not None and book_no is not None
                depth = bfeed.get_depth()
                bn_live = depth is not None
                report_feed("Polymarket", pm_live, feed.last_error)
                report_feed("Binance", bn_live, bfeed.last_error)
                obi_trad_raw = binance_obi(*depth) if bn_live else None
                err_yes = err_no = None

                # REST only for the feeds that are down, fetched CONCURRENTLY.
                rest = {}
                if not bn_live:
                    rest["bn"] = get_binance_obi(session, ticker)
                if not pm_live:
                    rest["yes"] = get_pm_book(session, token_yes, headers)
                    rest["no"] = get_pm_book(session, token_no, headers)
                if rest:
                    got = dict(zip(rest, await asyncio.gather(*rest.values())))
                    if not bn_live:
                        obi_trad_raw = got["bn"]
                    if not pm_live:
                        (book_yes, err_yes), (book_no, err_no) = got["yes"], got["no"]

                if obi_trad_raw is None:
                    stall_retry_count += 1
                    status(f"[WAITING] Binance book unavailable ({bfeed.last_error or 'connecting'}), retry #{stall_retry_count}...")
                    await asyncio.sleep(min(5.0, 0.5 * stall_retry_count))
                    continue

                if book_yes is None or book_no is None:
                    stall_retry_count += 1
                    status(f"[WAITING] Polymarket book unavailable ({err_yes or err_no}), retry #{stall_retry_count}...")
                    await asyncio.sleep(min(5.0, 0.5 * stall_retry_count))
                    continue

                bids_yes, asks_yes = parse_book(book_yes)
                bids_no, asks_no = parse_book(book_no)

                if not bids_yes and not asks_yes:
                    stall_retry_count += 1
                    status(f"[WAITING] Book empty, retry #{stall_retry_count}...")
                    await asyncio.sleep(0.5)
                    continue
                stall_retry_count = 0

                # REAL top of book straight from the CLOB (None = that side is empty)
                best_bid = bids_yes[0][0] if bids_yes else None
                best_ask = asks_yes[0][0] if asks_yes else None
                bid_no   = bids_no[0][0] if bids_no else None
                ask_no   = asks_no[0][0] if asks_no else None

                if best_bid is not None and best_ask is not None:
                    mid_yes = (best_bid + best_ask) / 2
                    bids_near = [b for b in bids_yes if abs(b[0] - mid_yes) <= OBI_BAND]
                    asks_near = [a for a in asks_yes if abs(a[0] - mid_yes) <= OBI_BAND]
                else:
                    bids_near, asks_near = bids_yes, asks_yes

                # SYMMETRIC EXTRACTION: Cut to the top 5 levels for Polymarket too
                v_b_pm = sum(size for _, size in bids_near[:OBI_LEVELS])
                v_a_pm = sum(size for _, size in asks_near[:OBI_LEVELS])

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

                last_row = (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, bid_no, None, 'N/A', slug, time.time())
                if db_cursor is not None:
                    db_cursor.execute(INSERT_SQL, last_row)
                    db_conn.commit()

                src = f"PM:{'WS' if pm_live else 'REST'} BN:{'WS' if bn_live else 'REST'}"
                status(f"Logging [{src}]... YES {fmt_px(best_bid)}/{fmt_px(best_ask)} | NO {fmt_px(bid_no)}/{fmt_px(ask_no)} | "
                       f"Z-Score: {z_score:.2f} | Spread: {divergence:.4f} | T-{max(0, int(market_end_ts - time.time()))}s")
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
