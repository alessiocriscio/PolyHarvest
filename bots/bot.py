import os
import sys
import time
import socket
import aiohttp
import asyncio
import numpy as np
import pandas as pd
import json
import re
import sqlite3
import datetime
import math
from collections import deque
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

# Signal parameters are in SECONDS, not samples, so they keep their meaning
# whatever the sampling rate (~10/s with both WebSocket feeds, slower on REST).
Z_MIN_COVERAGE = 0.9        # emit a Z-Score only once the window spans >= 90% of its length


def env_seconds(name, default):
    """Positive float from the environment (deploy/polyharvest.env), else the default."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
        if value > 0:
            return value
    except ValueError:
        pass
    print(f"[SYSTEM] Ignoring invalid {name}={raw!r}, using {default}")
    return default

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
    "pm_best_bid, pm_best_ask, pm_ask_no, pm_bid_no, order_id, fill_status, market_slug, ts_unix, "
    "ema_tau_s, z_window_s, "
    "btc_bid, btc_ask, btc_event_ms, pm_yes_ts_ms, pm_no_ts_ms, "
    "pm_bid_size, pm_ask_size, pm_bid_size_no, pm_ask_size_no, ptb_est, ptb_basis) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

# Polymarket publishes a market's price to beat only after the market closes, and its
# final price ~6-7 min later. The price to beat IS the Chainlink BTC/USD 60 s TWAP at
# the market's start (= previous market's final price), so live it is estimated as the
# Binance 60 s TWAP at the start plus the recent Chainlink - Binance basis.
META_POLL_SECONDS = 10      # how often Gamma is asked, from the market's close
FINAL_GIVE_UP_SECONDS = 1800  # stop waiting for a final price this long after expiry
TWAP_SECONDS = 60           # settlement TWAP length (resolution source: btc-usd-twap-60s)
BASIS_MARKETS = 12          # basis = median over the last 12 markets (1 hour)
BTC_HISTORY_SECONDS = 150   # BTC mid history kept for the TWAP


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
                    'ts_unix REAL',  # unix seconds with sub-second precision ("timestamp" has whole seconds only)
                    'ema_tau_s REAL', 'z_window_s REAL',  # signal parameters in force for the row
                    # inputs for the fair value of YES: BTC top of book with Binance's event
                    # time, exchange time of each Polymarket book, top-of-book sizes
                    'btc_bid REAL', 'btc_ask REAL', 'btc_event_ms INTEGER',
                    'pm_yes_ts_ms INTEGER', 'pm_no_ts_ms INTEGER',
                    'pm_bid_size REAL', 'pm_ask_size REAL', 'pm_bid_size_no REAL', 'pm_ask_size_no REAL',
                    'price_to_beat REAL',  # unused: the official value only exists after close (see markets)
                    'ptb_est REAL', 'ptb_basis REAL']:  # live estimate of the price to beat, basis it used
        try:
            c.execute(f'ALTER TABLE spread_log ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass  # column already exists
    # One row per market: Polymarket's official price to beat and final price
    # (Chainlink BTC/USD 60 s TWAP), with the time each was first seen.
    c.execute('''CREATE TABLE IF NOT EXISTS markets
                 (market_slug TEXT PRIMARY KEY,
                  slot_start INTEGER,
                  price_to_beat REAL,
                  ptb_seen_ts REAL,
                  final_price REAL,
                  final_seen_ts REAL)''')
    try:  # Binance mid 60 s TWAP at the market's start, to measure the Chainlink basis
        c.execute('ALTER TABLE markets ADD COLUMN binance_twap_start REAL')
    except sqlite3.OperationalError:
        pass
    conn.commit()
    return conn


def binance_obi(bids, asks):
    """OBI over the top 5 levels of a Binance book side pair ([[price, qty], ...], best first)."""
    vol_bids = sum(float(q) for _, q in bids[:5])
    vol_asks = sum(float(q) for _, q in asks[:5])
    df = pd.DataFrame({"bid_size": [vol_bids], "ask_size": [vol_asks]})
    return calculate_obi(df) if (vol_bids + vol_asks) > 0 else 0


async def get_binance_depth(session, symbol):
    """REST fallback for the depth stream: (bids, asks, event_ms), or None on failure."""
    # USD-M perpetual futures: more volume than spot, tends to lead it, more institutional flow
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=5"
    try:
        async with session.get(url, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                bids, asks = data.get('bids') or [], data.get('asks') or []
                if bids and asks:
                    return bids, asks, data.get('E')
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





def _int_or_none(raw):
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _num(raw):
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


async def fetch_event_metadata(session, slug, headers):
    """
    (price_to_beat, final_price) from the Gamma event's eventMetadata, each None
    until Polymarket publishes it; None altogether if the request fails.
    """
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None
    ev = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else {}
    meta = ev.get("eventMetadata") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except ValueError:
            meta = {}
    return _num(meta.get("priceToBeat")), _num(meta.get("finalPrice"))


def time_weighted_mean(samples, a, b, max_gap=5.0):
    """
    Mean over [a, b] of the step function through (t, value) samples sorted by t.
    None if there is no sample within max_gap before a, or a hole longer than max_gap.
    """
    before = [(t, v) for t, v in samples if t <= a]
    if not before or a - before[-1][0] > max_gap:
        return None
    seq = [(a, before[-1][1])] + [(t, v) for t, v in samples if a < t < b] + [(b, None)]
    total = 0.0
    for (t0, v0), (t1, _) in zip(seq, seq[1:]):
        if t1 - t0 > max_gap:
            return None
        total += v0 * (t1 - t0)
    return total / (b - a)


class MarketMeta:
    """
    Polls Gamma in the background, from each tracked market's close, for its
    official price to beat and final price, and stores them in the `markets`
    table with the time each was first seen. Also keeps the Binance 60 s TWAP at
    each market's start, so that the Chainlink - Binance basis can be measured
    and the live price to beat estimated.
    """

    def __init__(self, session, headers, db_conn):
        self.session, self.headers, self.db = session, headers, db_conn
        self.pending = {}   # slug -> {"slot", "ptb", "final"}
        self.added = set()
        self.binance_twap = {}   # slug -> Binance mid 60 s TWAP at the market's start
        self.basis = deque(maxlen=BASIS_MARKETS)   # official price to beat - Binance TWAP
        if db_conn is not None:
            rows = db_conn.execute(
                "SELECT price_to_beat - binance_twap_start FROM markets WHERE price_to_beat IS NOT NULL "
                "AND binance_twap_start IS NOT NULL ORDER BY slot_start DESC LIMIT ?", (BASIS_MARKETS,)).fetchall()
            self.basis.extend(r[0] for r in reversed(rows))

    def add(self, slug, slot_start):
        if slug in self.added:
            return
        self.added.add(slug)
        self.pending[slug] = {"slot": slot_start, "ptb": None, "final": None}
        if self.db is not None:
            self.db.execute("INSERT OR IGNORE INTO markets (market_slug, slot_start) VALUES (?, ?)",
                            (slug, slot_start))
            self.db.commit()

    def set_binance_twap(self, slug, value):
        self.binance_twap[slug] = value
        if self.db is not None:
            self.db.execute("UPDATE markets SET binance_twap_start = ? WHERE market_slug = ?", (value, slug))
            self.db.commit()

    def ptb_estimate(self, slug):
        """(estimated price to beat, basis used); basis None while no market has been measured."""
        twap = self.binance_twap.get(slug)
        if twap is None:
            return None, None
        basis = float(np.median(self.basis)) if self.basis else None
        return twap + (basis or 0.0), basis

    def _save(self, slug, column, value, seen_column, seen_ts):
        if self.db is not None:
            self.db.execute(f"UPDATE markets SET {column} = ?, {seen_column} = ? WHERE market_slug = ?",
                            (value, seen_ts, slug))
            self.db.commit()

    async def run(self):
        while True:
            now = time.time()
            for slug, m in list(self.pending.items()):
                if now < m["slot"] + SLOT_SECONDS:
                    continue  # nothing is published before the market closes
                want_ptb, want_final = m["ptb"] is None, m["final"] is None
                got = await fetch_event_metadata(self.session, slug, self.headers)
                if got is not None:
                    ptb, final = got
                    seen = time.time()
                    if want_ptb and ptb is not None:
                        m["ptb"] = ptb
                        self._save(slug, "price_to_beat", ptb, "ptb_seen_ts", seen)
                        if self.binance_twap.get(slug) is not None:
                            self.basis.append(ptb - self.binance_twap[slug])
                    if want_final and final is not None:
                        m["final"] = final
                        self._save(slug, "final_price", final, "final_seen_ts", seen)
                done = m["final"] is not None and m["ptb"] is not None
                if done or now > m["slot"] + SLOT_SECONDS + FINAL_GIVE_UP_SECONDS:
                    del self.pending[slug]
                    self.binance_twap.pop(slug, None)
            await asyncio.sleep(META_POLL_SECONDS)


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
        # Official price to beat / final price of each market, polled in the background.
        meta = MarketMeta(session, headers, db_conn)
        meta.add(slug, slot_start)
        meta_task = asyncio.create_task(meta.run())
        btc_hist = deque()  # (Binance event time in s, BTC mid) for the start-of-market TWAP
        feed_live = {"Polymarket": False, "Binance": False}

        def report_feed(name, live, err):
            if live != feed_live[name]:
                feed_live[name] = live
                if live:
                    print(f"\n[BOOK FEED] {name}: WebSocket live")
                else:
                    print(f"\n[BOOK FEED] {name}: REST fallback ({err or 'waiting for first update'})")

        ema_tau_s = env_seconds("PM_EMA_TAU_S", 2.8)
        z_window_s = env_seconds("PM_Z_WINDOW_S", 30.0)
        print(f"[SYSTEM] Binance EMA time constant: {ema_tau_s}s | Z-Score window: {z_window_s}s")
        ema_binance, ema_ts = None, None
        spread_history = deque()  # (ts, spread) over the last z_window_s seconds
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
                        spread_history.clear()
                        await feed.track([token_yes, token_no])
                        meta.add(slug, slot_start)
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
                        meta.add(next_market[4], next_market[0])

                # Both books normally come from memory (WebSocket feeds): no network wait.
                book_yes, book_no = feed.get_book(token_yes), feed.get_book(token_no)
                pm_live = book_yes is not None and book_no is not None
                depth = bfeed.get_depth()
                bn_live = depth is not None
                report_feed("Polymarket", pm_live, feed.last_error)
                report_feed("Binance", bn_live, bfeed.last_error)
                if bn_live:
                    depth = (*depth, bfeed.event_ms)
                err_yes = err_no = None

                # REST only for the feeds that are down, fetched CONCURRENTLY.
                rest = {}
                if not bn_live:
                    rest["bn"] = get_binance_depth(session, ticker)
                if not pm_live:
                    rest["yes"] = get_pm_book(session, token_yes, headers)
                    rest["no"] = get_pm_book(session, token_no, headers)
                if rest:
                    got = dict(zip(rest, await asyncio.gather(*rest.values())))
                    if not bn_live:
                        depth = got["bn"]
                    if not pm_live:
                        (book_yes, err_yes), (book_no, err_no) = got["yes"], got["no"]

                if depth is None or not depth[0] or not depth[1]:
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

                bn_bids, bn_asks, btc_event_ms = depth
                obi_trad_raw = binance_obi(bn_bids, bn_asks)
                btc_bid, btc_ask = float(bn_bids[0][0]), float(bn_asks[0][0])
                btc_mid = (btc_bid + btc_ask) / 2
                t_btc = btc_event_ms / 1000 if btc_event_ms else time.time()
                if not btc_hist or t_btc > btc_hist[-1][0]:
                    btc_hist.append((t_btc, btc_mid))
                while t_btc - btc_hist[0][0] > BTC_HISTORY_SECONDS:
                    btc_hist.popleft()
                # Binance 60 s TWAP ending at this market's start, once that window is over
                if slug not in meta.binance_twap and t_btc >= slot_start + 1:
                    meta.set_binance_twap(slug, time_weighted_mean(btc_hist, slot_start - TWAP_SECONDS, slot_start))
                ptb_est, ptb_basis = meta.ptb_estimate(slug)

                # REAL top of book straight from the CLOB (None = that side is empty)
                best_bid = bids_yes[0][0] if bids_yes else None
                best_ask = asks_yes[0][0] if asks_yes else None
                bid_no   = bids_no[0][0] if bids_no else None
                ask_no   = asks_no[0][0] if asks_no else None
                top_sizes = (bids_yes[0][1] if bids_yes else None, asks_yes[0][1] if asks_yes else None,
                             bids_no[0][1] if bids_no else None, asks_no[0][1] if asks_no else None)

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

                tick_ts = time.time()

                # Continuous-time EMA: alpha follows the real gap between samples,
                # so the smoothing horizon is ema_tau_s seconds at any sampling rate.
                if ema_binance is None:
                    ema_binance = obi_trad_raw
                else:
                    alpha = 1 - math.exp(-(tick_ts - ema_ts) / ema_tau_s)
                    ema_binance = (obi_trad_raw * alpha) + (ema_binance * (1 - alpha))
                ema_ts = tick_ts

                # DIRECTIONAL SPREAD: No abs() to maintain signal direction
                divergence = ema_binance - obi_pm

                # ROLLING WINDOW: every sample of the last z_window_s seconds
                spread_history.append((tick_ts, divergence))
                while tick_ts - spread_history[0][0] > z_window_s:
                    spread_history.popleft()

                z_score = 0
                if (len(spread_history) >= 2
                        and tick_ts - spread_history[0][0] >= Z_MIN_COVERAGE * z_window_s):
                    s_series = pd.Series([d for _, d in spread_history])
                    mean, std = s_series.mean(), s_series.std()
                    if std > 0:
                        z_score = (divergence - mean) / std

                last_row = (ticker, obi_trad_raw, ema_binance, obi_pm, divergence, z_score, best_bid, best_ask, ask_no, bid_no, None, 'N/A', slug, tick_ts, ema_tau_s, z_window_s,
                            btc_bid, btc_ask, btc_event_ms,
                            _int_or_none(book_yes.get("timestamp")), _int_or_none(book_no.get("timestamp")),
                            *top_sizes, ptb_est, ptb_basis)
                if db_cursor is not None:
                    db_cursor.execute(INSERT_SQL, last_row)
                    db_conn.commit()

                src = f"PM:{'WS' if pm_live else 'REST'} BN:{'WS' if bn_live else 'REST'}"
                vs_k = (f"K~{ptb_est:,.2f}{'' if ptb_basis is not None else ' (no basis yet)'} "
                        f"({100 * (btc_mid / ptb_est - 1):+.3f}%)" if ptb_est else "K n/a")
                status(f"Logging [{src}]... YES {fmt_px(best_bid)}/{fmt_px(best_ask)} | NO {fmt_px(bid_no)}/{fmt_px(ask_no)} | "
                       f"BTC {btc_mid:,.1f} vs {vs_k} | Z {z_score:.2f} | T-{max(0, int(market_end_ts - time.time()))}s")
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
