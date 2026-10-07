"""
backtest_pnl.py — Backtests the Z-Score strategy on market_data.db or a CSV export
(.csv or .csv.gz, as written by the server export).

Usage:
    python backtest_pnl.py                      # reads market_data.db
    python backtest_pnl.py export.csv.gz        # reads a CSV export

Mode 1 simulates the strategy on the logged books. Mode 2 analyses real orders
(fill_status = 'ok'), which only exist once the executor trades live.
"""

import csv
import gzip
import math
import sqlite3
import sys

import numpy as np
import pandas as pd

DB_PATH = "market_data.db"

EXIT_Z_THRESHOLD = 0.5
TICK = 0.01
PRICE_BAND = (0.05, 0.95)        # skip entries priced outside this band
FILL_TIMEOUT_S = 20.0            # a resting limit order is cancelled after this long

# Polymarket fees on crypto Up/Down markets: taker pays shares * rate * p * (1 - p),
# makers pay nothing and get back a share of the taker fees as a daily rebate.
TAKER_FEE_RATE = 0.07
MAKER_REBATE_SHARE = 0.20

# Risk-free reference for the Sharpe ratio: weighted average yield to maturity of the
# iShares EUR Govt Bond 7-10yr UCITS ETF (IBGM, IE00B1FZS806), 4.14% as of 2026-10-01.
# Update it from the fund page before each run.
RISK_FREE_ANNUAL = 0.0414

SLOT_SECONDS = 300                       # one 5-minute market = one return period
PERIODS_PER_YEAR = 365 * 24 * 3600 / SLOT_SECONDS   # crypto markets trade 24/7


def _open_text(path):
    return gzip.open(path, "rt", newline="") if path.endswith(".gz") else open(path, newline="")


# ─── data loading for the simulation (columnar: ~2M rows fit in a few hundred MB) ──────

def load_columns(csv_path=None):
    """
    Returns a dict of numpy arrays sorted by time: ts (unix seconds), label (text time),
    z, bid_yes, ask_yes, bid_no, ask_no, market (int id per contiguous market).
    Missing prices (empty book side) are NaN.
    """
    wanted = ["time_utc", "timestamp", "ts_unix", "z_score", "pm_best_bid", "pm_best_ask",
              "pm_bid_no", "pm_ask_no", "market_slug"]
    if csv_path:
        with _open_text(csv_path) as f:
            header = next(csv.reader(f))
        df = pd.read_csv(csv_path, usecols=[c for c in wanted if c in header])
    else:
        conn = sqlite3.connect(DB_PATH)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(spread_log)")]
        df = pd.read_sql_query(
            f"SELECT {', '.join(c for c in wanted if c in cols)} FROM spread_log ORDER BY rowid", conn)
        conn.close()
    if df.empty:
        return None

    if "ts_unix" in df and df["ts_unix"].notna().all():
        ts = df["ts_unix"].astype(float)
    else:
        print("[WARN] No ts_unix column: using whole-second timestamps.")
        ts = pd.to_datetime(df["timestamp"], utc=True).astype("int64") / 1e9
    df = df.assign(_ts=ts).sort_values("_ts", kind="stable").reset_index(drop=True)

    if "market_slug" in df and df["market_slug"].notna().all():
        slug = df["market_slug"]
        market = (slug != slug.shift()).cumsum().to_numpy()
    else:
        print("[WARN] No market_slug column: the whole file is treated as ONE market, "
              "so positions are not closed at market expiry.")
        market = np.zeros(len(df), dtype=int)

    label = df["time_utc"] if "time_utc" in df else df.get("timestamp", df["_ts"])
    num = lambda c: pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float) if c in df \
        else np.full(len(df), np.nan)
    return {
        "ts": df["_ts"].to_numpy(dtype=float),
        "label": label.astype(str).to_numpy(),
        "z": np.nan_to_num(num("z_score")),
        "bid_yes": num("pm_best_bid"), "ask_yes": num("pm_best_ask"),
        "bid_no": num("pm_bid_no"), "ask_no": num("pm_ask_no"),
        "market": market,
    }


# ─── simulation ──────────────────────────────────────────────────────────────────

def taker_fee(shares, price):
    return shares * TAKER_FEE_RATE * price * (1 - price)


def maker_rebate(shares, price):
    # estimate: our share of the fee the taker paid on the shares we provided
    return MAKER_REBATE_SHARE * taker_fee(shares, price)


def _valid(p):
    return not math.isnan(p) and p > 0


def run_simulated_backtest(d, capital, trade_size_pct, z_threshold):
    """
    - Entry: |z| > z_threshold, flat. z > 0 -> BUY Yes, z < 0 -> BUY No.
      Limit buy at bid + 1 tick. If that already reaches the ask the order crosses and
      fills at the ask as TAKER; otherwise it rests and fills as MAKER if, within
      FILL_TIMEOUT_S and the same market, that side's ask comes down to the limit.
    - Exit: |z| < EXIT_Z_THRESHOLD. Limit sell at ask - 1 tick, symmetric rules; if the
      resting sell is not filled in time, sell at the bid as TAKER.
    - Market expiry: a position still open on the market's last row is sold at that
      row's bid as TAKER (0 if nobody bids: the token is worthless).
    - Size: trade_size_pct of current capital in USD -> shares = USD / entry price.
    """
    ts, z, mkt, label = d["ts"], d["z"], d["market"], d["label"]
    n = len(ts)
    # index of the last row of each row's market
    last = np.empty(n, dtype=int)
    bounds = np.flatnonzero(np.diff(mkt)) + 1
    starts = np.r_[0, bounds]
    ends = np.r_[bounds, n] - 1
    for s, e in zip(starts, ends):
        last[s:e + 1] = e

    trades, equity = [], [(ts[0], capital)]
    skipped_extreme = 0
    i = 0
    while i < n:
        if abs(z[i]) <= z_threshold:
            i += 1
            continue
        yes = z[i] > 0
        bid, ask = (d["bid_yes"], d["ask_yes"]) if yes else (d["bid_no"], d["ask_no"])
        if not (_valid(bid[i]) and _valid(ask[i])):
            i += 1
            continue
        limit = round(bid[i] + TICK, 2)
        if limit < PRICE_BAND[0] or limit > PRICE_BAND[1]:
            skipped_extreme += 1
            i += 1
            continue

        # ── entry fill ──
        if limit >= ask[i]:
            entry_price, entry_maker, f = ask[i], False, i
        else:
            f = None
            k = i + 1
            while k <= last[i] and ts[k] - ts[i] <= FILL_TIMEOUT_S:
                if _valid(ask[k]) and ask[k] <= limit:
                    f = k
                    break
                k += 1
            if f is None:
                i += 1
                continue
            entry_price, entry_maker = limit, True

        stake = capital * trade_size_pct / 100
        shares = stake / entry_price
        z_entry = z[i]

        # ── hold until exit signal or market end ──
        j = f + 1
        while j <= last[f] and abs(z[j]) >= EXIT_Z_THRESHOLD:
            j += 1
        exit_maker = False
        if j > last[f]:
            x, reason = last[f], "EXPIRED"
        else:
            x, reason = None, "SIGNAL"
            passive = _valid(bid[j]) and _valid(ask[j]) and round(ask[j] - TICK, 2) > bid[j]
            if passive:
                target = round(ask[j] - TICK, 2)
                k = j + 1
                while k <= last[j] and ts[k] - ts[j] <= FILL_TIMEOUT_S:
                    if _valid(bid[k]) and bid[k] >= target:
                        x, exit_price, exit_maker = k, target, True
                        break
                    k += 1
                if x is None:
                    x = min(k, last[j])  # cancelled at timeout (or market end): cross the spread
            else:
                x = j                    # 1-tick spread: selling at the bid right away
        if not exit_maker:
            exit_price = bid[x] if _valid(bid[x]) else 0.0

        fees = (0 if entry_maker else taker_fee(shares, entry_price)) + \
               (0 if exit_maker else taker_fee(shares, exit_price))
        rebates = (maker_rebate(shares, entry_price) if entry_maker else 0) + \
                  (maker_rebate(shares, exit_price) if exit_maker else 0)
        pnl = shares * (exit_price - entry_price) - fees + rebates
        capital += pnl
        equity.append((ts[x], capital))
        trades.append({
            "entry_time": label[f], "exit_time": label[x],
            "holding_s": round(ts[x] - ts[f], 2),
            "side": "BUY Yes" if yes else "BUY No",
            "z_entry": round(z_entry, 2),
            "entry_price": round(entry_price, 4), "exit_price": round(exit_price, 4),
            "entry_fill": "maker" if entry_maker else "taker",
            "exit_fill": "maker" if exit_maker else "taker",
            "shares": round(shares, 4), "stake_usd": round(stake, 4),
            "fees": round(fees, 6), "rebates": round(rebates, 6),
            "pnl": round(pnl, 6), "capital_after": round(capital, 6),
            "exit_reason": reason, "is_expired": reason == "EXPIRED",
        })
        i = x + 1

    print(f"Skipped (extreme price): {skipped_extreme}")
    return trades, capital, equity


# ─── Sharpe ratio ────────────────────────────────────────────────────────────────

def sharpe_ratio(d, equity, rf_annual=RISK_FREE_ANNUAL):
    """
    Excess-return Sharpe on 5-minute periods (one per market), annualised 24/7.
    Positions never outlive their market, so equity at a slot boundary is exact.
    Only slots with logged data count: hours with the bot stopped are not zero returns.
    Returns (annual_sharpe, annual_standard_error, n_periods) or None.
    """
    slots = np.unique((d["ts"] // SLOT_SECONDS).astype(np.int64))
    if len(slots) < 3:
        return None
    eq_ts = np.array([t for t, _ in equity])
    eq_val = np.array([v for _, v in equity])
    # equity at the end of each slot = capital after the last trade closed in it
    slot_end = (slots + 1) * SLOT_SECONDS
    pos = np.searchsorted(eq_ts, slot_end, side="right") - 1
    eq_end = eq_val[pos]
    pos0 = np.searchsorted(eq_ts, slots * SLOT_SECONDS, side="right") - 1
    eq_start = eq_val[np.maximum(pos0, 0)]
    returns = eq_end / eq_start - 1
    rf_period = (1 + rf_annual) ** (1 / PERIODS_PER_YEAR) - 1
    excess = returns - rf_period
    sd = excess.std(ddof=1)
    if sd == 0:
        return None
    sr = excess.mean() / sd
    # standard error of a Sharpe estimate, iid approximation (Lo, 2002)
    se = math.sqrt((1 + 0.5 * sr * sr) / len(excess))
    k = math.sqrt(PERIODS_PER_YEAR)
    return sr * k, se * k, len(excess)


# ─── live-order mode (needs executor fills in the data) ──────────────────────────

def load_rows_from_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT rowid, timestamp, z_score, pm_best_bid, pm_best_ask, pm_ask_no, pm_bid_no, fill_status
            FROM spread_log
            ORDER BY rowid ASC
        """)
        rows = [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        # Fallback if pm_ask_no column does not exist in older db schemas
        cur.execute("""
            SELECT rowid, timestamp, z_score, pm_best_bid, pm_best_ask, fill_status
            FROM spread_log
            ORDER BY rowid ASC
        """)
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            d["pm_ask_no"] = 0.0
            d["pm_bid_no"] = 0.0
            rows.append(d)
    conn.close()
    return rows


def load_rows_from_csv(path):
    rows = []
    with _open_text(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "timestamp": r.get("timestamp", ""),
                "z_score": float(r["z_score"]) if (r.get("z_score") is not None and r.get("z_score") != "") else 0.0,
                "pm_best_bid": float(r["pm_best_bid"]) if (r.get("pm_best_bid") is not None and r.get("pm_best_bid") != "") else 0.0,
                "pm_best_ask": float(r["pm_best_ask"]) if (r.get("pm_best_ask") is not None and r.get("pm_best_ask") != "") else 0.0,
                "pm_ask_no": float(r["pm_ask_no"]) if (r.get("pm_ask_no") is not None and r.get("pm_ask_no") != "") else 0.0,
                "pm_bid_no": float(r["pm_bid_no"]) if (r.get("pm_bid_no") is not None and r.get("pm_bid_no") != "") else 0.0,
                "fill_status": r.get("fill_status", ""),
            })
    return rows


def run_backtest(capital, trade_size_pct):
    """
    Existing live order backtest (fill_status = 'ok').
    Requires live execution data to be populated in the database.
    """
    print("\n[NOTE] The live order backtest (fill_status='ok') requires live execution data.")

    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        print(f"[BACKTEST] Reading from CSV: {csv_path}")
        rows = load_rows_from_csv(csv_path)
    else:
        print(f"[BACKTEST] Reading from DB: {DB_PATH}")
        rows = load_rows_from_db()

    if not rows:
        print("[BACKTEST] No data found.")
        return

    trade_size = capital * trade_size_pct / 100

    trades = []
    i = 0
    while i < len(rows):
        row = rows[i]
        if row["fill_status"] == "ok":
            entry_price = round((row["pm_best_ask"] or 0) - 0.01, 2)
            z_at_entry = row["z_score"] or 0
            is_buy_yes = z_at_entry > 0  # z > 0 → BUY Yes; z < 0 → BUY No

            # Scan forward for exit: abs(z_score) < EXIT_Z_THRESHOLD
            exit_row = None
            for j in range(i + 1, len(rows)):
                if abs(rows[j]["z_score"] or 0) < EXIT_Z_THRESHOLD:
                    exit_row = rows[j]
                    i = j  # resume scanning after exit
                    break

            if exit_row is None:
                # No exit found — position still open
                print(f"[BACKTEST] Open position from {row['timestamp']} (entry {entry_price}) — no exit yet")
                i += 1
                continue

            if is_buy_yes:
                exit_price = exit_row["pm_best_bid"] or 0
                pnl = (exit_price - entry_price) * trade_size
            else:
                # BUY No exit: approximate No price = 1 - Yes ask (Yes + No = 1)
                exit_price = exit_row["pm_bid_no"] or 0.0
                pnl = (exit_price - entry_price) * trade_size

            trades.append({
                "entry_time": row["timestamp"],
                "exit_time": exit_row["timestamp"],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "side": "BUY Yes" if is_buy_yes else "BUY No",
                "pnl": round(pnl, 4),
                "z_entry": round(z_at_entry, 2),
            })
        elif row["fill_status"] == "expired":
            # Find the most recent preceding row with fill_status = 'ok'
            entry_row = None
            for j in range(i - 1, -1, -1):
                if rows[j]["fill_status"] == "ok":
                    entry_row = rows[j]
                    break

            if entry_row is not None:
                entry_price = round((entry_row["pm_best_ask"] or 0) - 0.01, 2)
                z_at_entry = entry_row["z_score"] or 0
                is_buy_yes = z_at_entry > 0

                exit_price = 0.5
                pnl = (exit_price - entry_price) * trade_size

                trades.append({
                    "entry_time": entry_row["timestamp"],
                    "exit_time": row["timestamp"],
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "side": "BUY Yes [EXPIRED]" if is_buy_yes else "BUY No [EXPIRED]",
                    "pnl": round(pnl, 4),
                    "z_entry": round(z_at_entry, 2),
                    "is_expired": True
                })
        i += 1

    print_report(trades)


# ─── report ──────────────────────────────────────────────────────────────────────

def print_report(trades, initial_capital=None, final_capital=None, sharpe=None,
                 rf_annual=RISK_FREE_ANNUAL):
    if not trades:
        print("[BACKTEST] No filled trades found.")
        return

    normal_trades = [t for t in trades if not t.get("is_expired", False)]
    expired_trades = [t for t in trades if t.get("is_expired", False)]

    total_pnl = sum(t["pnl"] for t in normal_trades)
    wins = sum(1 for t in normal_trades if t["pnl"] > 0)
    win_rate = (wins / len(normal_trades) * 100) if normal_trades else 0.0

    expired_count = len(expired_trades)
    expired_pnl = sum(t["pnl"] for t in expired_trades)

    total_fees = sum(t.get("fees", 0.0) for t in trades)
    total_rebates = sum(t.get("rebates", 0.0) for t in trades)

    # Max drawdown, in USD and as % of the running equity peak
    equity = initial_capital or 0.0
    peak = equity
    max_dd = max_dd_pct = 0.0
    for t in trades:
        equity += t["pnl"]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        if peak > 0:
            max_dd_pct = max(max_dd_pct, (peak - equity) / peak * 100)

    print("=" * 65)
    print("  BACKTEST P&L REPORT")
    print("=" * 65)
    print(f"  Normal Trades:   {len(normal_trades)}")
    print(f"  Normal P&L:      {total_pnl:+.4f}")
    print(f"  Avg Normal P&L:  {(total_pnl / len(normal_trades)):+.4f}" if normal_trades else "  Avg Normal P&L:  +0.0000")
    print(f"  Win rate:        {win_rate:.1f}%")
    print(f"  Expired Trades:  {expired_count}")
    print(f"  Expired P&L:     {expired_pnl:+.4f}")
    if "fees" in trades[0]:
        makers = sum((t["entry_fill"] == "maker") + (t["exit_fill"] == "maker") for t in trades)
        print(f"  Fills maker/taker: {makers}/{2 * len(trades) - makers}")
        print(f"  Avg holding:     {np.mean([t['holding_s'] for t in trades]):.1f}s")
        print(f"  Taker Fees:      {-total_fees:+.4f}")
    print(f"  Maker Rebates:   {total_rebates:+.4f}")
    print(f"  Max drawdown:    {max_dd:.4f}" + (f" ({max_dd_pct:.2f}%)" if initial_capital else ""))
    if initial_capital is not None and final_capital is not None:
        print(f"  Final Capital:   {final_capital:.4f}")
        print(f"  Total Return:    {((final_capital - initial_capital) / initial_capital * 100):+.2f}%")
    if sharpe is not None:
        sr, se, n = sharpe
        print(f"  Sharpe (annual): {sr:.2f}  ± {se:.2f} (1 s.e., {n} periods of 5 min)")
        print(f"  Risk-free:       {rf_annual * 100:.2f}% / year")
    elif initial_capital is not None:
        print("  Sharpe (annual): n/a (too few periods or no variance)")
    print("=" * 65)
    print()


def save_trades(trades, path="backtest_trades.csv"):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(trades[0].keys()))
        w.writeheader()
        w.writerows(trades)
    print(f"[BACKTEST] {len(trades)} trades written to {path}")


def ask_float(prompt, default):
    try:
        raw = input(prompt).strip()
        return float(raw) if raw else default
    except Exception:
        return default


if __name__ == "__main__":
    capital = ask_float("Enter total capital in USD (e.g. 25.0): ", 25.0)
    trade_size_pct = ask_float("Enter trade size % of capital (e.g. 5 for 5%): ", 5.0)

    print("Select Backtest Mode:")
    print("1. Simulated Trading Backtest (Default) - scans all data using Z-score strategy")
    print("2. Live Order Backtest - analyzes filled orders from DB/CSV ('fill_status' must be 'ok')")
    try:
        choice = input("Enter choice (1 or 2, default 1): ").strip()
    except Exception:
        choice = "1"

    if choice == "2":
        run_backtest(capital, trade_size_pct)
    else:
        z_threshold = ask_float("Enter Z-Score threshold used (default: 2.5): ", 2.5)
        rf = ask_float(f"Risk-free rate, % per year (default {RISK_FREE_ANNUAL * 100:.2f}): ",
                       RISK_FREE_ANNUAL * 100) / 100

        csv_path = sys.argv[1] if len(sys.argv) > 1 else None
        print(f"[BACKTEST] Reading from {'CSV: ' + csv_path if csv_path else 'DB: ' + DB_PATH}")
        data = load_columns(csv_path)
        if data is None:
            print("[BACKTEST] No data found.")
            sys.exit(0)
        print(f"[BACKTEST] {len(data['ts'])} rows, {data['market'][-1] - data['market'][0] + 1} markets")

        print(f"\n[BACKTEST] Running Simulated Backtest (z_threshold={z_threshold})...")
        trades, final_capital, equity = run_simulated_backtest(data, capital, trade_size_pct, z_threshold)
        sharpe = sharpe_ratio(data, equity, rf) if trades else None
        print_report(trades, initial_capital=capital, final_capital=final_capital,
                     sharpe=sharpe, rf_annual=rf)
        if trades:
            save_trades(trades)
