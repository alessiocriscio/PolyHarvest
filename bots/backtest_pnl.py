"""
backtest_pnl.py — Backtests filled orders from market_data.db or CSV log files.
Provides both simulated trading backtesting (default) and live order backtesting.
"""

import csv
import sqlite3
import sys

DB_PATH = "market_data.db"
try:
    _cap_input = input("Enter total capital in USD (e.g. 25.0): ").strip()
    capital = float(_cap_input) if _cap_input else 25.0
except Exception:
    capital = 25.0

try:
    _pct_input = input("Enter trade size % of capital (e.g. 5 for 5%): ").strip()
    trade_size_pct = float(_pct_input) if _pct_input else 5.0
except Exception:
    trade_size_pct = 5.0

EXIT_Z_THRESHOLD = 0.5


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
    with open(path, newline="") as f:
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


def run_simulated_backtest(rows, capital, trade_size_pct, z_threshold):
    """
    Simulated trading mode:
    - Scans rows chronologically.
    - Entry: when abs(z_score) > z_threshold AND in_trade == False.
      - is_buy_yes = z_score > 0
      - entry_price = pm_best_ask - 0.01 if BUY Yes, else pm_ask_no - 0.01 if BUY No
      - Skip if entry_price <= 0 or if entry_price < 0.05 or entry_price > 0.95
    - Fill Simulation:
      - considered filled only if in any of the next 60 rows pm_best_bid <= entry_price.
      - If not filled within 60 rows, skip the trade entirely.
    - Exit: when abs(z_score) < 0.5 AND in_trade == True.
      - exit_price = pm_best_bid if BUY Yes, else 1 - pm_best_ask if BUY No
    - Force exit: when fill_status = 'expired'
      - exit_price = 0.5
      - Mark as [EXPIRED]
    - P&L per trade = (exit_price - entry_price) * current_trade_size + rebate
    """
    trades = []
    in_trade = False
    entry_row = None
    entry_price_val = 0.0
    is_buy_yes_val = True
    z_entry_val = 0.0
    skipped_extreme = 0
    current_trade_size = capital * trade_size_pct / 100

    idx = 0
    while idx < len(rows):
        row = rows[idx]
        z_score = row.get("z_score") if row.get("z_score") is not None else 0.0
        
        if not in_trade:
            if abs(z_score) > z_threshold:
                pm_best_bid = row.get("pm_best_bid")
                pm_best_ask = row.get("pm_best_ask")
                if pm_best_bid is None or pm_best_ask is None or pm_best_bid == 0.0 or pm_best_ask == 0.0:
                    idx += 1
                    continue
                
                try:
                    pm_best_bid = float(pm_best_bid)
                    pm_best_ask = float(pm_best_ask)
                except (ValueError, TypeError):
                    idx += 1
                    continue
                
                is_buy_yes = z_score > 0
                if is_buy_yes:
                    entry_price = round(pm_best_bid + 0.01, 2)
                else:
                    pm_bid_no = row.get("pm_bid_no")
                    if pm_bid_no is None or float(pm_bid_no) == 0.0:
                        idx += 1
                        continue
                    entry_price = round(float(pm_bid_no) + 0.01, 2)
                
                if entry_price < 0.05 or entry_price > 0.95:
                    skipped_extreme += 1
                    in_trade = False
                    idx += 1
                    continue
                
                # Fill simulation
                filled = False
                fill_idx = -1
                for k in range(idx + 1, min(idx + 61, len(rows))):
                    r = rows[k]
                    r_bid = r.get("pm_best_bid") if r.get("pm_best_bid") is not None else 0.0
                    r_ask = r.get("pm_best_ask") if r.get("pm_best_ask") is not None else 0.0
                    r_ask_no = r.get("pm_ask_no") if r.get("pm_ask_no") is not None else 0.0
                    
                    if is_buy_yes:
                        if r_ask > 0 and r_ask <= entry_price:
                            filled = True
                            fill_idx = k
                            break
                    else:
                        r_ask_no = r.get("pm_ask_no") if r.get("pm_ask_no") is not None else 0.0
                        if r_ask_no > 0 and r_ask_no <= entry_price:
                            filled = True
                            fill_idx = k
                            break
                
                if filled:
                    in_trade = True
                    entry_row = row
                    entry_price_val = entry_price
                    is_buy_yes_val = is_buy_yes
                    z_entry_val = z_score
                    idx = fill_idx + 1
                else:
                    in_trade = False
                    idx += 1
            else:
                idx += 1
        else:
            # Check for force exit
            if row.get("fill_status") == "expired":
                exit_price = 0.5
                rebate = entry_price_val * current_trade_size * 0.0036
                pnl = (exit_price - entry_price_val) * current_trade_size + rebate
                trades.append({
                    "entry_time": entry_row["timestamp"],
                    "exit_time": row["timestamp"],
                    "entry_price": entry_price_val,
                    "exit_price": exit_price,
                    "side": "BUY Yes [EXPIRED]" if is_buy_yes_val else "BUY No [EXPIRED]",
                    "pnl": round(pnl, 4),
                    "rebate": round(rebate, 4),
                    "z_entry": round(z_entry_val, 2),
                    "is_expired": True
                })
                in_trade = False
                idx += 1
                if abs(pnl) >= 0.01:
                    capital += pnl
                    current_trade_size = capital * trade_size_pct / 100
            # Check for normal exit
            elif abs(z_score) < EXIT_Z_THRESHOLD:
                pm_best_bid = row.get("pm_best_bid") if row.get("pm_best_bid") is not None else 0.0
                pm_best_ask = row.get("pm_best_ask") if row.get("pm_best_ask") is not None else 0.0
                
                if is_buy_yes_val:
                    exit_price_target = round(pm_best_ask - 0.01, 2)
                else:
                    pm_ask_no = row.get("pm_ask_no") if row.get("pm_ask_no") is not None else 0.0
                    exit_price_target = round(float(pm_ask_no) - 0.01, 2)
                
                filled = False
                fill_idx = -1
                for k in range(idx + 1, min(idx + 61, len(rows))):
                    r = rows[k]
                    r_bid = r.get("pm_best_bid") if r.get("pm_best_bid") is not None else 0.0
                    r_ask = r.get("pm_best_ask") if r.get("pm_best_ask") is not None else 0.0
                    
                    if is_buy_yes_val:
                        if r_bid >= exit_price_target:
                            filled = True
                            fill_idx = k
                            break
                    else:
                        r_bid_no = r.get("pm_bid_no") if r.get("pm_bid_no") is not None else 0.0
                        if r_bid_no >= exit_price_target:
                            filled = True
                            fill_idx = k
                            break
                
                if filled:
                    exit_price = exit_price_target
                    rebate = entry_price_val * current_trade_size * 0.0036
                    pnl = (exit_price - entry_price_val) * current_trade_size + rebate
                    trades.append({
                        "entry_time": entry_row["timestamp"],
                        "exit_time": rows[fill_idx]["timestamp"],
                        "entry_price": entry_price_val,
                        "exit_price": exit_price,
                        "side": "BUY Yes" if is_buy_yes_val else "BUY No",
                        "pnl": round(pnl, 4),
                        "rebate": round(rebate, 4),
                        "z_entry": round(z_entry_val, 2),
                    })
                    idx = fill_idx + 1
                else:
                    last_idx = min(idx + 60, len(rows) - 1)
                    r = rows[last_idx]
                    r_bid = r.get("pm_best_bid") if r.get("pm_best_bid") is not None else 0.0
                    r_ask = r.get("pm_best_ask") if r.get("pm_best_ask") is not None else 0.0
                    
                    r_bid_no = r.get("pm_bid_no") if r.get("pm_bid_no") is not None else 0.0
                    exit_price = r_bid if is_buy_yes_val else r_bid_no
                    exit_price = round(exit_price, 2)
                    rebate = entry_price_val * current_trade_size * 0.0036
                    pnl = (exit_price - entry_price_val) * current_trade_size + rebate
                    trades.append({
                        "entry_time": entry_row["timestamp"],
                        "exit_time": r["timestamp"],
                        "entry_price": entry_price_val,
                        "exit_price": exit_price,
                        "side": "BUY Yes" if is_buy_yes_val else "BUY No",
                        "pnl": round(pnl, 4),
                        "rebate": round(rebate, 4),
                        "z_entry": round(z_entry_val, 2),
                    })
                    idx = last_idx + 1
                
                in_trade = False
                if abs(pnl) >= 0.01:
                    capital += pnl
                    current_trade_size = capital * trade_size_pct / 100
            else:
                idx += 1

    if in_trade:
        print(f"[BACKTEST] Open position at end of data: entry {entry_price_val} from {entry_row['timestamp']}")

    print(f"Skipped (extreme price): {skipped_extreme}")
    return trades, capital


def run_backtest():
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

    print_report(trades, trade_size)


def print_report(trades, trade_size, initial_capital=None, final_capital=None):
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

    total_rebates = sum(t.get("rebate", 0.0) for t in trades)

    # Max drawdown
    cumulative = 0
    peak = 0
    max_dd = 0
    for t in trades:
        cumulative += t["pnl"]
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    print("=" * 65)
    print("  BACKTEST P&L REPORT")
    print("=" * 65)
    print(f"  Normal Trades:   {len(normal_trades)}")
    print(f"  Normal P&L:      {total_pnl:+.4f}")
    print(f"  Avg Normal P&L:  {(total_pnl / len(normal_trades)):+.4f}" if normal_trades else "  Avg Normal P&L:  +0.0000")
    print(f"  Win rate:        {win_rate:.1f}%")
    print(f"  Expired Trades:  {expired_count}")
    print(f"  Expired P&L:     {expired_pnl:+.4f}")
    print(f"  Total Rebates:   {total_rebates:+.4f}")
    print(f"  Max drawdown:    {max_dd:.4f}")
    if initial_capital is not None and final_capital is not None:
        print(f"  Final Capital:   {final_capital:.4f}")
        print(f"  Total Return:    {((final_capital - initial_capital) / initial_capital * 100):+.2f}%")
    print("=" * 65)
    print()




if __name__ == "__main__":
    print("Select Backtest Mode:")
    print("1. Simulated Trading Backtest (Default) - scans all data using Z-score strategy")
    print("2. Live Order Backtest - analyzes filled orders from DB/CSV ('fill_status' must be 'ok')")
    try:
        choice = input("Enter choice (1 or 2, default 1): ").strip()
    except Exception:
        choice = "1"
    
    if choice == "2":
        run_backtest()
    else:
        try:
            z_input = input("Enter Z-Score threshold used (default: 2.5): ").strip()
            z_threshold = float(z_input) if z_input else 2.5
        except Exception:
            z_threshold = 2.5
            
        # Load rows
        if len(sys.argv) > 1:
            csv_path = sys.argv[1]
            print(f"[BACKTEST] Reading from CSV: {csv_path}")
            rows = load_rows_from_csv(csv_path)
        else:
            print(f"[BACKTEST] Reading from DB: {DB_PATH}")
            rows = load_rows_from_db()
            
        if not rows:
            print("[BACKTEST] No data found.")
            sys.exit(0)
            
        print(f"\n[BACKTEST] Running Simulated Backtest (z_threshold={z_threshold})...")
        trades, final_capital = run_simulated_backtest(rows, capital, trade_size_pct, z_threshold)
        print_report(trades, capital * trade_size_pct / 100, initial_capital=capital, final_capital=final_capital)
