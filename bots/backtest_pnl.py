"""
backtest_pnl.py — Backtests filled orders from market_data.db

For each row where fill_status = 'ok', treat as a trade entry.
Exit = next row where abs(z_score) < 0.5.
Entry price = pm_best_ask - 0.01 (the limit price sent to executor).
Exit price  = pm_best_bid at exit row (sell back at best bid).
"""

import csv
import sqlite3
import sys

DB_PATH = "market_data.db"
SIZE = 10
EXIT_Z_THRESHOLD = 0.5


def load_rows_from_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT rowid, timestamp, z_score, pm_best_bid, pm_best_ask, fill_status
        FROM spread_log
        ORDER BY rowid ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def load_rows_from_csv(path):
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "timestamp": r.get("timestamp", ""),
                "z_score": float(r["z_score"]) if r.get("z_score") else 0,
                "pm_best_bid": float(r["pm_best_bid"]) if r.get("pm_best_bid") else 0,
                "pm_best_ask": float(r["pm_best_ask"]) if r.get("pm_best_ask") else 0,
                "fill_status": r.get("fill_status", ""),
            })
    return rows


def run_backtest():
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
                pnl = (exit_price - entry_price) * SIZE
            else:
                # BUY No exit: approximate No price = 1 - Yes ask (Yes + No = 1)
                exit_price = round(1.0 - (exit_row["pm_best_ask"] or 0), 2)
                pnl = (exit_price - entry_price) * SIZE

            trades.append({
                "entry_time": row["timestamp"],
                "exit_time": exit_row["timestamp"],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "side": "BUY Yes" if is_buy_yes else "BUY No",
                "pnl": round(pnl, 4),
                "z_entry": round(z_at_entry, 2),
            })
        i += 1

    # ─── Report ───
    if not trades:
        print("[BACKTEST] No filled trades found (fill_status = 'ok').")
        return

    total_pnl = sum(t["pnl"] for t in trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    win_rate = wins / len(trades) * 100

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
    print(f"  Trades:          {len(trades)}")
    print(f"  Total P&L:       {total_pnl:+.4f}")
    print(f"  Avg P&L/trade:   {total_pnl / len(trades):+.4f}")
    print(f"  Win rate:        {win_rate:.1f}%")
    print(f"  Max drawdown:    {max_dd:.4f}")
    print("=" * 65)
    print()

    # Detail table
    print(f"{'#':<4} {'Side':<10} {'Entry':>8} {'Exit':>8} {'P&L':>10} {'Z':>6}  Entry Time")
    print("-" * 80)
    for i, t in enumerate(trades, 1):
        print(f"{i:<4} {t['side']:<10} {t['entry_price']:>8.2f} {t['exit_price']:>8.2f} {t['pnl']:>+10.4f} {t['z_entry']:>6.2f}  {t['entry_time']}")


if __name__ == "__main__":
    run_backtest()
