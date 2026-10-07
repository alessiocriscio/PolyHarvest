"""
latency_study.py — Does Polymarket reprice the 5-minute BTC Up/Down markets
instantly when BTC moves on Binance, or with a lag that can be traded?

Usage:
    python latency_study.py market_data_YYYYMMDD_HHMM.csv.gz [--spot] [--twap 30] [--z 2.0]

Part A needs only the export. Parts B-D also need BTC trades: the covered days are
downloaded once from data.binance.vision (public data) into ./binance_cache.

Fair value of YES (driftless Brownian log-price, see fair_value()):
    P(final reference price >= price to beat | BTC now)
Settlement uses Chainlink, sampled here through Binance: the price to beat and the
current price come from the same Binance series, so a constant Chainlink/Binance
basis cancels out in the ratio.
"""
import argparse
import math
import os
import urllib.request
import zipfile
from statistics import NormalDist

import numpy as np
import pandas as pd

SLOT = 300                  # seconds per market
DT = 0.1                    # analysis grid step, seconds
TICK = 0.01
TAKER_FEE_RATE = 0.07       # Polymarket crypto taker fee: shares * rate * p * (1 - p)
SIGMA_LOOKBACK = 1800       # realised volatility window before each market, seconds
SIGMA_STEP = 5              # return interval for the volatility estimate, seconds
HORIZONS_Z = [1, 2, 5, 10, 30, 60, 120]
SHOCK_WINDOW = 0.5          # BTC move measured over the last 0.5 s
HORIZONS_LAG = [0, 0.1, 0.2, 0.3, 0.5, 1, 2, 3, 5, 10, 20, 30]
QUICK_EXIT = 10             # alternative exit: sell at the bid after 10 s

_PHI = np.vectorize(NormalDist().cdf)


def fee(p):
    return TAKER_FEE_RATE * p * (1 - p)


def cluster_se(values, groups):
    """Standard error of the mean, clustered by market (observations in a market are correlated)."""
    values, groups = np.asarray(values, float), np.asarray(groups)
    n = len(values)
    if n < 2:
        return float("nan")
    e = values - values.mean()
    sums = pd.Series(e).groupby(groups).sum().to_numpy()
    return math.sqrt((sums ** 2).sum()) / n


# ─── data ────────────────────────────────────────────────────────────────────────

def load_pm(path):
    cols = ["ts_unix", "z_score", "pm_best_bid", "pm_best_ask", "pm_bid_no", "pm_ask_no",
            "market_slug", "ticker"]
    df = pd.read_csv(path, usecols=lambda c: c in cols)
    df = df.dropna(subset=["ts_unix", "market_slug"]).sort_values("ts_unix", kind="stable")
    df["slot"] = df["market_slug"].str.rsplit("-", n=1).str[-1].astype(np.int64)
    for c in ["pm_best_bid", "pm_best_ask", "pm_bid_no", "pm_ask_no", "z_score"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["mid"] = (df["pm_best_bid"] + df["pm_best_ask"]) / 2
    return df.reset_index(drop=True)


def load_binance(symbol, days, spot, cache="binance_cache"):
    """aggTrades for the given UTC days -> (time_s, price) sorted by time."""
    os.makedirs(cache, exist_ok=True)
    market = "spot" if spot else "futures/um"
    times, prices = [], []
    for day in days:
        name = f"{symbol}-aggTrades-{day}.zip"
        path = os.path.join(cache, ("spot-" if spot else "um-") + name)
        if not os.path.exists(path):
            url = f"https://data.binance.vision/data/{market}/daily/aggTrades/{symbol}/{name}"
            print(f"[BINANCE] downloading {url}")
            try:
                urllib.request.urlretrieve(url, path + ".part")
                os.replace(path + ".part", path)
            except Exception as e:
                print(f"[BINANCE] {day} not available ({e}); published the day after. Skipping.")
                continue
        with zipfile.ZipFile(path) as zf:
            member = zf.namelist()[0]
            with zf.open(member) as f:
                has_header = not f.readline().decode().strip()[:1].isdigit()
            with zf.open(member) as f:
                df = pd.read_csv(f, header=0 if has_header else None, usecols=[1, 5])
        t = df.iloc[:, 1].to_numpy(dtype=np.float64)
        unit = 1e6 if np.median(t) > 1e14 else 1e3      # microseconds since 2025 on some files
        times.append(t / unit)
        prices.append(df.iloc[:, 0].to_numpy(dtype=np.float64))
    if not times:
        return None
    t, p = np.concatenate(times), np.concatenate(prices)
    order = np.argsort(t, kind="stable")
    return t[order], p[order]


def log_price_at(bt, bp_log, when, max_age=5.0):
    i = np.searchsorted(bt, when, side="right") - 1
    ok = (i >= 0) & (when - bt[np.maximum(i, 0)] <= max_age)
    out = np.full(np.shape(when), np.nan)
    out[ok] = bp_log[i[ok]]
    return out


# ─── fair value ──────────────────────────────────────────────────────────────────

def fair_value(x_grid, t_grid, start, log_k, sigma, twap):
    """
    YES fair value on a slot's grid. x = log BTC price, sigma per sqrt(second).
    twap = L > 0: settlement compares the mean log price over [T-L, T] with log_k.
      t <= T-L: mean x_t,                       var sigma^2 * ((T-L-t) + L/3)
      t >  T-L: mean (I_known + (T-t) x_t) / L, var sigma^2 * (T-t)^3 / (3 L^2)
    twap = 0: snapshot settlement, var sigma^2 * (T-t).
    """
    end = start + SLOT
    rem = end - t_grid
    y = x_grid - log_k          # log distance from the price to beat (small numbers)
    if twap <= 0:
        mean, var = y, sigma ** 2 * rem
    else:
        in_avg = t_grid >= end - twap - DT / 2
        # integral of y over [T-L, t) from the grid itself
        cum = np.cumsum(np.where(in_avg, y, 0.0)) * DT
        known = np.r_[0.0, cum[:-1]]
        mean = np.where(in_avg, (known + rem * y) / twap, y)
        var = np.where(in_avg, sigma ** 2 * rem ** 3 / (3 * twap ** 2),
                       sigma ** 2 * ((end - twap - t_grid) + twap / 3))
    sd = np.sqrt(np.maximum(var, 1e-18))
    return _PHI(mean / sd)


# ─── part A: does the current Z signal predict the Polymarket price? ─────────────

def part_a(pm, thr):
    print("\n" + "=" * 72)
    print(f" A. Z-SCORE SIGNAL (|Z| > {thr}): YES mid move after the signal, in ticks")
    print("    signed in the signal's direction, no costs. Episodes = first row above threshold.")
    print("=" * 72)
    res = {h: ([], []) for h in HORIZONS_Z}
    costs = []
    for slot, g in pm.groupby("slot", sort=False):
        ts, z, mid = g["ts_unix"].to_numpy(), g["z_score"].to_numpy(), g["mid"].to_numpy()
        above = np.abs(np.nan_to_num(z)) > thr
        starts = np.flatnonzero(above & ~np.r_[False, above[:-1]])
        for i in starts:
            if np.isnan(mid[i]):
                continue
            side = np.sign(z[i])
            ask = g["pm_best_ask"].iat[i] if side > 0 else g["pm_ask_no"].iat[i]
            bid = g["pm_best_bid"].iat[i] if side > 0 else g["pm_bid_no"].iat[i]
            if not (np.isnan(ask) or np.isnan(bid)):
                costs.append((ask - bid + fee(ask) + fee(bid)) / TICK)
            for h in HORIZONS_Z:
                j = np.searchsorted(ts, ts[i] + h)
                if j < len(ts) and not np.isnan(mid[j]):
                    res[h][0].append(side * (mid[j] - mid[i]) / TICK)
                    res[h][1].append(slot)
    print(f"  {'horizon':>8} {'events':>7} {'mean ticks':>11} {'s.e.':>7} {'t-stat':>7}")
    for h in HORIZONS_Z:
        v, grp = res[h]
        if len(v) < 2:
            continue
        m, se = np.mean(v), cluster_se(v, grp)
        print(f"  {h:>7}s {len(v):>7} {m:>11.3f} {se:>7.3f} {m / se if se > 0 else float('nan'):>7.2f}")
    if costs:
        print(f"  Round-trip cost as taker (spread + 2 fees): {np.mean(costs):.2f} ticks on average")


# ─── parts B-D: BTC -> Polymarket ────────────────────────────────────────────────

def build_slots(pm, bt, bx, twap):
    """Per market: grid, fair value, Polymarket quotes on the grid, outcome."""
    slots = []
    agree = [0, 0]
    for slot, g in pm.groupby("slot", sort=False):
        t = slot + np.arange(0, SLOT, DT)
        x = log_price_at(bt, bx, t)
        if np.isnan(x).mean() > 0.05:
            continue
        x = pd.Series(x).ffill().bfill().to_numpy()
        # price to beat and realised volatility from Binance
        if twap > 0:
            k = np.nanmean(log_price_at(bt, bx, slot - twap + np.arange(0, twap, DT)))
            fin = np.nanmean(log_price_at(bt, bx, slot + SLOT - twap + np.arange(0, twap, DT)))
        else:
            k = log_price_at(bt, bx, np.array([slot]))[0]
            fin = log_price_at(bt, bx, np.array([slot + SLOT]))[0]
        r = np.diff(log_price_at(bt, bx, slot - SIGMA_LOOKBACK + np.arange(0, SIGMA_LOOKBACK + 1, SIGMA_STEP)))
        r = r[~np.isnan(r)]
        if len(r) < 60 or np.isnan(k) or np.isnan(fin):
            continue
        sigma = math.sqrt(np.mean(r ** 2) / SIGMA_STEP)
        fv = fair_value(x, t, slot, k, sigma, twap)

        ts = g["ts_unix"].to_numpy()
        i = np.searchsorted(ts, t, side="right") - 1
        ok = (i >= 0) & (t - ts[np.maximum(i, 0)] <= 1.0)
        q = {}
        for c in ["mid", "pm_best_bid", "pm_best_ask", "pm_bid_no", "pm_ask_no"]:
            v = np.full(len(t), np.nan)
            v[ok] = g[c].to_numpy()[i[ok]]
            q[c] = v

        up_binance = 1.0 if fin >= k else 0.0
        last_mid = g["mid"].dropna()
        outcome = up_binance
        if len(last_mid) and (last_mid.iat[-1] >= 0.95 or last_mid.iat[-1] <= 0.05):
            pm_out = 1.0 if last_mid.iat[-1] >= 0.95 else 0.0
            agree[0] += pm_out == up_binance
            agree[1] += 1
            outcome = pm_out
        slots.append(dict(slot=slot, t=t, fv=fv, y=outcome, sigma=sigma, **q))
    return slots, agree


def part_b(slots, agree):
    print("\n" + "=" * 72)
    print(" B. WHO FORECASTS THE OUTCOME BETTER? Brier score (lower = better)")
    print("    Binance fair value (zero-latency model) vs Polymarket mid, same instant")
    print("=" * 72)
    if agree[1]:
        print(f"  Outcome check: Binance reference agrees with Polymarket's final price in "
              f"{agree[0]}/{agree[1]} decided markets ({100 * agree[0] / agree[1]:.1f}%)")
    vol = np.median([s["sigma"] for s in slots]) * math.sqrt(SLOT)
    print(f"  Median 5-minute BTC volatility: {100 * vol:.3f}%")
    edges = [(240, 300), (180, 240), (120, 180), (60, 120), (30, 60), (0, 30)]
    print("  PM - FV > 0 means the Binance model forecasts better (s.e. clustered by market)")
    print(f"  {'time left':>10} {'points':>8} {'Brier PM':>9} {'Brier FV':>9} {'PM - FV':>9} {'s.e.':>8}")
    for lo, hi in edges:
        pm_e, fv_e, grp = [], [], []
        for s in slots:
            rem = s["t"][0] + SLOT - s["t"]
            sel = (rem > lo) & (rem <= hi) & ~np.isnan(s["mid"])
            sel &= (np.arange(len(sel)) % int(1 / DT)) == 0      # 1 point per second
            pm_e.append((s["mid"][sel] - s["y"]) ** 2)
            fv_e.append((s["fv"][sel] - s["y"]) ** 2)
            grp.append(np.full(sel.sum(), s["slot"]))
        bp, bf, grp = np.concatenate(pm_e), np.concatenate(fv_e), np.concatenate(grp)
        if len(bp) > 1:
            d = bp - bf
            print(f"  {lo:>4}-{hi:<4}s {len(bp):>8} {bp.mean():>9.4f} {bf.mean():>9.4f} "
                  f"{d.mean():>+9.4f} {cluster_se(d, grp):>8.4f}")

    print("\n  Calibration (deciles of fair value): does the model match reality?")
    print(f"  {'FV range':>12} {'mean FV':>8} {'PM mid':>8} {'outcome':>8}")
    fv = np.concatenate([s["fv"][::10] for s in slots])
    mid = np.concatenate([s["mid"][::10] for s in slots])
    y = np.concatenate([np.full(len(s["fv"][::10]), s["y"]) for s in slots])
    ok = ~np.isnan(mid)
    fv, mid, y = fv[ok], mid[ok], y[ok]
    bins = np.linspace(0, 1, 11)
    for lo, hi in zip(bins[:-1], bins[1:]):
        sel = (fv >= lo) & (fv < hi if hi < 1 else fv <= hi)
        if sel.sum():
            print(f"  {lo:>5.1f}-{hi:<5.1f} {fv[sel].mean():>8.3f} {mid[sel].mean():>8.3f} {y[sel].mean():>8.3f}")


def part_c(slots, twap):
    print("\n" + "=" * 72)
    print(f" C. HOW FAST DOES POLYMARKET FOLLOW BTC? Share of a BTC-driven fair value move")
    print(f"    (over {SHOCK_WINDOW}s) that shows up in the Polymarket mid after h seconds")
    print("=" * 72)
    w = int(round(SHOCK_WINDOW / DT))
    rows = []
    for h in HORIZONS_LAG:
        k = int(round(h / DT))
        xs, ys, gs = [], [], []
        for s in slots:
            n = len(s["t"])
            rem = s["t"][0] + SLOT - s["t"]
            idx = np.arange(w, n - k)
            sel = (rem[idx] > twap + 5) & (s["fv"][idx] > 0.05) & (s["fv"][idx] < 0.95)
            idx = idx[sel]
            dx = s["fv"][idx] - s["fv"][idx - w]
            dy = s["mid"][idx + k] - s["mid"][idx - w]
            ok = ~np.isnan(dy)
            xs.append(dx[ok]); ys.append(dy[ok]); gs.append(np.full(ok.sum(), s["slot"]))
        x, y, g = np.concatenate(xs), np.concatenate(ys), np.concatenate(gs)
        if len(x) < 10 or (x ** 2).sum() == 0:
            continue
        beta = (x * y).sum() / (x ** 2).sum()
        sums = pd.Series(x * (y - beta * x)).groupby(g).sum().to_numpy()
        se = math.sqrt((sums ** 2).sum()) / (x ** 2).sum()
        rows.append((h, beta, se, len(x)))
    print(f"  {'h':>6} {'absorbed':>9} {'s.e.':>7} {'points':>9}")
    for h, beta, se, n in rows:
        print(f"  {h:>5}s {100 * beta:>8.1f}% {100 * se:>6.1f}% {n:>9}")
    print("  ~100% already at h=0 means repricing faster than this data can see (<~0.2 s).")


def part_d(slots, margin):
    print("\n" + "=" * 72)
    print(f" D. TRADABLE GAPS: buy as taker when fair value - ask - fee > {margin:.2f}")
    print("=" * 72)
    eps = []
    for s in slots:
        rem = s["t"][0] + SLOT - s["t"]
        for side in ("YES", "NO"):
            ask = s["pm_best_ask"] if side == "YES" else s["pm_ask_no"]
            bid = s["pm_best_bid"] if side == "YES" else s["pm_bid_no"]
            p = s["fv"] if side == "YES" else 1 - s["fv"]
            payoff = s["y"] if side == "YES" else 1 - s["y"]
            edge = p - ask - fee(ask)
            on = np.nan_to_num(edge, nan=-1) > margin
            on &= rem >= 2
            starts = np.flatnonzero(on & ~np.r_[False, on[:-1]])
            for i in starts:
                j = i
                while j + 1 < len(on) and on[j + 1]:
                    j += 1
                k = i + int(QUICK_EXIT / DT)
                if k < len(bid) and not np.isnan(bid[k]):
                    quick = bid[k] - ask[i] - fee(ask[i]) - fee(bid[k])
                else:
                    quick = payoff - ask[i] - fee(ask[i])
                eps.append(dict(slot=s["slot"], time_left_s=round(rem[i], 1), side=side,
                                fair=p[i], ask=ask[i], edge=edge[i],
                                lasts_s=round((j - i + 1) * DT, 1),
                                pnl_settle=payoff - ask[i] - fee(ask[i]), pnl_quick=quick))
    if not eps:
        print("  No gaps above the threshold.")
        return
    df = pd.DataFrame(eps)
    hours = len(slots) * SLOT / 3600
    print(f"  Gaps found:          {len(df)} ({len(df) / hours:.1f} per hour of data)")
    print(f"  Mean edge at entry:  {100 * df.edge.mean():.2f} cents/share")
    print(f"  Gap duration:        median {df.lasts_s.median():.1f}s, "
          f"10th pct {df.lasts_s.quantile(0.1):.1f}s, 90th pct {df.lasts_s.quantile(0.9):.1f}s")
    for col, label in [("pnl_settle", "Hold to settlement"), ("pnl_quick", f"Sell at bid after {QUICK_EXIT}s")]:
        m, se = df[col].mean(), cluster_se(df[col], df.slot)
        print(f"  {label + ':':<26} {100 * m:+.2f} ± {100 * se:.2f} cents/share "
              f"(win {100 * (df[col] > 0).mean():.0f}%)")
    df.to_csv("latency_gaps.csv", index=False)
    print("  Every gap is saved in latency_gaps.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--z", type=float, default=2.0, help="Z threshold for part A")
    ap.add_argument("--twap", type=float, default=30, help="settlement TWAP length in s (0 = snapshot)")
    ap.add_argument("--spot", action="store_true", help="use Binance spot instead of USD-M futures")
    ap.add_argument("--margin", type=float, default=0.01, help="min edge after fee for part D")
    args = ap.parse_args()

    pm = load_pm(args.csv)
    print(f"[DATA] {len(pm)} rows, {pm.slot.nunique()} markets, "
          f"{pd.to_datetime(pm.ts_unix.iloc[0], unit='s')} -> {pd.to_datetime(pm.ts_unix.iloc[-1], unit='s')} UTC")
    part_a(pm, args.z)

    symbol = str(pm["ticker"].dropna().iat[0]) if "ticker" in pm and pm["ticker"].notna().any() else "BTCUSDT"
    first = pm.ts_unix.iloc[0] - SIGMA_LOOKBACK - args.twap
    days = pd.date_range(pd.to_datetime(first, unit="s").normalize(),
                         pd.to_datetime(pm.ts_unix.iloc[-1] + SLOT, unit="s").normalize(), freq="D")
    trades = load_binance(symbol, [d.strftime("%Y-%m-%d") for d in days], args.spot)
    if trades is None:
        print("\n[BINANCE] No trade data: parts B-D skipped.")
        return
    bt, bp = trades
    slots, agree = build_slots(pm, bt, np.log(bp), args.twap)
    print(f"\n[DATA] {len(slots)} markets with full Binance coverage "
          f"({'spot' if args.spot else 'USD-M futures'}, TWAP {args.twap:g}s)")
    if not slots:
        return
    part_b(slots, agree)
    part_c(slots, args.twap)
    part_d(slots, args.margin)


if __name__ == "__main__":
    main()
