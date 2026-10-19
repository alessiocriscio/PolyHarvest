# Server deployment

The bot runs on the VM as the systemd service `polyharvest` (user `azureuser`, repo in `~/bot_pm`).
Every 2 minutes `polyharvest-update.timer` runs `update.sh`: if `main` on GitHub has changed,
it pulls the new code and restarts the bot. If the code does not compile or the bot does not
stay up, the previous commit is restored and the broken one is never retried.

- To change asset or disable logging: edit `polyharvest.env`, commit and push to `main`.
- `market_data.db` and `pm_env/` live only on the server (they are in `.gitignore`).
- After changing the `.service` / `.timer` files, copy them by hand to `/etc/systemd/system/`
  and run `sudo systemctl daemon-reload`.

Order books are streamed live over WebSocket: Polymarket from the CLOB (`bots/pm_ws.py`), Binance
from the futures `depth5@100ms` stream (`bots/binance_ws.py`). While a socket is down or has not
sent data yet, the bot reads that book over REST. The journal records every switch with a line
`[BOOK FEED] <source>: WebSocket live` / `[BOOK FEED] <source>: REST fallback (...)`.

The database is `bots/market_data.db`. To reset it: stop the bot, delete the file, restart.
It has two tables:

- `spread_log`: one row per tick (~10/s). Besides books and signals it stores the BTC top of book
  with Binance's event time (`btc_bid`, `btc_ask`, `btc_event_ms`), the exchange time of each
  Polymarket book (`pm_yes_ts_ms`, `pm_no_ts_ms`), top-of-book sizes, and the live estimate of the
  price to beat (`ptb_est`) with the Chainlink - Binance basis it used (`ptb_basis`).
- `markets`: one row per market with Polymarket's official `price_to_beat` and `final_price`
  (Chainlink BTC/USD 60 s TWAP, from the Gamma event's `eventMetadata`), when each was first seen,
  and the Binance 60 s TWAP at the market's start (`binance_twap_start`).

Polymarket publishes the price to beat only after a market closes and the final price ~6-7 min
later; the price to beat equals the previous market's final price. Live, the bot therefore
estimates it as `binance_twap_start` + the median basis (official price to beat -
`binance_twap_start`) of the last 12 markets.

Useful commands on the server:

```bash
systemctl status polyharvest                      # bot status
journalctl -u polyharvest -n 30 -o cat            # latest bot output
journalctl -u polyharvest-update -n 20 -o cat     # update history
sudo systemctl restart polyharvest                # manual restart
```
