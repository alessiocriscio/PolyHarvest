# Deploy sul server

Il bot gira sulla VM come servizio systemd `polyharvest` (utente `azureuser`, repo in `~/bot_pm`).
Ogni 2 minuti `polyharvest-update.timer` esegue `update.sh`: se `main` su GitHub è cambiato,
scarica il nuovo codice e riavvia il bot. Se il codice non compila o il bot non resta attivo,
viene ripristinato il commit precedente e quello difettoso non viene più ritentato.

- Cambiare asset o disattivare il logging: modificare `polyharvest.env`, commit e push su `main`.
- `market_data.db` e `pm_env/` restano solo sul server (sono in `.gitignore`).
- Se si modificano i file `.service` / `.timer`, vanno ricopiati a mano in `/etc/systemd/system/`
  e va eseguito `sudo systemctl daemon-reload`.

I book di Polymarket arrivano dal WebSocket del CLOB (`bots/pm_ws.py`); finché il socket è giù
o non ha ancora mandato lo snapshot, il bot legge `/book` via REST. Il journal registra ogni
passaggio con una riga `[BOOK FEED] WebSocket live` / `[BOOK FEED] REST fallback (...)`.

Comandi utili sul server:

```bash
systemctl status polyharvest                      # stato del bot
journalctl -u polyharvest -n 30 -o cat            # ultime righe del bot
journalctl -u polyharvest-update -n 20 -o cat     # storico degli aggiornamenti
sudo systemctl restart polyharvest                # riavvio manuale
```
