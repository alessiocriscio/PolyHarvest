"""
Live Polymarket order books from the CLOB WebSocket market channel.

The feed keeps a local copy of each subscribed token's book: a `book` event
replaces it with a full snapshot, a `price_change` event updates single levels
(size 0 removes the level). A book is only served while the connection that
built it is alive, so a dropped socket is never mistaken for a quiet market.
"""
import asyncio
import json
import time

import aiohttp

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_SECONDS = 10      # the server drops connections that stay silent ~10s
STALE_SECONDS = 30     # no message at all (not even PONG) for this long -> reconnect
MAX_BACKOFF_SECONDS = 30


class PMBookFeed:
    def __init__(self, session, headers=None):
        self.session = session
        self.headers = headers or {}
        self.wanted = set()     # token ids we want books for
        self.books = {}         # token id -> {"bids": {price: size}, "asks": {price: size}}
        self.ws = None
        self.connected = False
        self.last_msg_ts = 0.0
        self.last_error = None
        self._task = None

    def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def track(self, token_ids):
        """Make the feed follow exactly these tokens (subscribe new, drop the rest)."""
        token_ids = {str(t) for t in token_ids}
        added, removed = token_ids - self.wanted, self.wanted - token_ids
        self.wanted = token_ids
        for t in removed:
            self.books.pop(t, None)
        if self.ws is None or self.ws.closed or not self.connected:
            return  # the next (re)connect subscribes to self.wanted
        try:
            if removed:
                await self.ws.send_str(json.dumps(
                    {"assets_ids": sorted(removed), "operation": "unsubscribe"}))
            if added:
                await self.ws.send_str(json.dumps(
                    {"assets_ids": sorted(added), "operation": "subscribe"}))
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            await self.ws.close()

    def get_book(self, token_id):
        """
        Book in the same shape as the REST /book response, or None when the
        feed has no live snapshot for this token.
        """
        if not self.connected or time.time() - self.last_msg_ts > STALE_SECONDS:
            return None
        book = self.books.get(str(token_id))
        if book is None:
            return None
        return {
            "bids": [{"price": p, "size": s} for p, s in book["bids"].items()],
            "asks": [{"price": p, "size": s} for p, s in book["asks"].items()],
        }

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with self.session.ws_connect(
                        WS_URL, headers=self.headers, heartbeat=None,
                        receive_timeout=STALE_SECONDS) as ws:
                    self.ws = ws
                    self.books = {}
                    self.last_msg_ts = time.time()
                    self.last_error = None
                    # mark connected first, so a track() racing this send
                    # subscribes its new tokens itself instead of losing them
                    self.connected = True
                    if self.wanted:
                        await ws.send_str(json.dumps(
                            {"assets_ids": sorted(self.wanted), "type": "market"}))
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type == aiohttp.WSMsgType.ERROR:
                                    self.last_error = f"ws error: {ws.exception()}"
                                break
                            self.last_msg_ts = time.time()
                            backoff = 1
                            self._handle(msg.data)
                    finally:
                        pinger.cancel()
                    self.last_error = self.last_error or f"closed (code {ws.close_code})"
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
            self.connected = False
            self.ws = None
            self.books = {}
            await asyncio.sleep(backoff)
            backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)

    async def _ping(self, ws):
        while not ws.closed:
            await asyncio.sleep(PING_SECONDS)
            await ws.send_str("PING")

    def _handle(self, data):
        if data == "PONG":
            return
        try:
            payload = json.loads(data)
        except ValueError:
            return
        # the first snapshot after subscribing arrives as a list of events
        for ev in payload if isinstance(payload, list) else [payload]:
            if not isinstance(ev, dict):
                continue
            kind = ev.get("event_type")
            if kind == "book":
                self._on_book(ev)
            elif kind == "price_change":
                self._on_price_change(ev)

    def _on_book(self, ev):
        token = str(ev.get("asset_id"))
        if token not in self.wanted:
            return
        self.books[token] = {"bids": _levels(ev.get("bids")), "asks": _levels(ev.get("asks"))}

    def _on_price_change(self, ev):
        for ch in ev.get("price_changes") or []:
            book = self.books.get(str(ch.get("asset_id")))
            if book is None:
                continue  # no snapshot yet: a partial update alone is not a book
            side = {"BUY": "bids", "SELL": "asks"}.get(ch.get("side"))
            try:
                price, size = float(ch["price"]), float(ch["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if side is None:
                continue
            if size > 0:
                book[side][price] = size
            else:
                book[side].pop(price, None)


def _levels(raw):
    out = {}
    for lvl in raw or []:
        try:
            price, size = float(lvl["price"]), float(lvl["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if size > 0:
            out[price] = size
    return out
