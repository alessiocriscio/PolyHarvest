"""
Live Binance USD-M futures top-of-book from the partial depth WebSocket stream.

`<symbol>@depth5@100ms` pushes the top 5 bid/ask levels every 100 ms, so the
latest message IS the book: no local reconstruction needed. A book is only
served while it is fresh, so a silent or dropped socket falls back to REST.
"""
import asyncio
import json
import time

import aiohttp

# Since March 2026 order book streams live under /public; the legacy
# un-prefixed URLs were decommissioned on 2026-04-23.
WS_BASE = "wss://fstream.binance.com/public/ws"
STALE_SECONDS = 2.0        # pushes arrive every 100 ms: 2 s of silence = feed is dead
RECEIVE_TIMEOUT = 10       # reconnect after this long without any frame
MAX_BACKOFF_SECONDS = 30


class BinanceDepthFeed:
    def __init__(self, session, symbol):
        self.session = session
        self.url = f"{WS_BASE}/{symbol.lower()}@depth5@100ms"
        self.bids = None        # [[price, qty], ...] as sent, best first
        self.asks = None
        self.last_msg_ts = 0.0
        self.connected = False
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

    def get_depth(self):
        """(bids, asks) of the latest push, or None when the feed is not live."""
        if not self.connected or self.bids is None:
            return None
        if time.time() - self.last_msg_ts > STALE_SECONDS:
            return None
        return self.bids, self.asks

    async def _run(self):
        backoff = 1
        while True:
            try:
                # Binance pings every few minutes; aiohttp answers with pong itself
                async with self.session.ws_connect(
                        self.url, receive_timeout=RECEIVE_TIMEOUT) as ws:
                    self.connected = True
                    self.last_error = None
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type == aiohttp.WSMsgType.ERROR:
                                self.last_error = f"ws error: {ws.exception()}"
                            break
                        if self._handle(msg.data):
                            backoff = 1
                    # Binance also drops every connection after 24 h: just reconnect
                    self.last_error = self.last_error or f"closed (code {ws.close_code})"
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
            self.connected = False
            self.bids = self.asks = None
            await asyncio.sleep(backoff)
            backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)

    def _handle(self, data):
        try:
            ev = json.loads(data)
        except ValueError:
            return False
        # futures payload uses "b"/"a"; accept the spot-style names too
        bids = ev.get("b", ev.get("bids"))
        asks = ev.get("a", ev.get("asks"))
        if not isinstance(bids, list) or not isinstance(asks, list):
            return False
        self.bids, self.asks = bids, asks
        self.last_msg_ts = time.time()
        return True
