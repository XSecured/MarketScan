import asyncio
import json
import logging
import os
import random
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, Any
from itertools import cycle

import aiohttp
from telegram import Bot
from telegram.error import BadRequest
from tqdm.asyncio import tqdm

# ==========================================
# CONFIGURATION
# ==========================================

@dataclass
class Config:
    FETCH_TIMEOUT_TOTAL: int = 15
    REQUEST_TIMEOUT: int = 5
    MAX_RETRIES: int = 3
    MAX_TG_CHARS: int = 4000
    MAX_CONCURRENCY: int = 50
    
    # Files
    LEVELS_FILE: str = "detected_levels.json"
    MSG_FILE: str = "level_alert_message.json"
    
    # Environment
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    PROXY_URL: str = os.getenv("PROXY_LIST_URL", "")
    RUN_MODE: str = os.getenv("RUN_MODE", "full_scan")

    IGNORED_SYMBOLS: Set[str] = field(default_factory=lambda: {
        "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
        "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
        "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26",
        "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
    })

CONFIG = Config()

# ==========================================
# DATA MODELS
# ==========================================

@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float

@dataclass
class LevelHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    level_price: float
    current_price: float = 0.0
    timestamp: str = ""
    
    def to_dict(self):
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "market": self.market,
            "interval": self.interval,
            "price": self.level_price,
            "signal_type": self.signal_type,
            "timestamp": self.timestamp
        }

@dataclass
class LowMovementHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    movement_percent: float

# ==========================================
# ROBUST PROXY POOL (FIXED)
# ==========================================

class AsyncProxyPool:
    def __init__(self, max_pool_size=25):
        self.queue = asyncio.Queue()
        self.max_pool_size = max_pool_size

    async def populate(self, url: str, session: aiohttp.ClientSession):
        """Fetches AND validates proxies with high-speed early exit."""
        if not url:
            logging.warning("⚠️ No Proxy URL provided! Running without proxies.")
            return

        # 1. Fetch Proxy List
        raw_proxies = []
        try:
            logging.info(f"📥 Fetching proxies from {url}...")
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    for line in text.splitlines():
                        p = line.strip()
                        if p:
                            raw_proxies.append(p if "://" in p else f"http://{p}")
        except Exception as e:
            logging.error(f"❌ Failed to fetch proxy list: {e}")
            return

        logging.info(f"🔎 Validating {len(raw_proxies)} proxies (Target: {self.max_pool_size})...")
        
        # Temporary list to hold valid proxies before filling queue
        valid_proxies = []
        
        # Shuffle to avoid getting stuck in a block of bad IPs from the same subnet
        random.shuffle(raw_proxies)
        
        # 2. Create Tasks with Semaphore (Throttling)
        sem = asyncio.Semaphore(200)

        async def protected_test(p):
            async with sem:
                return await self._test_proxy(p, session)

        tasks = [asyncio.create_task(protected_test(p)) for p in raw_proxies]

        # 3. Process results AS SOON AS THEY FINISH
        for future in asyncio.as_completed(tasks):
            try:
                proxy, is_good = await future
                if is_good:
                    valid_proxies.append(proxy)
                    # EXIT IMMEDIATELY once we have enough
                    if len(valid_proxies) >= self.max_pool_size:
                        break
            except:
                pass
        
        # 4. Cleanup: Cancel remaining tasks
        cancelled_count = 0
        for t in tasks:
            if not t.done():
                t.cancel()
                cancelled_count += 1
        
        await asyncio.sleep(0.1)
        
        if valid_proxies:
            # Populate the Queue with our valid proxies
            for p in valid_proxies:
                self.queue.put_nowait(p)
                
            logging.info(f"✅ Proxy Pool Ready: {self.queue.qsize()} working proxies (Cancelled {cancelled_count} redundant checks).")
        else:
            logging.error("❌ NO WORKING PROXIES FOUND! Calls will likely fail.")

    async def _test_proxy(self, proxy: str, session: aiohttp.ClientSession) -> Tuple[str, bool]:
        try:
            # Simple connection test to Binance API
            test_url = "https://api.binance.com/api/v3/time"
            async with session.get(test_url, proxy=proxy, timeout=5) as resp:
                return proxy, resp.status == 200
        except:
            return proxy, False

    async def get_proxy(self) -> Optional[str]:
        """Returns a proxy from the queue and rotates it to the back."""
        if self.queue.empty():
            return None
        
        # Get a proxy (No waiting for lock)
        proxy = await self.queue.get()
        
        # Put it back immediately (Round Robin rotation)
        self.queue.put_nowait(proxy)
        
        return proxy
        
# ==========================================
# EXCHANGE CLIENTS
# ==========================================

class ExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        self.session = session
        self.proxies = proxy_pool
        # Higher concurrency only if we have proxies, otherwise limit strictly
        limit = CONFIG.MAX_CONCURRENCY if proxy_pool.proxies else 5
        self.sem = asyncio.Semaphore(limit)

    async def _request(self, url: str, params: dict = None) -> Any:
        """Robust async request with retries."""
        for attempt in range(CONFIG.MAX_RETRIES):
            proxy = await self.proxies.get_proxy()
            try:
                async with self.sem: 
                    async with self.session.get(
                        url, params=params, proxy=proxy, timeout=CONFIG.REQUEST_TIMEOUT
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            await asyncio.sleep(5) # Rate limit backoff
            except Exception:
                pass 
            
            await asyncio.sleep(0.5 * attempt)
        return None

class BinanceClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('https://fapi.binance.com/fapi/v1/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] 
                if s.get('contractType') == 'PERPETUAL' and s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']

    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('https://api.binance.com/api/v3/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] 
                if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int = 3) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        data = await self._request(base, {'symbol': symbol, 'interval': interval, 'limit': limit})
        if not data or not isinstance(data, list): return []
        
        candles = []
        for c in data:
            try:
                candles.append(Candle(open=float(c[1]), high=float(c[2]), low=float(c[3]), close=float(c[4])))
            except: continue
        # Reverse: Binance gives Oldest->Newest. We want Newest->Oldest (Index 0 = Newest)
        return candles[::-1]

class BybitClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('https://api.bybit.com/v5/market/instruments-info', {'category': 'linear'})
        if not data: return []
        return [s['symbol'] for s in data['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT']

    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('https://api.bybit.com/v5/market/instruments-info', {'category': 'spot'})
        if not data: return []
        return [s['symbol'] for s in data['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT']

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int = 3) -> List[Candle]:
        url = 'https://api.bybit.com/v5/market/kline'
        cat = 'linear' if market == 'perp' else 'spot'
        bybit_int = {"1M": "M", "1w": "W", "1d": "D"}.get(interval, "D")
        
        data = await self._request(url, {'category': cat, 'symbol': symbol, 'interval': bybit_int, 'limit': limit})
        if not data: return []
        
        raw_list = data.get('result', {}).get('list', [])
        if not raw_list: return []
        
        candles = []
        for c in raw_list:
             candles.append(Candle(open=float(c[1]), high=float(c[2]), low=float(c[3]), close=float(c[4])))
        
        # Bybit V5 gives Newest->Oldest. No reverse needed. Index 0 is Newest.
        return candles 

# ==========================================
# CORE LOGIC
# ==========================================

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    return abs(a - b) <= rel_tol * ((a + b) / 2.0)

def check_reversal(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    """Analyzes 3 candles. Expects Index 0 = Newest."""
    if len(candles) < 3: return None, None

    # 0=Live, 1=Last Closed, 2=Prev Closed
    last_closed = candles[1]
    second_last_closed = candles[2]

    # Logic: Prev Close vs Last Open
    if floats_are_equal(second_last_closed.close, last_closed.open):
        equal_price = second_last_closed.close
        
        # Colors
        second_last_is_red = second_last_closed.close < second_last_closed.open
        second_last_is_green = second_last_closed.close > second_last_closed.open
        last_is_green = last_closed.close > last_closed.open
        last_is_red = last_closed.close < last_closed.open

        # Bullish: Red then Green
        if second_last_is_red and last_is_green:
            return equal_price, "bullish"
        
        # Bearish: Green then Red
        elif second_last_is_green and last_is_red:
            return equal_price, "bearish"

    return None, None

def current_candle_touched_price(candles: List[Candle], price: float) -> bool:
    if not candles: return False
    curr = candles[0] # Newest
    return curr.low <= price <= curr.high

def check_low_movement(candles: List[Candle], threshold_percent: float = 1.0) -> Optional[float]:
    if len(candles) < 2: return None
    last = candles[1] # Last closed
    if last.open == 0: return None
    
    move_pct = abs((last.close - last.open) / last.open) * 100
    if move_pct < threshold_percent: 
        return move_pct
    return None

# ==========================================
# STATE & UTILS
# ==========================================

def load_levels() -> Dict[str, Any]:
    try:
        if os.path.exists(CONFIG.LEVELS_FILE):
            with open(CONFIG.LEVELS_FILE, "r") as f:
                return json.load(f).get("levels", {})
    except Exception: pass
    return {}

def save_levels(results: List[LevelHit], utc_now_str: str):
    data = {"last_updated": utc_now_str, "levels": {}}
    for r in results:
        key = f"{r.exchange}_{r.symbol}_{r.market}_{r.interval}"
        data["levels"][key] = r.to_dict()
    with open(CONFIG.LEVELS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def normalize_symbol(symbol: str) -> str:
    s = symbol.upper().split(':')[0]
    if '/' in s: return s
    return f"{s[:-4]}/{s[-4:]}" if s.endswith("USDT") else s

def save_message_ids(ids: List[int]):
    with open(CONFIG.MSG_FILE, "w") as f:
        json.dump({"message_ids": ids, "timestamp": datetime.now(timezone.utc).isoformat()}, f)

def load_message_ids() -> List[int]:
    if not os.path.exists(CONFIG.MSG_FILE): return []
    try:
        with open(CONFIG.MSG_FILE) as f: return json.load(f).get("message_ids", [])
    except: return []

def clear_message_ids():
    if os.path.exists(CONFIG.MSG_FILE): os.remove(CONFIG.MSG_FILE)

# ==========================================
# MAIN BOT CLASS
# ==========================================

class MarketScanBot:
    def __init__(self):
        self.utc_now = datetime.now(timezone.utc)
        self.tg_bot = Bot(token=CONFIG.TELEGRAM_TOKEN) if CONFIG.TELEGRAM_TOKEN else None

    async def run(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        
        if not CONFIG.TELEGRAM_TOKEN or not CONFIG.CHAT_ID:
            logging.error("❌ Missing Telegram Env Vars")
            return

        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            # 1. Setup Proxy (Critical Fix)
            proxies = AsyncProxyPool()
            if CONFIG.PROXY_URL:
                await proxies.populate(CONFIG.PROXY_URL, session)
            
            # 2. Clients
            binance = BinanceClient(session, proxies)
            bybit = BybitClient(session, proxies)

            if CONFIG.RUN_MODE == "price_check":
                await self.run_price_check(binance, bybit)
            else:
                await self.run_full_scan(binance, bybit)

    # ==========================================
    # PRICE CHECK MODE
    # ==========================================
    async def run_price_check(self, binance: BinanceClient, bybit: BybitClient):
        logging.info("🚀 Starting Price Check...")
        levels_data = load_levels()
        
        if not levels_data:
            logging.info("⚠️ No levels found in file.")
            await self.send_or_update_alert_report([])
            return

        tasks = []
        for key, data in levels_data.items():
            client = binance if data['exchange'] == 'Binance' else bybit
            tasks.append(self.check_single_level(client, data))

        hits = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking Levels"):
            res = await f
            if res: hits.append(res)

        await self.send_or_update_alert_report(hits)

    async def check_single_level(self, client: ExchangeClient, data: dict) -> Optional[LevelHit]:
        candles = await client.fetch_ohlcv(data['symbol'], data['interval'], data['market'], limit=2)
        if not candles: return None
        
        curr = candles[0]
        target = data['price']
        
        if curr.low <= target <= curr.high:
            return LevelHit(
                exchange=data['exchange'], symbol=data['symbol'], market=data['market'],
                interval=data['interval'], signal_type=data['signal_type'],
                level_price=target, current_price=curr.close, timestamp=data['timestamp']
            )
        return None

    # ==========================================
    # FULL SCAN MODE (Fixed Deduplication)
    # ==========================================
    async def run_full_scan(self, binance: BinanceClient, bybit: BybitClient):
        logging.info("🌍 Starting FULL Market Scan...")
        clear_message_ids()
        
        # 1. Fetch Symbols
        b_perp_t = asyncio.create_task(binance.get_perp_symbols())
        b_spot_t = asyncio.create_task(binance.get_spot_symbols())
        y_perp_t = asyncio.create_task(bybit.get_perp_symbols())
        y_spot_t = asyncio.create_task(bybit.get_spot_symbols())
        
        bp, bs, yp, ys = await asyncio.gather(b_perp_t, b_spot_t, y_perp_t, y_spot_t)
        
        if not (bp or bs or yp or ys):
            logging.error("❌ Failed to fetch symbols. Check proxies/connection!")
            return

        # 2. Deduplication & Filtering Logic
        # Priority: Binance Perp > Binance Spot > Bybit Perp > Bybit Spot
        
        seen_normalized = set()
        
        def filter_unique(symbols: List[str]) -> List[str]:
            unique_list = []
            for s in symbols:
                # A. Global Ignore List
                if s in CONFIG.IGNORED_SYMBOLS: 
                    continue
                
                # B. PATTERN FILTER: Strict "Must end with USDT" check.
                # This instantly kills symbols like "BTCUSDT-27MAR26" or "ETHUSDT-26DEC25"
                # because they end with numbers, not "USDT".
                if not s.endswith("USDT"):
                    continue

                # C. Normalize & Deduplicate
                norm = normalize_symbol(s)
                if norm not in seen_normalized:
                    unique_list.append(s)
                    seen_normalized.add(norm)
            return unique_list

        # Apply filter sequentially to respect priority
        final_bp = filter_unique(bp)
        final_bs = filter_unique(bs)
        final_yp = filter_unique(yp)
        final_ys = filter_unique(ys)

        logging.info(f"Scanning: B-Perp:{len(final_bp)} B-Spot:{len(final_bs)} Y-Perp:{len(final_yp)} Y-Spot:{len(final_ys)}")

        # 3. Build Scan Tasks
        scan_tasks = []
        def add_tasks(client, syms, mkt, ex_name):
            for s in syms:
                scan_tasks.append(self.scan_symbol_all_tfs(client, s, mkt, ex_name))

        add_tasks(binance, final_bp, 'perp', 'Binance')
        add_tasks(binance, final_bs, 'spot', 'Binance')
        add_tasks(bybit, final_yp, 'perp', 'Bybit')
        add_tasks(bybit, final_ys, 'spot', 'Bybit')

        # 4. Execute Scan
        results: List[LevelHit] = []
        low_movements: List[LowMovementHit] = []
        
        for f in tqdm(asyncio.as_completed(scan_tasks), total=len(scan_tasks), desc="Scanning"):
            revs, low = await f
            results.extend(revs)
            if low: low_movements.append(low)

        # 5. Report
        save_levels(results, self.utc_now.isoformat())
        await self.send_full_report(results, low_movements)
        await self.send_or_update_alert_report([]) 

    async def scan_symbol_all_tfs(self, client: ExchangeClient, symbol: str, market: str, exchange: str):
        reversals = []
        low_move = None
        
        # Define the intervals we need to fetch
        intervals = ["1M", "1w", "1d"]
        
        # Create a list of tasks to fetch all timeframes simultaneously
        tasks = [
            client.fetch_ohlcv(symbol, interval, market, limit=3) 
            for interval in intervals
        ]
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, candles in enumerate(results_list):
            interval = intervals[i]

            if isinstance(candles, Exception) or not candles:
                continue

            # Logic 1: Reversal
            price, sig = check_reversal(candles)
            if price and sig:
                # Only add if the current candle hasn't already invalidated/touched the price
                if not current_candle_touched_price(candles, price):
                    reversals.append(LevelHit(exchange, symbol, market, interval, sig, price))
            
            # Logic 2: Low Movement
            if interval == "1d":
                lm = check_low_movement(candles, threshold_percent=1.0)
                if lm is not None:
                    low_move = LowMovementHit(exchange, symbol, market, interval, lm)
                    
        return reversals, low_move

    # ==========================================
    # TELEGRAM LOGIC
    # ==========================================

    async def send_chunks(self, text: str) -> List[int]:
        """Splits text and sends messages, returning list of IDs."""
        ids = []
        chunks = []
        temp_chunk = ''
        for line in text.splitlines(keepends=True):
            if len(temp_chunk) + len(line) > CONFIG.MAX_TG_CHARS:
                chunks.append(temp_chunk)
                temp_chunk = ''
            temp_chunk += line
        if temp_chunk.strip():
            chunks.append(temp_chunk)
        
        for chunk in chunks:
            try:
                m = await self.tg_bot.send_message(
                    chat_id=CONFIG.CHAT_ID,
                    text=chunk,
                    parse_mode='Markdown'
                )
                ids.append(m.message_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"Failed to send chunk: {e}")
        return ids

    async def send_or_update_alert_report(self, hits: List[LevelHit]):
        """Updates the pinned alert message or sends new one with New Look."""
        timestamp = self.utc_now.astimezone(timezone(timedelta(hours=3))).strftime("%d %b %H:%M UTC+3")
        
        def _clean_sym(s: str) -> str:
            return s.replace("USDT", "")
        
        def _fmt_price(p: float) -> str:
            # Formats price cleanly: 0.0045 or 65000 (no trailing zeros)
            return f"${p:g}"

        if not hits:
            text = (
                "🚨 *LEVEL ALERT SYSTEM*\n"
                "═════════════════════\n"
                "❌ No active level hits right now.\n"
                "═════════════════════\n"
                f"🕒 {timestamp}"
            )
        else:
            lines = [
                "🚨 *LEVEL ALERT SYSTEM*",
                "═════════════════════\n",
                f"⚡ *{len(hits)} LEVELS HIT!*",
                ""
            ]
            
            grouped = {} 
            for h in hits:
                grouped.setdefault(h.interval, {}).setdefault(h.exchange, {}).setdefault(h.signal_type, []).append(h)
            
            # Map internal keys to nice display labels
            tf_labels = {"1M": "MONTHLY (1M)", "1w": "WEEKLY (1w)", "1d": "DAILY (1d)"}

            for interval in ["1M", "1w", "1d"]:
                if interval not in grouped: continue
                
                lines.append(f"📅 *{tf_labels.get(interval, interval)}*\n")
                
                exchanges = [("Binance", "🟡"), ("Bybit", "⚫")]
                
                for ex_name, ex_icon in exchanges:
                    ex_data = grouped[interval].get(ex_name, {})
                    bull = sorted(ex_data.get("bullish", []), key=lambda x: x.symbol)
                    bear = sorted(ex_data.get("bearish", []), key=lambda x: x.symbol)
                    
                    if not bull and not bear:
                        continue

                    lines.append(f"┌ {ex_icon} *{ex_name.upper()}*")
                    
                    # Helper to create the string list: "BTC ($95000), SOL ($140)"
                    def fmt_list(items):
                        return ", ".join([f"{_clean_sym(x.symbol)} ({_fmt_price(x.level_price)})" for x in items])

                    if bull and bear:
                        lines.append(f"│ 🍏 *Bull*: {fmt_list(bull)}")
                        lines.append(f"└ 🔻 *Bear*: {fmt_list(bear)}")
                    elif bull:
                        lines.append(f"└ 🍏 *Bull*: {fmt_list(bull)}")
                    elif bear:
                        lines.append(f"└ 🔻 *Bear*: {fmt_list(bear)}")
                    
                    lines.append("") # Spacer
                
            lines.append("═════════════════════")
            lines.append(f"🕒 {timestamp}")
            text = "\n".join(lines)

        # ---------------------------------------------------------
        # Message ID Management (Pinned/Updated Message Logic)
        # ---------------------------------------------------------
        prev_ids = load_message_ids()
        new_ids = []
        
        chunks = []
        temp_chunk = ''
        for line in text.splitlines(keepends=True):
            if len(temp_chunk) + len(line) > CONFIG.MAX_TG_CHARS:
                chunks.append(temp_chunk)
                temp_chunk = ''
            temp_chunk += line
        if temp_chunk.strip(): chunks.append(temp_chunk)
        
        for idx, chunk in enumerate(chunks):
            # Try to edit existing message to reduce spam
            if idx < len(prev_ids):
                try:
                    await self.tg_bot.edit_message_text(chat_id=CONFIG.CHAT_ID, message_id=prev_ids[idx], text=chunk, parse_mode='Markdown')
                    new_ids.append(prev_ids[idx])
                    continue
                except Exception:
                    pass # If edit fails (e.g. message deleted), fall through to send new
            
            # Send new message if needed
            try:
                m = await self.tg_bot.send_message(chat_id=CONFIG.CHAT_ID, text=chunk, parse_mode='Markdown')
                new_ids.append(m.message_id)
            except Exception as e:
                logging.error(f"TG Send Error: {e}")

        save_message_ids(new_ids)

    async def send_full_report(self, results: List[LevelHit], low_movements: List[LowMovementHit]):
        timestamp = self.utc_now.astimezone(timezone(timedelta(hours=3))).strftime("%d %b %H:%M UTC+3")
        
        # Helper to strip USDT for clean display
        def _clean_sym(s: str) -> str:
            return s.replace("USDT", "")

        # 1. Header
        lines = [
            "🚨 *REVERSAL ALERT SYSTEM*",
            "═════════════════════\n"
        ]
        
        # 2. Reversal Sections
        grouped = {}
        for r in results:
            grouped.setdefault(r.interval, {}).setdefault(r.exchange, {}).setdefault(r.signal_type, []).append(_clean_sym(r.symbol))
            
        has_any_data = False
        for tf_label, tf_key in [("MONTHLY (1M)", "1M"), ("WEEKLY (1w)", "1w"), ("DAILY (1d)", "1d")]:
            if tf_key not in grouped: continue
            
            tf_data = grouped[tf_key]
            # Check if this timeframe actually has content to print
            if not any(tf_data.values()): continue
            
            lines.append(f"📅 *{tf_label}*\n")
            has_any_data = True

            # Define specific order and styling
            exchanges = [("Binance", "🟡"), ("Bybit", "⚫")]
            
            for ex_name, ex_icon in exchanges:
                ex_data = tf_data.get(ex_name, {})
                bull = sorted(ex_data.get("bullish", []))
                bear = sorted(ex_data.get("bearish", []))
                
                if not bull and not bear:
                    continue

                lines.append(f"┌ {ex_icon} *{ex_name.upper()}*")
                
                # Logic to handle the tree lines (│ vs └)
                # If we have both bull and bear, first one gets │, last gets └
                # If we only have one, it gets └
                
                if bull and bear:
                    lines.append(f"│ 🍏 *Bull*: {', '.join(bull)}")
                    lines.append(f"└ 🔻 *Bear*: {', '.join(bear)}")
                elif bull:
                    lines.append(f"└ 🍏 *Bull*: {', '.join(bull)}")
                elif bear:
                    lines.append(f"└ 🔻 *Bear*: {', '.join(bear)}")
                
                lines.append("") # Spacer between exchanges

            lines.append("═════════════════════\n")

        if not has_any_data:
            lines.append("❌ No Reversal Signals Detected.\n")

        # 3. Low Movement Section
        if low_movements:
            low_movements.sort(key=lambda x: x.movement_percent)
            lines.append("📉 *SQUEEZE ALERT (<1%)*")
            
            # Group by exchange
            lm_grouped = {"Binance": [], "Bybit": []}
            for x in low_movements:
                if x.exchange in lm_grouped:
                    lm_grouped[x.exchange].append(x)
            
            for ex_name, ex_icon in [("Binance", "🟡"), ("Bybit", "⚫")]:
                items = lm_grouped.get(ex_name, [])
                if not items: continue
                
                lines.append(f"┌ {ex_icon} *{ex_name.upper()}*")
                # Limit to top 10 per exchange to save space
                top_items = items[:10]
                
                for i, item in enumerate(top_items):
                    is_last = (i == len(top_items) - 1)
                    prefix = "└" if is_last else "│"
                    clean_s = _clean_sym(item.symbol)
                    lines.append(f"{prefix} {clean_s} ({item.movement_percent:.2f}%)")
                
                if len(items) > 10:
                    lines.append(f"└ ...and {len(items)-10} more")
                lines.append("")

        # 4. Footer
        lines.append(f"🕒 {timestamp}")

        # Join and Send
        full_text = "\n".join(lines)
        await self.send_chunks(full_text)

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    bot = MarketScanBot()
    asyncio.run(bot.run())
