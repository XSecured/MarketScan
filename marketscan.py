import asyncio
import aiohttp
import logging
import os
import json
import random
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, NamedTuple, Any
from dataclasses import dataclass
from enum import Enum

# Try to import telegram, gracefully fail if not installed (though context implies it is)
try:
    from telegram import Bot
    from telegram.error import BadRequest
except ImportError:
    logging.error("python-telegram-bot is missing. Please install it.")
    Bot = None
    BadRequest = Exception

# -----------------------------------------------------------------------------
# Configuration & Constants
# -----------------------------------------------------------------------------

LOG_FORMAT = '%(asctime)s | %(levelname)s | %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# Tuning Parameters for 2025 Performance
CONCURRENCY_LIMIT = 100      # Simultaneous connections (safe for async)
HTTP_TIMEOUT = 10            # Seconds
MAX_RETRIES = 3
RETRY_DELAY = 1.0
MAX_TG_CHARS = 4000

IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
    "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26",
    "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
}

LEVELS_FILE = "detected_levels.json"
MSG_ID_FILE = "level_alert_message.json"

# -----------------------------------------------------------------------------
# Data Structures (Lightweight Replacements for Pandas)
# -----------------------------------------------------------------------------

class Candle(NamedTuple):
    """Lightweight structure to replace heavy Pandas DataFrames"""
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class LevelHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    level_price: float
    current_price: float
    current_low: float
    current_high: float
    original_timestamp: str

@dataclass
class LowMovementResult:
    exchange: str
    symbol: str
    market: str
    interval: str
    movement_percent: float

# -----------------------------------------------------------------------------
# Core Logic Helpers
# -----------------------------------------------------------------------------

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    """Return True if a and b differ by less than 0.3%"""
    if a == 0 or b == 0: return False
    return abs(a - b) <= rel_tol * ((a + b) / 2.0)

def normalize_symbol(symbol: str) -> str:
    """Normalize symbol to BASE/QUOTE format."""
    s = symbol.upper()
    if ':' in s: s = s.split(':')[0]
    if '/' in s: return s
    # Basic heuristic for USDT pairs
    if s.endswith('USDT'): return f"{s[:-4]}/USDT"
    return s

# -----------------------------------------------------------------------------
# Async Proxy Manager
# -----------------------------------------------------------------------------

class AsyncProxyManager:
    def __init__(self, url: Optional[str]):
        self.url = url
        self.proxies: List[str] = []
        self.iterator = None
        self.lock = asyncio.Lock()
        self.bad_proxies: Set[str] = set()

    async def refresh_proxies(self):
        if not self.url:
            return
        
        logging.info("Fetching proxies...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, timeout=10) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        candidates = [
                            p.strip() if "://" in p.strip() else f"http://{p.strip()}"
                            for p in text.splitlines() if p.strip()
                        ]
                        # Quick self-test (optional, skipping for speed to rely on fail-fast)
                        self.proxies = candidates
                        self.iterator = itertools.cycle(self.proxies)
                        self.bad_proxies.clear()
                        logging.info(f"Loaded {len(self.proxies)} proxies.")
        except Exception as e:
            logging.error(f"Failed to fetch proxies: {e}")

    def get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        # Simple round-robin with skip logic
        for _ in range(len(self.proxies)):
            p = next(self.iterator)
            if p not in self.bad_proxies:
                return p
        return None

    def mark_bad(self, proxy: str):
        if proxy:
            self.bad_proxies.add(proxy)

# -----------------------------------------------------------------------------
# Abstracted Exchange Client (Async)
# -----------------------------------------------------------------------------

class ExchangeClient:
    def __init__(self, name: str, proxy_mgr: AsyncProxyManager):
        self.name = name
        self.proxy_mgr = proxy_mgr
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        # We do not set a base_url here because different endpoints use different domains
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            connector=aiohttp.TCPConnector(limit=0, ssl=False) # limit=0, we control via Semaphore
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def _get(self, url: str, params: dict) -> Any:
        attempt = 0
        while attempt < MAX_RETRIES:
            proxy = self.proxy_mgr.get_proxy()
            try:
                async with self.session.get(url, params=params, proxy=proxy) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status in [418, 429]: # Rate limited
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    else:
                        resp.raise_for_status()
            except Exception as e:
                # logging.debug(f"{self.name} req failed: {e}") # Verbose
                if proxy:
                    self.proxy_mgr.mark_bad(proxy)
            
            attempt += 1
            await asyncio.sleep(0.2) # Backoff
        raise Exception(f"Max retries reached for {url}")

    async def fetch_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        raise NotImplementedError

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        """Returns (perp_symbols, spot_symbols)"""
        raise NotImplementedError

class BinanceAsync(ExchangeClient):
    def __init__(self, proxy_mgr):
        super().__init__("Binance", proxy_mgr)

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        perps, spots = set(), set()
        
        # Fetch Perps
        try:
            data = await self._get('https://fapi.binance.com/fapi/v1/exchangeInfo', {})
            perps = {s['symbol'] for s in data['symbols'] 
                     if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'}
        except Exception as e:
            logging.error(f"Binance Perp Info failed: {e}")

        # Fetch Spots
        try:
            data = await self._get('https://api.binance.com/api/v3/exchangeInfo', {})
            spots = {s['symbol'] for s in data['symbols'] 
                     if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'}
        except Exception as e:
            logging.error(f"Binance Spot Info failed: {e}")
            
        return perps, spots

    async def fetch_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        # Limit 3 is enough for our logic (current, last closed, 2nd last closed)
        data = await self._get(base, {'symbol': symbol, 'interval': interval, 'limit': 4})
        
        # Parse into Candle objects (newest last in raw response usually, but we standardize)
        # Binance returns [open_time, open, high, low, close, vol, ...]
        # We want newest at index 0 for consistency with legacy logic logic
        candles = []
        for k in data:
            candles.append(Candle(
                open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]), volume=float(k[5])
            ))
        return list(reversed(candles)) # Return [Newest, LastClosed, 2ndLastClosed, ...]

class BybitAsync(ExchangeClient):
    def __init__(self, proxy_mgr):
        super().__init__("Bybit", proxy_mgr)

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        perps, spots = set(), set()
        try:
            data = await self._get('https://api.bybit.com/v5/market/instruments-info', {'category': 'linear'})
            perps = {s['symbol'] for s in data['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'}
        except Exception as e:
            logging.error(f"Bybit Perp Info failed: {e}")

        try:
            data = await self._get('https://api.bybit.com/v5/market/instruments-info', {'category': 'spot'})
            spots = {s['symbol'] for s in data['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'}
        except Exception as e:
            logging.error(f"Bybit Spot Info failed: {e}")
            
        return perps, spots

    async def fetch_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        category = 'linear' if market == 'perp' else 'spot'
        bybit_int = {"1M": "M", "1w": "W", "1d": "D"}[interval]
        
        data = await self._get('https://api.bybit.com/v5/market/kline', 
                               {'category': category, 'symbol': symbol, 'interval': bybit_int, 'limit': 4})
        
        # Bybit V5 returns [startTime, open, high, low, close, volume, turnover]
        # Bybit returns Newest First natively.
        candles = []
        for k in data['result']['list']:
            candles.append(Candle(
                open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]), volume=float(k[5])
            ))
        return candles # Already [Newest, LastClosed, ...]

# -----------------------------------------------------------------------------
# Business Logic (Pure Functions)
# -----------------------------------------------------------------------------

def analyze_candles(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    """
    Logic:
    idx 0: Current (Live)
    idx 1: Last Closed
    idx 2: 2nd Last Closed
    
    Check if idx 2 Close == idx 1 Open
    """
    if len(candles) < 3:
        return None, None
    
    second_last = candles[2]
    last_closed = candles[1]
    
    # Determine colors
    second_is_red = second_last.close < second_last.open
    second_is_green = second_last.close > second_last.open
    last_is_green = last_closed.close > last_closed.open
    last_is_red = last_closed.close < last_closed.open
    
    # Check equality: 2nd Close vs 1st Open
    if floats_are_equal(second_last.close, last_closed.open):
        price = second_last.close
        # Bullish Reversal: Red -> Green
        if second_is_red and last_is_green:
            return price, "bullish"
        # Bearish Reversal: Green -> Red
        elif second_is_green and last_is_red:
            return price, "bearish"
            
    return None, None

def analyze_low_movement(candles: List[Candle]) -> Optional[float]:
    """Check daily candle movement < 1.0% (or 2.5% in main)"""
    if len(candles) < 2:
        return None
    last_closed = candles[1]
    if last_closed.open == 0: return None
    
    movement = abs((last_closed.close - last_closed.open) / last_closed.open) * 100
    return movement

def check_current_touch(candles: List[Candle], price: float) -> bool:
    if not candles: return False
    curr = candles[0]
    return curr.low <= price <= curr.high

# -----------------------------------------------------------------------------
# Telegram Logic
# -----------------------------------------------------------------------------

class TelegramManager:
    def __init__(self, token: str, chat_id: str):
        self.bot = Bot(token=token)
        self.chat_id = chat_id

    async def send_chunks(self, text: str) -> List[int]:
        """Sends text, splitting it if necessary. Returns list of Msg IDs."""
        ids = []
        if not text.strip(): return ids

        # Split text respecting Markdown safety
        chunks = []
        current_chunk = ""
        for line in text.splitlines(keepends=True):
            if len(current_chunk) + len(line) > MAX_TG_CHARS:
                chunks.append(current_chunk)
                current_chunk = ""
            current_chunk += line
        if current_chunk: chunks.append(current_chunk)

        for chunk in chunks:
            try:
                msg = await self.bot.send_message(chat_id=self.chat_id, text=chunk, parse_mode='Markdown')
                ids.append(msg.message_id)
                await asyncio.sleep(0.3) # Avoid TG rate limits
            except Exception as e:
                logging.error(f"TG Send Error: {e}")
        return ids

    async def edit_or_send(self, ids: List[int], text: str) -> List[int]:
        """Edits existing messages or sends new ones if text grew."""
        new_ids = []
        chunks = []
        
        # Split text logic (duplicate of above, but necessary for mapping)
        current_chunk = ""
        for line in text.splitlines(keepends=True):
            if len(current_chunk) + len(line) > MAX_TG_CHARS:
                chunks.append(current_chunk)
                current_chunk = ""
            current_chunk += line
        if current_chunk: chunks.append(current_chunk)

        for i, chunk in enumerate(chunks):
            if i < len(ids):
                # Try Edit
                try:
                    await self.bot.edit_message_text(
                        chat_id=self.chat_id, message_id=ids[i], text=chunk, parse_mode='Markdown'
                    )
                    new_ids.append(ids[i])
                except BadRequest as e:
                    # Message deleted or can't be modified -> Send new
                    logging.warning(f"Could not edit msg {ids[i]}, sending new. {e}")
                    m = await self.bot.send_message(chat_id=self.chat_id, text=chunk, parse_mode='Markdown')
                    new_ids.append(m.message_id)
                except Exception as e:
                    logging.error(f"Edit failed generic: {e}")
            else:
                # Send New
                try:
                    m = await self.bot.send_message(chat_id=self.chat_id, text=chunk, parse_mode='Markdown')
                    new_ids.append(m.message_id)
                except Exception as e:
                    logging.error(f"Send failed: {e}")
            await asyncio.sleep(0.5)
            
        return new_ids

# -----------------------------------------------------------------------------
# Reporting Generators
# -----------------------------------------------------------------------------

def generate_alert_report(hits: List[LevelHit], timestamp: datetime) -> str:
    if not hits:
        return "💥 *LEVEL ALERTS* 💥\n\n❌ No levels got hit at this time."
    
    # Grouping
    data = {} # Interval -> Exchange -> Type -> List
    for h in hits:
        if h.interval not in data: data[h.interval] = {"Binance": {}, "Bybit": {}}
        if h.signal_type not in data[h.interval][h.exchange]: data[h.interval][h.exchange][h.signal_type] = []
        data[h.interval][h.exchange][h.signal_type].append(h)
        
    ts_str = timestamp.astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S UTC+3")
    msg = f"🚨 *LEVEL ALERTS* 🚨\n\n⚡ {len(hits)} levels got hit!\n\n🕒 {ts_str}\n\n"
    
    for interval in ["1M", "1w", "1d"]:
        if interval not in data: continue
        msg += f"📅 *{interval} Alerts*\n\n"
        for exc in ["Binance", "Bybit"]:
            bull = data[interval][exc].get("bullish", [])
            bear = data[interval][exc].get("bearish", [])
            if not bull and not bear: continue
            
            msg += f"*{exc}*:\n"
            if bull:
                msg += f"🍏 *Bullish ({len(bull)})*\n" + "".join([f"• {h.symbol} @ ${h.level_price:.6f}\n" for h in bull]) + "\n"
            if bear:
                msg += f"🔻 *Bearish ({len(bear)})*\n" + "".join([f"• {h.symbol} @ ${h.level_price:.6f}\n" for h in bear]) + "\n"
        msg += "——————————————————\n\n"
    return msg.strip()

def generate_full_report(results: List[tuple], low_mv: List[LowMovementResult], timestamp: datetime) -> List[str]:
    sections = []
    
    # 1. Reversal Summary
    ts_str = timestamp.astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S UTC+3")
    bin_cnt = len([r for r in results if r[0] == "Binance"])
    byb_cnt = len([r for r in results if r[0] == "Bybit"])
    
    header = (f"💥 *Reversal Level Scanner*\n\n✅ Total Signals: {len(results)}\n\n"
              f"*Binance*: {bin_cnt} | *Bybit*: {byb_cnt}\n\n🕒 {ts_str}")
    sections.append(header)
    
    # 2. Grouping
    # Structure: Interval -> Exchange -> Signal -> List of symbols
    tree = {}
    for r in results:
        # r = (exchange, symbol, market, interval, signal_type, price)
        ex, sym, _, iv, sig, _ = r
        if iv not in tree: tree[iv] = {"Binance": {"bullish": [], "bearish": []}, "Bybit": {"bullish": [], "bearish": []}}
        tree[iv][ex][sig].append(sym)
        
    for iv in ["1M", "1w", "1d"]:
        if iv not in tree: continue
        count = sum(len(tree[iv][ex][s]) for ex in tree[iv] for s in tree[iv][ex])
        if count == 0: continue
        
        sec = f"📅 *{iv} Timeframe* ({count} signals)\n\n"
        for ex in ["Binance", "Bybit"]:
            bull = sorted(tree[iv][ex]["bullish"])
            bear = sorted(tree[iv][ex]["bearish"])
            if not bull and not bear: continue
            
            sec += f"*{ex}*:\n"
            if bull: sec += f"🍏 *Bullish ({len(bull)})*\n" + "\n".join([f"• {s}" for s in bull]) + "\n\n"
            if bear: sec += f"🔻 *Bearish ({len(bear)})*\n" + "\n".join([f"• {s}" for s in bear]) + "\n\n"
        
        sec += "——————————————————\n\n"
        sections.append(sec.strip())
        
    # 3. Low Movement
    if low_mv:
        low_sec = "📉 *Low Movement Daily Candles (<1.0%)*\n\n"
        low_mv.sort(key=lambda x: x.movement_percent)
        
        bin_low = [x for x in low_mv if x.exchange == "Binance"]
        byb_low = [x for x in low_mv if x.exchange == "Bybit"]
        
        if bin_low:
            low_sec += "*Binance*:\n" + "".join([f"• {x.symbol} ({x.movement_percent:.2f}%)\n" for x in bin_low]) + "\n"
        if byb_low:
            low_sec += "*Bybit*:\n" + "".join([f"• {x.symbol} ({x.movement_percent:.2f}%)\n" for x in byb_low]) + "\n"
        
        low_sec += "——————————————————\n\n"
        sections.append(low_sec.strip())

    return sections

# -----------------------------------------------------------------------------
# Main Controllers
# -----------------------------------------------------------------------------

import itertools

async def run_full_scan(clients: List[ExchangeClient], tg: TelegramManager):
    logging.info("🚀 Starting FULL SCAN...")
    
    # 1. Discovery Phase
    all_tasks = []
    # Semaphores for discovery are not strictly needed if clients use single request, 
    # but good practice if get_symbols becomes complex.
    
    # Maps to hold prioritized symbols
    # structure: raw_data[exchange_name] = (perp_set, spot_set)
    raw_data = {} 

    async def fetch_symbols_wrapper(client):
        p, s = await client.get_symbols()
        return client.name, p, s

    results = await asyncio.gather(*(fetch_symbols_wrapper(c) for c in clients))
    for r in results:
        raw_data[r[0]] = (r[1], r[2])

    # 2. Priority Logic (Set Arithmetic)
    # Binance Perps > Binance Spot > Bybit Perps > Bybit Spot
    
    def norm_set(s: Set[str]) -> Dict[str, str]:
        # Returns {normalized: original}
        return {normalize_symbol(x): x for x in s if x not in IGNORED_SYMBOLS}

    b_perp = norm_set(raw_data["Binance"][0])
    b_spot = norm_set(raw_data["Binance"][1])
    by_perp = norm_set(raw_data["Bybit"][0])
    by_spot = norm_set(raw_data["Bybit"][1])

    final_targets = [] # List of (Client, Symbol, Market)

    # Step 1: Binance Perps (Take all)
    seen = set(b_perp.keys())
    final_targets.extend([(clients[0], sym, "perp") for sym in b_perp.values()]) # Assume clients[0] is Binance

    # Step 2: Binance Spots (Take if not in seen)
    unique_b_spot = {k: v for k, v in b_spot.items() if k not in seen}
    seen.update(unique_b_spot.keys())
    final_targets.extend([(clients[0], sym, "spot") for sym in unique_b_spot.values()])

    # Step 3: Bybit Perps
    unique_by_perp = {k: v for k, v in by_perp.items() if k not in seen}
    seen.update(unique_by_perp.keys())
    final_targets.extend([(clients[1], sym, "perp") for sym in unique_by_perp.values()]) # Assume clients[1] is Bybit

    # Step 4: Bybit Spots
    unique_by_spot = {k: v for k, v in by_spot.items() if k not in seen}
    final_targets.extend([(clients[1], sym, "spot") for sym in unique_by_spot.values()])

    logging.info(f"Total symbols to scan: {len(final_targets)}")

    # 3. Scanning Phase
    scan_results = []
    low_mv_results = []
    
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def worker(client, symbol, market):
        async with sem:
            # Check all 3 intervals
            for interval in ["1M", "1w", "1d"]:
                try:
                    candles = await client.fetch_candles(symbol, interval, market)
                    
                    # Reversal Check
                    price, sig = analyze_candles(candles)
                    if price and sig and not check_current_touch(candles, price):
                        scan_results.append((client.name, symbol, market, interval, sig, price))
                    
                    # Low Movement Check (only 1d)
                    if interval == "1d":
                        mv = analyze_low_movement(candles)
                        if mv is not None and mv < 1.0:
                            low_mv_results.append(LowMovementResult(client.name, symbol, market, interval, mv))
                            
                except Exception as e:
                    pass # logging.debug(f"Err {symbol}: {e}")

    # Run workers
    total_tasks = [worker(c, s, m) for c, s, m in final_targets]
    # Use tqdm? In async, straightforward iteration with tqdm is harder, 
    # but we can use a simple progress logger or just await gather.
    # For simplicity in single file, just await gather.
    
    start_time = time.time()
    chunk_size = 1000
    # Chunking to prevent memory explosion on gather for 5000+ tasks
    for i in range(0, len(total_tasks), chunk_size):
        chunk = total_tasks[i:i+chunk_size]
        await asyncio.gather(*chunk)
        logging.info(f"Processed {min(i+chunk_size, len(total_tasks))}/{len(total_tasks)} symbols...")
    
    logging.info(f"Scan finished in {time.time() - start_time:.2f}s")

    # 4. Reporting Phase
    timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)
    report_sections = generate_full_report(scan_results, low_mv_results, timestamp)
    
    for section in report_sections:
        await tg.send_chunks(section)
    
    # 5. Init Alerts Msg
    alert_ids = await tg.send_chunks("💥 *LEVEL ALERTS* 💥\n\n❌ No levels got hit at this time.")
    with open(MSG_ID_FILE, 'w') as f:
        json.dump({"message_ids": alert_ids}, f)
        
    # 6. Save Levels
    save_data = {"last_updated": timestamp.isoformat(), "levels": {}}
    for r in scan_results:
        # r = (exchange, symbol, market, interval, signal_type, price)
        key = f"{r[0]}_{r[1]}_{r[2]}_{r[3]}"
        save_data["levels"][key] = {
            "exchange": r[0], "symbol": r[1], "market": r[2], "interval": r[3],
            "signal_type": r[4], "price": r[5], "timestamp": timestamp.isoformat()
        }
    
    with open(LEVELS_FILE, 'w') as f:
        json.dump(save_data, f, indent=2)

async def run_price_check(clients: List[ExchangeClient], tg: TelegramManager):
    logging.info("🔍 Running Price Check...")
    
    if not os.path.exists(LEVELS_FILE):
        logging.info("No levels file found.")
        return

    with open(LEVELS_FILE, 'r') as f:
        data = json.load(f)
        levels = data.get("levels", {})

    if not levels:
        logging.info("No levels to check.")
        return

    # Group by Symbol/Exchange/Market to batch checks? 
    # Actually, fetch_candles is cheap per symbol.
    # But one symbol might have 1M, 1w, 1d levels. We should fetch candles once per symbol per interval.
    
    tasks = []
    hits = []
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    # Determine unique fetches needed: (Client, Symbol, Market, Interval)
    # But wait, checking a level requires the specific timeframe candle.
    
    async def check_level(lvl_key, lvl_data):
        async with sem:
            try:
                client = next((c for c in clients if c.name == lvl_data['exchange']), None)
                if not client: return
                
                candles = await client.fetch_candles(lvl_data['symbol'], lvl_data['interval'], lvl_data['market'])
                if not candles: return
                
                # Check hit logic: Current candle Low/High touches Level Price
                curr = candles[0]
                price = lvl_data['price']
                
                if curr.low <= price <= curr.high:
                    hits.append(LevelHit(
                        exchange=lvl_data['exchange'],
                        symbol=lvl_data['symbol'],
                        market=lvl_data['market'],
                        interval=lvl_data['interval'],
                        signal_type=lvl_data['signal_type'],
                        level_price=price,
                        current_price=curr.close,
                        current_low=curr.low,
                        current_high=curr.high,
                        original_timestamp=lvl_data['timestamp']
                    ))
            except Exception as e:
                pass

    await asyncio.gather(*(check_level(k, v) for k, v in levels.items()))
    
    logging.info(f"Hits detected: {len(hits)}")
    
    # Update Telegram
    timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)
    report = generate_alert_report(hits, timestamp)
    
    prev_ids = []
    if os.path.exists(MSG_ID_FILE):
        try:
            with open(MSG_ID_FILE, 'r') as f:
                prev_ids = json.load(f).get("message_ids", [])
        except: pass

    new_ids = await tg.edit_or_send(prev_ids, report)
    
    with open(MSG_ID_FILE, 'w') as f:
        json.dump({"message_ids": new_ids, "timestamp": timestamp.isoformat()}, f)


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------

async def main():
    # Env vars
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    proxy_url = os.getenv("PROXY_LIST_URL")
    mode = os.getenv("RUN_MODE", "full_scan")

    if not token or not chat_id:
        logging.error("Missing Telegram Config")
        return

    # Init Components
    tg = TelegramManager(token, chat_id)
    proxy_mgr = AsyncProxyManager(proxy_url)
    await proxy_mgr.refresh_proxies()

    # Start proxy background refresher
    async def proxy_refresher():
        while True:
            await asyncio.sleep(600)
            await proxy_mgr.refresh_proxies()
    
    refresher_task = asyncio.create_task(proxy_refresher())

    # Context Manage Clients
    binance = BinanceAsync(proxy_mgr)
    bybit = BybitAsync(proxy_mgr)

    async with binance, bybit:
        clients = [binance, bybit]
        
        if mode == "price_check":
            await run_price_check(clients, tg)
        else:
            await run_full_scan(clients, tg)

    refresher_task.cancel()

if __name__ == "__main__":
    try:
        # Use uvloop if available for extra speed (optional)
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
