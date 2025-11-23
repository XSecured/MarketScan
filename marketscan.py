import asyncio
import aiohttp
import logging
import os
import json
import random
import time
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, NamedTuple, Any, Union
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path

# Try to import telegram, gracefully fail if not installed
try:
    from telegram import Bot
    from telegram.error import BadRequest
except ImportError:
    # Define mocks for headless environment
    Bot = None
    BadRequest = Exception

# -----------------------------------------------------------------------------
# 1. System Configuration & Constants
# -----------------------------------------------------------------------------

# Configure high-performance logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MarketScan")

# Try to use uvloop for maximum performance on Linux (GitHub Actions)
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("🚀 High-Performance uvloop active")
    except ImportError:
        pass

class Config:
    # Infrastructure
    CONCURRENCY_LIMIT = 200      # Higher concurrency for async
    HTTP_TIMEOUT = 8             # Fast fail
    MAX_RETRIES = 2
    PROXY_CHECK_URL = "https://api.binance.com/api/v3/time" # Lightweight check

    # File Paths
    LEVELS_FILE = Path("detected_levels.json")
    HITS_FILE = Path("daily_hits.json")     # Persist hits throughout the day
    MSG_ID_FILE = Path("msg_ids.json")      # Stores Telegram Message IDs
    
    # Logic
    LOW_MOVEMENT_THRESHOLD = 1.0 # Exact requirement
    IGNORED_SYMBOLS = {
        "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
        "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
        "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26",
        "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
    }

# -----------------------------------------------------------------------------
# 2. Data Models (Lightweight & Typed)
# -----------------------------------------------------------------------------

class Candle(NamedTuple):
    """Memory-efficient candle structure"""
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class MarketLevel:
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    price: float
    timestamp: str

@dataclass
class LevelHit:
    """Represents a confirmed hit of a level"""
    id: str # Unique ID (exchange_symbol_interval_price) to prevent duplicates
    exchange: str
    symbol: str
    market: str
    interval: str
    signal_type: str
    level_price: float
    hit_price: float
    hit_time: str

@dataclass
class LowVolResult:
    exchange: str
    symbol: str
    market: str
    movement_percent: float

# -----------------------------------------------------------------------------
# 3. High-Performance Networking & Proxy System
# -----------------------------------------------------------------------------

class SmartProxyManager:
    """
    Manages proxies with a 'Warmup' phase.
    Only allows 'Verified' proxies to be used for scanning.
    """
    def __init__(self, proxy_url: str):
        self.proxy_url = proxy_url
        self.proxies: List[str] = []
        self.verified_proxies: List[str] = []
        self._bad_proxies: Set[str] = set()
        self.lock = asyncio.Lock()

    async def load_and_verify(self):
        """Fetches proxies and tests them concurrently before the main scan."""
        logger.info("🌐 Fetching proxy list...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.proxy_url, timeout=10) as resp:
                    if resp.status != 200:
                        raise Exception(f"Proxy list returned {resp.status}")
                    text = await resp.text()
                    raw_proxies = [
                        p.strip() if "://" in p.strip() else f"http://{p.strip()}"
                        for p in text.splitlines() if p.strip()
                    ]
        except Exception as e:
            logger.error(f"Failed to load proxies: {e}")
            return

        logger.info(f"🧪 Verifying {len(raw_proxies)} proxies (Warmup Phase)...")
        
        sem = asyncio.Semaphore(100) # Fast verification
        valid = []

        async def verify(p):
            async with sem:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(Config.PROXY_CHECK_URL, proxy=p, timeout=4) as r:
                            if r.status == 200:
                                valid.append(p)
                except:
                    pass

        await asyncio.gather(*(verify(p) for p in raw_proxies))
        
        self.verified_proxies = valid
        if not self.verified_proxies:
            logger.warning("⚠️ NO VALID PROXIES FOUND! Falling back to raw list (High Risk)")
            self.verified_proxies = raw_proxies
        else:
            logger.info(f"✅ Active Proxy Pool: {len(self.verified_proxies)} high-speed proxies")

    def get_proxy(self) -> str:
        """Returns a random verified proxy to spread load."""
        if not self.verified_proxies:
            return None
        return random.choice(self.verified_proxies)

    def mark_bad(self, proxy: str):
        pass

# -----------------------------------------------------------------------------
# 4. Exchange Clients (Async & Persistent Session)
# -----------------------------------------------------------------------------

class ExchangeClient:
    def __init__(self, name: str, proxy_mgr: SmartProxyManager):
        self.name = name
        self.proxy_mgr = proxy_mgr
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)

    async def __aenter__(self):
        conn = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, ssl=False)
        self.session = aiohttp.ClientSession(connector=conn)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def _request(self, url: str, params: dict) -> Any:
        """Reliable request wrapper with retries and proxy rotation."""
        for attempt in range(Config.MAX_RETRIES):
            proxy = self.proxy_mgr.get_proxy()
            try:
                async with self.session.get(
                    url, params=params, proxy=proxy, timeout=Config.HTTP_TIMEOUT
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        await asyncio.sleep(1 + attempt)
                    elif resp.status >= 500:
                        await asyncio.sleep(0.5)
            except Exception:
                pass
            
            await asyncio.sleep(0.2)
        
        return None

class BinanceClient(ExchangeClient):
    def __init__(self, proxy_mgr): super().__init__("Binance", proxy_mgr)

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        perps, spots = set(), set()
        t1 = self._request('https://fapi.binance.com/fapi/v1/exchangeInfo', {})
        t2 = self._request('https://api.binance.com/api/v3/exchangeInfo', {})
        r1, r2 = await asyncio.gather(t1, t2)

        if r1:
            perps = {s['symbol'] for s in r1['symbols'] 
                     if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'}
        if r2:
            spots = {s['symbol'] for s in r2['symbols'] 
                     if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'}
        return perps, spots

    async def fetch_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        data = await self._request(base, {'symbol': symbol, 'interval': interval, 'limit': 4})
        
        if not data: return []
        
        return [
            Candle(float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]))
            for x in reversed(data)
        ]

class BybitClient(ExchangeClient):
    def __init__(self, proxy_mgr): super().__init__("Bybit", proxy_mgr)

    async def get_symbols(self) -> Tuple[Set[str], Set[str]]:
        perps, spots = set(), set()
        
        t1 = self._request('https://api.bybit.com/v5/market/instruments-info', {'category': 'linear'})
        t2 = self._request('https://api.bybit.com/v5/market/instruments-info', {'category': 'spot'})
        r1, r2 = await asyncio.gather(t1, t2)

        if r1 and 'result' in r1:
            perps = {s['symbol'] for s in r1['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'}
        if r2 and 'result' in r2:
            spots = {s['symbol'] for s in r2['result']['list'] if s['status'] == 'Trading' and s['quoteCoin'] == 'USDT'}
        return perps, spots

    async def fetch_candles(self, symbol: str, interval: str, market: str) -> List[Candle]:
        cat = 'linear' if market == 'perp' else 'spot'
        b_int = {"1M": "M", "1w": "W", "1d": "D"}[interval]
        
        data = await self._request('https://api.bybit.com/v5/market/kline', 
                                   {'category': cat, 'symbol': symbol, 'interval': b_int, 'limit': 4})
        
        if not data or 'result' not in data or 'list' not in data['result']: return []
        
        return [
            Candle(float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]))
            for x in data['result']['list']
        ]

# -----------------------------------------------------------------------------
# 5. Core Analysis Logic (Pure Functions)
# -----------------------------------------------------------------------------

def detect_reversal_pattern(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    """
    Checks if 2nd Last Close == 1st Last Open (with 0.3% tolerance).
    """
    if len(candles) < 3: return None, None
    
    last = candles[1]
    prev = candles[2]
    
    avg_price = (prev.close + last.open) / 2.0
    if avg_price == 0: return None, None
    if abs(prev.close - last.open) > (0.003 * avg_price):
        return None, None
        
    price = prev.close
    
    prev_red = prev.close < prev.open
    prev_green = prev.close > prev.open
    last_red = last.close < last.open
    last_green = last.close > last.open
    
    if prev_red and last_green:
        return price, "bullish"
    if prev_green and last_red:
        return price, "bearish"
        
    return None, None

def detect_low_volatility(candles: List[Candle]) -> Optional[float]:
    """Check if last closed candle moved < 1.0%"""
    if len(candles) < 2: return None
    c = candles[1]
    if c.open == 0: return None
    
    move = abs((c.close - c.open) / c.open) * 100
    if move < Config.LOW_MOVEMENT_THRESHOLD:
        return move
    return None

def check_level_hit(candles: List[Candle], level_price: float) -> Tuple[bool, float]:
    """Checks if CURRENT (Live) candle touched the price."""
    if not candles: return False, 0.0
    curr = candles[0]
    if curr.low <= level_price <= curr.high:
        return True, curr.close
    return False, 0.0

# -----------------------------------------------------------------------------
# 6. Persistence & State Management
# -----------------------------------------------------------------------------

class StateManager:
    @staticmethod
    def save_levels(levels: List[MarketLevel]):
        data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "levels": {
                f"{l.exchange}_{l.symbol}_{l.market}_{l.interval}": asdict(l)
                for l in levels
            }
        }
        with open(Config.LEVELS_FILE, 'w') as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def load_levels() -> List[MarketLevel]:
        if not Config.LEVELS_FILE.exists(): return []
        try:
            with open(Config.LEVELS_FILE, 'r') as f:
                data = json.load(f)
                return [MarketLevel(**v) for v in data.get("levels", {}).values()]
        except Exception as e:
            logger.error(f"Levels load error: {e}")
            return []

    @staticmethod
    def load_daily_hits() -> List[LevelHit]:
        """Loads hits that already occurred today."""
        if not Config.HITS_FILE.exists(): return []
        try:
            with open(Config.HITS_FILE, 'r') as f:
                data = json.load(f)
                hits = [LevelHit(**h) for h in data]
                if hits:
                    last_hit = datetime.fromisoformat(hits[0].hit_time)
                    if datetime.now(timezone.utc).date() > last_hit.date():
                        logger.info("Hits file is from yesterday. Resetting.")
                        return []
                return hits
        except:
            return []

    @staticmethod
    def save_daily_hits(hits: List[LevelHit]):
        unique = {h.id: h for h in hits}.values()
        with open(Config.HITS_FILE, 'w') as f:
            json.dump([asdict(h) for h in unique], f, indent=2)

    @staticmethod
    def load_msg_ids() -> List[int]:
        if not Config.MSG_ID_FILE.exists(): return []
        try:
            with open(Config.MSG_ID_FILE) as f:
                # Ensure it returns a list of integers, not just a single ID or corrupted data
                ids = json.load(f).get("ids", [])
                return [int(i) for i in ids if isinstance(i, (int, str)) and str(i).isdigit()]
        except: return []

    @staticmethod
    def save_msg_ids(ids: List[int]):
        with open(Config.MSG_ID_FILE, 'w') as f:
            json.dump({"ids": ids, "updated": datetime.now(timezone.utc).isoformat()}, f)

# -----------------------------------------------------------------------------
# 7. Telegram Reporter (Smart Editing)
# -----------------------------------------------------------------------------

class TelegramBot:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id and Bot is not None)
        if self.enabled:
            self.bot = Bot(token=self.token)

    async def send_report(self, text_sections: List[str], message_ids_to_edit: List[int] = None) -> List[int]:
        if not self.enabled: return []

        # 1. Flatten sections into 4096-char chunks
        final_chunks = []
        current_chunk = ""
        
        for section in text_sections:
            if len(current_chunk) + len(section) + 2 > 4000:
                final_chunks.append(current_chunk.strip())
                current_chunk = ""
            current_chunk += section + "\n\n"
        
        if current_chunk.strip():
            final_chunks.append(current_chunk.strip())

        # 2. Update or Send
        new_ids = []
        edit_limit = len(message_ids_to_edit) if message_ids_to_edit else 0
        
        for i, chunk in enumerate(final_chunks):
            sent_id = None
            if i < edit_limit:
                try:
                    # Edit existing message
                    await self.bot.edit_message_text(
                        chat_id=self.chat_id,
                        message_id=message_ids_to_edit[i],
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    sent_id = message_ids_to_edit[i]
                except BadRequest as e:
                    # Message likely deleted or content is identical, skip or send new
                    if 'message is not modified' in str(e):
                        sent_id = message_ids_to_edit[i] # Content is identical, treat as successful edit
                    else:
                        logger.warning(f"Could not edit msg {message_ids_to_edit[i]}: {e}. Sending new.")
                except Exception as e:
                    logger.error(f"TG Edit Error: {e}")

            if sent_id is None:
                # Send new message
                try:
                    msg = await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    sent_id = msg.message_id
                except Exception as e:
                    logger.error(f"TG Send Error: {e}")

            if sent_id:
                new_ids.append(sent_id)
            
            await asyncio.sleep(0.3) # Rate limit protection

        # 3. Delete leftover messages if the new report is shorter
        if edit_limit > len(final_chunks):
            for i in range(len(final_chunks), edit_limit):
                try:
                    await self.bot.delete_message(
                        chat_id=self.chat_id, 
                        message_id=message_ids_to_edit[i]
                    )
                except Exception as e:
                    logger.warning(f"Failed to delete old message ID {message_ids_to_edit[i]}: {e}")

        return new_ids

# -----------------------------------------------------------------------------
# 8. Report Generators
# -----------------------------------------------------------------------------

def build_full_scan_report(levels: List[MarketLevel], low_vol: List[LowVolResult]) -> List[str]:
    sections = []
    ts = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
    # Header
    bin_c = len([l for l in levels if l.exchange == "Binance"])
    byb_c = len([l for l in levels if l.exchange == "Bybit"])
    sections.append(f"💥 *Reversal Level Scanner*\n\n✅ Total: {len(levels)}\n*Binance*: {bin_c} | *Bybit*: {byb_c}\n🕒 {ts}")

    # Reversal Levels
    grouped = {}
    for l in levels:
        if l.interval not in grouped: grouped[l.interval] = {"Binance": {}, "Bybit": {}}
        ex_dict = grouped[l.interval][l.exchange]
        if l.signal_type not in ex_dict: ex_dict[l.signal_type] = []
        ex_dict[l.signal_type].append(l.symbol)

    for interval in ["1M", "1w", "1d"]:
        if interval not in grouped: continue
        
        block = f"📅 *{interval} Timeframe*\n"
        has_data = False
        
        for ex in ["Binance", "Bybit"]:
            data = grouped[interval][ex]
            if not data: continue
            
            bullish = sorted(data.get("bullish", []))
            bearish = sorted(data.get("bearish", []))
            if not bullish and not bearish: continue
            
            has_data = True
            block += f"\n*{ex}*:\n"
            if bullish: block += f"🍏 *Bullish ({len(bullish)})*\n" + "\n".join([f"• {s}" for s in bullish]) + "\n"
            if bearish: block += f"🔻 *Bearish ({len(bearish)})*\n" + "\n".join([f"• {s}" for s in bearish]) + "\n"

        if has_data:
            block += "——————————————————"
            sections.append(block)

    # Low Volatility
    if low_vol:
        low_vol.sort(key=lambda x: x.movement_percent)
        block = "📉 *Low Movement Daily (<1.0%)*\n"
        
        bin_l = [x for x in low_vol if x.exchange == "Binance"]
        byb_l = [x for x in low_vol if x.exchange == "Bybit"]
        
        if bin_l: block += "\n*Binance*:\n" + "".join([f"• {x.symbol} ({x.movement_percent:.2f}%)\n" for x in bin_l])
        if byb_l: block += "\n*Bybit*:\n" + "".join([f"• {x.symbol} ({x.movement_percent:.2f}%)\n" for x in byb_l])
        
        block += "\n——————————————————"
        sections.append(block)

    return sections

def build_alert_report(hits: List[LevelHit]) -> List[str]:
    ts = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
    if not hits:
        # Crucial: Send an empty hits list to retain message ID structure
        return [f"💥 *LEVEL ALERTS* 💥\n\n❌ No levels got hit today.\n\n🕒 {ts}"]

    header = f"🚨 *LEVEL ALERTS* 🚨\n\n⚡ {len(hits)} levels hit today!\n🕒 {ts}\n"
    sections = [header]

    # Group by Interval -> Exchange -> Signal
    grouped = {}
    for h in hits:
        if h.interval not in grouped: grouped[h.interval] = {"Binance": {}, "Bybit": {}}
        d = grouped[h.interval][h.exchange]
        if h.signal_type not in d: d[h.signal_type] = []
        d[h.signal_type].append(h)

    for interval in ["1M", "1w", "1d"]:
        if interval not in grouped: continue
        block = f"📅 *{interval} Alerts*\n"
        has_content = False
        
        for ex in ["Binance", "Bybit"]:
            d = grouped[interval][ex]
            if not d: continue
            
            bull = d.get("bullish", [])
            bear = d.get("bearish", [])
            if not bull and not bear: continue
            
            has_content = True
            block += f"\n*{ex}*:\n"
            if bull: 
                block += f"🍏 *Bullish ({len(bull)})*\n"
                block += "".join([f"• {h.symbol} @ ${h.level_price:.4f}\n" for h in bull])
            if bear: 
                block += f"🔻 *Bearish ({len(bear)})*\n"
                block += "".join([f"• {h.symbol} @ ${h.level_price:.4f}\n" for h in bear])
        
        if has_content:
            block += "\n——————————————————"
            sections.append(block)
            
    return sections

# -----------------------------------------------------------------------------
# 9. Main Orchestration
# -----------------------------------------------------------------------------

async def run_full_scan(clients: List[ExchangeClient], tg: TelegramBot):
    logger.info("🕵️ STARTING FULL MARKET SCAN")
    
    logger.info("Fetching symbols...")
    raw_syms = {}
    async def get_syms_safe(c):
        try:
            return c.name, await c.get_symbols()
        except Exception as e:
            logger.error(f"{c.name} symbol fetch failed: {e}")
            return c.name, (set(), set())
            
    results = await asyncio.gather(*(get_syms_safe(c) for c in clients))
    for r in results: raw_syms[r[0]] = r[1]

    def norm(s): return s.replace(":", "").replace("/", "")
    tasks = []
    seen = set()
    
    priorities = [
        ("Binance", 0, "perp"), ("Binance", 1, "spot"),
        ("Bybit", 0, "perp"), ("Bybit", 1, "spot")
    ]
    
    for ex_name, idx, market in priorities:
        if ex_name not in raw_syms: continue
        pool = raw_syms[ex_name][idx]
        client = next(c for c in clients if c.name == ex_name)
        
        for s in pool:
            n = norm(s)
            if n not in seen and s not in Config.IGNORED_SYMBOLS:
                seen.add(n)
                tasks.append((client, s, market))

    logger.info(f"Targeting {len(tasks)} unique symbols.")

    levels = []
    low_vols = []
    sem = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)
    
    async def scan_worker(client, symbol, market):
        async with sem:
            for interval in ["1d", "1w", "1M"]:
                try:
                    candles = await client.fetch_candles(symbol, interval, market)
                    
                    price, sig = detect_reversal_pattern(candles)
                    if price:
                        touched, _ = check_level_hit(candles, price)
                        if not touched:
                            levels.append(MarketLevel(
                                client.name, symbol, market, interval, sig, price, 
                                datetime.now(timezone.utc).isoformat()
                            ))

                    if interval == "1d":
                        mv = detect_low_volatility(candles)
                        if mv:
                            low_vols.append(LowVolResult(client.name, symbol, market, mv))
                            
                except Exception:
                    pass

    logger.info("Executing concurrent scan...")
    chunk_size = 500
    total = len(tasks)
    for i in range(0, total, chunk_size):
        chunk = tasks[i:i+chunk_size]
        await asyncio.gather(*(scan_worker(*args) for args in chunk))
        logger.info(f"Progress: {min(i+chunk_size, total)}/{total}")

    logger.info(f"Scan Complete. Found {len(levels)} levels, {len(low_vols)} low vol.")
    
    StateManager.save_levels(levels)
    
    # 3. Reporting and Resetting States
    
    # Send Full Scan Report (New messages every time)
    report_sections = build_full_scan_report(levels, low_vols)
    await tg.send_report(report_sections)
    
    # Clear and re-initialize DAILY HIT states
    StateManager.save_daily_hits([])
    StateManager.save_msg_ids([]) 
    
    # Initialize the ALERTS message (The message that gets edited hourly)
    alert_ids = await tg.send_report(build_alert_report([]))
    StateManager.save_msg_ids(alert_ids)


async def run_price_check(clients: List[ExchangeClient], tg: TelegramBot):
    logger.info("🔍 PRICE CHECK MODE")
    
    levels = StateManager.load_levels()
    if not levels:
        logger.warning("No levels found to check.")
        return

    confirmed_hits = StateManager.load_daily_hits()
    initial_hit_count = len(confirmed_hits)
    
    CheckTask = NamedTuple("CheckTask", [("client_name", str), ("symbol", str), ("market", str), ("interval", str)])
    grouped_levels: Dict[CheckTask, List[MarketLevel]] = {}
    
    for l in levels:
        key = CheckTask(l.exchange, l.symbol, l.market, l.interval)
        if key not in grouped_levels: grouped_levels[key] = []
        grouped_levels[key].append(l)

    sem = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)
    
    async def check_worker(key: CheckTask, levels_list: List[MarketLevel]):
        async with sem:
            client = next((c for c in clients if c.name == key.client_name), None)
            if not client: return
            
            try:
                candles = await client.fetch_candles(key.symbol, key.interval, key.market)
                if not candles: return
                
                for lvl in levels_list:
                    hit, close_price = check_level_hit(candles, lvl.price)
                    if hit:
                        hit_id = f"{lvl.exchange}_{lvl.symbol}_{lvl.interval}_{lvl.price}"
                        
                        confirmed_hits.append(LevelHit(
                            hit_id, lvl.exchange, lvl.symbol, lvl.market, lvl.interval,
                            lvl.signal_type, lvl.price, close_price,
                            datetime.now(timezone.utc).isoformat()
                        ))
            except:
                pass

    logger.info(f"Checking {len(grouped_levels)} unique candle contexts...")
    await asyncio.gather(*(check_worker(k, v) for k, v in grouped_levels.items()))

    unique_hits_map = {h.id: h for h in confirmed_hits}
    final_hits = list(unique_hits_map.values())
    
    logger.info(f"Hits before: {initial_hit_count}, Hits after: {len(final_hits)}")

    # Save state
    StateManager.save_daily_hits(final_hits)
    
    # ⭐️⭐️⭐️ FIX IS HERE ⭐️⭐️⭐️
    # 1. Load the message IDs saved during the Full Scan
    msg_ids = StateManager.load_msg_ids() 
    
    # 2. Build the report based on ALL accumulated hits today
    report_sections = build_alert_report(final_hits)
    
    # 3. Use the loaded IDs for editing. If IDs are found, the message is edited.
    # If not found, a new message is sent and its IDs are saved.
    new_ids = await tg.send_report(report_sections, msg_ids)
    
    # 4. Save the new/edited IDs for the next hourly run
    StateManager.save_msg_ids(new_ids)

async def main():
    run_mode = os.getenv("RUN_MODE", "full_scan")
    proxy_url = os.getenv("PROXY_LIST_URL")

    if not proxy_url:
        logger.error("❌ PROXY_LIST_URL is missing")
        return

    proxy_mgr = SmartProxyManager(proxy_url)
    await proxy_mgr.load_and_verify()

    binance = BinanceClient(proxy_mgr)
    bybit = BybitClient(proxy_mgr)
    tg_bot = TelegramBot()

    async with binance, bybit:
        clients = [binance, bybit]
        if run_mode == "price_check":
            await run_price_check(clients, tg_bot)
        else:
            await run_full_scan(clients, tg_bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
