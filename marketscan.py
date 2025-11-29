import asyncio
import json
import logging
import os
import random
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple, Any
from itertools import cycle
import aiohttp
from telegram import Bot
from tqdm.asyncio import tqdm
import re

# ==========================================
# CONFIGURATION
# ==========================================

@dataclass
class Config:
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
    PROXY_URL: str = os.getenv("PROXY_LIST_URL", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt")
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
    impact_percent: float = 0.0

    def to_dict(self):
        return asdict(self)

@dataclass
class LowMovementHit:
    exchange: str
    symbol: str
    market: str
    interval: str
    movement_percent: float

# ==========================================
# ROBUST PROXY POOL (HEAVY TEST)
# ==========================================

class AsyncProxyPool:
    def __init__(self, max_pool_size=20):
        self.proxies: List[str] = []
        self.max_pool_size = max_pool_size
        self.iterator = None
        self._lock = asyncio.Lock()
        self.failures: Dict[str, int] = {}

    async def populate(self, url: str, session: aiohttp.ClientSession):
        if not url: return
        
        raw = []
        try:
            logging.info(f"📥 Fetching proxies from {url}...")
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    for line in text.splitlines():
                        p = line.strip()
                        if p: raw.append(p if "://" in p else f"http://{p}")
        except Exception as e:
            logging.error(f"❌ Proxy fetch failed: {e}")
            return

        logging.info(f"🔎 Validating {len(raw)} proxies...")
        self.proxies = []
        random.shuffle(raw)
        
        # High concurrency for initial check to speed up startup
        sem = asyncio.Semaphore(200)
        
        async def protected_test(p):
            async with sem: return await self._test_proxy(p, session)

        tasks = [asyncio.create_task(protected_test(p)) for p in raw]
        
        for future in asyncio.as_completed(tasks):
            try:
                proxy, is_good = await future
                if is_good:
                    self.proxies.append(proxy)
                    if len(self.proxies) >= self.max_pool_size: break
            except: pass
        
        for t in tasks:
            if not t.done(): t.cancel()
        await asyncio.sleep(0.1)
        
        if self.proxies:
            self.iterator = cycle(self.proxies)
            logging.info(f"✅ Proxy Pool Ready: {len(self.proxies)} warriors.")
        else:
            logging.error("❌ NO WORKING PROXIES FOUND!")

    async def _test_proxy(self, proxy: str, session: aiohttp.ClientSession) -> Tuple[str, bool]:
        try:
            # HEAVY TEST: Actual Futures Data to filter out weak/WAF-blocked proxies
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": "BTCUSDT", "interval": "1m", "limit": "2"}
            async with session.get(url, params=params, proxy=proxy, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        return proxy, True
                return proxy, False
        except: return proxy, False

    async def get_proxy(self) -> Optional[str]:
        if not self.proxies: return None
        async with self._lock: 
            if not self.proxies: return None
            return next(self.iterator)

    async def report_failure(self, proxy: str):
        async with self._lock:
            self.failures[proxy] = self.failures.get(proxy, 0) + 1
            # 10 Strikes Rule - Aggressive removal of bad proxies
            if self.failures[proxy] >= 10:
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    if self.proxies: self.iterator = cycle(self.proxies)
                    logging.warning(f"🚫 Banned Proxy {proxy} (10 failures)")

# ==========================================
# EXCHANGE CLIENTS
# ==========================================

class ExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        self.session = session
        self.proxies = proxy_pool
        limit = CONFIG.MAX_CONCURRENCY if proxy_pool.proxies else 5
        self.sem = asyncio.Semaphore(limit)

    async def _request(self, url: str, params: dict = None) -> Any:
        for attempt in range(CONFIG.MAX_RETRIES):
            proxy = await self.proxies.get_proxy()
            if not proxy: await asyncio.sleep(1); continue
            
            try:
                async with self.sem:
                    async with self.session.get(url, params=params, proxy=proxy, timeout=CONFIG.REQUEST_TIMEOUT) as resp:
                        if resp.status == 200: return await resp.json()
                        elif resp.status == 429:
                            await self.proxies.report_failure(proxy) 
                            await asyncio.sleep(2)
                        elif resp.status == 418:
                            await self.proxies.report_failure(proxy)
            except Exception as e:
                await self.proxies.report_failure(proxy)
            
            await asyncio.sleep(0.5)
        return None

class BinanceClient(ExchangeClient):
    async def get_perp_symbols(self) -> List[str]:
        data = await self._request('https://fapi.binance.com/fapi/v1/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] if s.get('contractType') == 'PERPETUAL' and s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']
    
    async def get_spot_symbols(self) -> List[str]:
        data = await self._request('https://api.binance.com/api/v3/exchangeInfo')
        if not data: return []
        return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s.get('quoteAsset') == 'USDT']

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int = 3) -> List[Candle]:
        base = 'https://api.binance.com/api/v3/klines' if market == "spot" else 'https://fapi.binance.com/fapi/v1/klines'
        data = await self._request(base, {'symbol': symbol, 'interval': interval, 'limit': limit})
        if not data or not isinstance(data, list): return []
        
        candles = []
        for c in data:
            try: candles.append(Candle(float(c[1]), float(c[2]), float(c[3]), float(c[4])))
            except: continue
        # Binance returns Oldest->Newest. Reverse to get Newest at index 0.
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
        raw = data.get('result', {}).get('list', [])
        if not raw: return []
        
        candles = []
        for c in raw:
            candles.append(Candle(float(c[1]), float(c[2]), float(c[3]), float(c[4])))
        return candles # Bybit V5 returns Newest first. No reverse needed.

# ==========================================
# CORE LOGIC
# ==========================================

def floats_are_equal(a: float, b: float, rel_tol: float = 0.003) -> bool:
    return abs(a - b) <= rel_tol * ((a + b) / 2.0)

def check_reversal(candles: List[Candle]) -> Tuple[Optional[float], Optional[str]]:
    # Expects candles[0] = Newest
    if len(candles) < 3: return None, None
    last_closed = candles[1]
    second_last_closed = candles[2]
    
    # Check if Close of candle[2] == Open of candle[1]
    if floats_are_equal(second_last_closed.close, last_closed.open):
        price = second_last_closed.close
        # Color logic
        s_red = second_last_closed.close < second_last_closed.open
        s_green = second_last_closed.close > second_last_closed.open
        l_green = last_closed.close > last_closed.open
        l_red = last_closed.close < last_closed.open
        
        if s_red and l_green: return price, "bullish"
        if s_green and l_red: return price, "bearish"
    return None, None

def current_candle_touched_price(candles: List[Candle], price: float) -> bool:
    if not candles: return False
    curr = candles[0]
    return curr.low <= price <= curr.high

def check_low_movement(candles: List[Candle], threshold: float = 1.0) -> Optional[float]:
    if len(candles) < 2: return None
    last = candles[1]
    if last.open == 0: return None
    pct = abs((last.close - last.open) / last.open) * 100
    return pct if pct < threshold else None

# ==========================================
# STATE & UTILS
# ==========================================

def load_levels() -> Dict[str, Any]:
    try:
        if os.path.exists(CONFIG.LEVELS_FILE):
            with open(CONFIG.LEVELS_FILE, "r") as f:
                return json.load(f).get("levels", {})
    except: pass
    return {}

def save_levels(results: List[LevelHit], utc_now_str: str):
    data = {"last_updated": utc_now_str, "levels": {}}
    for r in results:
        key = f"{r.exchange}_{r.symbol}_{r.market}_{r.interval}"
        data["levels"][key] = r.to_dict()
    with open(CONFIG.LEVELS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_accumulated_hits() -> List[LevelHit]:
    try:
        if os.path.exists("accumulated_hits.json"):
            with open("accumulated_hits.json", "r") as f:
                data = json.load(f)
                return [LevelHit(**h) for h in data.get("hits", [])]
    except:
        pass
    return []

def save_accumulated_hits(hits: List[LevelHit]):
    data = {"hits": [h.to_dict() for h in hits]}
    with open("accumulated_hits.json", "w") as f:
        json.dump(data, f, indent=2)
        
def save_message_ids(ids: List[int]):
    with open(CONFIG.MSG_FILE, "w") as f:
        json.dump({"message_ids": ids, "timestamp": datetime.now(timezone.utc).isoformat()}, f)

def load_message_ids() -> List[int]:
    if not os.path.exists(CONFIG.MSG_FILE): return []
    try:
        with open(CONFIG.MSG_FILE) as f:
            return json.load(f).get("message_ids", [])
    except:
        return []

def clear_message_ids():
    if os.path.exists(CONFIG.MSG_FILE):
        os.remove(CONFIG.MSG_FILE)

# ==========================================
# MAIN BOT CLASS
# ==========================================

class MarketScanBot:
    def __init__(self):
        self.utc_now = datetime.now(timezone.utc)
        self.tg_bot = Bot(token=CONFIG.TELEGRAM_TOKEN) if CONFIG.TELEGRAM_TOKEN else None

    async def run(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        async with aiohttp.ClientSession() as session:
            # 1. Proxies
            proxies = AsyncProxyPool()
            if CONFIG.PROXY_URL: await proxies.populate(CONFIG.PROXY_URL, session)
            
            # 2. Clients
            binance = BinanceClient(session, proxies)
            bybit = BybitClient(session, proxies)
            
            if CONFIG.RUN_MODE == "price_check":
                await self.run_price_check(binance, bybit)
            else:
                await self.run_full_scan(binance, bybit)

    async def run_price_check(self, binance: BinanceClient, bybit: BybitClient):
        logging.info("🚀 Starting Price Check...")
        levels_data = load_levels()
        if not levels_data:
            logging.info("⚠️ No levels found.")
            await self.send_or_update_alert_report([])
            return
    
        tasks = []
        for key, data in levels_data.items():
            client = binance if data['exchange'] == 'Binance' else bybit
            tasks.append(self.check_single_level(client, data))
        
        hits = []
        hit_keys = set()
        
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking Levels"):
            res = await f
            if res:
                hits.append(res)
                hit_keys.add(f"{res.exchange}_{res.symbol}_{res.market}_{res.interval}")  # ← NEW: Track key
        
        # NEW: Clean hit levels
        if hit_keys:
            levels_data = load_levels()
            levels_dict = levels_data.get("levels", {})
            for key in hit_keys:
                levels_dict.pop(key, None)
            levels_data["levels"] = levels_dict
            levels_data["last_updated"] = self.utc_now.isoformat()
            with open(CONFIG.LEVELS_FILE, "w") as f:
                json.dump(levels_data, f, indent=2)
            logging.info(f"🗑️ Cleaned {len(hit_keys)} hit levels")
        
        await self.send_or_update_alert_report(hits)

    async def check_single_level(self, client: ExchangeClient, data: dict) -> Optional[LevelHit]:
        candles = await client.fetch_ohlcv(data['symbol'], data['interval'], data['market'], limit=2)
        if not candles: 
            return None
        
        curr = candles[0]
        target = data.get('level_price', data.get('price'))
        if target is None:
            return None
        
        if curr.low <= target <= curr.high:
            hit_time = datetime.now(timezone.utc).isoformat()  # 🆕 Hit time
            impact_percent = abs(curr.close - target) / target * 100  # 🆕 Impact %
            
            return LevelHit(
                exchange=data['exchange'], symbol=data['symbol'], market=data['market'],
                interval=data['interval'], signal_type=data['signal_type'],
                level_price=target, current_price=curr.close,
                timestamp=hit_time,
                impact_percent=impact_percent
            )
        return None
        
    async def run_full_scan(self, binance, bybit):
        logging.info("🌍 Starting FULL Market Scan...")
        clear_message_ids()
        
        # 1. Fetch Symbols
        t1 = asyncio.create_task(binance.get_perp_symbols())
        t2 = asyncio.create_task(binance.get_spot_symbols())
        t3 = asyncio.create_task(bybit.get_perp_symbols())
        t4 = asyncio.create_task(bybit.get_spot_symbols())
        bp, bs, yp, ys = await asyncio.gather(t1, t2, t3, t4)
        
        counts = [len(x) for x in [bp, bs, yp, ys]]
        logging.info(f"✅ Symbols: BP:{counts[0]} | BS:{counts[1]} | YP:{counts[2]} | YS:{counts[3]}")
        
        # 2. Deduplicate
        seen = set()
        def filter_u(syms):
            res = []
            for s in syms:
                if s in CONFIG.IGNORED_SYMBOLS or not s.endswith("USDT"): continue
                norm = s.replace("USDT", "")
                if norm not in seen:
                    res.append(s)
                    seen.add(norm)
            return res
            
        final_bp, final_bs, final_yp, final_ys = filter_u(bp), filter_u(bs), filter_u(yp), filter_u(ys)
        
        # 3. Scan Setup
        async def scan_one(client, s, mkt, ex):
            revs, low_m = [], None
            current_time = datetime.now(timezone.utc).isoformat()  # ← NEW: Timestamp
            
            for interval in ["1M", "1w", "1d"]:
                candles = await client.fetch_ohlcv(s, interval, mkt)
                if not candles: continue
                
                p, sig = check_reversal(candles)
                if p and sig and not current_candle_touched_price(candles, p):
                    revs.append(LevelHit(ex, s, mkt, interval, sig, p, timestamp=current_time))  # ← ADD timestamp
                
                if interval == "1d":
                    v = check_low_movement(candles, 1.0)
                    if v is not None:
                        low_m = LowMovementHit(ex, s, mkt, interval, v)
            return revs, low_m, True

        # 4. Create Tasks
        tasks = []
        for client, syms, mkt, ex in [
            (binance, final_bp, 'perp', 'Binance'),
            (binance, final_bs, 'spot', 'Binance'),
            (bybit, final_yp, 'perp', 'Bybit'),
            (bybit, final_ys, 'spot', 'Bybit')
        ]:
            for s in syms:
                tasks.append(scan_one(client, s, mkt, ex))
        
        # 5. Execute
        final_results = []
        final_lows = []
        success_count = 0
        
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scanning"):
            try:
                r, l, ok = await f
                if r: final_results.extend(r)
                if l: final_lows.append(l)
                if ok: success_count += 1
            except: pass
            
        logging.info(f"Scan Complete: Success {success_count}/{len(tasks)}")
        
        save_levels(final_results, self.utc_now.isoformat())
        await self.send_full_report(final_results, final_lows)
        await self.send_or_update_alert_report([])

    async def send_chunks(self, text: str):
        chunks = []
        curr = ""
        for line in text.splitlines(keepends=True):
            if len(curr) + len(line) > 4000:
                chunks.append(curr)
                curr = ""
            curr += line
        if curr: chunks.append(curr)
        
        for c in chunks:
            try:
                await self.tg_bot.send_message(CONFIG.CHAT_ID, text=c, parse_mode='Markdown')
                await asyncio.sleep(0.5)
            except: pass

    # --- NEW REPORTING FORMAT ---
    async def send_full_report(self, results: List[LevelHit], low_movements: List[LowMovementHit]):
        ts = self.utc_now.strftime("%d %b %H:%M UTC")
    
        number_prefix_re = re.compile(r"^(1000|10000|100000|1000000)")
    
        def clean(s: str) -> str:
            s = s.replace("USDT", "")
            s = number_prefix_re.sub("", s)
            return s
    
        def format_space_padded_grid(symbols: List[str], per_line: int = 3, col_width: int = 10) -> List[str]:
            symbols = [clean(s) for s in sorted(symbols)]
            lines = []
            for i in range(0, len(symbols), per_line):
                chunk = symbols[i:i+per_line]
                padded = [sym.ljust(col_width) for sym in chunk]
                lines.append("│ " + " ".join(padded))
            return lines
    
        lines = ["💳 *REVERSAL ALERTS*", "─────────────────────"]
    
        # 1. REVERSALS - Group by timeframe -> exchange -> direction
        grouped = {}
        for r in results:
            grouped.setdefault(r.interval, {}).setdefault(r.exchange, {}).setdefault(r.signal_type, []).append(r.symbol)
    
        for tf_key, tf_label in [("1M", "MONTHLY (1M)"), ("1w", "WEEKLY (1w)"), ("1d", "DAILY (1d)")]:
            if tf_key not in grouped:
                continue
    
            tf_data = grouped[tf_key]
            if not any(tf_data.values()):
                continue
    
            # Count Bull/Bear for header
            bull_total = sum(len(tf_data.get(ex, {}).get("bullish", [])) for ex in tf_data)
            bear_total = sum(len(tf_data.get(ex, {}).get("bearish", [])) for ex in tf_data)
            count_str = f"[{bull_total} Bull/{bear_total} Bear]" if bull_total or bear_total else "[0]"
    
            lines.append(f"\n📅 *{tf_label}* {count_str}")
    
            for ex, ex_icon in [("Binance", "🟡"), ("Bybit", "⚫")]:
                if ex not in tf_data:
                    continue
    
                ex_data = tf_data[ex]
                bull_list = sorted(ex_data.get("bullish", []))
                bear_list = sorted(ex_data.get("bearish", []))
    
                if not bull_list and not bear_list:
                    continue
    
                total_symbols = len(bull_list) + len(bear_list)
                lines.append(f"┌ {ex_icon} *{ex.upper()}* ({total_symbols} Symbols)")
    
                # Bullish section
                if bull_list:
                    lines.append(f"│ 🍏 *Bullish* ({len(bull_list)})")
                    lines.extend(format_space_padded_grid(bull_list, per_line=3))
                    lines.append("│")  # Spacer
    
                # Bearish section
                if bear_list:
                    lines.append(f"│ 🔻 *Bearish* ({len(bear_list)})")
                    lines.extend(format_space_padded_grid(bear_list, per_line=3))
    
                lines.append("└")
    
        # 2. SQUEEZE - Top 30 per exchange
        if low_movements:
            lines.append("\n📉 *SQUEEZE ALERT (<1%)*")
            lines.append("─────────────────────")
    
            lm_grp = {"Binance": [], "Bybit": []}
            for x in low_movements:
                if x.exchange in lm_grp:
                    lm_grp[x.exchange].append(x)
    
            for ex, ex_icon in [("Binance", "🟡"), ("Bybit", "⚫")]:
                items = lm_grp.get(ex, [])
                if not items:
                    continue
    
                items.sort(key=lambda x: x.movement_percent)
                top_items = items[:30]
                total_count = len(items)
    
                lines.append(f"┌ {ex_icon} *{ex.upper()}* (Top 30/{total_count})")
                for i, item in enumerate(top_items):
                    clean_sym = clean(item.symbol)
                    pct = f"{item.movement_percent:.2f}%"
                    pct_pad = " " * (6 - len(pct)) + pct  # Right-align pct in 6 spaces
                    lines.append(f"│ {clean_sym}{pct_pad}")
    
                if total_count > 30:
                    lines.append(f"│ ...and {total_count-30} more")
                lines.append("└")
    
        lines.append("\n─────────────────────")
        lines.append(f"🕒 {ts}")
    
        await self.send_chunks("\n".join(lines))

    async def send_or_update_alert_report(self, hits: List[LevelHit]):
        """
        Price Check Mode - Line-by-line SYMBOL @ $PRICE format
        Edits existing messages or sends new ones.
        Grouped by exchange and timeframe with counts, no impact percent.
        """
        def _fmt_price(p: float) -> str:
            return f"${p:g}"
        
        ts_local = self.utc_now.astimezone(timezone(timedelta(hours=3))).strftime("%d %b %H:%M UTC+3")
        
        if not hits:
            text = (
                "🚨 *LEVEL ALERTS*\n"
                "─────────────────────\n"
                "❌ No active level hits right now.\n"
                "─────────────────────\n"
                f"🕒 {ts_local}"
            )
        else:
            lines = [
                "🚨 *LEVEL ALERTS*",
                "─────────────────────",
                f"⚡ *{len(hits)} LEVELS HIT{'S' if len(hits) != 1 else ''}!*",
                ""
            ]
    
            # Group by exchange -> timeframe -> direction
            grouped = {}
            for h in hits:
                grouped.setdefault(h.exchange, {}).setdefault(h.interval, {}).setdefault(h.signal_type, []).append(h)
            
            exchanges = [("Binance", "🟡"), ("Bybit", "⚫")]
            for ex_name, ex_icon in exchanges:
                if ex_name not in grouped:
                    continue
                
                ex_data = grouped[ex_name]
    
                # Count total hits per exchange
                total_hits = sum(len(tf_data.get("bullish", [])) + len(tf_data.get("bearish", [])) for tf_data in ex_data.values())
                if total_hits == 0:
                    continue
    
                lines.append(f"┌ {ex_icon} *{ex_name.upper()} ({total_hits} Hit{'s' if total_hits != 1 else ''})*")
    
                tf_order = ["1M", "1w", "1d"]
                tf_label_map = {"1M": "📅 1M", "1w": "📅 1w", "1d": "📅 1d"}
    
                for tf in tf_order:
                    if tf not in ex_data:
                        continue
    
                    tf_data = ex_data[tf]
    
                    bull_hits = tf_data.get("bullish", [])
                    bear_hits = tf_data.get("bearish", [])
    
                    bull_count = len(bull_hits)
                    bear_count = len(bear_hits)
    
                    lines.append(f"{tf_label_map.get(tf, tf)} ({bull_count} Bullish, {bear_count} Bearish)")
    
                    if bull_hits:
                        lines.append("│ 🍏 Bullish")
                        for hit in bull_hits:
                            sym = hit.symbol
                            price = _fmt_price(hit.level_price)
                            lines.append(f"│ {sym} @ {price}")
                        lines.append("│")  # Spacer
    
                    if bear_hits:
                        lines.append("│ 🔻 Bearish")
                        for hit in bear_hits:
                            sym = hit.symbol
                            price = _fmt_price(hit.level_price)
                            lines.append(f"│ {sym} @ {price}")
    
                lines.append("└")
                lines.append("")
    
            lines.append("─────────────────────")
            lines.append(f"🕒 {ts_local}")
            text = "\n".join(lines)
    
        # Existing chunking and message edit/send logic unchanged
        chunks = []
        temp_chunk = ""
        for line in text.splitlines(keepends=True):
            if len(temp_chunk) + len(line) > CONFIG.MAX_TG_CHARS:
                chunks.append(temp_chunk)
                temp_chunk = ""
            temp_chunk += line
        if temp_chunk.strip():
            chunks.append(temp_chunk)
    
        prev_ids = load_message_ids()
        new_ids = []
    
        for idx, chunk in enumerate(chunks):
            if idx < len(prev_ids):
                msg_id = prev_ids[idx]
                try:
                    await self.tg_bot.edit_message_text(
                        chat_id=CONFIG.CHAT_ID,
                        message_id=msg_id,
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    new_ids.append(msg_id)
                    continue
                except Exception as e:
                    logging.warning(f"Edit failed for msg {msg_id}: {e}")
    
            try:
                m = await self.tg_bot.send_message(
                    chat_id=CONFIG.CHAT_ID,
                    text=chunk,
                    parse_mode='Markdown'
                )
                new_ids.append(m.message_id)
                await asyncio.sleep(0.3)
            except Exception as e:
                logging.error(f"TG Send Error (price_check): {e}")
    
        save_message_ids(new_ids)

if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(MarketScanBot().run())
