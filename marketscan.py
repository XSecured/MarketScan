import aiohttp
import asyncio
import pandas as pd
import itertools
import logging
import os
import random
import time
import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Set
from telegram import Bot
import re

# Configuration Management
@dataclass
class Config:
    MAX_WORKERS: int = 25
    PROXY_POOL_SIZE: int = 25
    MAX_RETRIES: int = 5
    RETRY_DELAY: int = 2
    REQUEST_TIMEOUT: int = 10
    PRICE_TOLERANCE: float = 1e-6
    PROXY_CHECK_INTERVAL: int = 600
    MAX_PROXY_FAILURES: int = 3
    FETCH_LIMIT: int = 100
    OHLCV_MIN_CANDLES: int = 3
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 5
    CIRCUIT_BREAKER_TIMEOUT: int = 60
    TELEGRAM_MESSAGE_MAX_LENGTH: int = 4000

# Initialize configuration
config = Config()

# Symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT", 
    "USDYUSDT", "USDRUSDT", "ETH3LUSDT", "ETHUSDT-27MAR26", 
    "ETHUSDT-27JUN25", "ETHUSDT-26DEC25", "ETHUSDT-26SEP25"
}

# Custom Exception Hierarchy
class TradingBotException(Exception):
    """Base exception for trading bot"""
    pass

class ProxyException(TradingBotException):
    """Raised when proxy-related issues occur"""
    pass

class ExchangeAPIException(TradingBotException):
    """Raised when exchange API calls fail"""
    pass

class DataValidationException(TradingBotException):
    """Raised when data validation fails"""
    pass

class ConfigurationException(TradingBotException):
    """Raised when configuration is invalid"""
    pass

# Circuit Breaker Pattern
class CircuitBreaker:
    def __init__(self, failure_threshold: int = None, timeout: int = None):
        self.failure_threshold = failure_threshold or config.CIRCUIT_BREAKER_FAILURE_THRESHOLD
        self.timeout = timeout or config.CIRCUIT_BREAKER_TIMEOUT
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def call_succeeded(self):
        """Reset circuit breaker on successful call"""
        self.failure_count = 0
        self.state = "CLOSED"

    def call_failed(self):
        """Increment failure count and potentially open circuit"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logging.warning(f"Circuit breaker opened after {self.failure_count} failures")

    def can_call(self) -> bool:
        """Check if calls are allowed through circuit breaker"""
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
                logging.info("Circuit breaker moved to HALF_OPEN state")
                return True
            return False
        return True

# Data Validation Functions
def validate_ohlcv_data(df: pd.DataFrame) -> bool:
    """Validate OHLCV DataFrame structure and content"""
    required_columns = {'open', 'high', 'low', 'close', 'volume', 'openTime'}
    
    if df.empty:
        return False
    
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        logging.error(f"Missing required columns: {missing}")
        return False
    
    if len(df) < config.OHLCV_MIN_CANDLES:
        logging.error(f"Insufficient data: {len(df)} < {config.OHLCV_MIN_CANDLES}")
        return False
    
    # Check for valid numeric data
    try:
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if not pd.api.types.is_numeric_dtype(df[col]):
                logging.error(f"Non-numeric data in column: {col}")
                return False
            if df[col].isna().any():
                logging.error(f"NaN values found in column: {col}")
                return False
    except Exception as e:
        logging.error(f"Data validation error: {e}")
        return False
    
    return True

def floats_are_equal(a: float, b: float, rel_tol: float = None, abs_tol: float = None) -> bool:
    """Compare floats with relative and absolute tolerance"""
    rel_tol = rel_tol or config.PRICE_TOLERANCE
    abs_tol = abs_tol or 1e-9
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)

def sort_ohlcv_by_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Sort DataFrame by timestamp explicitly (newest first)"""
    if 'openTime' in df.columns:
        df = df.copy()
        df['openTime'] = pd.to_numeric(df['openTime'])
        return df.sort_values('openTime', ascending=False).reset_index(drop=True)
    return df

def validate_symbol_list(symbols: List[str]) -> List[str]:
    """Validate and clean symbol list"""
    if not symbols:
        return []
    
    valid_symbols = []
    for symbol in symbols:
        if isinstance(symbol, str) and symbol.strip():
            clean_symbol = symbol.strip().upper()
            if re.match(r'^[A-Z0-9]+$', clean_symbol):
                valid_symbols.append(clean_symbol)
            else:
                logging.warning(f"Invalid symbol format: {symbol}")
        else:
            logging.warning(f"Invalid symbol type: {symbol}")
    
    return valid_symbols

# Async Proxy Pool
class AsyncProxyPool:
    def __init__(self, max_pool_size: int = None, proxy_check_interval: int = None, max_failures: int = None):
        self.max_pool_size = max_pool_size or config.PROXY_POOL_SIZE
        self.proxy_check_interval = proxy_check_interval or config.PROXY_CHECK_INTERVAL
        self.max_failures = max_failures or config.MAX_PROXY_FAILURES

        self.proxies: List[str] = []
        self.proxy_failures: dict = {}
        self.failed_proxies: Set[str] = set()
        self.proxy_cycle = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self.circuit_breaker = CircuitBreaker()

    async def get_next_proxy(self) -> Optional[str]:
        """Get next available proxy from pool"""
        if not self.circuit_breaker.can_call():
            raise ProxyException("Circuit breaker is open - proxy pool unavailable")

        async with self._lock:
            if not self.proxies:
                logging.warning("Proxy pool empty when requesting next proxy")
                self.circuit_breaker.call_failed()
                return None
            
            for _ in range(len(self.proxies)):
                try:
                    proxy = next(self.proxy_cycle)
                    if proxy not in self.failed_proxies:
                        self.circuit_breaker.call_succeeded()
                        return proxy
                except (StopIteration, TypeError):
                    # Recreate cycle if needed
                    self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
                    if self.proxy_cycle:
                        proxy = next(self.proxy_cycle)
                        if proxy not in self.failed_proxies:
                            self.circuit_breaker.call_succeeded()
                            return proxy
            
            logging.warning("All proxies in pool are marked as failed")
            self.circuit_breaker.call_failed()
            return None

    async def mark_proxy_failure(self, proxy: str):
        """Mark proxy as failed and potentially remove from pool"""
        if proxy is None:
            return
        
        async with self._lock:
            count = self.proxy_failures.get(proxy, 0) + 1
            self.proxy_failures[proxy] = count
            logging.warning(f"Proxy {proxy} failure count: {count}/{self.max_failures}")
            
            if count >= self.max_failures:
                self.failed_proxies.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    logging.warning(f"Proxy {proxy} removed from pool due to repeated failures")
                    # Recreate cycle with remaining proxies
                    self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None

    async def populate_from_url(self, url: str, default_scheme: str = "http"):
        """Populate proxy pool from URL"""
        try:
            new_proxies = await self._fetch_proxies_from_url(url, default_scheme)
            working = await self._test_proxies_concurrently(new_proxies, max_working=self.max_pool_size)
            
            async with self._lock:
                self.proxies = working[:self.max_pool_size]
                self.proxy_failures.clear()
                self.failed_proxies.clear()
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
                logging.info(f"Proxy pool filled with {len(self.proxies)} working proxies from URL")
                
            if self.proxies:
                self.circuit_breaker.call_succeeded()
            else:
                self.circuit_breaker.call_failed()
                
        except Exception as e:
            logging.error(f"Failed to populate proxy pool: {e}")
            self.circuit_breaker.call_failed()
            raise ProxyException(f"Failed to populate proxy pool: {e}")

    async def start_checker(self):
        """Start background proxy health checker"""
        asyncio.create_task(self._proxy_checker_loop())

    async def _proxy_checker_loop(self):
        """Background loop to check proxy health"""
        while not self._stop_event.is_set():
            try:
                logging.info("Running proxy pool health check...")
                await self._check_proxies()
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.proxy_check_interval)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"Error in proxy checker loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    async def _check_proxies(self):
        """Check health of existing proxies"""
        if not self.proxies:
            return

        working = await self._test_proxies_concurrently(
            self.proxies, 
            max_workers=config.MAX_WORKERS, 
            max_working=len(self.proxies)
        )
        
        async with self._lock:
            initial_count = len(self.proxies)
            self.proxies = working
            removed = initial_count - len(working)
            
            # Clean up failure tracking for removed proxies
            self.proxy_failures = {p: self.proxy_failures.get(p, 0) for p in self.proxies}
            self.failed_proxies = set()
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
            
        if removed > 0:
            logging.info(f"Removed {removed} dead proxies during health check")
            await self._populate_to_max()

    async def _populate_to_max(self):
        """Populate proxy pool to maximum size"""
        async with self._lock:
            needed = self.max_pool_size - len(self.proxies)
            if needed <= 0:
                return

        try:
            new_proxies = await self._get_new_proxies(needed)
            async with self._lock:
                self.proxies.extend(new_proxies)
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
                logging.info(f"Proxy pool populated to size: {len(self.proxies)}/{self.max_pool_size}")
        except Exception as e:
            logging.error(f"Failed to populate proxy pool to max: {e}")

    async def _get_new_proxies(self, count: int) -> List[str]:
        """Get new working proxies"""
        backup_url = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"
        try:
            new_proxies = await self._fetch_proxies_from_url(backup_url)
            working = await self._test_proxies_concurrently(new_proxies, max_working=count)
            return working
        except Exception as e:
            logging.error(f"Failed to get new proxies: {e}")
            return []

    async def _fetch_proxies_from_url(self, url: str, default_scheme: str = "http") -> List[str]:
        """Fetch proxy list from URL"""
        proxies = []
        try:
            timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    lines = text.strip().splitlines()
                    
                    for line in lines:
                        proxy = line.strip()
                        if not proxy:
                            continue
                        if "://" in proxy:
                            proxies.append(proxy)
                        else:
                            proxies.append(f"{default_scheme}://{proxy}")
                            
            logging.info(f"Fetched {len(proxies)} proxies from {url}")
        except Exception as e:
            logging.error(f"Error fetching proxies from URL {url}: {e}")
            raise
        
        return proxies

    async def _test_proxy(self, proxy: str, timeout: int = None) -> bool:
        """Test if proxy is working"""
        test_url = "https://api.binance.com/api/v3/time"
        timeout = timeout or config.REQUEST_TIMEOUT
        
        try:
            client_timeout = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=client_timeout) as session:
                async with session.get(test_url, proxy=proxy) as response:
                    return 200 <= response.status < 300
        except Exception:
            return False

    async def _test_proxies_concurrently(self, proxies: List[str], max_workers: int = None, max_working: int = None) -> List[str]:
        """Test multiple proxies concurrently"""
        max_workers = max_workers or config.MAX_WORKERS
        max_working = max_working or config.PROXY_POOL_SIZE
        
        working = []
        semaphore = asyncio.Semaphore(max_workers)

        async def test_and_collect(proxy):
            async with semaphore:
                if await self._test_proxy(proxy):
                    working.append(proxy)
                    if len(working) % 5 == 0:
                        logging.info(f"Proxy testing progress: {len(working)} working proxies found")
                    return True
                return False

        tasks = [test_and_collect(proxy) for proxy in proxies]
        
        try:
            for task in asyncio.as_completed(tasks):
                await task
                if len(working) >= max_working:
                    # Cancel remaining tasks
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    break
        except Exception as e:
            logging.error(f"Error during proxy testing: {e}")

        logging.info(f"Found {len(working)} working proxies out of {len(proxies)} tested")
        return working[:max_working]

    def stop(self):
        """Stop the proxy pool"""
        self._stop_event.set()

# Base Async Exchange Client
class BaseAsyncExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        self.session = session
        self.proxy_pool = proxy_pool
        self.circuit_breaker = CircuitBreaker()

    async def _get_proxy(self) -> str:
        """Get working proxy from pool"""
        proxy = await self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise ProxyException("No working proxies available")
        return proxy

    async def _make_request(self, url: str, params: dict = None, max_retries: int = None) -> dict:
        """Make HTTP request with retry logic and proxy rotation"""
        max_retries = max_retries or config.MAX_RETRIES
        
        for attempt in range(1, max_retries + 1):
            if not self.circuit_breaker.can_call():
                raise ExchangeAPIException("Circuit breaker is open")

            try:
                proxy = await self._get_proxy()
                timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
                
                async with self.session.get(
                    url, 
                    params=params, 
                    proxy=proxy, 
                    timeout=timeout
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    self.circuit_breaker.call_succeeded()
                    return data
                    
            except ProxyException:
                self.circuit_breaker.call_failed()
                raise
            except aiohttp.ClientError as e:
                logging.warning(f"Attempt {attempt} failed: {e}")
                self.circuit_breaker.call_failed()
                if 'proxy' in locals():
                    await self.proxy_pool.mark_proxy_failure(proxy)
            except Exception as e:
                logging.warning(f"Attempt {attempt} unexpected error: {e}")
                self.circuit_breaker.call_failed()
                
            if attempt < max_retries:
                delay = config.RETRY_DELAY * attempt + random.uniform(0, 1)
                await asyncio.sleep(delay)

        raise ExchangeAPIException(f"All {max_retries} attempts failed for {url}")

# Async Binance Client
class AsyncBinanceClient(BaseAsyncExchangeClient):
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        super().__init__(session, proxy_pool)
        self.base_url = "https://api.binance.com"
        self.futures_url = "https://fapi.binance.com"

    async def get_perp_symbols(self) -> List[str]:
        """Get perpetual futures symbols"""
        url = f"{self.futures_url}/fapi/v1/exchangeInfo"
        
        try:
            data = await self._make_request(url)
            symbols = [
                s['symbol'] for s in data.get('symbols', [])
                if (s.get('contractType') == 'PERPETUAL' and 
                    s.get('status') == 'TRADING' and 
                    s.get('quoteAsset') == 'USDT')
            ]
            
            validated_symbols = validate_symbol_list(symbols)
            logging.info(f"Fetched {len(validated_symbols)} Binance perp USDT symbols")
            return validated_symbols
            
        except Exception as e:
            logging.error(f"Failed to fetch Binance perp symbols: {e}")
            raise ExchangeAPIException(f"Failed to fetch Binance perp symbols: {e}")

    async def get_spot_symbols(self) -> List[str]:
        """Get spot trading symbols"""
        url = f"{self.base_url}/api/v3/exchangeInfo"
        
        try:
            data = await self._make_request(url)
            symbols = [
                s['symbol'] for s in data.get('symbols', [])
                if (s.get('status') == 'TRADING' and 
                    s.get('quoteAsset') == 'USDT')
            ]
            
            validated_symbols = validate_symbol_list(symbols)
            logging.info(f"Fetched {len(validated_symbols)} Binance spot USDT symbols")
            return validated_symbols
            
        except Exception as e:
            logging.error(f"Failed to fetch Binance spot symbols: {e}")
            raise ExchangeAPIException(f"Failed to fetch Binance spot symbols: {e}")

    async def fetch_ohlcv(self, symbol: str, interval: str, limit: int = None, market: str = "spot") -> pd.DataFrame:
        """Fetch OHLCV data for symbol"""
        limit = limit or config.FETCH_LIMIT
        
        if market == "spot":
            url = f"{self.base_url}/api/v3/klines"
        elif market == "perp":
            url = f"{self.futures_url}/fapi/v1/klines"
        else:
            raise ValueError(f"Unknown market type: {market}")

        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        try:
            data = await self._make_request(url, params)
            
            if not data:
                raise DataValidationException(f"No kline data returned for {symbol}")
            
            df = pd.DataFrame(data, columns=[
                'openTime', 'open', 'high', 'low', 'close', 'volume',
                'closeTime', 'quoteAssetVolume', 'numberOfTrades',
                'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'
            ])
            
            # Convert to proper types
            numeric_cols = ['high', 'low', 'close', 'open', 'volume']
            df[numeric_cols] = df[numeric_cols].astype(float)
            df['openTime'] = pd.to_numeric(df['openTime'])
            
            # Sort by timestamp (newest first)
            df = sort_ohlcv_by_timestamp(df)
            
            if not validate_ohlcv_data(df):
                raise DataValidationException(f"Invalid OHLCV data for {symbol}")
                
            return df
            
        except Exception as e:
            if isinstance(e, (DataValidationException, ExchangeAPIException)):
                raise
            logging.error(f"Failed to fetch OHLCV for {symbol} ({market}) {interval}: {e}")
            raise ExchangeAPIException(f"Failed to fetch OHLCV for {symbol} ({market}) {interval}: {e}")

# Async Bybit Client
class AsyncBybitClient(BaseAsyncExchangeClient):
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: AsyncProxyPool):
        super().__init__(session, proxy_pool)
        self.base_url = "https://api.bybit.com"

    async def get_perp_symbols(self) -> List[str]:
        """Get perpetual futures symbols"""
        url = f"{self.base_url}/v5/market/instruments-info"
        params = {'category': 'linear'}
        
        try:
            data = await self._make_request(url, params)
            symbols = [
                s['symbol'] for s in data.get('result', {}).get('list', [])
                if (s.get('status') == 'Trading' and 
                    s.get('quoteCoin') == 'USDT')
            ]
            
            validated_symbols = validate_symbol_list(symbols)
            logging.info(f"Fetched {len(validated_symbols)} Bybit perp USDT symbols")
            return validated_symbols
            
        except Exception as e:
            logging.error(f"Failed to fetch Bybit perp symbols: {e}")
            raise ExchangeAPIException(f"Failed to fetch Bybit perp symbols: {e}")

    async def get_spot_symbols(self) -> List[str]:
        """Get spot trading symbols"""
        url = f"{self.base_url}/v5/market/instruments-info"
        params = {'category': 'spot'}
        
        try:
            data = await self._make_request(url, params)
            symbols = [
                s['symbol'] for s in data.get('result', {}).get('list', [])
                if (s.get('status') == 'Trading' and 
                    s.get('quoteCoin') == 'USDT')
            ]
            
            validated_symbols = validate_symbol_list(symbols)
            logging.info(f"Fetched {len(validated_symbols)} Bybit spot USDT symbols")
            return validated_symbols
            
        except Exception as e:
            logging.error(f"Failed to fetch Bybit spot symbols: {e}")
            raise ExchangeAPIException(f"Failed to fetch Bybit spot symbols: {e}")

    async def fetch_ohlcv(self, symbol: str, interval: str, limit: int = None, market: str = "spot") -> pd.DataFrame:
        """Fetch OHLCV data for symbol"""
        limit = limit or config.FETCH_LIMIT
        
        url = f"{self.base_url}/v5/market/kline"
        category = 'linear' if market == 'perp' else 'spot'
        
        # Map intervals to Bybit format
        interval_map = {
            "1M": "M",
            "1w": "W", 
            "1d": "D"
        }
        
        bybit_interval = interval_map.get(interval)
        if bybit_interval is None:
            raise ValueError(f"Unsupported interval {interval} for Bybit")

        params = {
            'category': category,
            'symbol': symbol,
            'interval': bybit_interval,
            'limit': limit
        }
        
        try:
            data = await self._make_request(url, params)
            klines = data.get('result', {}).get('list', [])
            
            if not klines:
                raise DataValidationException(f"No kline data returned for {symbol}")
            
            df = pd.DataFrame(klines, columns=[
                'openTime', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])
            
            # Convert to proper types
            numeric_cols = ['high', 'low', 'close', 'open', 'volume']
            df[numeric_cols] = df[numeric_cols].astype(float)
            df['openTime'] = pd.to_numeric(df['openTime'])
            
            # Sort by timestamp (newest first)
            df = sort_ohlcv_by_timestamp(df)
            
            if not validate_ohlcv_data(df):
                raise DataValidationException(f"Invalid OHLCV data for {symbol}")
                
            return df
            
        except Exception as e:
            if isinstance(e, (DataValidationException, ExchangeAPIException)):
                raise
            logging.error(f"Failed to fetch Bybit OHLCV for {symbol} ({market}) {interval}: {e}")
            raise ExchangeAPIException(f"Failed to fetch Bybit OHLCV for {symbol} ({market}) {interval}: {e}")

# Pattern Detection Functions
def deduplicate_symbols(perp_list: List[str], spot_list: List[str]) -> List[str]:
    """Remove duplicates and ignored symbols"""
    spot_only = set(spot_list) - set(perp_list)
    all_symbols = list(perp_list) + list(spot_only)
    return [s for s in all_symbols if s not in IGNORED_SYMBOLS]

def check_equal_price_and_classify(df: pd.DataFrame) -> Tuple[Optional[float], Optional[str]]:
    """Check if last two CLOSED candles have equal close=open AND opposite colors for reversal signal"""
    if not validate_ohlcv_data(df):
        return None, None
    
    # Ensure we have sorted data (newest first)
    df = sort_ohlcv_by_timestamp(df)
    
    # df.iloc[0] = Current candle (ignore)
    # df.iloc[1] = Last closed candle
    # df.iloc[2] = Second to last closed candle
    
    try:
        second_last_closed = df.iloc[2]  # Older candle
        last_closed = df.iloc[1]         # Newer candle
        
        # Extract values
        second_last_open = float(second_last_closed['open'])
        second_last_close = float(second_last_closed['close'])
        last_open = float(last_closed['open'])
        last_close = float(last_closed['close'])
        
        # Determine candle colors
        second_last_is_green = second_last_close > second_last_open
        second_last_is_red = second_last_close < second_last_open
        last_is_green = last_close > last_open
        last_is_red = last_close < last_open
        
        # Check if second_last_close == last_open
        if floats_are_equal(second_last_close, last_open):
            equal_price = second_last_close
            
            # BULLISH: RED candle followed by GREEN candle (reversal pattern)
            if second_last_is_red and last_is_green:
                return equal_price, "bullish"
            
            # BEARISH: GREEN candle followed by RED candle (reversal pattern)  
            elif second_last_is_green and last_is_red:
                return equal_price, "bearish"
            
            # FILTERED OUT: Same color candles (no reversal signal)
        
        return None, None
        
    except (IndexError, KeyError, ValueError) as e:
        logging.warning(f"Error in pattern detection: {e}")
        return None, None

def current_candle_touched_price(df: pd.DataFrame, price: float) -> bool:
    """Check if current candle touched the specified price"""
    if not validate_ohlcv_data(df):
        return False
    
    try:
        # Current candle is df.iloc[0] (newest/live)
        current_candle = df.iloc[0]
        current_low = float(current_candle['low'])
        current_high = float(current_candle['high'])
        
        return current_low <= price <= current_high
        
    except (IndexError, KeyError, ValueError) as e:
        logging.warning(f"Error checking price touch: {e}")
        return False

# Telegram Reporting
def create_beautiful_telegram_report(results: List[Tuple]) -> List[str]:
    """Create clean telegram report with bullet points, minimal parts"""
    
    if not results:
        return ["💥 *Reversal Level Scanner*\n\n❌ No qualifying reversal patterns found at this time."]
    
    # Organize by timeframe first
    timeframes = {}
    for exchange, symbol, market, interval, signal_type in results:
        if interval not in timeframes:
            timeframes[interval] = {
                "Binance": {"bullish": [], "bearish": []}, 
                "Bybit": {"bullish": [], "bearish": []}
            }
        timeframes[interval][exchange][signal_type].append(symbol)
    
    # Get timestamp
    utc_now = datetime.now(timezone.utc)
    utc_plus_3 = timezone(timedelta(hours=3))
    current_time = utc_now.astimezone(utc_plus_3)
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S UTC+3")
    
    # Count totals
    total_signals = len(results)
    binance_count = len([r for r in results if r[0] == "Binance"])
    bybit_count = len([r for r in results if r[0] == "Bybit"])
    
    messages = []
    
    # Summary message
    summary = "💥 *Reversal Level Scanner*\n\n"
    summary += f"✅ Total Signals: {total_signals}\n\n"
    summary += f"*Binance*: {binance_count} | *Bybit*: {bybit_count}\n\n"
    summary += f"🕒 {timestamp}"
    messages.append(summary)
    
    # Process each timeframe
    timeframe_order = ["1M", "1w", "1d"]
    for timeframe in timeframe_order:
        if timeframe not in timeframes:
            continue
            
        tf_data = timeframes[timeframe]
        tf_total = sum(len(tf_data[ex][st]) for ex in tf_data for st in tf_data[ex])
        
        if tf_total == 0:
            continue
        
        # Start building timeframe message
        current_msg = f"📅 *{timeframe} Timeframe* ({tf_total} signals)\n\n"
        
        # Process each exchange
        for exchange in ["Binance", "Bybit"]:
            exchange_bullish = sorted(tf_data[exchange]["bullish"])
            exchange_bearish = sorted(tf_data[exchange]["bearish"])
            
            if not exchange_bullish and not exchange_bearish:
                continue
                
            current_msg += f"*{exchange}*:\n"
            
            # Add bullish symbols
            if exchange_bullish:
                section_start = f"🍏 *Bullish ({len(exchange_bullish)})*\n"
                
                # Check if we need to split
                estimated_length = len(current_msg) + len(section_start) + (len(exchange_bullish) * 15)
                
                if estimated_length > config.TELEGRAM_MESSAGE_MAX_LENGTH:
                    # Start new message for this section
                    messages.append(current_msg.strip())
                    current_msg = f"📅 *{timeframe} - {exchange} Bullish*\n\n"
                
                current_msg += section_start
                for symbol in exchange_bullish:
                    current_msg += f"• {symbol}\n"
                current_msg += "\n"
            
            # Add bearish symbols
            if exchange_bearish:
                section_start = f"🔻 *Bearish ({len(exchange_bearish)})*\n"
                
                # Check if we need to split
                estimated_length = len(current_msg) + len(section_start) + (len(exchange_bearish) * 15)
                
                if estimated_length > config.TELEGRAM_MESSAGE_MAX_LENGTH:
                    # Start new message for this section
                    messages.append(current_msg.strip())
                    current_msg = f"📅 *{timeframe} - {exchange} Bearish*\n\n"
                
                current_msg += section_start
                for symbol in exchange_bearish:
                    current_msg += f"• {symbol}\n"
                current_msg += "\n"
            
            current_msg += "——————————————————\n\n"
        
        # Add the completed timeframe message
        if current_msg.strip():
            messages.append(current_msg.strip())
    
    return messages

# Main Scanning Logic
async def scan_symbol(exchange_name: str, client, symbol: str, perp_set: Set[str], spot_set: Set[str], results: List, lock: asyncio.Lock):
    """Scan single symbol for reversal patterns"""
    market = "perp" if symbol in perp_set else "spot"
    
    for interval in ["1M", "1w", "1d"]:
        try:
            df = await client.fetch_ohlcv(symbol, interval, limit=config.OHLCV_MIN_CANDLES, market=market)
            equal_price, signal_type = check_equal_price_and_classify(df)
            
            if equal_price is not None and signal_type is not None:
                if not current_candle_touched_price(df, equal_price):
                    async with lock:
                        results.append((exchange_name, symbol, market, interval, signal_type))
                        
        except Exception as e:
            logging.warning(f"Failed scanning {exchange_name} {symbol} {market} {interval}: {e}")

async def validate_environment() -> Tuple[str, str, str]:
    """Validate required environment variables"""
    proxy_url = os.getenv("PROXY_LIST_URL")
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN") 
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not proxy_url:
        raise ConfigurationException("PROXY_LIST_URL environment variable not set")
    
    if not telegram_token:
        raise ConfigurationException("TELEGRAM_BOT_TOKEN environment variable not set")
        
    if not telegram_chat_id:
        raise ConfigurationException("TELEGRAM_CHAT_ID environment variable not set")
    
    return proxy_url, telegram_token, telegram_chat_id

# Main Application
async def main():
    """Main application entry point"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('trading_bot.log')
        ]
    )
    
    try:
        # Validate environment
        proxy_url, telegram_token, telegram_chat_id = await validate_environment()
        
        # Initialize proxy pool
        proxy_pool = AsyncProxyPool(
            max_pool_size=config.PROXY_POOL_SIZE,
            proxy_check_interval=config.PROXY_CHECK_INTERVAL,
            max_failures=config.MAX_PROXY_FAILURES
        )
        
        # Populate and start proxy pool
        await proxy_pool.populate_from_url(proxy_url)
        await proxy_pool.start_checker()
        
        # Create aiohttp session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=config.MAX_WORKERS * 2,
            limit_per_host=config.MAX_WORKERS,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'TradingBot/1.0'}
        ) as session:
            
            # Initialize exchange clients
            binance_client = AsyncBinanceClient(session, proxy_pool)
            bybit_client = AsyncBybitClient(session, proxy_pool)
            
            # Fetch symbols concurrently
            logging.info("Fetching symbols from exchanges...")
            
            symbol_tasks = [
                binance_client.get_perp_symbols(),
                binance_client.get_spot_symbols(),
                bybit_client.get_perp_symbols(),
                bybit_client.get_spot_symbols()
            ]
            
            try:
                binance_perp, binance_spot, bybit_perp, bybit_spot = await asyncio.gather(*symbol_tasks)
            except Exception as e:
                logging.error(f"Failed to fetch symbols: {e}")
                return
            
            # Prepare symbol sets
            binance_perp_set = set(binance_perp)
            binance_spot_set = set(binance_spot)
            bybit_perp_set = set(bybit_perp)
            bybit_spot_set = set(bybit_spot)
            
            # Deduplicate symbols
            binance_symbols = deduplicate_symbols(binance_perp, binance_spot)
            bybit_symbols = deduplicate_symbols(bybit_perp, bybit_spot)
            
            logging.info(f"Binance symbols to scan: {len(binance_symbols)}")
            logging.info(f"Bybit symbols to scan: {len(bybit_symbols)}")
            
            # Initialize results storage
            results = []
            lock = asyncio.Lock()
            
            # Create scanning tasks
            scan_tasks = []
            
            # Binance scanning tasks
            for symbol in binance_symbols:
                task = scan_symbol("Binance", binance_client, symbol, binance_perp_set, binance_spot_set, results, lock)
                scan_tasks.append(task)
            
            # Bybit scanning tasks  
            for symbol in bybit_symbols:
                task = scan_symbol("Bybit", bybit_client, symbol, bybit_perp_set, bybit_spot_set, results, lock)
                scan_tasks.append(task)
            
            # Execute scanning with concurrency control
            logging.info(f"Starting scan of {len(scan_tasks)} symbol-timeframe combinations...")
            
            # Use semaphore to control concurrency
            semaphore = asyncio.Semaphore(config.MAX_WORKERS)
            
            async def controlled_scan(task):
                async with semaphore:
                    await task
            
            controlled_tasks = [controlled_scan(task) for task in scan_tasks]
            
            # Execute all scanning tasks
            completed = 0
            for task in asyncio.as_completed(controlled_tasks):
                await task
                completed += 1
                if completed % 100 == 0:
                    logging.info(f"Scanning progress: {completed}/{len(controlled_tasks)} completed")
            
            logging.info(f"Scanning completed. Found {len(results)} signals.")
            
            # Generate and send telegram reports
            messages = create_beautiful_telegram_report(results)
            
            # Send messages via Telegram
            bot = Bot(token=telegram_token)
            for i, msg in enumerate(messages):
                try:
                    await bot.send_message(
                        chat_id=int(telegram_chat_id), 
                        text=msg, 
                        parse_mode='Markdown'
                    )
                    logging.info(f"Telegram report {i+1}/{len(messages)} sent successfully")
                    
                    # Add delay between messages to avoid rate limiting
                    if i < len(messages) - 1:
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    logging.error(f"Failed to send Telegram message {i+1}: {e}")
            
            logging.info("Scanning and reporting completed successfully")
            
    except Exception as e:
        logging.error(f"Application error: {e}")
        raise
    finally:
        # Cleanup
        if 'proxy_pool' in locals():
            proxy_pool.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Application interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        exit(1)
