#!/usr/bin/env python3
"""
Real-Time Crypto Signal Scraper & Dashboard
FIXED: Enhanced signal parser for multiple channel formats
"""

import re
import asyncio
import logging
import json
import time
import os
import sys
import signal
import unicodedata
import html
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Set, List
from dataclasses import dataclass, asdict, field
from enum import Enum

import certifi
import websockets
from websockets.server import serve as ws_serve
from websockets.exceptions import ConnectionClosed
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, PyMongoError
import aiohttp

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

class Config:
    """Application configuration from environment variables."""
    
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    SESSION_STRING: str = os.getenv("SESSION_STRING", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ALERT_CHAT_ID: int = int(os.getenv("ALERT_CHAT_ID", "0"))
    
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME: str = os.getenv("DB_NAME", "crypto_signals")
    
    WS_HOST: str = "0.0.0.0"
    WS_PORT: int = int(os.getenv("PORT", "10000"))
    SHUTDOWN_TIMEOUT: int = int(os.getenv("SHUTDOWN_TIMEOUT", "30"))
    
    CHANNEL_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("CHANNEL_IDS", "").split(",") 
        if x.strip().lstrip('-').isdigit()
    ]
    
    VIP_CHANNEL_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("VIP_CHANNEL_IDS", "").split(",") 
        if x.strip().lstrip('-').isdigit()
    ]
    
    ADMIN_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") 
        if x.strip().lstrip('-').isdigit()
    ]
    
    BACKFILL_ENABLED: bool = os.getenv("BACKFILL_ENABLED", "true").lower() == "true"
    BACKFILL_LIMIT: int = int(os.getenv("BACKFILL_LIMIT", "100"))
    
    RENDER_SERVICE_NAME: str = os.getenv("RENDER_SERVICE_NAME", "crypto-signal-scraper")
    RENDER_EXTERNAL_URL: str = os.getenv("RENDER_EXTERNAL_URL", "")

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-18s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    for lib in ["websockets", "telethon", "pymongo", "aiohttp", "asyncio"]:
        logging.getLogger(lib).setLevel(logging.WARNING)
    
    return logging.getLogger("SignalBot")

logger = setup_logging()

# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class SignalDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"

@dataclass
class Signal:
    id: str
    channel_id: int
    channel_name: str
    message_id: int
    pair: str
    direction: str
    entry: str
    targets: List[str]
    stop_loss: str
    leverage: str
    raw_text: str
    timestamp: datetime
    is_vip: bool = False
    status: str = "ACTIVE"
    hit_targets: List[int] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "channel_id": self.channel_id,
            "channel_name": self.channel_name,
            "message_id": self.message_id,
            "pair": self.pair,
            "direction": self.direction,
            "entry": self.entry,
            "targets": self.targets,
            "stop_loss": self.stop_loss,
            "leverage": self.leverage,
            "raw_text": self.raw_text,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp,
            "is_vip": self.is_vip,
            "status": self.status,
            "hit_targets": self.hit_targets
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Signal':
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if 'hit_targets' not in data:
            data['hit_targets'] = []
        data.pop('_id', None)
        return cls(**data)

# ═══════════════════════════════════════════════════════════════════════════════
# ENHANCED SIGNAL PARSER - HANDLES YOUR CHANNEL FORMATS
# ═══════════════════════════════════════════════════════════════════════════════

class SignalParser:
    """
    Enhanced signal parser that handles multiple channel formats:
    
    Format 1: ⚡BEATUSDT⚡ with "Long position" / "Short position"
    Format 2: BOB/USDT with "Sell / Short Above - 0.0200"
    Format 3: ONDO/𝑼𝑺𝑫𝑻 with fancy Unicode fonts
    """
    
    # Common crypto pairs that might appear without USDT suffix
    KNOWN_COINS = {
        'BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'DOT', 'MATIC', 'SHIB',
        'LTC', 'TRX', 'AVAX', 'LINK', 'ATOM', 'UNI', 'XMR', 'ETC', 'XLM', 'BCH',
        'APT', 'FIL', 'LDO', 'ARB', 'OP', 'INJ', 'SUI', 'SEI', 'TIA', 'JUP',
        'PEPE', 'WIF', 'BONK', 'FLOKI', 'MEME', 'ORDI', 'SATS', 'RATS', '1000SATS',
        'ONDO', 'BOB', 'BEAT', 'RENDER', 'FET', 'AGIX', 'OCEAN', 'TAO', 'WLD',
        'NEAR', 'ICP', 'VET', 'ALGO', 'GRT', 'FTM', 'SAND', 'MANA', 'AXS', 'GALA',
        'ENJ', 'IMX', 'BLUR', 'MAGIC', 'GMT', 'APE', 'DYDX', 'GMX', 'SNX', 'CRV',
        'AAVE', 'MKR', 'COMP', 'SUSHI', 'YFI', 'BAL', 'ZRX', '1INCH', 'LQTY',
        'PENDLE', 'EIGEN', 'ENA', 'ETHFI', 'W', 'STRK', 'PYTH', 'JTO', 'NTRN',
        'DYM', 'ALT', 'MANTA', 'PIXEL', 'PORTAL', 'AEVO', 'BOME', 'SLERF'
    }

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize Unicode text - converts fancy fonts to regular ASCII.
        Examples: 𝑳𝒐𝒏𝒈 -> Long, 𝑼𝑺𝑫𝑻 -> USDT
        """
        # NFKC normalization converts mathematical/fancy Unicode to ASCII
        normalized = unicodedata.normalize('NFKC', text)
        return normalized
    
    @staticmethod
    def remove_emojis(text: str) -> str:
        """Remove emojis from text for cleaner parsing."""
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "\U0001f926-\U0001f937"
            "\U00010000-\U0010ffff"
            "\u2640-\u2642"
            "\u2600-\u2B55"
            "\u200d"
            "\u23cf"
            "\u23e9"
            "\u231a"
            "\ufe0f"
            "\u3030"
            "\u2066"
            "\u2069"
            "⚡🔴🟢🎯🛑⛔️💰📈📉🥀🌹⬆️⬇️🔼🔽"
            "]+", 
            flags=re.UNICODE
        )
        return emoji_pattern.sub(' ', text)
    
    @staticmethod
    def clean_number(value: str) -> str:
        """Clean and extract number from string."""
        if not value:
            return ""
        # Remove everything except digits, dots, and minus
        value = re.sub(r'[^\d.,\-]', '', value)
        value = value.replace(',', '').strip('.-')
        return value

    @classmethod
    def extract_pair(cls, text: str, normalized: str) -> Optional[str]:
        """
        Extract trading pair from text.
        Handles: ⚡BEATUSDT⚡, BOB/USDT, ONDO/USDT(emojis), #BTCUSDT, \$BTC
        """
        text_upper = normalized.upper()
        
        # Pattern 1: Emoji-wrapped pairs like ⚡BEATUSDT⚡
        emoji_wrapped = re.search(r'[⚡💎🔥✨]+\s*([A-Z0-9]{2,15}(?:USDT|BUSD|USD)?)\s*[⚡💎🔥✨]+', text_upper)
        if emoji_wrapped:
            pair = emoji_wrapped.group(1)
            if not pair.endswith(('USDT', 'BUSD', 'USD')):
                pair += 'USDT'
            return pair.replace('BUSD', 'USDT').replace('USD', 'USDT').rstrip('T') + 'T' if not pair.endswith('USDT') else pair
        
        # Pattern 2: Pair at start of line with /USDT format: BOB/USDT, ONDO/USDT
        line_start = re.search(r'^([A-Z0-9]{2,10})\s*/\s*USDT', text_upper, re.MULTILINE)
        if line_start:
            return line_start.group(1) + 'USDT'
        
        # Pattern 3: Hashtag pairs: #BTCUSDT, #BTC
        hashtag = re.search(r'#([A-Z0-9]{2,15}(?:USDT|PERP)?)\b', text_upper)
        if hashtag:
            pair = hashtag.group(1).replace('PERP', '')
            if not pair.endswith('USDT'):
                pair += 'USDT'
            return pair
        
        # Pattern 4: Dollar sign pairs: \$BTC, \$ETH
        dollar = re.search(r'\$([A-Z]{2,10})\b', text_upper)
        if dollar:
            return dollar.group(1) + 'USDT'
        
        # Pattern 5: Standard XXXUSDT format anywhere in text
        standard = re.search(r'\b([A-Z0-9]{2,10}USDT)\b', text_upper)
        if standard:
            return standard.group(1)
        
        # Pattern 6: Known coin names followed by direction keywords
        for coin in cls.KNOWN_COINS:
            pattern = rf'\b{coin}\b.*(?:LONG|SHORT|BUY|SELL)'
            if re.search(pattern, text_upper, re.IGNORECASE):
                return coin + 'USDT'
        
        # Pattern 7: Any potential pair format XXX/USDT or XXX/USD
        slash_pair = re.search(r'\b([A-Z0-9]{2,10})\s*/\s*(?:USDT|USD|BUSD)', text_upper)
        if slash_pair:
            return slash_pair.group(1) + 'USDT'
        
        # Pattern 8: Pair with emoji suffix like ONDO/USDT(🥀🌹🥀)
        emoji_suffix = re.search(r'([A-Z0-9]{2,10})\s*/\s*USDT\s*\(', text_upper)
        if emoji_suffix:
            return emoji_suffix.group(1) + 'USDT'
        
        return None
    
    @classmethod
    def extract_direction(cls, normalized: str) -> SignalDirection:
        """
        Extract trade direction.
        Handles: "Long position", "Short position", "Sell / Short", "Buy Long"
        """
        text_upper = normalized.upper()
        
        # Long patterns
        long_patterns = [
            r'\bLONG\s*POSITION\b',
            r'\bLONG\b',
            r'\bBUY\s*/?\s*LONG\b',
            r'\bBUY\s+LONG\b',
            r'\bBUY\b(?!\s*ZONE)',  # BUY but not "BUY ZONE"
            r'\bBULLISH\b',
            r'🟢',
            r'📈',
            r'⬆️',
            r'🔼',
        ]
        
        # Short patterns
        short_patterns = [
            r'\bSHORT\s*POSITION\b',
            r'\bSHORT\b',
            r'\bSELL\s*/?\s*SHORT\b',
            r'\bSELL\s+SHORT\b',
            r'\bSELL\b(?!\s*ZONE)',  # SELL but not "SELL ZONE"
            r'\bBEARISH\b',
            r'🔴',
            r'📉',
            r'⬇️',
            r'🔽',
        ]
        
        # Check for Long
        for pattern in long_patterns:
            if re.search(pattern, text_upper) or re.search(pattern, normalized):
                # Make sure it's not negated by "Short" appearing more prominently
                long_pos = re.search(pattern, text_upper)
                short_match = re.search(r'\bSHORT\b', text_upper)
                if long_pos:
                    if not short_match or long_pos.start() < short_match.start():
                        return SignalDirection.LONG
        
        # Check for Short
        for pattern in short_patterns:
            if re.search(pattern, text_upper) or re.search(pattern, normalized):
                return SignalDirection.SHORT
        
        return SignalDirection.UNKNOWN
    
    @classmethod
    def extract_entry(cls, normalized: str) -> str:
        """
        Extract entry price.
        Handles: "Entry market price", "Above - 0.0200", "Above-0.4603", "Entry: 50000"
        """
        text_upper = normalized.upper()
        
        # Check for market entry
        market_patterns = [
            r'ENTRY\s*(?::|-)?\s*MARKET',
            r'MARKET\s*PRICE',
            r'ENTRY\s*(?:AT\s*)?MARKET',
            r'CMP',
            r'CURRENT\s*(?:MARKET\s*)?PRICE',
            r'MARKET\s*ENTRY',
        ]
        
        for pattern in market_patterns:
            if re.search(pattern, text_upper):
                return "Market"
        
        # Entry with "Above" or "Below" - common format
        above_below = re.search(
            r'(?:ABOVE|BELOW|AROUND|NEAR|AT)\s*[-:=]?\s*([\d.,]+)', 
            text_upper
        )
        if above_below:
            return cls.clean_number(above_below.group(1))
        
        # Standard entry patterns
        entry_patterns = [
            r'ENTRY\s*(?:PRICE|ZONE|POINT)?[\s:=-]*([\d.,]+(?:\s*[-–]\s*[\d.,]+)?)',
            r'ENTER\s*(?:AT|AROUND|NEAR)?[\s:=-]*([\d.,]+)',
            r'BUY\s*(?:AT|AROUND|NEAR|ZONE)?[\s:=-]*([\d.,]+)',
            r'SELL\s*(?:AT|AROUND|NEAR|ZONE)?[\s:=-]*([\d.,]+)',
            r'EP[\s:=-]*([\d.,]+)',
            r'PRICE[\s:=-]*([\d.,]+)',
        ]
        
        for pattern in entry_patterns:
            match = re.search(pattern, text_upper)
            if match:
                entry = cls.clean_number(match.group(1))
                if entry and float(entry) > 0:
                    return entry
        
        return "Market"
    
    @classmethod
    def extract_targets(cls, normalized: str) -> List[str]:
        """
        Extract take profit targets.
        Handles: "TP-0.002", "TAKE PROFIT: 300%", "Targets 35%/45%/55%", "TP1: 100, TP2: 200"
        """
        targets = []
        seen = set()
        text_upper = normalized.upper()
        
        # Pattern 1: Multiple percentage targets like "35%/45%/55%"
        pct_targets = re.search(r'TARGETS?\s*[\n:]?\s*([\d.]+%(?:\s*/\s*[\d.]+%)+)', text_upper)
        if pct_targets:
            percentages = re.findall(r'([\d.]+)%', pct_targets.group(1))
            for pct in percentages:
                if pct not in seen:
                    targets.append(f"{pct}%")
                    seen.add(pct)
        
        # Pattern 2: Single percentage target like "TAKE PROFIT: 300%"
        single_pct = re.search(r'(?:TAKE\s*PROFIT|TP)\s*[:\-=]?\s*([\d.]+)\s*%', text_upper)
        if single_pct and single_pct.group(1) not in seen:
            targets.append(f"{single_pct.group(1)}%")
            seen.add(single_pct.group(1))
        
        # Pattern 3: TP with price (no space): TP-0.002, TP:100
        tp_no_space = re.findall(r'TP\s*[-:=]?\s*([\d.,]+)(?!\s*%)', text_upper)
        for tp in tp_no_space:
            cleaned = cls.clean_number(tp)
            if cleaned and cleaned not in seen:
                targets.append(cleaned)
                seen.add(cleaned)
        
        # Pattern 4: Numbered targets: TP1: 100, TP2: 200
        numbered_tp = re.findall(r'TP\s*\d*\s*[:\-=]\s*([\d.,]+)', text_upper)
        for tp in numbered_tp:
            cleaned = cls.clean_number(tp)
            if cleaned and cleaned not in seen:
                targets.append(cleaned)
                seen.add(cleaned)
        
        # Pattern 5: TARGET/TAKE PROFIT with price
        target_patterns = [
            r'TARGET\s*\d*\s*[:\-=]\s*([\d.,]+)',
            r'TAKE\s*PROFIT\s*\d*\s*[:\-=]\s*([\d.,]+)',
            r'🎯\s*([\d.,]+)',
        ]
        
        for pattern in target_patterns:
            matches = re.findall(pattern, text_upper)
            for match in matches:
                cleaned = cls.clean_number(match)
                if cleaned and cleaned not in seen:
                    targets.append(cleaned)
                    seen.add(cleaned)
        
        return targets[:6]  # Max 6 targets
    
    @classmethod
    def extract_stop_loss(cls, normalized: str) -> str:
        """
        Extract stop loss price.
        Handles: "SL- 0.022", "STOP LOSS: 0.4350", "SL:100"
        """
        text_upper = normalized.upper()
        
        patterns = [
            r'(?:STOP\s*LOSS|STOPLOSS|SL)\s*[:\-=]\s*([\d.,]+)',
            r'SL\s*[:\-=]?\s*([\d.,]+)',
            r'STOP\s*[:\-=]\s*([\d.,]+)',
            r'🛑\s*([\d.,]+)',
            r'⛔\s*([\d.,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_upper)
            if match:
                return cls.clean_number(match.group(1))
        
        return ""
    
    @classmethod
    def extract_leverage(cls, normalized: str) -> str:
        """
        Extract leverage.
        Handles: "LEVERAGE - 10X (cross)", "10X", "10x Cross"
        """
        text_upper = normalized.upper()
        
        patterns = [
            r'LEVERAGE\s*[-:=]?\s*(\d+)\s*X',
            r'LEV\s*[-:=]?\s*(\d+)\s*X',
            r'(\d+)\s*X\s*(?:CROSS|ISOLATED|LEVERAGE)?',
            r'(\d+)X',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_upper)
            if match:
                lev = match.group(1)
                if lev.isdigit() and 1 <= int(lev) <= 125:
                    return f"{lev}x"
        
        return ""

    @classmethod
    def parse(cls, text: str, channel_id: int, channel_name: str, 
              message_id: int, is_vip: bool = False) -> Optional[Signal]:
        """
        Parse message text into a Signal object.
        """
        if not text or len(text) < 10:
            return None
        
        # Step 1: Normalize Unicode (converts fancy fonts to ASCII)
        normalized = cls.normalize_text(text)
        
        # Step 2: Log for debugging (first 100 chars)
        logger.debug(f"Parsing message from {channel_name}: {normalized[:100]}...")
        
        # Step 3: Extract pair
        pair = cls.extract_pair(text, normalized)
        if not pair:
            logger.debug(f"No pair found in message from {channel_name}")
            if not is_vip:
                return None
            pair = "UNKNOWN"
        
        # Step 4: Extract direction
        direction = cls.extract_direction(normalized)
        if direction == SignalDirection.UNKNOWN and not is_vip:
            logger.debug(f"No direction found in message from {channel_name}")
            return None
        
        # Step 5: Extract other fields
        entry = cls.extract_entry(normalized)
        targets = cls.extract_targets(normalized)
        stop_loss = cls.extract_stop_loss(normalized)
        leverage = cls.extract_leverage(normalized)
        
        # Step 6: Validation
        # For VIP channels, we're more lenient
        if not is_vip:
            # Must have either specific entry or targets
            if entry == "Market" and not targets:
                logger.debug(f"No entry or targets found in message from {channel_name}")
                return None
        
        # Step 7: Create signal
        signal = Signal(
            id=f"{channel_id}:{message_id}",
            channel_id=channel_id,
            channel_name=channel_name,
            message_id=message_id,
            pair=pair,
            direction=direction.value,
            entry=entry,
            targets=targets,
            stop_loss=stop_loss,
            leverage=leverage,
            raw_text=text[:2000],
            timestamp=datetime.now(timezone.utc),
            is_vip=is_vip,
            status="ACTIVE"
        )
        
        logger.info(f"✅ Parsed: {pair} {direction.value} | Entry: {entry} | Targets: {len(targets)} | SL: {stop_loss or 'N/A'}")
        
        return signal

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None
        self.signals = None
        self.deleted = None
        self._connected = False
        self._lock = asyncio.Lock()
    
    async def connect(self) -> bool:
        async with self._lock:
            if self._connected:
                return True
            
            for attempt in range(3):
                try:
                    logger.info(f"🔌 MongoDB: Connecting (attempt {attempt + 1}/3)...")
                    
                    self.client = AsyncIOMotorClient(
                        self.uri,
                        tlsCAFile=certifi.where(),
                        serverSelectionTimeoutMS=10000,
                        connectTimeoutMS=10000,
                        retryWrites=True
                    )
                    
                    await self.client.admin.command('ping')
                    
                    self.db = self.client[self.db_name]
                    self.signals = self.db['signals']
                    self.deleted = self.db['deleted_signals']
                    
                    await self.signals.create_index("id", unique=True)
                    await self.signals.create_index([("timestamp", -1)])
                    await self.signals.create_index("pair")
                    await self.signals.create_index("status")
                    await self.deleted.create_index("id", unique=True)
                    
                    self._connected = True
                    logger.info(f"✅ MongoDB: Connected to '{self.db_name}'")
                    return True
                    
                except Exception as e:
                    logger.error(f"❌ MongoDB: Connection failed - {e}")
                    if attempt < 2:
                        await asyncio.sleep(5)
            
            return False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    async def is_deleted(self, signal_id: str) -> bool:
        if not self._connected:
            return False
        try:
            doc = await self.deleted.find_one({"id": signal_id})
            return doc is not None
        except PyMongoError:
            return False
    
    async def mark_deleted(self, signal_id: str) -> bool:
        if not self._connected:
            return False
        try:
            await self.deleted.update_one(
                {"id": signal_id},
                {"\$set": {"id": signal_id, "deleted_at": datetime.now(timezone.utc)}},
                upsert=True
            )
            await self.signals.delete_one({"id": signal_id})
            return True
        except PyMongoError as e:
            logger.error(f"DB Error (mark_deleted): {e}")
            return False
    
    async def upsert_signal(self, signal: Signal) -> tuple[bool, bool]:
        if not self._connected:
            return False, False
        
        try:
            if await self.is_deleted(signal.id):
                return False, False
            
            existing = await self.signals.find_one({"id": signal.id})
            is_new = existing is None
            
            signal_data = signal.to_dict()
            
            await self.signals.update_one(
                {"id": signal.id},
                {"\$set": signal_data},
                upsert=True
            )
            
            return True, is_new
            
        except PyMongoError as e:
            logger.error(f"DB Error (upsert): {e}")
            return False, False
    
    async def get_recent_signals(self, limit: int = 50, status: str = None) -> List[Dict]:
        if not self._connected:
            return []
        try:
            query = {}
            if status:
                query["status"] = status
            cursor = self.signals.find(query).sort("timestamp", -1).limit(limit)
            results = await cursor.to_list(length=limit)
            for r in results:
                if '_id' in r:
                    r['_id'] = str(r['_id'])
            return results
        except PyMongoError as e:
            logger.error(f"DB Error (get_recent): {e}")
            return []
    
    async def get_stats(self) -> Dict[str, Any]:
        if not self._connected:
            return {"total": 0, "active": 0, "hit_tp": 0, "hit_sl": 0, "win_rate": 0}
        try:
            total = await self.signals.count_documents({})
            active = await self.signals.count_documents({"status": "ACTIVE"})
            hit_tp = await self.signals.count_documents({"status": "HIT_TP"})
            hit_sl = await self.signals.count_documents({"status": "HIT_SL"})
            
            total_closed = hit_tp + hit_sl
            win_rate = round(hit_tp / total_closed * 100, 1) if total_closed > 0 else 0
            
            return {
                "total": total,
                "active": active,
                "hit_tp": hit_tp,
                "hit_sl": hit_sl,
                "win_rate": win_rate
            }
        except PyMongoError as e:
            logger.error(f"DB Error (get_stats): {e}")
            return {"total": 0, "active": 0, "hit_tp": 0, "hit_sl": 0, "win_rate": 0}
    
    async def reset_signals(self) -> int:
        if not self._connected:
            return 0
        try:
            result = await self.signals.delete_many({})
            await self.deleted.delete_many({})
            return result.deleted_count
        except PyMongoError:
            return 0
    
    async def close(self):
        if self.client:
            self.client.close()
            self._connected = False

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET BROADCASTER
# ═══════════════════════════════════════════════════════════════════════════════

class WebSocketBroadcaster:
    def __init__(self):
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.server = None
        self._lock = asyncio.Lock()
    
    async def register(self, websocket):
        async with self._lock:
            self.clients.add(websocket)
        logger.info(f"📱 WS: Client connected | Total: {len(self.clients)}")
    
    async def unregister(self, websocket):
        async with self._lock:
            self.clients.discard(websocket)
        logger.info(f"📴 WS: Client disconnected | Total: {len(self.clients)}")
    
    async def broadcast(self, message: Dict[str, Any]):
        if not self.clients:
            return
        
        message_str = json.dumps(message, default=str)
        dead_clients = set()
        
        async with self._lock:
            clients_copy = self.clients.copy()
        
        for client in clients_copy:
            try:
                await asyncio.wait_for(client.send(message_str), timeout=5.0)
            except (ConnectionClosed, asyncio.TimeoutError):
                dead_clients.add(client)
            except Exception:
                dead_clients.add(client)
        
        if dead_clients:
            async with self._lock:
                self.clients -= dead_clients
    
    async def broadcast_signal(self, signal: Signal, event_type: str = "new_signal"):
        await self.broadcast({
            "type": event_type,
            "data": signal.to_dict(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def broadcast_delete(self, signal_id: str):
        await self.broadcast({
            "type": "delete_signal",
            "signal_id": signal_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def send_to_client(self, websocket, message: Dict):
        try:
            await websocket.send(json.dumps(message, default=str))
        except Exception as e:
            logger.error(f"WS Send error: {e}")
    
    async def close_all(self, reason: str = "Server shutting down"):
        async with self._lock:
            for client in self.clients.copy():
                try:
                    await client.close(1001, reason)
                except:
                    pass
            self.clients.clear()

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM ALERT BOT
# ═══════════════════════════════════════════════════════════════════════════════

class TelegramAlertBot:
    def __init__(self, bot_token: str, chat_id: int):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._enabled = bool(bot_token and chat_id)
        
        if self._enabled:
            logger.info(f"✅ Alert Bot enabled, sending to chat: {chat_id}")
        else:
            logger.warning("⚠️ Alert Bot disabled (missing BOT_TOKEN or ALERT_CHAT_ID)")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    def format_signal(self, signal: Signal) -> str:
        emoji = "🟢" if signal.direction == "LONG" else "🔴" if signal.direction == "SHORT" else "⚪"
        vip = " ⭐ VIP" if signal.is_vip else ""
        
        # Format targets
        if signal.targets:
            targets = "\n".join([f"  • TP{i+1}: <code>{t}</code>" for i, t in enumerate(signal.targets)])
        else:
            targets = "  • Market targets"
        
        safe_channel = html.escape(signal.channel_name)
        
        return f"""{emoji} <b>NEW SIGNAL</b>{vip}

<b>Pair:</b> <code>{signal.pair}</code>
<b>Direction:</b> {signal.direction}
<b>Entry:</b> <code>{signal.entry}</code>
<b>Leverage:</b> {signal.leverage or "N/A"}

<b>Targets:</b>
{targets}

<b>Stop Loss:</b> <code>{signal.stop_loss or "N/A"}</code>

📡 <i>{safe_channel}</i>
🕐 <i>{signal.timestamp.strftime("%Y-%m-%d %H:%M UTC")}</i>

<a href="https://www.tradingview.com/chart/?symbol=BINANCE:{signal.pair}.P">📊 TradingView</a> | <a href="https://www.binance.com/en/futures/{signal.pair}">📈 Binance</a>"""
    
    async def send_alert(self, signal: Signal) -> bool:
        if not self._enabled:
            logger.warning("Alert Bot not enabled, skipping alert")
            return False
        
        try:
            session = await self._get_session()
            message = self.format_signal(signal)
            
            logger.info(f"📨 Sending alert for {signal.pair} {signal.direction}...")
            
            async with session.post(
                f"{self.api_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
            ) as resp:
                if resp.status == 200:
                    logger.info(f"✅ Alert sent: {signal.pair} {signal.direction}")
                    return True
                else:
                    error = await resp.text()
                    logger.error(f"❌ Alert failed [{resp.status}]: {error[:200]}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Alert error: {e}")
            return False

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class CryptoSignalScraper:
    def __init__(self):
        self.config = Config()
        self.db = DatabaseManager(self.config.MONGO_URI, self.config.DB_NAME)
        self.broadcaster = WebSocketBroadcaster()
        self.alert_bot = TelegramAlertBot(self.config.BOT_TOKEN, self.config.ALERT_CHAT_ID)
        self.telegram_client: Optional[TelegramClient] = None
        self.channel_cache: Dict[int, str] = {}
        
        self._running = False
        self._start_time = time.time()
        self._shutdown_event = asyncio.Event()
        self._deploy_id = os.getenv("RENDER_GIT_COMMIT", "local")[:8]
        self._signals_processed = 0
        self._signals_parsed = 0
    
    async def health_check(self, path: str, request_headers):
        upgrade_header = request_headers.get("Upgrade", "").lower()
        connection_header = request_headers.get("Connection", "").lower()
        
        if "websocket" in upgrade_header or "upgrade" in connection_header:
            return None
        
        if path in ('/', '/health', '/healthz', '/ping', '/ready'):
            uptime = int(time.time() - self._start_time)
            
            health_data = {
                "status": "healthy",
                "service": self.config.RENDER_SERVICE_NAME,
                "deploy_id": self._deploy_id,
                "uptime_seconds": uptime,
                "uptime_human": f"{uptime // 3600}h {(uptime % 3600) // 60}m",
                "stats": {
                    "messages_processed": self._signals_processed,
                    "signals_parsed": self._signals_parsed,
                },
                "connections": {
                    "websocket_clients": len(self.broadcaster.clients),
                    "database": "connected" if self.db.is_connected else "disconnected",
                    "telegram": "connected" if (self.telegram_client and self.telegram_client.is_connected()) else "connecting"
                },
                "channels_monitored": len(self.config.CHANNEL_IDS) + len(self.config.VIP_CHANNEL_IDS),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            body = json.dumps(health_data, indent=2).encode()
            
            return (
                200,
                [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(body))),
                    ("Cache-Control", "no-cache"),
                    ("Access-Control-Allow-Origin", "*")
                ],
                body
            )
        
        return None
    
    async def websocket_handler(self, websocket):
        await self.broadcaster.register(websocket)
        
        try:
            signals = await self.db.get_recent_signals(50)
            await self.broadcaster.send_to_client(websocket, {
                "type": "initial_data",
                "data": signals,
                "server_time": datetime.now(timezone.utc).isoformat(),
                "deploy_id": self._deploy_id
            })
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self._handle_ws_message(websocket, data)
                except json.JSONDecodeError:
                    pass
                    
        except ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"WS Handler error: {e}")
        finally:
            await self.broadcaster.unregister(websocket)
    
    async def _handle_ws_message(self, websocket, data: Dict):
        msg_type = data.get("type", "")
        
        if msg_type == "ping":
            await self.broadcaster.send_to_client(websocket, {
                "type": "pong",
                "server_time": time.time()
            })
        elif msg_type == "get_stats":
            stats = await self.db.get_stats()
            await self.broadcaster.send_to_client(websocket, {
                "type": "stats",
                "data": stats
            })
        elif msg_type == "get_signals":
            limit = min(data.get("limit", 50), 200)
            signals = await self.db.get_recent_signals(limit)
            await self.broadcaster.send_to_client(websocket, {
                "type": "signals",
                "data": signals
            })
    
    async def on_new_message(self, event):
        """Handle new messages from monitored channels."""
        try:
            chat_id = event.chat_id
            
            all_channels = set(self.config.CHANNEL_IDS) | set(self.config.VIP_CHANNEL_IDS)
            if chat_id not in all_channels:
                return
            
            message = event.message
            if not message or not message.text:
                return
            
            self._signals_processed += 1
            
            channel_name = self.channel_cache.get(chat_id, f"Channel {chat_id}")
            is_vip = chat_id in self.config.VIP_CHANNEL_IDS
            
            # Log incoming message for debugging
            logger.info(f"📩 New message from {channel_name} (VIP: {is_vip})")
            logger.debug(f"Message text: {message.text[:200]}...")
            
            # Parse signal
            signal = SignalParser.parse(
                text=message.text,
                channel_id=chat_id,
                channel_name=channel_name,
                message_id=message.id,
                is_vip=is_vip
            )
            
            if not signal:
                logger.info(f"⏭️ Message not parsed as signal from {channel_name}")
                return
            
            self._signals_parsed += 1
            
            # Save to database
            success, is_new = await self.db.upsert_signal(signal)
            
            if success:
                log_prefix = "🆕 NEW" if is_new else "🔄 UPD"
                logger.info(f"{log_prefix} | {signal.pair:12} | {signal.direction:5} | Entry: {signal.entry} | {channel_name[:25]}")
                
                # Broadcast to WebSocket clients
                event_type = "new_signal" if is_new else "update_signal"
                await self.broadcaster.broadcast_signal(signal, event_type)
                logger.info(f"📡 Broadcasted to {len(self.broadcaster.clients)} WS clients")
                
                # Send Telegram alert for NEW signals only
                if is_new:
                    alert_sent = await self.alert_bot.send_alert(signal)
                    if not alert_sent:
                        logger.warning(f"⚠️ Failed to send Telegram alert for {signal.pair}")
                    
        except Exception as e:
            logger.error(f"❌ Message handler error: {e}", exc_info=True)
    
    async def on_message_edited(self, event):
        await self.on_new_message(event)
    
    async def on_message_deleted(self, event):
        try:
            chat_id = event.chat_id
            for msg_id in event.deleted_ids:
                signal_id = f"{chat_id}:{msg_id}"
                if await self.db.mark_deleted(signal_id):
                    await self.broadcaster.broadcast_delete(signal_id)
                    logger.info(f"🗑️ DEL | {signal_id}")
        except Exception as e:
            logger.error(f"Delete handler error: {e}")
    
    async def handle_admin_command(self, event):
        sender_id = event.sender_id
        
        if sender_id not in self.config.ADMIN_IDS:
            return
        
        text = event.message.text.strip().lower()
        
        try:
            if text == "/status":
                stats = await self.db.get_stats()
                uptime = int(time.time() - self._start_time)
                h, m, s = uptime // 3600, (uptime % 3600) // 60, uptime % 60
                
                status_msg = f"""📊 <b>System Status</b>

<b>Server:</b>
  • Uptime: {h}h {m}m {s}s
  • Deploy: <code>{self._deploy_id}</code>
  • WS Clients: {len(self.broadcaster.clients)}

<b>Processing:</b>
  • Messages Processed: {self._signals_processed}
  • Signals Parsed: {self._signals_parsed}
  • Parse Rate: {(self._signals_parsed/self._signals_processed*100) if self._signals_processed > 0 else 0:.1f}%

<b>Connections:</b>
  • Database: {'✅' if self.db.is_connected else '❌'}
  • Telegram: {'✅' if self.telegram_client and self.telegram_client.is_connected() else '❌'}
  • Alert Bot: {'✅' if self.alert_bot._enabled else '❌'}

<b>Signals:</b>
  • Total: {stats.get('total', 0)}
  • Active: {stats.get('active', 0)}
  • Win Rate: {stats.get('win_rate', 0)}%"""
                
                await event.respond(status_msg, parse_mode='html')
            
            elif text == "/channels":
                all_monitored = set(self.config.CHANNEL_IDS) | set(self.config.VIP_CHANNEL_IDS)
                
                if all_monitored:
                    lines = []
                    for cid in all_monitored:
                        name = self.channel_cache.get(cid, f"Unknown ({cid})")
                        safe_name = html.escape(name)
                        icon = "⭐" if cid in self.config.VIP_CHANNEL_IDS else "📡"
                        lines.append(f"  {icon} {safe_name}")
                    channel_list = "\n".join(lines)
                else:
                    channel_list = "  No channels configured"
                
                await event.respond(
                    f"📡 <b>Monitored Channels ({len(all_monitored)}):</b>\n\n{channel_list}", 
                    parse_mode='html'
                )
            
            elif text == "/reset":
                count = await self.db.reset_signals()
                self._signals_processed = 0
                self._signals_parsed = 0
                await event.respond(f"🗑️ Deleted {count} signals and reset counters.")
            
            elif text == "/test":
                # Send a test alert
                test_signal = Signal(
                    id="test:0",
                    channel_id=0,
                    channel_name="Test Channel",
                    message_id=0,
                    pair="BTCUSDT",
                    direction="LONG",
                    entry="50000",
                    targets=["51000", "52000", "53000"],
                    stop_loss="49000",
                    leverage="10x",
                    raw_text="Test signal",
                    timestamp=datetime.now(timezone.utc),
                    is_vip=False,
                    status="ACTIVE"
                )
                alert_sent = await self.alert_bot.send_alert(test_signal)
                await self.broadcaster.broadcast_signal(test_signal, "new_signal")
                await event.respond(f"✅ Test signal sent!\n• Alert Bot: {'✅' if alert_sent else '❌'}\n• WS Clients: {len(self.broadcaster.clients)}")
            
            elif text.startswith("/broadcast "):
                msg = text[11:].strip()
                if msg:
                    await self.broadcaster.broadcast({
                        "type": "announcement",
                        "message": msg,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
                    await event.respond(f"📢 Sent to {len(self.broadcaster.clients)} clients")
            
            elif text == "/help":
                await event.respond(
                    "🤖 <b>Commands:</b>\n\n"
                    "/status - System status\n"
                    "/channels - List channels\n"
                    "/reset - Clear signals\n"
                    "/test - Send test signal\n"
                    "/broadcast - Send to WS clients\n"
                    "/help - This message",
                    parse_mode='html'
                )
                
        except Exception as e:
            logger.error(f"Admin command error: {e}")
            await event.respond(f"❌ Error: {str(e)[:100]}", parse_mode=None)
    
    async def discover_channels(self):
        logger.info("🔍 Discovering channels...")
        
        try:
            async for dialog in self.telegram_client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, (Channel, Chat)):
                    chat_id = dialog.id
                    chat_name = dialog.name or f"Unknown ({chat_id})"
                    self.channel_cache[chat_id] = chat_name
                    
                    all_channels = set(self.config.CHANNEL_IDS) | set(self.config.VIP_CHANNEL_IDS)
                    if chat_id in all_channels:
                        vip = " ⭐" if chat_id in self.config.VIP_CHANNEL_IDS else ""
                        logger.info(f"  📡 {chat_name}{vip} (ID: {chat_id})")
            
            logger.info(f"✅ Cached {len(self.channel_cache)} channels")
        except Exception as e:
            logger.error(f"Channel discovery error: {e}")
    
    async def backfill_signals(self):
        if not self.config.BACKFILL_ENABLED:
            logger.info("⏭️ Backfill disabled")
            return
        
        logger.info(f"📥 Starting backfill (limit: {self.config.BACKFILL_LIMIT})...")
        
        all_channels = set(self.config.CHANNEL_IDS) | set(self.config.VIP_CHANNEL_IDS)
        total = 0
        
        for channel_id in all_channels:
            try:
                channel_name = self.channel_cache.get(channel_id, f"Channel {channel_id}")
                is_vip = channel_id in self.config.VIP_CHANNEL_IDS
                count = 0
                
                async for message in self.telegram_client.iter_messages(
                    channel_id, 
                    limit=self.config.BACKFILL_LIMIT
                ):
                    if not message or not message.text:
                        continue
                    
                    try:
                        signal = SignalParser.parse(
                            text=message.text,
                            channel_id=channel_id,
                            channel_name=channel_name,
                            message_id=message.id,
                            is_vip=is_vip
                        )
                        
                        if signal:
                            signal.timestamp = message.date.replace(tzinfo=timezone.utc)
                            success, is_new = await self.db.upsert_signal(signal)
                            if success and is_new:
                                count += 1
                    except Exception as e:
                        logger.debug(f"Parse error: {e}")
                        continue
                
                if count > 0:
                    logger.info(f"  📥 {channel_name[:40]}: {count} signals")
                    total += count
                    
            except Exception as e:
                logger.error(f"Backfill error ({channel_id}): {e}")
                continue
        
        logger.info(f"✅ Backfill complete: {total} signals")
    
    async def setup_telegram(self) -> bool:
        logger.info("📱 Connecting to Telegram...")
        
        if not self.config.SESSION_STRING:
            logger.error("❌ SESSION_STRING not configured!")
            return False
        
        try:
            self.telegram_client = TelegramClient(
                StringSession(self.config.SESSION_STRING),
                self.config.API_ID,
                self.config.API_HASH,
                connection_retries=5,
                auto_reconnect=True
            )
            
            await self.telegram_client.connect()
            
            if not await self.telegram_client.is_user_authorized():
                logger.error("❌ Telegram session not authorized!")
                return False
            
            me = await self.telegram_client.get_me()
            logger.info(f"✅ Telegram: {me.first_name} (@{me.username or 'N/A'})")
            
            await self.discover_channels()
            
            all_channels = list(set(self.config.CHANNEL_IDS) | set(self.config.VIP_CHANNEL_IDS))
            
            if all_channels:
                self.telegram_client.add_event_handler(
                    self.on_new_message,
                    events.NewMessage(chats=all_channels)
                )
                self.telegram_client.add_event_handler(
                    self.on_message_edited,
                    events.MessageEdited(chats=all_channels)
                )
                self.telegram_client.add_event_handler(
                    self.on_message_deleted,
                    events.MessageDeleted(chats=all_channels)
                )
                logger.info(f"✅ Monitoring {len(all_channels)} channels")
            else:
                logger.warning("⚠️ No channels configured to monitor!")
            
            if self.config.ADMIN_IDS:
                self.telegram_client.add_event_handler(
                    self.handle_admin_command,
                    events.NewMessage(pattern=r'^/', from_users=self.config.ADMIN_IDS)
                )
                logger.info(f"✅ Admin commands enabled for {len(self.config.ADMIN_IDS)} users")
            
            await self.backfill_signals()
            return True
            
        except Exception as e:
            logger.error(f"❌ Telegram setup error: {e}")
            return False
    
    def handle_shutdown_signal(self, sig):
        logger.info(f"🛑 Received {signal.Signals(sig).name}, shutting down...")
        self._shutdown_event.set()
    
    async def shutdown(self):
        logger.info("🛑 Shutting down...")
        self._running = False
        
        await self.broadcaster.broadcast({
            "type": "server_shutdown",
            "message": "Server restarting"
        })
        
        await self.broadcaster.close_all()
        
        if self.broadcaster.server:
            self.broadcaster.server.close()
            await self.broadcaster.server.wait_closed()
        
        if self.telegram_client:
            await self.telegram_client.disconnect()
        
        await self.alert_bot.close()
        await self.db.close()
        
        logger.info("👋 Shutdown complete")
    
    async def run(self):
        logger.info("=" * 60)
        logger.info("  🚀 Crypto Signal Scraper - Enhanced Parser")
        logger.info("=" * 60)
        
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: self.handle_shutdown_signal(s))
        
        logger.info(f"🌐 Starting WebSocket on port {self.config.WS_PORT}...")
        
        try:
            self.broadcaster.server = await ws_serve(
                self.websocket_handler,
                self.config.WS_HOST,
                self.config.WS_PORT,
                process_request=self.health_check,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
                max_size=2**20,
                compression=None
            )
            logger.info(f"✅ WebSocket ready on port {self.config.WS_PORT}")
        except Exception as e:
            logger.error(f"❌ WebSocket failed: {e}")
            return
        
        asyncio.create_task(self.db.connect())
        
        async def delayed_telegram():
            await asyncio.sleep(1)
            await self.setup_telegram()
        
        asyncio.create_task(delayed_telegram())
        
        self._running = True
        
        logger.info("=" * 60)
        logger.info("  ✅ Server Ready - Monitoring for signals")
        logger.info("=" * 60)
        
        heartbeat_interval = 45
        last_heartbeat = time.time()
        
        try:
            while self._running:
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=heartbeat_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass
                
                now = time.time()
                if now - last_heartbeat >= heartbeat_interval:
                    stats = await self.db.get_stats()
                    logger.info(
                        f"💓 Heartbeat | "
                        f"WS: {len(self.broadcaster.clients)} | "
                        f"DB: {'✅' if self.db.is_connected else '❌'} | "
                        f"TG: {'✅' if self.telegram_client and self.telegram_client.is_connected() else '❌'} | "
                        f"Processed: {self._signals_processed} | "
                        f"Parsed: {self._signals_parsed}"
                    )
                    last_heartbeat = now
                    
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    app = CryptoSignalScraper()
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print("\n👋 Interrupted")
    except Exception as e:
        logging.error(f"Fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
