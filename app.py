#!/usr/bin/env python3
"""
Real-Time Crypto Signal Scraper & Dashboard
═══════════════════════════════════════════════════════════════════
Optimized for Render.com PAID Tier

FIXES:
✅ MongoDB - Using replace_one instead of update_one (no $ operators needed)
✅ Enhanced signal parser for multiple channel formats
✅ WebSocket upgrade detection
✅ HTML escaping for Telegram messages
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
from dataclasses import dataclass, field
from enum import Enum

import certifi
import websockets
from websockets.server import serve as ws_serve
from websockets.exceptions import ConnectionClosed
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError, DuplicateKeyError
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
    """Trading signal data structure."""
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
        """Convert to MongoDB-compatible dictionary."""
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


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class SignalParser:
    """Enhanced signal parser for multiple channel formats."""
    
    KNOWN_COINS = {
        'BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'DOT', 'MATIC', 'SHIB',
        'LTC', 'TRX', 'AVAX', 'LINK', 'ATOM', 'UNI', 'XMR', 'ETC', 'XLM', 'BCH',
        'APT', 'FIL', 'LDO', 'ARB', 'OP', 'INJ', 'SUI', 'SEI', 'TIA', 'JUP',
        'PEPE', 'WIF', 'BONK', 'FLOKI', 'MEME', 'ORDI', 'SATS', 'RATS',
        'ONDO', 'BOB', 'BEAT', 'RENDER', 'FET', 'AGIX', 'OCEAN', 'TAO', 'WLD',
        'NEAR', 'ICP', 'VET', 'ALGO', 'GRT', 'FTM', 'SAND', 'MANA', 'AXS', 'GALA',
        'SAPIEN', 'COOKIE', 'AI16Z', 'VIRTUAL', 'AIXBT', 'ZEREBRO', 'TURBO',
        'GRIFFAIN', 'GOAT', 'AVA', 'ELIZA', 'ARC', 'SWARMS', 'ALCH', 'AGENT',
        'ENJ', 'IMX', 'BLUR', 'MAGIC', 'GMT', 'APE', 'DYDX', 'GMX', 'SNX', 'CRV',
        'PIPPIN', 'PARTI', 'LIGHT', 'DEEP', 'B2', 'JCT', 'ANIME', 'UAI', 'MERL',
        'FOLKS', 'MON', 'ZORA', 'ORCA', 'BAND', 'DENT', 'TNSR', 'MAVIA', 'LSK',
        'BANANA', 'ARIA', 'SUPER', 'TRADOOR', 'KAS', 'BAT', 'COMP', 'IN',
    }

    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize Unicode - converts fancy fonts to ASCII."""
        return unicodedata.normalize('NFKC', text)
    
    @staticmethod
    def clean_number(value: str) -> str:
        """Clean and extract number from string."""
        if not value:
            return ""
        cleaned = re.sub(r'[^\d.\-]', '', value)
        return cleaned.strip('.-')

    @classmethod
    def extract_pair(cls, text: str, normalized: str) -> Optional[str]:
        """Extract trading pair from text."""
        text_upper = normalized.upper()
        
        # Pattern 1: Emoji-wrapped pairs like ⚡BEATUSDT⚡
        emoji_match = re.search(
            r'[⚡💎🔥✨🚀]+\s*([A-Z0-9]{2,15}(?:USDT|BUSD|USD)?)\s*[⚡💎🔥✨🚀]+', 
            text_upper
        )
        if emoji_match:
            pair = emoji_match.group(1)
            if not pair.endswith(('USDT', 'BUSD', 'USD')):
                pair += 'USDT'
            return pair.replace('BUSD', 'USDT')
        
        # Pattern 2: Pair with /USDT format
        slash_match = re.search(r'\b([A-Z0-9]{2,10})\s*/\s*USDT', text_upper)
        if slash_match:
            return slash_match.group(1) + 'USDT'
        
        # Pattern 3: Hashtag pairs
        hashtag_match = re.search(r'#([A-Z0-9]{2,15}(?:USDT|PERP)?)\b', text_upper)
        if hashtag_match:
            pair = hashtag_match.group(1).replace('PERP', '')
            if not pair.endswith('USDT'):
                pair += 'USDT'
            return pair
        
        # Pattern 4: Dollar sign pairs
        dollar_match = re.search(r'\$([A-Z]{2,10})\b', text_upper)
        if dollar_match:
            return dollar_match.group(1) + 'USDT'
        
        # Pattern 5: Standard XXXUSDT format
        standard_match = re.search(r'\b([A-Z0-9]{2,10}USDT)\b', text_upper)
        if standard_match:
            return standard_match.group(1)
        
        # Pattern 6: Known coin followed by direction
        for coin in cls.KNOWN_COINS:
            if re.search(rf'\b{coin}\b', text_upper):
                if re.search(r'\b(LONG|SHORT|BUY|SELL|POSITION)\b', text_upper):
                    return coin + 'USDT'
        
        return None
    
    @classmethod
    def extract_direction(cls, text: str, normalized: str) -> SignalDirection:
        """Extract trade direction."""
        text_upper = normalized.upper()
        
        # Check emojis first
        if '🟢' in text or '📈' in text or '⬆️' in text or '🔼' in text:
            return SignalDirection.LONG
        if '🔴' in text or '📉' in text or '⬇️' in text or '🔽' in text:
            return SignalDirection.SHORT
        
        # Long patterns
        if re.search(r'\bLONG\s*POSITION\b', text_upper):
            return SignalDirection.LONG
        if re.search(r'\bLONG\b', text_upper):
            return SignalDirection.LONG
        if re.search(r'\bBUY\s*/?\s*LONG\b', text_upper):
            return SignalDirection.LONG
        if re.search(r'\bBULLISH\b', text_upper):
            return SignalDirection.LONG
        
        # Short patterns
        if re.search(r'\bSHORT\s*POSITION\b', text_upper):
            return SignalDirection.SHORT
        if re.search(r'\bSHORT\b', text_upper):
            return SignalDirection.SHORT
        if re.search(r'\bSELL\s*/?\s*SHORT\b', text_upper):
            return SignalDirection.SHORT
        if re.search(r'\bBEARISH\b', text_upper):
            return SignalDirection.SHORT
        
        # Standalone BUY/SELL
        has_buy = re.search(r'\bBUY\b', text_upper)
        has_sell = re.search(r'\bSELL\b', text_upper)
        
        if has_buy and not has_sell:
            return SignalDirection.LONG
        if has_sell and not has_buy:
            return SignalDirection.SHORT
        
        return SignalDirection.UNKNOWN
    
    @classmethod
    def extract_entry(cls, normalized: str) -> str:
        """Extract entry price."""
        text_upper = normalized.upper()
        
        # Market entry
        if re.search(r'ENTRY\s*(?::|-)?\s*MARKET', text_upper):
            return "Market"
        if re.search(r'MARKET\s*PRICE', text_upper):
            return "Market"
        if re.search(r'\bCMP\b', text_upper):
            return "Market"
        
        # Entry with Above/Below
        match = re.search(r'(?:ABOVE|BELOW|AROUND|AT)\s*[-:=]?\s*([\d.,]+)', text_upper)
        if match:
            cleaned = cls.clean_number(match.group(1))
            if cleaned:
                return cleaned
        
        # Standard entry
        patterns = [
            r'ENTRY\s*(?:PRICE|ZONE)?[\s:=\-]*([\d.,]+)',
            r'EP[\s:=\-]*([\d.,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_upper)
            if match:
                cleaned = cls.clean_number(match.group(1))
                if cleaned:
                    try:
                        if float(cleaned) > 0:
                            return cleaned
                    except ValueError:
                        pass
        
        return "Market"
    
    @classmethod
    def extract_targets(cls, normalized: str) -> List[str]:
        """Extract take profit targets."""
        targets = []
        seen = set()
        text_upper = normalized.upper()
        
        # Percentage targets
        pct_matches = re.findall(r'(\d+(?:\.\d+)?)\s*%', text_upper)
        for pct in pct_matches:
            if pct not in seen and float(pct) > 0:
                targets.append(f"{pct}%")
                seen.add(pct)
        
        # TP with price
        tp_matches = re.findall(r'TP\s*\d*\s*[-:=]?\s*([\d.,]+)(?!\s*%)', text_upper)
        for match in tp_matches:
            cleaned = cls.clean_number(match)
            if cleaned and cleaned not in seen:
                targets.append(cleaned)
                seen.add(cleaned)
        
        return targets[:6]
    
    @classmethod
    def extract_stop_loss(cls, normalized: str) -> str:
        """Extract stop loss price."""
        text_upper = normalized.upper()
        
        patterns = [
            r'(?:STOP\s*LOSS|STOPLOSS|SL)\s*[-:=]?\s*([\d.,]+)',
            r'\bSL\s*[-:=]\s*([\d.,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_upper)
            if match:
                return cls.clean_number(match.group(1))
        
        return ""
    
    @classmethod
    def extract_leverage(cls, normalized: str) -> str:
        """Extract leverage."""
        text_upper = normalized.upper()
        
        match = re.search(r'(\d+)\s*[Xx×]', text_upper)
        if match:
            lev = match.group(1)
            if lev.isdigit() and 1 <= int(lev) <= 125:
                return f"{lev}x"
        
        return ""

    @classmethod
    def parse(cls, text: str, channel_id: int, channel_name: str, 
              message_id: int, is_vip: bool = False) -> Optional[Signal]:
        """Parse message text into a Signal object."""
        
        if not text or len(text) < 10:
            return None
        
        normalized = cls.normalize_text(text)
        
        pair = cls.extract_pair(text, normalized)
        direction = cls.extract_direction(text, normalized)
        entry = cls.extract_entry(normalized)
        targets = cls.extract_targets(normalized)
        stop_loss = cls.extract_stop_loss(normalized)
        leverage = cls.extract_leverage(normalized)
        
        # Validation
        if not is_vip:
            if not pair:
                return None
            if direction == SignalDirection.UNKNOWN:
                return None
            if entry == "Market" and not targets:
                return None
        else:
            # VIP - still need some data
            if not pair and direction == SignalDirection.UNKNOWN:
                return None
            if not pair:
                pair = "UNKNOWN"
        
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
# DATABASE MANAGER - FIXED WITH replace_one
# ═══════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    """
    Async MongoDB manager.
    FIXED: Uses replace_one instead of update_one to avoid $ operator issues.
    """
    
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
        """Connect to MongoDB."""
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
                    
                    # Create indexes
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
        """Check if signal was previously deleted."""
        if not self._connected:
            return False
        try:
            doc = await self.deleted.find_one({"id": signal_id})
            return doc is not None
        except Exception:
            return False
    
    async def mark_deleted(self, signal_id: str) -> bool:
        """Mark a signal as deleted."""
        if not self._connected:
            return False
        try:
            # Use replace_one instead of update_one
            doc = {
                "id": signal_id,
                "deleted_at": datetime.now(timezone.utc)
            }
            await self.deleted.replace_one(
                {"id": signal_id},
                doc,
                upsert=True
            )
            await self.signals.delete_one({"id": signal_id})
            return True
        except Exception as e:
            logger.error(f"DB Error (mark_deleted): {e}")
            return False
    
    async def upsert_signal(self, signal: Signal) -> tuple[bool, bool]:
        """
        Insert or update a signal using replace_one.
        Returns: (success, is_new)
        
        FIXED: Uses replace_one instead of update_one to avoid $ operator requirement.
        """
        if not self._connected:
            logger.warning("Database not connected")
            return False, False
        
        try:
            # Check if deleted
            if await self.is_deleted(signal.id):
                return False, False
            
            # Convert signal to dict
            signal_dict = signal.to_dict()
            
            # Use replace_one with upsert - this REPLACES the entire document
            # and doesn't require $ operators
            result = await self.signals.replace_one(
                {"id": signal.id},  # Filter
                signal_dict,         # Replacement document (full document)
                upsert=True          # Insert if not exists
            )
            
            # Check if it was a new insert or an update
            is_new = result.upserted_id is not None
            
            return True, is_new
            
        except DuplicateKeyError:
            # Race condition - already exists
            return True, False
        except Exception as e:
            logger.error(f"DB Error (upsert): {e}")
            return False, False
    
    async def get_recent_signals(self, limit: int = 50, status: str = None) -> List[Dict]:
        """Get recent signals."""
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
        except Exception as e:
            logger.error(f"DB Error (get_recent): {e}")
            return []
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get signal statistics."""
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
        except Exception as e:
            logger.error(f"DB Error (get_stats): {e}")
            return {"total": 0, "active": 0, "hit_tp": 0, "hit_sl": 0, "win_rate": 0}
    
    async def reset_signals(self) -> int:
        """Delete all signals."""
        if not self._connected:
            return 0
        try:
            result = await self.signals.delete_many({})
            await self.deleted.delete_many({})
            return result.deleted_count
        except Exception:
            return 0
    
    async def close(self):
        """Close database connection."""
        if self.client:
            self.client.close()
            self._connected = False


# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET BROADCASTER
# ═══════════════════════════════════════════════════════════════════════════════

class WebSocketBroadcaster:
    """WebSocket connection manager."""
    
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
    
    async def close_all(self):
        async with self._lock:
            for client in self.clients.copy():
                try:
                    await client.close(1001, "Server shutting down")
                except:
                    pass
            self.clients.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM ALERT BOT
# ═══════════════════════════════════════════════════════════════════════════════

class TelegramAlertBot:
    """Send alerts via Telegram Bot API."""
    
    def __init__(self, bot_token: str, chat_id: int):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._enabled = bool(bot_token and chat_id)
        
        if self._enabled:
            logger.info(f"✅ Alert Bot enabled for chat: {chat_id}")
        else:
            logger.warning("⚠️ Alert Bot disabled")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    def format_signal(self, signal: Signal) -> str:
        emoji = "🟢" if signal.direction == "LONG" else "🔴" if signal.direction == "SHORT" else "⚪"
        vip = " ⭐ VIP" if signal.is_vip else ""
        
        if signal.targets:
            targets_str = "\n".join([f"  • TP{i+1}: <code>{t}</code>" for i, t in enumerate(signal.targets)])
        else:
            targets_str = "  • Market targets"
        
        safe_channel = html.escape(signal.channel_name)
        
        return f"""{emoji} <b>NEW SIGNAL</b>{vip}

<b>Pair:</b> <code>{signal.pair}</code>
<b>Direction:</b> {signal.direction}
<b>Entry:</b> <code>{signal.entry}</code>
<b>Leverage:</b> {signal.leverage or "N/A"}

<b>Targets:</b>
{targets_str}

<b>Stop Loss:</b> <code>{signal.stop_loss or "N/A"}</code>

📡 <i>{safe_channel}</i>
🕐 <i>{signal.timestamp.strftime("%Y-%m-%d %H:%M UTC")}</i>

<a href="https://www.tradingview.com/chart/?symbol=BINANCE:{signal.pair}.P">📊 TradingView</a> | <a href="https://www.binance.com/en/futures/{signal.pair}">📈 Binance</a>"""
    
    async def send_alert(self, signal: Signal) -> bool:
        if not self._enabled:
            return False
        
        try:
            session = await self._get_session()
            
            async with session.post(
                f"{self.api_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": self.format_signal(signal),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
            ) as resp:
                if resp.status == 200:
                    logger.info(f"📨 Alert sent: {signal.pair} {signal.direction}")
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
    """Main application class."""
    
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
        self._messages_processed = 0
        self._signals_parsed = 0
    
    async def health_check(self, path: str, request_headers):
        """HTTP health check - allows WebSocket upgrades."""
        upgrade = request_headers.get("Upgrade", "").lower()
        connection = request_headers.get("Connection", "").lower()
        
        if "websocket" in upgrade or "upgrade" in connection:
            return None
        
        if path in ('/', '/health', '/healthz', '/ping'):
            uptime = int(time.time() - self._start_time)
            
            health_data = {
                "status": "healthy",
                "service": self.config.RENDER_SERVICE_NAME,
                "deploy_id": self._deploy_id,
                "uptime_seconds": uptime,
                "stats": {
                    "messages_processed": self._messages_processed,
                    "signals_parsed": self._signals_parsed,
                },
                "connections": {
                    "websocket_clients": len(self.broadcaster.clients),
                    "database": "connected" if self.db.is_connected else "disconnected",
                    "telegram": "connected" if (self.telegram_client and self.telegram_client.is_connected()) else "connecting"
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            body = json.dumps(health_data, indent=2).encode()
            
            return (
                200,
                [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(body))),
                    ("Access-Control-Allow-Origin", "*")
                ],
                body
            )
        
        return None
    
    async def websocket_handler(self, websocket):
        """Handle WebSocket connections."""
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
            
            self._messages_processed += 1
            
            channel_name = self.channel_cache.get(chat_id, f"Channel {chat_id}")
            is_vip = chat_id in self.config.VIP_CHANNEL_IDS
            
            logger.info(f"📩 New message from {channel_name}")
            
            signal = SignalParser.parse(
                text=message.text,
                channel_id=chat_id,
                channel_name=channel_name,
                message_id=message.id,
                is_vip=is_vip
            )
            
            if not signal:
                return
            
            self._signals_parsed += 1
            
            success, is_new = await self.db.upsert_signal(signal)
            
            if success:
                log_prefix = "🆕 NEW" if is_new else "🔄 UPD"
                logger.info(f"{log_prefix} | {signal.pair} {signal.direction} saved to DB")
                
                event_type = "new_signal" if is_new else "update_signal"
                await self.broadcaster.broadcast_signal(signal, event_type)
                
                if is_new:
                    await self.alert_bot.send_alert(signal)
            else:
                logger.warning(f"⚠️ Failed to save signal {signal.pair} to database")
                    
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
        """Handle admin commands."""
        sender_id = event.sender_id
        
        if sender_id not in self.config.ADMIN_IDS:
            return
        
        text = event.message.text.strip().lower()
        
        try:
            if text == "/status":
                stats = await self.db.get_stats()
                uptime = int(time.time() - self._start_time)
                h, m, s = uptime // 3600, (uptime % 3600) // 60, uptime % 60
                
                parse_rate = (self._signals_parsed / self._messages_processed * 100) if self._messages_processed > 0 else 0
                
                status_msg = f"""📊 <b>System Status</b>

<b>Server:</b>
  • Uptime: {h}h {m}m {s}s
  • Deploy: <code>{self._deploy_id}</code>
  • WS Clients: {len(self.broadcaster.clients)}

<b>Processing:</b>
  • Messages: {self._messages_processed}
  • Signals: {self._signals_parsed}
  • Parse Rate: {parse_rate:.1f}%

<b>Connections:</b>
  • Database: {'✅' if self.db.is_connected else '❌'}
  • Telegram: {'✅' if self.telegram_client and self.telegram_client.is_connected() else '❌'}
  • Alert Bot: {'✅' if self.alert_bot._enabled else '❌'}

<b>Signals in DB:</b>
  • Total: {stats.get('total', 0)}
  • Active: {stats.get('active', 0)}"""
                
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
                self._messages_processed = 0
                self._signals_parsed = 0
                await event.respond(f"🗑️ Deleted {count} signals and reset counters.")
            
            elif text == "/test":
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
                
                # Test database
                db_success, _ = await self.db.upsert_signal(test_signal)
                
                # Test alert
                alert_sent = await self.alert_bot.send_alert(test_signal)
                
                # Test broadcast
                await self.broadcaster.broadcast_signal(test_signal, "new_signal")
                
                await event.respond(
                    f"✅ Test complete!\n"
                    f"• Database: {'✅' if db_success else '❌'}\n"
                    f"• Alert Bot: {'✅' if alert_sent else '❌'}\n"
                    f"• WS Clients: {len(self.broadcaster.clients)}"
                )
            
            elif text == "/help":
                await event.respond(
                    "🤖 <b>Admin Commands:</b>\n\n"
                    "/status - System status\n"
                    "/channels - List channels\n"
                    "/reset - Clear signals\n"
                    "/test - Test all systems\n"
                    "/help - This message",
                    parse_mode='html'
                )
                
        except Exception as e:
            logger.error(f"Admin command error: {e}")
            await event.respond(f"❌ Error: {str(e)[:100]}")
    
    async def discover_channels(self):
        """Discover and cache channel names."""
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
                        logger.info(f"  📡 {chat_name}{vip}")
            
            logger.info(f"✅ Cached {len(self.channel_cache)} channels")
        except Exception as e:
            logger.error(f"Channel discovery error: {e}")
    
    async def backfill_signals(self):
        """Backfill recent messages from channels."""
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
                    except Exception:
                        continue
                
                if count > 0:
                    logger.info(f"  📥 {channel_name[:40]}: {count} signals")
                    total += count
                    
            except Exception as e:
                logger.error(f"Backfill error ({channel_id}): {e}")
                continue
        
        logger.info(f"✅ Backfill complete: {total} signals")
    
    async def setup_telegram(self) -> bool:
        """Initialize Telegram client."""
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
            
            if self.config.ADMIN_IDS:
                self.telegram_client.add_event_handler(
                    self.handle_admin_command,
                    events.NewMessage(pattern=r'^/', from_users=self.config.ADMIN_IDS)
                )
            
            await self.backfill_signals()
            return True
            
        except Exception as e:
            logger.error(f"❌ Telegram setup error: {e}")
            return False
    
    def handle_shutdown_signal(self, sig):
        logger.info(f"🛑 Received shutdown signal, stopping...")
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
        """Main entry point."""
        logger.info("=" * 60)
        logger.info("  🚀 Crypto Signal Scraper")
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
                ping_timeout=10
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
        logger.info("  ✅ Server Ready")
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
                        f"Processed: {self._messages_processed} | "
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
