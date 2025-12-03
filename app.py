#!/usr/bin/env python3
"""
Real-Time Crypto Signal Scraper & Dashboard
Optimized for Render.com PAID Tier
FIXED: WebSocket connections, MongoDB operators, HTML escaping
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
    
    # Telegram API
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    SESSION_STRING: str = os.getenv("SESSION_STRING", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ALERT_CHAT_ID: int = int(os.getenv("ALERT_CHAT_ID", "0"))
    
    # MongoDB
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME: str = os.getenv("DB_NAME", "crypto_signals")
    
    # Server
    WS_HOST: str = "0.0.0.0"
    WS_PORT: int = int(os.getenv("PORT", "10000"))
    SHUTDOWN_TIMEOUT: int = int(os.getenv("SHUTDOWN_TIMEOUT", "30"))
    
    # Channels
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
    
    # Backfill
    BACKFILL_ENABLED: bool = os.getenv("BACKFILL_ENABLED", "true").lower() == "true"
    BACKFILL_LIMIT: int = int(os.getenv("BACKFILL_LIMIT", "100"))
    
    # Render
    RENDER_SERVICE_NAME: str = os.getenv("RENDER_SERVICE_NAME", "crypto-signal-scraper")
    RENDER_EXTERNAL_URL: str = os.getenv("RENDER_EXTERNAL_URL", "")

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    """Configure logging."""
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
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Signal':
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if 'hit_targets' not in data:
            data['hit_targets'] = []
        data.pop('_id', None)
        return cls(**data)

# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class SignalParser:
    """Parse trading signals from message text."""
    
    PAIR_PATTERNS = [
        r'(?:pair|coin|symbol|ticker)[:\s]*#?([A-Z0-9]+(?:/|\.)?(?:USDT|BUSD|USD|PERP)?)',
        r'#([A-Z0-9]{2,10}(?:USDT|PERP)?)\b',
        r'\$([A-Z]{2,10})\b',
        r'\b([A-Z]{2,10})(?:/|\.)?USDT\b',
        r'\b([A-Z]{2,10}USDT)\b',
        r'\b([A-Z]{3,10})\s*(?:LONG|SHORT|Long|Short)\b',
    ]
    
    DIRECTION_PATTERNS = [
        (r'\b(LONG|Long|long|BUY|Buy|buy|BULLISH|Bullish|🟢|🔼|⬆️|📈)\b', SignalDirection.LONG),
        (r'\b(SHORT|Short|short|SELL|Sell|sell|BEARISH|Bearish|🔴|🔽|⬇️|📉)\b', SignalDirection.SHORT),
    ]
    
    ENTRY_PATTERNS = [
        r'(?:entry|enter|buy|sell|price|ep)[:\s]*(?:zone)?[:\s]*[\$]?([\d.,\s\-–to]+)',
        r'(?:entry|enter|ep)[:\s]*(?:market|now|cmp|current)',
        r'(?:cmp|current\s*price)[:\s]*[\$]?([\d.,]+)',
    ]
    
    TARGET_PATTERNS = [
        r'(?:tp|target|take[\s]?profit)\s*\d*[:\s]*[\$]?([\d.,]+)',
        r'🎯\s*[\$]?([\d.,]+)',
    ]
    
    STOPLOSS_PATTERNS = [
        r'(?:sl|stop[\s]?loss|stoploss|stop)[:\s]*[\$]?([\d.,]+)',
        r'🛑\s*[\$]?([\d.,]+)',
    ]
    
    LEVERAGE_PATTERNS = [
        r'(?:leverage|lev)[:\s]*(?:cross|isolated)?[:\s]*(\d+)[xX×]?',
        r'(\d+)[xX×]\s*(?:leverage|lev)?',
    ]

    @staticmethod
    def normalize_text(text: str) -> str:
        return unicodedata.normalize('NFKC', text)
    
    @staticmethod
    def clean_number(value: str) -> str:
        if not value:
            return ""
        value = re.sub(r'\s+', '', value)
        value = value.replace(',', '').replace('to', '-').replace('–', '-')
        return value.strip('-')

    @classmethod
    def parse(cls, text: str, channel_id: int, channel_name: str, 
              message_id: int, is_vip: bool = False) -> Optional[Signal]:
        """Parse message text into a Signal object."""
        
        normalized = cls.normalize_text(text)
        text_upper = normalized.upper()
        
        # Extract pair
        pair = None
        for pattern in cls.PAIR_PATTERNS:
            match = re.search(pattern, text_upper, re.IGNORECASE | re.MULTILINE)
            if match:
                pair = match.group(1).replace('/', '').replace('.', '').upper()
                pair = pair.replace('PERP', '').replace('BUSD', 'USDT')
                if not pair.endswith('USDT'):
                    pair += 'USDT'
                break
        
        if not pair and not is_vip:
            return None
        
        # Extract direction
        direction = SignalDirection.UNKNOWN
        for pattern, dir_type in cls.DIRECTION_PATTERNS:
            if re.search(pattern, normalized):
                direction = dir_type
                break
        
        # Extract entry
        entry = "Market"
        for pattern in cls.ENTRY_PATTERNS:
            match = re.search(pattern, normalized, re.IGNORECASE)
            if match:
                matched_text = match.group(0).lower()
                if any(kw in matched_text for kw in ['market', 'now', 'cmp', 'current']):
                    entry = "Market"
                elif match.lastindex and match.group(1):
                    entry = cls.clean_number(match.group(1))
                break
        
        # Extract targets
        targets = []
        seen = set()
        for pattern in cls.TARGET_PATTERNS:
            for match in re.finditer(pattern, normalized, re.IGNORECASE):
                if match.lastindex:
                    cleaned = cls.clean_number(match.group(1))
                    if cleaned and cleaned not in seen:
                        targets.append(cleaned)
                        seen.add(cleaned)
        
        # Extract stop loss
        stop_loss = ""
        for pattern in cls.STOPLOSS_PATTERNS:
            match = re.search(pattern, normalized, re.IGNORECASE)
            if match and match.lastindex:
                stop_loss = cls.clean_number(match.group(1))
                break
        
        # Extract leverage
        leverage = ""
        for pattern in cls.LEVERAGE_PATTERNS:
            match = re.search(pattern, normalized, re.IGNORECASE)
            if match and match.lastindex:
                lev_value = match.group(1)
                if lev_value.isdigit() and 1 <= int(lev_value) <= 125:
                    leverage = f"{lev_value}x"
                break
        
        # Validation
        if not is_vip:
            if not pair or direction == SignalDirection.UNKNOWN:
                return None
            if entry == "Market" and not targets:
                return None
        
        return Signal(
            id=f"{channel_id}:{message_id}",
            channel_id=channel_id,
            channel_name=channel_name,
            message_id=message_id,
            pair=pair or "UNKNOWN",
            direction=direction.value,
            entry=entry,
            targets=targets[:6],
            stop_loss=stop_loss,
            leverage=leverage,
            raw_text=text[:2000],
            timestamp=datetime.now(timezone.utc),
            is_vip=is_vip,
            status="ACTIVE"
        )

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    """Async MongoDB manager."""
    
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
        except PyMongoError:
            return False
    
    async def mark_deleted(self, signal_id: str) -> bool:
        """Mark a signal as deleted."""
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
        """Insert or update a signal. Returns: (success, is_new)"""
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
        except PyMongoError as e:
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
        except PyMongoError as e:
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
        except PyMongoError:
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
    """Send alerts via Telegram Bot API."""
    
    def __init__(self, bot_token: str, chat_id: int):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._enabled = bool(bot_token and chat_id)
    
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
        
        targets = "\n".join([f"  • TP{i+1}: <code>{t}</code>" for i, t in enumerate(signal.targets)]) or "  • Not specified"
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

<a href="https://www.tradingview.com/chart/?symbol=BINANCE:{signal.pair}.P">📊 Chart</a>"""
    
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
                    logger.error(f"Alert failed [{resp.status}]: {error[:100]}")
                    return False
                    
        except Exception as e:
            logger.error(f"Alert error: {e}")
            return False

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class CryptoSignalScraper:
    """Main application."""
    
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
    
    # ─────────────────────────────────────────────────────────────────────────
    # Health Check (FIXED - Allows WebSocket Upgrades)
    # ─────────────────────────────────────────────────────────────────────────
    
    async def health_check(self, path: str, request_headers):
        """
        HTTP health check handler.
        FIXED: Properly detects WebSocket upgrade requests and lets them through.
        """
        # Check if this is a WebSocket upgrade request
        upgrade_header = request_headers.get("Upgrade", "").lower()
        connection_header = request_headers.get("Connection", "").lower()
        
        # If it's a WebSocket upgrade request, let it through
        if "websocket" in upgrade_header or "upgrade" in connection_header:
            logger.debug(f"WS: Allowing WebSocket upgrade for path: {path}")
            return None
        
        # For regular HTTP requests to health endpoints
        if path in ('/', '/health', '/healthz', '/ping', '/ready'):
            uptime = int(time.time() - self._start_time)
            
            health_data = {
                "status": "healthy",
                "service": self.config.RENDER_SERVICE_NAME,
                "deploy_id": self._deploy_id,
                "uptime_seconds": uptime,
                "uptime_human": f"{uptime // 3600}h {(uptime % 3600) // 60}m",
                "connections": {
                    "websocket_clients": len(self.broadcaster.clients),
                    "database": "connected" if self.db.is_connected else "disconnected",
                    "telegram": "connected" if (self.telegram_client and self.telegram_client.is_connected()) else "connecting"
                },
                "channels_monitored": len(self.channel_cache),
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
        
        # For any other path, let it through (might be WebSocket)
        return None
    
    # ─────────────────────────────────────────────────────────────────────────
    # WebSocket Handler
    # ─────────────────────────────────────────────────────────────────────────
    
    async def websocket_handler(self, websocket):
        """Handle WebSocket connections."""
        await self.broadcaster.register(websocket)
        
        try:
            # Send initial data
            signals = await self.db.get_recent_signals(50)
            await self.broadcaster.send_to_client(websocket, {
                "type": "initial_data",
                "data": signals,
                "server_time": datetime.now(timezone.utc).isoformat(),
                "deploy_id": self._deploy_id
            })
            
            # Handle messages
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
        """Process WebSocket messages."""
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
    
    # ─────────────────────────────────────────────────────────────────────────
    # Telegram Event Handlers
    # ─────────────────────────────────────────────────────────────────────────
    
    async def on_new_message(self, event):
        """Handle new messages."""
        try:
            chat_id = event.chat_id
            
            all_channels = set(self.config.CHANNEL_IDS) | set(self.config.VIP_CHANNEL_IDS)
            if chat_id not in all_channels:
                return
            
            message = event.message
            if not message or not message.text:
                return
            
            channel_name = self.channel_cache.get(chat_id, f"Channel {chat_id}")
            is_vip = chat_id in self.config.VIP_CHANNEL_IDS
            
            signal = SignalParser.parse(
                text=message.text,
                channel_id=chat_id,
                channel_name=channel_name,
                message_id=message.id,
                is_vip=is_vip
            )
            
            if not signal:
                return
            
            success, is_new = await self.db.upsert_signal(signal)
            
            if success:
                log_prefix = "🆕 NEW" if is_new else "🔄 UPD"
                logger.info(f"{log_prefix} | {signal.pair:12} | {signal.direction:5} | {channel_name[:30]}")
                
                event_type = "new_signal" if is_new else "update_signal"
                await self.broadcaster.broadcast_signal(signal, event_type)
                
                if is_new:
                    asyncio.create_task(self.alert_bot.send_alert(signal))
                    
        except Exception as e:
            logger.error(f"Message handler error: {e}")
    
    async def on_message_edited(self, event):
        """Handle edited messages."""
        await self.on_new_message(event)
    
    async def on_message_deleted(self, event):
        """Handle deleted messages."""
        try:
            chat_id = event.chat_id
            for msg_id in event.deleted_ids:
                signal_id = f"{chat_id}:{msg_id}"
                if await self.db.mark_deleted(signal_id):
                    await self.broadcaster.broadcast_delete(signal_id)
                    logger.info(f"🗑️ DEL | {signal_id}")
        except Exception as e:
            logger.error(f"Delete handler error: {e}")
    
    # ─────────────────────────────────────────────────────────────────────────
    # Admin Commands
    # ─────────────────────────────────────────────────────────────────────────
    
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
                
                status_msg = f"""📊 <b>System Status</b>

<b>Server:</b>
  • Uptime: {h}h {m}m {s}s
  • Deploy: <code>{self._deploy_id}</code>
  • WS Clients: {len(self.broadcaster.clients)}
  • Channels: {len(self.channel_cache)}

<b>Connections:</b>
  • Database: {'✅' if self.db.is_connected else '❌'}
  • Telegram: {'✅' if self.telegram_client and self.telegram_client.is_connected() else '❌'}

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
                await event.respond(f"🗑️ Deleted {count} signals.")
            
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
                    "/broadcast - Send to WS clients\n"
                    "/help - This message",
                    parse_mode='html'
                )
                
        except Exception as e:
            logger.error(f"Admin command error: {e}")
            await event.respond(f"❌ Error: {str(e)[:100]}", parse_mode=None)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Channel Discovery & Backfill
    # ─────────────────────────────────────────────────────────────────────────
    
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
        """Backfill recent messages."""
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
    
    # ─────────────────────────────────────────────────────────────────────────
    # Telegram Setup
    # ─────────────────────────────────────────────────────────────────────────
    
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
    
    # ─────────────────────────────────────────────────────────────────────────
    # Shutdown
    # ─────────────────────────────────────────────────────────────────────────
    
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
    
    # ─────────────────────────────────────────────────────────────────────────
    # Main Entry
    # ─────────────────────────────────────────────────────────────────────────
    
    async def run(self):
        """Main entry point."""
        logger.info("=" * 60)
        logger.info("  🚀 Crypto Signal Scraper")
        logger.info("=" * 60)
        
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: self.handle_shutdown_signal(s))
        
        # Start WebSocket server FIRST
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
        
        # Start background tasks
        asyncio.create_task(self.db.connect())
        
        async def delayed_telegram():
            await asyncio.sleep(1)
            await self.setup_telegram()
        
        asyncio.create_task(delayed_telegram())
        
        self._running = True
        
        logger.info("=" * 60)
        logger.info("  ✅ Server Ready - WebSocket accepting connections")
        logger.info("=" * 60)
        
        # Main loop
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
                        f"Signals: {stats.get('active', 0)}"
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
