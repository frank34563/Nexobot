# Full bot.py — Latest trading features implementation with:
# - httpx-based Binance price fetcher with TTL cache and simulated fallback price walk
# - DB models: UserTradeConfig (per-user config), DailySummary (daily records), Config (key/value store)
# - Admin commands: /set_trades_per_day, /set_daily_range, /set_trade_range, /set_user_trade, /set_trading_hours, /trading_hours_status, /trading_status, /trading_summary
# - Enforced ranges: daily 1.25%-1.5%, per-trade 0.05%-0.25% with validation
# - Trading engine: uses per-user config or global config, fetches live prices with cache, updates balances
# - Trading hours: configurable NY timezone window (default 5 AM - 6 PM)
# - Daily summary job at 23:59 UTC: persists DailySummary, sends formatted summary to users
# - Fixed-point decimal formatting (Decimal + format_price helpers)
# - CallbackQueryHandler ordering: admin/history callbacks before generic menu handler
# - Preserved: invest/withdraw/history/admin flows, admin_pending_command diagnostics
#
# Replace your existing bot.py with this file and restart the bot.

import os
import logging
import random
import re
import asyncio
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from dotenv import load_dotenv

from decimal import Decimal, ROUND_HALF_UP
import httpx

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from sqlalchemy import (
    Column, Integer, String, DateTime,
    BigInteger, select, Numeric, text, update as sa_update
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# === CONFIG ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set!")

# ADMIN_ID parsing with logging
try:
    ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
except Exception:
    ADMIN_ID = 0

ADMIN_LOG_CHAT_ID = os.getenv('ADMIN_LOG_CHAT_ID')  # optional admin log chat id
MASTER_WALLET = os.getenv('MASTER_WALLET', 'TAbc...')
MASTER_NETWORK = os.getenv('MASTER_NETWORK', 'TRC20')
SUPPORT_USER = os.getenv('SUPPORT_USER', '@NexoAi_Support')
SUPPORT_URL = os.getenv('SUPPORT_URL', f"https://t.me/NexoAi_Support")

MENU_FULL_TWO_COLUMN = os.getenv('MENU_FULL_TWO_COLUMN', 'true').lower() in ('1','true','yes','on')
DATABASE_URL = os.getenv('DATABASE_URL')

# Main menu image configuration
MAIN_MENU_IMAGE_URL = os.getenv('MAIN_MENU_IMAGE_URL', 'assets/Main Menu.jpg')  # Can be URL or local file path
MAIN_MENU_CAPTION = (
    "🚀 Welcome to Nexo Trading Bot - Your Smart Path to Crypto Growth!\n\n"
    "🤖 Your Personal AI Trading Assistant\n"
    "💹 Automated Crypto Trading 24/7\n\n"
    "📋 Main Menu"
)

# Binance and trading config
BINANCE_CACHE_TTL = int(os.getenv('BINANCE_CACHE_TTL', '10'))  # seconds
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price"
USE_BINANCE = True  # Can be toggled by admin commands

# Global trading configuration (can be modified by admin commands)
GLOBAL_DAILY_PERCENT = 1.375  # default 1.375% daily (mid-range between 1.25% and 1.5%)
GLOBAL_TRADE_PERCENT = 0.15  # default 0.15% per trade (mid-range between 0.05% and 0.25%)
GLOBAL_TRADES_PER_DAY = 15  # default 15 trades per day (96 minute frequency)
GLOBAL_NEGATIVE_TRADES_PER_DAY = 1  # default 1 negative trade per day

# Minimum deposit amount in USDT
MIN_DEPOSIT_AMOUNT = 10.0  # Minimum investment deposit is 10 USDT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Configured ADMIN_ID=%s", ADMIN_ID)
if ADMIN_ID == 0:
    logger.warning("ADMIN_ID not configured or set to 0 — admin-only features will be unavailable or not work as expected.")

# === DATABASE ===
Base = declarative_base()

if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
    elif DATABASE_URL.startswith("postgresql://") and "+psycopg" not in DATABASE_URL:
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
else:
    DATABASE_URL = "sqlite+aiosqlite:///bot.db"

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# === MODELS ===
class User(Base):
    __tablename__ = 'users'
    id = Column(BigInteger, primary_key=True)
    balance = Column(Numeric(18, 6), default=0.0)
    balance_in_process = Column(Numeric(18, 6), default=0.0)
    daily_profit = Column(Numeric(18, 6), default=0.0)
    total_profit = Column(Numeric(18, 6), default=0.0)
    referral_count = Column(Integer, default=0)
    referral_earnings = Column(Numeric(18, 6), default=0.0)
    referrer_id = Column(BigInteger, nullable=True)
    wallet_address = Column(String)
    wallet_network = Column(String)
    preferred_language = Column(String, nullable=True)
    joined_at = Column(DateTime, default=datetime.utcnow)


class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    ref = Column(String)            # random 5-digit reference
    type = Column(String)           # 'invest' or 'withdraw' or 'profit' or 'trade'
    amount = Column(Numeric(18, 6))
    status = Column(String)         # 'pending','credited','rejected','completed'
    proof = Column(String)          # txid or file_id (format: 'photo:<file_id>' for photos)
    wallet = Column(String)
    network = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserTradeConfig(Base):
    __tablename__ = 'user_trade_configs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, unique=True)
    pair = Column(String)  # e.g., 'BTCUSDT'
    percent_per_trade = Column(Numeric(10, 6))  # e.g., 0.5 for 0.5%
    created_at = Column(DateTime, default=datetime.utcnow)


class DailySummary(Base):
    __tablename__ = 'daily_summaries'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    date = Column(DateTime)  # date for the summary
    daily_percent = Column(Numeric(10, 6))  # percent gained
    profit_amount = Column(Numeric(18, 6))  # profit in USDT
    total_balance = Column(Numeric(18, 6))  # balance at end of day
    created_at = Column(DateTime, default=datetime.utcnow)


class Config(Base):
    __tablename__ = 'config'
    key = Column(String, primary_key=True)
    value = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DepositWallet(Base):
    __tablename__ = 'deposit_wallets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    coin = Column(String)  # e.g., 'USDT', 'BTC', 'ETH'
    network = Column(String)  # e.g., 'TRC20', 'ERC20', 'BTC'
    address = Column(String)
    is_primary = Column(Integer, default=0)  # 1 for primary, 0 for secondary
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ErrorLog(Base):
    __tablename__ = 'error_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    error_type = Column(String)  # Exception type
    error_message = Column(String)  # Error message
    user_id = Column(BigInteger, nullable=True)  # User who triggered the error (if applicable)
    command = Column(String, nullable=True)  # Command that caused the error
    traceback = Column(String)  # Full traceback
    created_at = Column(DateTime, default=datetime.utcnow)


# DB init helpers
async def _create_all_with_timeout(engine_to_use):
    async with engine_to_use.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def ensure_columns():
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_network VARCHAR"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS preferred_language VARCHAR"))
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS proof VARCHAR"))
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS wallet VARCHAR"))
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS network VARCHAR"))
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS ref VARCHAR"))
        except Exception:
            pass

async def init_db(retries: int = 5, backoff: float = 2.0, fallback_to_sqlite: bool = True):
    global engine, async_session, DATABASE_URL
    last_exc = None
    attempt = 0
    current_engine = engine
    while attempt < retries:
        attempt += 1
        try:
            await _create_all_with_timeout(current_engine)
            try:
                await ensure_columns()
            except Exception as col_exc:
                logger.warning("ensure_columns warning: %s", col_exc)
            logger.info("Database initialized successfully.")
            return
        except Exception as e:
            last_exc = e
            logger.warning("Database init attempt %d/%d failed: %s", attempt, retries, e)
            await asyncio.sleep(backoff * (2 ** (attempt - 1)))

    logger.error("All %d database init attempts failed. Last error: %s", retries, last_exc)
    if fallback_to_sqlite:
        try:
            sqlite_url = "sqlite+aiosqlite:///bot_fallback.db"
            logger.warning("Falling back to sqlite DB at %s", sqlite_url)
            from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
            DATABASE_URL = sqlite_url
            engine = _create_async_engine(DATABASE_URL, echo=False, future=True)
            async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            await _create_all_with_timeout(engine)
            try:
                await ensure_columns()
            except Exception as col_exc:
                logger.warning("ensure_columns fallback warning: %s", col_exc)
            logger.info("Fallback sqlite DB initialized.")
            return
        except Exception as e2:
            logger.exception("Fallback to sqlite failed: %s", e2)

    logger.critical("Unable to initialize database and fallback failed — exiting.")
    raise SystemExit(1)

# DB helpers
async def get_user(session: AsyncSession, user_id: int) -> Dict:
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        user = User(id=user_id)
        session.add(user)
        await session.commit()
        return await get_user(session, user_id)
    return {c.name: getattr(user, c.name) for c in user.__table__.columns}

async def update_user(session: AsyncSession, user_id: int, **kwargs):
    await session.execute(sa_update(User).where(User.id == user_id).values(**kwargs))
    await session.commit()

async def log_transaction(session: AsyncSession, **data):
    if 'ref' not in data or not data.get('ref'):
        data['ref'] = f"{random.randint(10000,99999)}"
    tx = Transaction(**data)
    session.add(tx)
    await session.commit()
    await session.refresh(tx)
    return tx.id, data['ref']

# User trade config helpers
async def get_user_trade_config(session: AsyncSession, user_id: int) -> Optional[Dict]:
    """Get per-user trade configuration"""
    result = await session.execute(select(UserTradeConfig).where(UserTradeConfig.user_id == user_id))
    config = result.scalar_one_or_none()
    if not config:
        return None
    return {
        'pair': config.pair,
        'percent_per_trade': float(config.percent_per_trade),
    }

async def set_user_trade_config(session: AsyncSession, user_id: int, pair: str, percent_per_trade: float):
    """Set per-user trade configuration"""
    result = await session.execute(select(UserTradeConfig).where(UserTradeConfig.user_id == user_id))
    config = result.scalar_one_or_none()
    if config:
        config.pair = pair
        config.percent_per_trade = percent_per_trade
    else:
        config = UserTradeConfig(user_id=user_id, pair=pair, percent_per_trade=percent_per_trade)
        session.add(config)
    await session.commit()

# Config helpers for range management
async def get_config(session: AsyncSession, key: str, default: Optional[str] = None) -> Optional[str]:
    """Get config value by key"""
    result = await session.execute(select(Config).where(Config.key == key))
    config = result.scalar_one_or_none()
    return config.value if config else default

async def set_config(session: AsyncSession, key: str, value: str):
    """Set config value by key"""
    result = await session.execute(select(Config).where(Config.key == key))
    config = result.scalar_one_or_none()
    if config:
        config.value = value
        config.updated_at = datetime.utcnow()
    else:
        config = Config(key=key, value=value)
        session.add(config)
    await session.commit()

# Deposit wallet helpers
async def get_deposit_wallets(session: AsyncSession, coin: Optional[str] = None) -> List[Dict]:
    """Get all deposit wallets, optionally filtered by coin"""
    if coin:
        result = await session.execute(
            select(DepositWallet).where(DepositWallet.coin == coin.upper()).order_by(DepositWallet.is_primary.desc(), DepositWallet.created_at)
        )
    else:
        result = await session.execute(select(DepositWallet).order_by(DepositWallet.coin, DepositWallet.is_primary.desc()))
    wallets = result.scalars().all()
    return [
        {
            'id': w.id,
            'coin': w.coin,
            'network': w.network,
            'address': w.address,
            'is_primary': bool(w.is_primary),
            'created_at': w.created_at
        }
        for w in wallets
    ]

async def get_primary_deposit_wallet(session: AsyncSession, coin: str) -> Optional[Dict]:
    """Get the primary deposit wallet for a specific coin"""
    result = await session.execute(
        select(DepositWallet).where(
            DepositWallet.coin == coin.upper(),
            DepositWallet.is_primary == 1
        )
    )
    wallet = result.scalar_one_or_none()
    if wallet:
        return {
            'id': wallet.id,
            'coin': wallet.coin,
            'network': wallet.network,
            'address': wallet.address,
            'is_primary': True
        }
    
    # If no primary wallet, return the first wallet for this coin
    result = await session.execute(
        select(DepositWallet).where(DepositWallet.coin == coin.upper()).order_by(DepositWallet.created_at)
    )
    wallet = result.scalar_one_or_none()
    if wallet:
        return {
            'id': wallet.id,
            'coin': wallet.coin,
            'network': wallet.network,
            'address': wallet.address,
            'is_primary': False
        }
    return None

async def set_deposit_wallet(session: AsyncSession, coin: str, network: str, address: str, is_primary: bool = False):
    """Add or update a deposit wallet"""
    coin = coin.upper()
    
    # If setting as primary, unmark all other wallets for this coin
    if is_primary:
        await session.execute(
            sa_update(DepositWallet).where(DepositWallet.coin == coin).values(is_primary=0)
        )
    
    # Check if wallet already exists
    result = await session.execute(
        select(DepositWallet).where(
            DepositWallet.coin == coin,
            DepositWallet.network == network,
            DepositWallet.address == address
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        existing.is_primary = 1 if is_primary else 0
        existing.updated_at = datetime.utcnow()
    else:
        wallet = DepositWallet(
            coin=coin,
            network=network,
            address=address,
            is_primary=1 if is_primary else 0
        )
        session.add(wallet)
    
    await session.commit()

# Error logging helper
async def log_error(error_type: str, error_message: str, user_id: Optional[int] = None, 
                   command: Optional[str] = None, traceback_str: Optional[str] = None):
    """Log an error to the database"""
    try:
        async with async_session() as session:
            error_log = ErrorLog(
                error_type=error_type,
                error_message=error_message[:500],  # Limit message length
                user_id=user_id,
                command=command,
                traceback=traceback_str[:2000] if traceback_str else None,  # Limit traceback length
                created_at=datetime.utcnow()
            )
            session.add(error_log)
            await session.commit()
    except Exception as e:
        logger.exception(f"Failed to log error to database: {e}")

async def mark_primary_deposit_wallet(session: AsyncSession, wallet_id: int):
    """Mark a deposit wallet as primary"""
    result = await session.execute(select(DepositWallet).where(DepositWallet.id == wallet_id))
    wallet = result.scalar_one_or_none()
    
    if not wallet:
        return False
    
    # Unmark all other wallets for this coin
    await session.execute(
        sa_update(DepositWallet).where(DepositWallet.coin == wallet.coin).values(is_primary=0)
    )
    
    # Mark this wallet as primary
    wallet.is_primary = 1
    wallet.updated_at = datetime.utcnow()
    await session.commit()
    return True

async def delete_deposit_wallet(session: AsyncSession, wallet_id: int):
    """Delete a deposit wallet"""
    result = await session.execute(select(DepositWallet).where(DepositWallet.id == wallet_id))
    wallet = result.scalar_one_or_none()
    
    if wallet:
        await session.delete(wallet)
        await session.commit()
        return True
    return False

# Binance price cache with TTL
_binance_price_cache = {}  # {symbol: (price, timestamp)}
_binance_cache_lock = asyncio.Lock()

async def fetch_binance_price(symbol: str) -> Optional[float]:
    """
    Fetch price from Binance API with TTL cache.
    Returns None if fetch fails or if USE_BINANCE is disabled.
    """
    global _binance_price_cache
    
    # Check if Binance is enabled
    if not USE_BINANCE:
        logger.debug(f"Binance API disabled, skipping fetch for {symbol}")
        return None
    
    async with _binance_cache_lock:
        # Check cache
        if symbol in _binance_price_cache:
            price, timestamp = _binance_price_cache[symbol]
            if (datetime.utcnow() - timestamp).total_seconds() < BINANCE_CACHE_TTL:
                logger.debug(f"Cache hit for {symbol}: {price}")
                return price
        
        # Fetch from Binance
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(BINANCE_API_URL, params={'symbol': symbol})
                response.raise_for_status()
                data = response.json()
                price = float(data['price'])
                _binance_price_cache[symbol] = (price, datetime.utcnow())
                logger.info(f"Fetched Binance price for {symbol}: {price}")
                return price
        except Exception as e:
            logger.warning(f"Failed to fetch Binance price for {symbol}: {e}")
            return None

# Conversation states
INVEST_AMOUNT, INVEST_NETWORK, INVEST_PROOF, INVEST_CONFIRM, WITHDRAW_AMOUNT, WITHDRAW_WALLET, WITHDRAW_CONFIRM, HISTORY_PAGE, HISTORY_DETAILS = range(9)

# -----------------------
# I18N: translations and helpers
# -----------------------
TRANSLATIONS = {
    "en": {
        "main_menu_title": "📋 Main Menu",
        "settings_title": "⚙️ Settings",
        "settings_language": "🌍 Language",
        "change_language": "Change Language",
        "settings_wallet": "💳 Set/Update Withdrawal Wallet",
        "select_option": "Select an option:",
        "back_to_menu": "« Back to Menu",
        "lang_auto": "🔄 Auto (from Telegram)",
        "lang_en": "🇬🇧 English",
        "lang_fr": "🇫🇷 Français",
        "lang_es": "🇪🇸 Español",
        "lang_ar": "🇸🇦 العربية",
        "lang_zh": "🇨🇳 中文",
        "lang_set_success": "✅ Language changed successfully!",
        "lang_current": "Current language: {lang}",
        "welcome_text": (
            "🎉 <b>Welcome to Nexo Trading Bot!</b>\n\n"
            "🤖 Your Personal AI Trading Assistant\n"
            "💹 Automated Crypto Trading 24/7\n"
            "📊 Daily Profit: 1.25% - 1.5%\n\n"
            "👇 Select an option below to get started"
        ),
        "info_text": (
            "🚀 <b>Welcome to Nexo Trading Bot - Your Smart Path to Crypto Growth!</b>\n\n"
            "Ready to effortlessly grow your crypto investments? Nexo Trading Bot harnesses cutting-edge Artificial Intelligence to trade cryptocurrencies 24/7, aiming for consistent daily profits. Our unique multi-level analytical services mean the AI is constantly learning and improving, making every trade smarter than the last.\n\n"
            "<b>Why Choose Nexo Trading Bot?</b>\n\n"
            "• 🤖 <b>AI-Powered Trading:</b> Advanced algorithms work around the clock, taking the guesswork out of crypto trading for you.\n\n"
            "• 📈 <b>Daily Returns:</b> Enjoy competitive daily profits ranging from 1.25% to 1.5% on your investments.\n\n"
            "• 🔒 <b>Secure &amp; Automated:</b> Your funds are managed securely, and trading is fully automated – just deposit and watch your portfolio grow!\n\n"
            "• 🌐 <b>Widely Connected:</b> We integrate with leading crypto exchanges like Binance and Coinbase, offering you a robust and reliable platform.\n\n"
            "• 📊 <b>Transparent &amp; Informed:</b> Receive real-time notifications for every completed trade and a detailed daily report on your profit, amount, and balance.\n\n"
            "• 💸 <b>Flexible Withdrawals:</b> Request payouts to your wallet anytime after your first 24 hours. Requests are processed promptly within 12 hours.\n\n"
            "• 👨‍💼 <b>Dedicated Support:</b> Get personalized assistance from your assigned personal manager, with 24/7 support available.\n\n"
            "<b>Getting Started is Simple:</b>\n\n"
            "1. <b>Register &amp; Deposit:</b> To deposit, tap \"Invest,\" enter the amount you wish to deposit, select your preferred crypto network, copy the displayed wallet address, send the funds, confirm your deposit, and wait for blockchain confirmation.\n\n"
            "2. <b>Activate &amp; Trade:</b> Your deposit becomes active from the next trading cycle, and the AI begins trading for you automatically.\n\n"
            "3. <b>Track Your Progress:</b> Monitor your trading history in \"My History\" for the last 14 days and receive daily performance reports.\n\n"
            "4. <b>Withdraw Your Profits:</b> Easily request a payout whenever you're ready!\n\n"
            "Nexo Trading Bot is more than just a bot; it's your personal, intelligent trading partner designed for public use, making crypto investment accessible and profitable for everyone."
        ),
        # Invest flow translations
        "invest_enter_amount": "📈 Enter the amount you want to invest (minimum 10 USDT, e.g., 100.50). Send /cancel to abort.",
        "invest_invalid_amount": "Invalid amount. Send a positive number like 100 or 50.50, or /cancel.",
        "invest_minimum_amount": "❌ Minimum deposit is 10 USDT. Please enter at least 10 USDT or /cancel.",
        "invest_send_proof": "📥 Deposit {amount:.2f}$\nSend to wallet:\nWallet: <code>{wallet}</code>\nNetwork: <b>{network}</b>\n\nAfter sending, upload a screenshot OR send the transaction hash (txid).",
        "invest_no_amount": "No pending invest amount. Start again with /invest.",
        "invest_upload_proof": "Please upload a screenshot or send the txid, or /cancel.",
        "invest_confirm_prompt": "Proof received: <code>{proof}</code>\nIf you sent exactly {amount:.2f}$, press confirm. Otherwise Cancel.",
        "invest_confirm_yes": "✅ I sent the exact amount",
        "invest_confirm_no": "❌ Cancel",
        "invest_missing_data": "Missing data. Restart invest flow.",
        "invest_request_success": "🧾 Deposit Request Successful\nTransaction ID: D-{ref}\nAmount: {amount:.2f} USDT ({network})\nWallet: {wallet}\nNetwork: {network}\nStatus: Pending Approval\nDate: {date}\n\nOnce confirmed, your balance will be updated automatically.",
        # Withdraw flow translations
        "withdraw_enter_amount": "💸 Enter the amount you want to withdraw (numbers only). Send /cancel to abort.",
        "withdraw_invalid_amount": "Invalid amount. Send a positive number like 50 or 25.75, or /cancel.",
        "withdraw_insufficient": "Insufficient balance. Available: {balance:.2f}$. Enter smaller amount or /cancel.",
        "withdraw_saved_wallet": "Your saved wallet:\n<code>{wallet}</code>\nNetwork: <b>{network}</b>\n\nOr send a new wallet and optional network.",
        "withdraw_no_saved": "No saved wallet. Send wallet address and optional network (e.g., 0xabc... ERC20).",
        "withdraw_use_saved": "Use saved wallet",
        "withdraw_no_saved_found": "No saved wallet found. Please send wallet address.",
        "withdraw_send_wallet": "Please send wallet address and optional network.",
        "withdraw_looks_command": "Looks like a command. Send only the wallet address and optional network.",
        "withdraw_invalid_wallet": "This address doesn't look valid. Send 'yes' to save anyway or send correct address.",
        "withdraw_confirm_prompt": "Confirm withdrawal:\nAmount: {amount:.2f}$\nWallet: <code>{wallet}</code>\nNetwork: <b>{network}</b>",
        "withdraw_confirm_yes": "✅ Confirm",
        "withdraw_confirm_no": "❌ Cancel",
        "withdraw_wallet_saved": "✅ Wallet saved:\n<code>{wallet}</code>\nNetwork: {network}",
        "withdraw_missing_data": "Missing withdrawal data. Start withdraw again.",
        "withdraw_request_success": "🧾 Withdrawal Request Successful\nTransaction ID: W-{ref}\nAmount: {amount:.2f} USDT ({network})\nWallet: {wallet}\nNetwork: {network}\nStatus: Pending Approval\nDate: {date}\n\nOnce confirmed by admin, your withdrawal will be processed.",
        # Referral translations
        "referral_title": "👥 Referral Program",
        "referral_intro": "Share your referral link and earn rewards!",
        "referral_link_label": "🔗 Your Link:",
        "referral_tap_to_copy": "👆 Tap the link above to copy it",
        "referral_stats_title": "📊 Your Stats:",
        "referral_total_count": "👤 Total Referrals: {count}",
        "referral_earnings": "💰 Earnings: {earnings:.2f} USDT",
        "referral_how_it_works": "💡 How it works:",
        "referral_step1": "• Share your link with friends",
        "referral_step2": "• Earn 2% commission on their first deposit",
        "referral_step3": "• Earnings added to your balance instantly",
        "referral_commission_info": "🎁 Get 2% of your friend's first deposit!",
        # Wallet command
        "wallet_saved": "Saved wallet:\n<code>{wallet}</code>\nNetwork: {network}",
        "wallet_use_button": "Use this wallet for next withdrawal",
        "wallet_not_saved": "No withdrawal wallet saved. Set it with /wallet <address> [network]",
        "wallet_send_address": "Send your withdrawal wallet address and optional network (e.g., 0xabc... ERC20).",
        # Help command
        "help_message": "Need assistance? Click below to chat with support:",
        "help_button": "💬 Open Support Chat",
        # Balance page
        "balance_title": "Your Account Balance",
        "balance_available": "Available",
        "balance_in_process": "In Process",
        "balance_today_profit": "Today's Profit",
        "balance_total_profit": "Total Profit",
        "balance_manager": "Manager",
        # History page
        "history_no_transactions": "🧾 History: no transactions found.",
        "history_prev": "⬅ Prev",
        "history_next": "Next ➡",
        "history_exit": "Exit ❌",
    },
    "fr": {
        "main_menu_title": "📋 Menu Principal",
        "settings_title": "⚙️ Paramètres",
        "settings_language": "🌍 Langue",
        "change_language": "Changer la langue",
        "settings_wallet": "💳 Définir/Mettre à jour le portefeuille de retrait",
        "select_option": "Sélectionnez une option:",
        "back_to_menu": "« Retour au menu",
        "lang_auto": "🔄 Auto (Telegram)",
        "lang_en": "🇬🇧 Anglais",
        "lang_fr": "🇫🇷 Français",
        "lang_es": "🇪🇸 Espagnol",
        "lang_ar": "🇸🇦 العربية",
        "lang_set_success": "✅ Langue modifiée avec succès!",
        "lang_current": "Langue actuelle : {lang}",
        "welcome_text": (
            "🎉 <b>Bienvenue sur Nexo Trading Bot!</b>\n\n"
            "🤖 Votre Assistant de Trading IA Personnel\n"
            "💹 Trading Crypto Automatisé 24/7\n"
            "📊 Profit Quotidien: 1.25% - 1.5%\n\n"
            "👇 Sélectionnez une option ci-dessous pour commencer"
        ),
        "info_text": (
            "🚀 <b>Bienvenue sur Nexo Trading Bot - Votre Chemin Intelligent vers la Croissance Crypto!</b>\n\n"
            "Prêt à développer sans effort vos investissements crypto? Nexo Trading Bot exploite l'Intelligence Artificielle de pointe pour trader les cryptomonnaies 24/7, visant des profits quotidiens constants. Nos services analytiques multi-niveaux uniques signifient que l'IA apprend et s'améliore constamment, rendant chaque trade plus intelligent que le précédent.\n\n"
            "<b>Pourquoi Choisir Nexo Trading Bot?</b>\n\n"
            "• 🤖 <b>Trading Alimenté par l'IA:</b> Des algorithmes avancés travaillent 24h/24, éliminant les conjectures du trading crypto pour vous.\n\n"
            "• 📈 <b>Rendements Quotidiens:</b> Profitez de profits quotidiens compétitifs allant de 1,25% à 1,5% sur vos investissements.\n\n"
            "• 🔒 <b>Sécurisé &amp; Automatisé:</b> Vos fonds sont gérés en toute sécurité, et le trading est entièrement automatisé – déposez simplement et regardez votre portefeuille croître!\n\n"
            "• 🌐 <b>Largement Connecté:</b> Nous nous intégrons avec les principales bourses crypto comme Binance et Coinbase, vous offrant une plateforme robuste et fiable.\n\n"
            "• 📊 <b>Transparent &amp; Informé:</b> Recevez des notifications en temps réel pour chaque trade complété et un rapport quotidien détaillé sur vos profits, montant et solde.\n\n"
            "• 💸 <b>Retraits Flexibles:</b> Demandez des paiements vers votre portefeuille à tout moment après vos premières 24 heures. Les demandes sont traitées rapidement sous 12 heures.\n\n"
            "• 👨‍💼 <b>Support Dédié:</b> Obtenez une assistance personnalisée de votre gestionnaire personnel assigné, avec un support 24/7 disponible.\n\n"
            "<b>Commencer est Simple:</b>\n\n"
            "1. <b>S'inscrire &amp; Déposer:</b> Pour déposer, appuyez sur \"Investir\", entrez le montant que vous souhaitez déposer, sélectionnez votre réseau crypto préféré, copiez l'adresse du portefeuille affichée, envoyez les fonds, confirmez votre dépôt et attendez la confirmation blockchain.\n\n"
            "2. <b>Activer &amp; Trader:</b> Votre dépôt devient actif dès le prochain cycle de trading, et l'IA commence à trader pour vous automatiquement.\n\n"
            "3. <b>Suivre Votre Progression:</b> Surveillez votre historique de trading dans \"Mon Historique\" pour les 14 derniers jours et recevez des rapports de performance quotidiens.\n\n"
            "4. <b>Retirer Vos Profits:</b> Demandez facilement un paiement quand vous êtes prêt!\n\n"
            "Nexo Trading Bot est plus qu'un simple bot; c'est votre partenaire de trading intelligent personnel conçu pour un usage public, rendant l'investissement crypto accessible et rentable pour tous."
        ),
        # Invest flow translations
        "invest_enter_amount": "📈 Entrez le montant que vous souhaitez investir (minimum 10 USDT, ex: 100.50). Envoyez /cancel pour annuler.",
        "invest_invalid_amount": "Montant invalide. Envoyez un nombre positif comme 100 ou 50.50, ou /cancel.",
        "invest_minimum_amount": "❌ Le dépôt minimum est de 10 USDT. Veuillez entrer au moins 10 USDT ou /cancel.",
        "invest_send_proof": "📥 Dépôt de {amount:.2f}$\nEnvoyez à:\nPortefeuille: <code>{wallet}</code>\nRéseau: <b>{network}</b>\n\nAprès l'envoi, téléchargez une capture d'écran OU envoyez le hash de transaction (txid).",
        "invest_no_amount": "Aucun montant d'investissement en attente. Recommencez avec /invest.",
        "invest_upload_proof": "Veuillez télécharger une capture d'écran ou envoyer le txid, ou /cancel.",
        "invest_confirm_prompt": "Preuve reçue: <code>{proof}</code>\nSi vous avez envoyé exactement {amount:.2f}$, appuyez sur confirmer. Sinon Annuler.",
        "invest_confirm_yes": "✅ J'ai envoyé le montant exact",
        "invest_confirm_no": "❌ Annuler",
        "invest_missing_data": "Données manquantes. Redémarrez le flux d'investissement.",
        "invest_request_success": "🧾 Demande de Dépôt Réussie\nID de transaction: D-{ref}\nMontant: {amount:.2f} USDT ({network})\nPortefeuille: {wallet}\nRéseau: {network}\nStatut: En attente d'approbation\nDate: {date}\n\nUne fois confirmé, votre solde sera mis à jour automatiquement.",
        # Withdraw flow translations
        "withdraw_enter_amount": "💸 Entrez le montant que vous souhaitez retirer (chiffres uniquement). Envoyez /cancel pour annuler.",
        "withdraw_invalid_amount": "Montant invalide. Envoyez un nombre positif comme 50 ou 25.75, ou /cancel.",
        "withdraw_insufficient": "Solde insuffisant. Disponible: {balance:.2f}$. Entrez un montant plus petit ou /cancel.",
        "withdraw_saved_wallet": "Votre portefeuille enregistré:\n<code>{wallet}</code>\nRéseau: <b>{network}</b>\n\nOu envoyez un nouveau portefeuille et réseau facultatif.",
        "withdraw_no_saved": "Aucun portefeuille enregistré. Envoyez l'adresse du portefeuille et le réseau facultatif (ex: 0xabc... ERC20).",
        "withdraw_use_saved": "Utiliser le portefeuille enregistré",
        "withdraw_no_saved_found": "Aucun portefeuille enregistré trouvé. Veuillez envoyer l'adresse du portefeuille.",
        "withdraw_send_wallet": "Veuillez envoyer l'adresse du portefeuille et le réseau facultatif.",
        "withdraw_looks_command": "Ressemble à une commande. Envoyez uniquement l'adresse du portefeuille et le réseau facultatif.",
        "withdraw_invalid_wallet": "Cette adresse ne semble pas valide. Envoyez 'yes' pour enregistrer quand même ou envoyez la bonne adresse.",
        "withdraw_confirm_prompt": "Confirmer le retrait:\nMontant: {amount:.2f}$\nPortefeuille: <code>{wallet}</code>\nRéseau: <b>{network}</b>",
        "withdraw_confirm_yes": "✅ Confirmer",
        "withdraw_confirm_no": "❌ Annuler",
        "withdraw_wallet_saved": "✅ Portefeuille enregistré:\n<code>{wallet}</code>\nRéseau: {network}",
        "withdraw_missing_data": "Données de retrait manquantes. Recommencez le retrait.",
        "withdraw_request_success": "🧾 Demande de Retrait Réussie\nID de transaction: W-{ref}\nMontant: {amount:.2f} USDT ({network})\nPortefeuille: {wallet}\nRéseau: {network}\nStatut: En attente d'approbation\nDate: {date}\n\nUne fois confirmé par l'admin, votre retrait sera traité.",
        # Referral translations
        "referral_title": "👥 Programme de Parrainage",
        "referral_intro": "Partagez votre lien de parrainage et gagnez des récompenses!",
        "referral_link_label": "🔗 Votre Lien:",
        "referral_tap_to_copy": "👆 Appuyez sur le lien ci-dessus pour le copier",
        "referral_stats_title": "📊 Vos Statistiques:",
        "referral_total_count": "👤 Total Parrainages: {count}",
        "referral_earnings": "💰 Gains: {earnings:.2f} USDT",
        "referral_how_it_works": "💡 Comment ça marche:",
        "referral_step1": "• Partagez votre lien avec vos amis",
        "referral_step2": "• Gagnez 2% de commission sur leur premier dépôt",
        "referral_step3": "• Gains ajoutés à votre solde instantanément",
        "referral_commission_info": "🎁 Obtenez 2% du premier dépôt de votre ami!",
        # Wallet command
        "wallet_saved": "Portefeuille enregistré:\n<code>{wallet}</code>\nRéseau: {network}",
        "wallet_use_button": "Utiliser ce portefeuille pour le prochain retrait",
        "wallet_not_saved": "Aucun portefeuille de retrait enregistré. Configurez-le avec /wallet <adresse> [réseau]",
        "wallet_send_address": "Envoyez l'adresse de votre portefeuille de retrait et le réseau facultatif (ex: 0xabc... ERC20).",
        # Help command
        "help_message": "Besoin d'aide? Cliquez ci-dessous pour discuter avec le support:",
        "help_button": "💬 Ouvrir le Chat de Support",
        # Balance page
        "balance_title": "Solde de Votre Compte",
        "balance_available": "Disponible",
        "balance_in_process": "En Cours",
        "balance_today_profit": "Profit d'Aujourd'hui",
        "balance_total_profit": "Profit Total",
        "balance_manager": "Gestionnaire",
        # History page
        "history_no_transactions": "🧾 Historique: aucune transaction trouvée.",
        "history_prev": "⬅ Préc",
        "history_next": "Suiv ➡",
        "history_exit": "Quitter ❌",
    },
    "es": {
        "main_menu_title": "📋 Menú Principal",
        "settings_title": "⚙️ Configuración",
        "settings_language": "🌍 Idioma",
        "change_language": "Cambiar idioma",
        "settings_wallet": "💳 Establecer/Actualizar billetera de retiro",
        "select_option": "Selecciona una opción:",
        "back_to_menu": "« Volver al menú",
        "lang_auto": "🔄 Auto (Telegram)",
        "lang_en": "🇬🇧 Inglés",
        "lang_fr": "🇫🇷 Francés",
        "lang_es": "🇪🇸 Español",
        "lang_ar": "🇸🇦 العربية",
        "lang_set_success": "✅ ¡Idioma cambiado exitosamente!",
        "lang_current": "Idioma actual: {lang}",
        "welcome_text": (
            "🎉 <b>¡Bienvenido a Nexo Trading Bot!</b>\n\n"
            "🤖 Tu Asistente de Trading IA Personal\n"
            "💹 Trading Cripto Automatizado 24/7\n"
            "📊 Ganancias Diarias: 1.25% - 1.5%\n\n"
            "👇 Selecciona una opción a continuación para empezar"
        ),
        "info_text": (
            "🚀 <b>¡Bienvenido a Nexo Trading Bot - Tu Camino Inteligente hacia el Crecimiento Cripto!</b>\n\n"
            "¿Listo para hacer crecer sin esfuerzo tus inversiones cripto? Nexo Trading Bot aprovecha la Inteligencia Artificial de vanguardia para operar criptomonedas 24/7, buscando ganancias diarias consistentes. Nuestros servicios analíticos multinivel únicos significan que la IA está aprendiendo y mejorando constantemente, haciendo cada operación más inteligente que la anterior.\n\n"
            "<b>¿Por Qué Elegir Nexo Trading Bot?</b>\n\n"
            "• 🤖 <b>Trading Impulsado por IA:</b> Algoritmos avanzados trabajan las 24 horas, eliminando las conjeturas del trading cripto para ti.\n\n"
            "• 📈 <b>Retornos Diarios:</b> Disfruta de ganancias diarias competitivas que van del 1.25% al 1.5% en tus inversiones.\n\n"
            "• 🔒 <b>Seguro &amp; Automatizado:</b> Tus fondos se gestionan de forma segura, y el trading está completamente automatizado – ¡solo deposita y observa crecer tu cartera!\n\n"
            "• 🌐 <b>Ampliamente Conectado:</b> Nos integramos con los principales exchanges cripto como Binance y Coinbase, ofreciéndote una plataforma robusta y confiable.\n\n"
            "• 📊 <b>Transparente &amp; Informado:</b> Recibe notificaciones en tiempo real para cada operación completada y un informe diario detallado sobre tus ganancias, cantidad y saldo.\n\n"
            "• 💸 <b>Retiros Flexibles:</b> Solicita pagos a tu billetera en cualquier momento después de tus primeras 24 horas. Las solicitudes se procesan rápidamente en 12 horas.\n\n"
            "• 👨‍💼 <b>Soporte Dedicado:</b> Obtén asistencia personalizada de tu gestor personal asignado, con soporte 24/7 disponible.\n\n"
            "<b>Comenzar es Simple:</b>\n\n"
            "1. <b>Registrarse &amp; Depositar:</b> Para depositar, toca \"Invertir\", ingresa la cantidad que deseas depositar, selecciona tu red cripto preferida, copia la dirección de billetera mostrada, envía los fondos, confirma tu depósito y espera la confirmación blockchain.\n\n"
            "2. <b>Activar &amp; Operar:</b> Tu depósito se activa desde el próximo ciclo de trading, y la IA comienza a operar para ti automáticamente.\n\n"
            "3. <b>Seguir Tu Progreso:</b> Monitorea tu historial de trading en \"Mi Historial\" de los últimos 14 días y recibe informes de rendimiento diarios.\n\n"
            "4. <b>Retirar Tus Ganancias:</b> ¡Solicita fácilmente un pago cuando estés listo!\n\n"
            "Nexo Trading Bot es más que un simple bot; es tu socio de trading inteligente personal diseñado para uso público, haciendo la inversión cripto accesible y rentable para todos."
        ),
        # Invest flow translations
        "invest_enter_amount": "📈 Ingresa el monto que deseas invertir (mínimo 10 USDT, ej: 100.50). Envía /cancel para cancelar.",
        "invest_invalid_amount": "Monto inválido. Envía un número positivo como 100 o 50.50, o /cancel.",
        "invest_minimum_amount": "❌ El depósito mínimo es 10 USDT. Por favor ingresa al menos 10 USDT o /cancel.",
        "invest_send_proof": "📥 Depósito de {amount:.2f}$\nEnviar a:\nBilletera: <code>{wallet}</code>\nRed: <b>{network}</b>\n\nDespués de enviar, sube una captura de pantalla O envía el hash de transacción (txid).",
        "invest_no_amount": "No hay monto de inversión pendiente. Comienza de nuevo con /invest.",
        "invest_upload_proof": "Por favor sube una captura de pantalla o envía el txid, o /cancel.",
        "invest_confirm_prompt": "Comprobante recibido: <code>{proof}</code>\nSi enviaste exactamente {amount:.2f}$, presiona confirmar. De lo contrario Cancelar.",
        "invest_confirm_yes": "✅ Envié el monto exacto",
        "invest_confirm_no": "❌ Cancelar",
        "invest_missing_data": "Datos faltantes. Reinicia el flujo de inversión.",
        "invest_request_success": "🧾 Solicitud de Depósito Exitosa\nID de transacción: D-{ref}\nMonto: {amount:.2f} USDT ({network})\nBilletera: {wallet}\nRed: {network}\nEstado: Pendiente de aprobación\nFecha: {date}\n\nUna vez confirmado, tu saldo se actualizará automáticamente.",
        # Withdraw flow translations
        "withdraw_enter_amount": "💸 Ingresa el monto que deseas retirar (solo números). Envía /cancel para cancelar.",
        "withdraw_invalid_amount": "Monto inválido. Envía un número positivo como 50 o 25.75, o /cancel.",
        "withdraw_insufficient": "Saldo insuficiente. Disponible: {balance:.2f}$. Ingresa un monto menor o /cancel.",
        "withdraw_saved_wallet": "Tu billetera guardada:\n<code>{wallet}</code>\nRed: <b>{network}</b>\n\nO envía una nueva billetera y red opcional.",
        "withdraw_no_saved": "No hay billetera guardada. Envía la dirección de billetera y red opcional (ej: 0xabc... ERC20).",
        "withdraw_use_saved": "Usar billetera guardada",
        "withdraw_no_saved_found": "No se encontró billetera guardada. Por favor envía la dirección de billetera.",
        "withdraw_send_wallet": "Por favor envía la dirección de billetera y red opcional.",
        "withdraw_looks_command": "Parece un comando. Envía solo la dirección de billetera y red opcional.",
        "withdraw_invalid_wallet": "Esta dirección no parece válida. Envía 'yes' para guardar de todos modos o envía la dirección correcta.",
        "withdraw_confirm_prompt": "Confirmar retiro:\nMonto: {amount:.2f}$\nBilletera: <code>{wallet}</code>\nRed: <b>{network}</b>",
        "withdraw_confirm_yes": "✅ Confirmar",
        "withdraw_confirm_no": "❌ Cancelar",
        "withdraw_wallet_saved": "✅ Billetera guardada:\n<code>{wallet}</code>\nRed: {network}",
        "withdraw_missing_data": "Datos de retiro faltantes. Comienza el retiro de nuevo.",
        "withdraw_request_success": "🧾 Solicitud de Retiro Exitosa\nID de transacción: W-{ref}\nMonto: {amount:.2f} USDT ({network})\nBilletera: {wallet}\nRed: {network}\nEstado: Pendiente de aprobación\nFecha: {date}\n\nUna vez confirmado por el admin, tu retiro será procesado.",
        # Referral translations
        "referral_title": "👥 Programa de Referidos",
        "referral_intro": "¡Comparte tu enlace de referido y gana recompensas!",
        "referral_link_label": "🔗 Tu Enlace:",
        "referral_tap_to_copy": "👆 Toca el enlace arriba para copiarlo",
        "referral_stats_title": "📊 Tus Estadísticas:",
        "referral_total_count": "👤 Total Referidos: {count}",
        "referral_earnings": "💰 Ganancias: {earnings:.2f} USDT",
        "referral_how_it_works": "💡 Cómo funciona:",
        "referral_step1": "• Comparte tu enlace con amigos",
        "referral_step2": "• Gana 2% de comisión en su primer depósito",
        "referral_step3": "• Ganancias añadidas a tu saldo al instante",
        "referral_commission_info": "🎁 ¡Obtén 2% del primer depósito de tu amigo!",
        # Wallet command
        "wallet_saved": "Billetera guardada:\n<code>{wallet}</code>\nRed: {network}",
        "wallet_use_button": "Usar esta billetera para el próximo retiro",
        "wallet_not_saved": "No hay billetera de retiro guardada. Configúrala con /wallet <dirección> [red]",
        "wallet_send_address": "Envía la dirección de tu billetera de retiro y red opcional (ej: 0xabc... ERC20).",
        # Help command
        "help_message": "¿Necesitas ayuda? Haz clic abajo para chatear con soporte:",
        "help_button": "💬 Abrir Chat de Soporte",
        # Balance page
        "balance_title": "Saldo de Tu Cuenta",
        "balance_available": "Disponible",
        "balance_in_process": "En Proceso",
        "balance_today_profit": "Ganancia de Hoy",
        "balance_total_profit": "Ganancia Total",
        "balance_manager": "Gerente",
        # History page
        "history_no_transactions": "🧾 Historial: no se encontraron transacciones.",
        "history_prev": "⬅ Ant",
        "history_next": "Sig ➡",
        "history_exit": "Salir ❌",
    },
    "ar": {
        "main_menu_title": "📋 القائمة الرئيسية",
        "settings_title": "⚙️ الإعدادات",
        "settings_language": "🌍 اللغة",
        "change_language": "تغيير اللغة",
        "settings_wallet": "💳 تعيين/تحديث محفظة السحب",
        "select_option": "اختر خياراً:",
        "back_to_menu": "« العودة إلى القائمة",
        "lang_auto": "🔄 تلقائي (من تيليجرام)",
        "lang_en": "🇬🇧 الإنجليزية",
        "lang_fr": "🇫🇷 الفرنسية",
        "lang_es": "🇪🇸 الإسبانية",
        "lang_ar": "🇸🇦 العربية",
        "lang_set_success": "✅ تم تغيير اللغة بنجاح!",
        "lang_current": "اللغة الحالية: {lang}",
        "welcome_text": (
            "🎉 <b>مرحباً بك في Nexo Trading Bot!</b>\n\n"
            "🤖 مساعدك الشخصي للتداول بالذكاء الاصطناعي\n"
            "💹 تداول العملات المشفرة تلقائياً 24/7\n"
            "📊 الأرباح اليومية: 1.25% - 1.5%\n\n"
            "👇 اختر خياراً أدناه للبدء"
        ),
        "info_text": (
            "🚀 <b>مرحباً بك في Nexo Trading Bot - طريقك الذكي لنمو العملات المشفرة!</b>\n\n"
            "هل أنت مستعد لتنمية استثماراتك في العملات المشفرة بسهولة؟ يستخدم Nexo Trading Bot الذكاء الاصطناعي المتطور لتداول العملات المشفرة على مدار الساعة طوال أيام الأسبوع، بهدف تحقيق أرباح يومية ثابتة. خدماتنا التحليلية متعددة المستويات الفريدة تعني أن الذكاء الاصطناعي يتعلم ويتحسن باستمرار، مما يجعل كل صفقة أذكى من السابقة.\n\n"
            "<b>لماذا تختار Nexo Trading Bot؟</b>\n\n"
            "• 🤖 <b>تداول مدعوم بالذكاء الاصطناعي:</b> خوارزميات متقدمة تعمل على مدار الساعة، تزيل التخمين من تداول العملات المشفرة لك.\n\n"
            "• 📈 <b>عوائد يومية:</b> استمتع بأرباح يومية تنافسية تتراوح من 1.25٪ إلى 1.5٪ على استثماراتك.\n\n"
            "• 🔒 <b>آمن &amp; تلقائي:</b> تُدار أموالك بشكل آمن، والتداول تلقائي بالكامل – فقط أودع وشاهد محفظتك تنمو!\n\n"
            "• 🌐 <b>متصل على نطاق واسع:</b> نتكامل مع بورصات العملات المشفرة الرائدة مثل Binance و Coinbase، نقدم لك منصة قوية وموثوقة.\n\n"
            "• 📊 <b>شفاف &amp; مُطلع:</b> احصل على إشعارات في الوقت الفعلي لكل صفقة مكتملة وتقرير يومي مفصل عن أرباحك ومبلغك ورصيدك.\n\n"
            "• 💸 <b>سحوبات مرنة:</b> اطلب المدفوعات إلى محفظتك في أي وقت بعد أول 24 ساعة. تتم معالجة الطلبات بسرعة خلال 12 ساعة.\n\n"
            "• 👨‍💼 <b>دعم مخصص:</b> احصل على مساعدة شخصية من مديرك الشخصي المعين، مع دعم متاح على مدار الساعة طوال أيام الأسبوع.\n\n"
            "<b>البدء بسيط:</b>\n\n"
            "1. <b>التسجيل &amp; الإيداع:</b> للإيداع، اضغط على \"استثمر\"، أدخل المبلغ الذي تريد إيداعه، حدد شبكة العملات المشفرة المفضلة لديك، انسخ عنوان المحفظة المعروض، أرسل الأموال، أكد إيداعك، وانتظر تأكيد البلوكشين.\n\n"
            "2. <b>التفعيل &amp; التداول:</b> يصبح إيداعك نشطاً من دورة التداول التالية، ويبدأ الذكاء الاصطناعي بالتداول لك تلقائياً.\n\n"
            "3. <b>تتبع تقدمك:</b> راقب سجل تداولك في \"سجلي\" لآخر 14 يوماً واحصل على تقارير أداء يومية.\n\n"
            "4. <b>سحب أرباحك:</b> اطلب بسهولة دفعة عندما تكون مستعداً!\n\n"
            "Nexo Trading Bot هو أكثر من مجرد بوت؛ إنه شريك التداول الذكي الشخصي المصمم للاستخدام العام، مما يجعل استثمار العملات المشفرة في متناول الجميع ومربحاً للجميع."
        ),
        # Invest flow translations
        "invest_enter_amount": "📈 أدخل المبلغ الذي تريد استثماره (الحد الأدنى 10 USDT، مثال: 100.50). أرسل /cancel للإلغاء.",
        "invest_invalid_amount": "مبلغ غير صحيح. أرسل رقماً موجباً مثل 100 أو 50.50، أو /cancel.",
        "invest_minimum_amount": "❌ الحد الأدنى للإيداع هو 10 USDT. الرجاء إدخال 10 USDT على الأقل أو /cancel.",
        "invest_send_proof": "📥 إيداع {amount:.2f}$\nأرسل إلى:\nالمحفظة: <code>{wallet}</code>\nالشبكة: <b>{network}</b>\n\nبعد الإرسال، قم بتحميل لقطة شاشة أو أرسل معرف المعاملة (txid).",
        "invest_no_amount": "لا يوجد مبلغ استثمار معلق. ابدأ من جديد بـ /invest.",
        "invest_upload_proof": "الرجاء تحميل لقطة شاشة أو إرسال txid، أو /cancel.",
        "invest_confirm_prompt": "تم استلام الإثبات: <code>{proof}</code>\nإذا أرسلت بالضبط {amount:.2f}$، اضغط تأكيد. وإلا ألغِ.",
        "invest_confirm_yes": "✅ أرسلت المبلغ الدقيق",
        "invest_confirm_no": "❌ إلغاء",
        "invest_missing_data": "بيانات مفقودة. أعد بدء عملية الاستثمار.",
        "invest_request_success": "🧾 طلب الإيداع ناجح\nمعرف المعاملة: D-{ref}\nالمبلغ: {amount:.2f} USDT ({network})\nالمحفظة: {wallet}\nالشبكة: {network}\nالحالة: في انتظار الموافقة\nالتاريخ: {date}\n\nبمجرد التأكيد، سيتم تحديث رصيدك تلقائياً.",
        # Withdraw flow translations
        "withdraw_enter_amount": "💸 أدخل المبلغ الذي تريد سحبه (أرقام فقط). أرسل /cancel للإلغاء.",
        "withdraw_invalid_amount": "مبلغ غير صحيح. أرسل رقماً موجباً مثل 50 أو 25.75، أو /cancel.",
        "withdraw_insufficient": "رصيد غير كافٍ. المتاح: {balance:.2f}$. أدخل مبلغاً أصغر أو /cancel.",
        "withdraw_saved_wallet": "محفظتك المحفوظة:\n<code>{wallet}</code>\nالشبكة: <b>{network}</b>\n\nأو أرسل محفظة جديدة وشبكة اختيارية.",
        "withdraw_no_saved": "لا توجد محفظة محفوظة. أرسل عنوان المحفظة والشبكة الاختيارية (مثال: 0xabc... ERC20).",
        "withdraw_use_saved": "استخدم المحفظة المحفوظة",
        "withdraw_no_saved_found": "لم يتم العثور على محفظة محفوظة. الرجاء إرسال عنوان المحفظة.",
        "withdraw_send_wallet": "الرجاء إرسال عنوان المحفظة والشبكة الاختيارية.",
        "withdraw_looks_command": "يبدو كأمر. أرسل عنوان المحفظة والشبكة الاختيارية فقط.",
        "withdraw_invalid_wallet": "هذا العنوان لا يبدو صحيحاً. أرسل 'yes' للحفظ على أي حال أو أرسل العنوان الصحيح.",
        "withdraw_confirm_prompt": "تأكيد السحب:\nالمبلغ: {amount:.2f}$\nالمحفظة: <code>{wallet}</code>\nالشبكة: <b>{network}</b>",
        "withdraw_confirm_yes": "✅ تأكيد",
        "withdraw_confirm_no": "❌ إلغاء",
        "withdraw_wallet_saved": "✅ تم حفظ المحفظة:\n<code>{wallet}</code>\nالشبكة: {network}",
        "withdraw_missing_data": "بيانات السحب مفقودة. ابدأ السحب من جديد.",
        "withdraw_request_success": "🧾 طلب السحب ناجح\nمعرف المعاملة: W-{ref}\nالمبلغ: {amount:.2f} USDT ({network})\nالمحفظة: {wallet}\nالشبكة: {network}\nالحالة: في انتظار الموافقة\nالتاريخ: {date}\n\nبمجرد تأكيد المسؤول، سيتم معالجة سحبك.",
        # Referral translations
        "referral_title": "👥 برنامج الإحالة",
        "referral_intro": "شارك رابط الإحالة الخاص بك واكسب المكافآت!",
        "referral_link_label": "🔗 رابطك:",
        "referral_tap_to_copy": "👆 اضغط على الرابط أعلاه لنسخه",
        "referral_stats_title": "📊 إحصائياتك:",
        "referral_total_count": "👤 إجمالي الإحالات: {count}",
        "referral_earnings": "💰 الأرباح: {earnings:.2f} USDT",
        "referral_how_it_works": "💡 كيف يعمل:",
        "referral_step1": "• شارك رابطك مع الأصدقاء",
        "referral_step2": "• اكسب عمولة 2٪ من أول إيداع لهم",
        "referral_step3": "• الأرباح تضاف إلى رصيدك فوراً",
        "referral_commission_info": "🎁 احصل على 2٪ من أول إيداع لصديقك!",
        # Wallet command
        "wallet_saved": "المحفظة المحفوظة:\n<code>{wallet}</code>\nالشبكة: {network}",
        "wallet_use_button": "استخدم هذه المحفظة للسحب التالي",
        "wallet_not_saved": "لا توجد محفظة سحب محفوظة. قم بتعيينها باستخدام /wallet <العنوان> [الشبكة]",
        "wallet_send_address": "أرسل عنوان محفظة السحب والشبكة الاختيارية (مثال: 0xabc... ERC20).",
        # Help command
        "help_message": "تحتاج مساعدة؟ انقر أدناه للدردشة مع الدعم:",
        "help_button": "💬 فتح محادثة الدعم",
        # Balance page
        "balance_title": "رصيد حسابك",
        "balance_available": "متاح",
        "balance_in_process": "قيد المعالجة",
        "balance_today_profit": "ربح اليوم",
        "balance_total_profit": "الربح الإجمالي",
        "balance_manager": "المدير",
        # History page
        "history_no_transactions": "🧾 السجل: لم يتم العثور على معاملات.",
        "history_prev": "⬅ السابق",
        "history_next": "التالي ➡",
        "history_exit": "خروج ❌",
    },
    "zh": {
        "main_menu_title": "📋 主菜单",
        "settings_title": "⚙️ 设置",
        "settings_language": "🌍 语言",
        "change_language": "更改语言",
        "settings_wallet": "💳 设置/更新提现钱包",
        "select_option": "选择一个选项：",
        "back_to_menu": "« 返回菜单",
        "lang_auto": "🔄 自动（从Telegram）",
        "lang_en": "🇬🇧 英语",
        "lang_fr": "🇫🇷 法语",
        "lang_es": "🇪🇸 西班牙语",
        "lang_ar": "🇸🇦 阿拉伯语",
        "lang_zh": "🇨🇳 中文",
        "lang_set_success": "✅ 语言更改成功！",
        "lang_current": "当前语言：{lang}",
        "welcome_text": (
            "🎉 <b>欢迎来到Nexo Trading Bot！</b>\n\n"
            "🤖 您的个人AI交易助手\n"
            "💹 全天候自动加密货币交易\n"
            "📊 每日利润：1.25% - 1.5%\n\n"
            "👇 选择下面的选项开始使用"
        ),
        "info_text": (
            "🚀 <b>欢迎来到Nexo Trading Bot - 您的加密货币增长智能之路！</b>\n\n"
            "准备好轻松增加您的加密货币投资了吗？Nexo Trading Bot利用尖端的人工智能全天候交易加密货币，旨在获得稳定的每日利润。我们独特的多层分析服务意味着AI不断学习和改进，使每笔交易都比上一笔更智能。\n\n"
            "<b>为什么选择Nexo Trading Bot？</b>\n\n"
            "• 🤖 <b>AI驱动交易：</b>先进的算法全天候工作，为您消除加密货币交易的猜测。\n\n"
            "• 📈 <b>每日回报：</b>享受1.25%至1.5%的有竞争力的每日投资利润。\n\n"
            "• 🔒 <b>安全与自动化：</b>您的资金被安全管理，交易完全自动化——只需存款并观看您的投资组合增长！\n\n"
            "• 🌐 <b>广泛连接：</b>我们与Binance和Coinbase等领先的加密货币交易所集成，为您提供强大可靠的平台。\n\n"
            "• 📊 <b>透明与知情：</b>接收每笔完成交易的实时通知，以及关于您的利润、金额和余额的详细每日报告。\n\n"
            "• 💸 <b>灵活提现：</b>在您的前24小时后随时请求支付到您的钱包。请求会在12小时内及时处理。\n\n"
            "• 👨‍💼 <b>专属支持：</b>从您指定的个人经理那里获得个性化帮助，24/7支持随时可用。\n\n"
            "<b>开始很简单：</b>\n\n"
            "1. <b>注册与存款：</b>要存款，点击\"投资\"，输入您想存入的金额，选择您偏好的加密网络，复制显示的钱包地址，发送资金，确认您的存款，并等待区块链确认。\n\n"
            "2. <b>激活与交易：</b>您的存款从下一个交易周期开始生效，AI开始自动为您交易。\n\n"
            "3. <b>追踪进度：</b>在\"我的历史\"中监控最近14天的交易历史，并接收每日表现报告。\n\n"
            "4. <b>提取利润：</b>随时轻松请求支付！\n\n"
            "Nexo Trading Bot不仅仅是一个机器人；它是您的个人智能交易伙伴，专为公众使用而设计，使加密货币投资对每个人都可访问且有利可图。"
        ),
        # Invest flow translations
        "invest_enter_amount": "📈 输入您想要投资的金额（最低10 USDT，例如100.50）。发送/cancel取消。",
        "invest_invalid_amount": "金额无效。发送一个正数，如100或50.50，或/cancel。",
        "invest_minimum_amount": "❌ 最低存款为10 USDT。请至少输入10 USDT或/cancel。",
        "invest_send_proof": "📥 存款{amount:.2f}$\n发送到钱包：\n钱包：<code>{wallet}</code>\n网络：<b>{network}</b>\n\n发送后，上传截图或发送交易哈希（txid）。",
        "invest_no_amount": "没有待处理的投资金额。使用/invest重新开始。",
        "invest_upload_proof": "请上传截图或发送txid，或/cancel。",
        "invest_confirm_prompt": "收到凭证：<code>{proof}</code>\n如果您确实发送了{amount:.2f}$，请按确认。否则取消。",
        "invest_confirm_yes": "✅ 我已发送准确金额",
        "invest_confirm_no": "❌ 取消",
        "invest_missing_data": "数据缺失。重新启动投资流程。",
        "invest_request_success": "🧾 存款请求成功\n交易ID：D-{ref}\n金额：{amount:.2f} USDT（{network}）\n钱包：{wallet}\n网络：{network}\n状态：等待批准\n日期：{date}\n\n一旦确认，您的余额将自动更新。",
        # Withdraw flow translations
        "withdraw_enter_amount": "💸 输入您想要提现的金额（仅数字）。发送/cancel取消。",
        "withdraw_invalid_amount": "金额无效。发送一个正数，如50或25.75，或/cancel。",
        "withdraw_insufficient": "余额不足。可用：{balance:.2f}$。输入较小金额或/cancel。",
        "withdraw_saved_wallet": "您保存的钱包：\n<code>{wallet}</code>\n网络：<b>{network}</b>\n\n或发送新钱包和可选网络。",
        "withdraw_no_saved": "没有保存的钱包。发送钱包地址和可选网络（例如，0xabc... ERC20）。",
        "withdraw_use_saved": "使用保存的钱包",
        "withdraw_no_saved_found": "未找到保存的钱包。请发送钱包地址。",
        "withdraw_send_wallet": "请发送钱包地址和可选网络。",
        "withdraw_looks_command": "看起来像一个命令。仅发送钱包地址和可选网络。",
        "withdraw_invalid_wallet": "此地址看起来无效。发送'yes'仍然保存或发送正确地址。",
        "withdraw_confirm_prompt": "确认提现：\n金额：{amount:.2f}$\n钱包：<code>{wallet}</code>\n网络：<b>{network}</b>",
        "withdraw_confirm_yes": "✅ 确认",
        "withdraw_confirm_no": "❌ 取消",
        "withdraw_wallet_saved": "✅ 钱包已保存：\n<code>{wallet}</code>\n网络：{network}",
        "withdraw_missing_data": "提现数据缺失。重新开始提现。",
        "withdraw_request_success": "🧾 提现请求成功\n交易ID：W-{ref}\n金额：{amount:.2f} USDT（{network}）\n钱包：{wallet}\n网络：{network}\n状态：等待批准\n日期：{date}\n\n管理员确认后，您的提现将被处理。",
        # Referral translations
        "referral_title": "👥 推荐计划",
        "referral_intro": "分享您的推荐链接并赚取奖励！",
        "referral_link_label": "🔗 您的链接：",
        "referral_tap_to_copy": "👆 点击上面的链接复制",
        "referral_stats_title": "📊 您的统计：",
        "referral_total_count": "👤 总推荐数：{count}",
        "referral_earnings": "💰 收益：{earnings:.2f} USDT",
        "referral_how_it_works": "💡 如何运作：",
        "referral_step1": "• 与朋友分享您的链接",
        "referral_step2": "• 从他们的首次存款中赚取2%佣金",
        "referral_step3": "• 收益立即添加到您的余额",
        "referral_commission_info": "🎁 获得您朋友首次存款的2%！",
        # Wallet command
        "wallet_saved": "已保存的钱包：\n<code>{wallet}</code>\n网络：{network}",
        "wallet_use_button": "下次提现使用此钱包",
        "wallet_not_saved": "未保存提现钱包。使用/wallet <地址> [网络]设置。",
        "wallet_send_address": "发送您的提现钱包地址和可选网络（例如，0xabc... ERC20）。",
        # Help command
        "help_message": "需要帮助？点击下面与支持聊天：",
        "help_button": "💬 打开支持聊天",
        # Balance page
        "balance_title": "您的账户余额",
        "balance_available": "可用",
        "balance_in_process": "处理中",
        "balance_today_profit": "今日利润",
        "balance_total_profit": "总利润",
        "balance_manager": "经理",
        # History page
        "history_no_transactions": "🧾 历史：未找到交易记录。",
        "history_prev": "⬅ 上一页",
        "history_next": "下一页 ➡",
        "history_exit": "退出 ❌",
    }
}
DEFAULT_LANG = "en"
SUPPORTED_LANGS = ["en", "fr", "es", "ar", "zh"]
LANG_DISPLAY = {"en":"English","fr":"Français","es":"Español","ar":"العربية","zh":"中文"}

def t(lang: str, key: str, **kwargs) -> str:
    bundle = TRANSLATIONS.get(lang, TRANSLATIONS[DEFAULT_LANG])
    txt = bundle.get(key, TRANSLATIONS[DEFAULT_LANG].get(key, key))
    if kwargs:
        return txt.format(**kwargs)
    return txt

async def get_user_language(session: AsyncSession, user_id: int, update: Optional[Update] = None) -> str:
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    preferred = None
    if user:
        preferred = getattr(user, "preferred_language", None)

    if preferred and preferred != "auto":
        if preferred in SUPPORTED_LANGS:
            return preferred

    if update and getattr(update, "effective_user", None):
        tlang = (update.effective_user.language_code or "").lower()
        if "-" in tlang:
            tlang = tlang.split("-")[0]
        if tlang in SUPPORTED_LANGS:
            return tlang

    if preferred == "auto" and update and getattr(update, "effective_user", None):
        tlang = (update.effective_user.language_code or "").lower()
        if "-" in tlang:
            tlang = tlang.split("-")[0]
        if tlang in SUPPORTED_LANGS:
            return tlang

    return DEFAULT_LANG

# -----------------------
# UI helpers and menu keyboard
# -----------------------
ZWSP = "\u200b"

def _compact_pad(label: str, target: int = 10) -> str:
    plain = label.replace(ZWSP, "")
    if len(plain) >= target:
        return label
    needed = target - len(plain)
    left = needed // 2
    right = needed - left
    return (" " * left) + label + (" " * right) + ZWSP

def build_back_to_menu_keyboard(lang: str = DEFAULT_LANG) -> InlineKeyboardMarkup:
    """Create a keyboard with a single 'Back to Menu' button"""
    return InlineKeyboardMarkup([[InlineKeyboardButton(t(lang, "back_to_menu"), callback_data="menu_exit")]])

def build_main_menu_keyboard(full_two_column: bool = MENU_FULL_TWO_COLUMN, lang: str = DEFAULT_LANG) -> InlineKeyboardMarkup:
    labels = {
        "balance": "💰 " + {"en":"Balance","fr":"Solde","es":"Saldo","ar":"الرصيد","zh":"余额"}.get(lang, "Balance"),
        "invest": "📈 " + {"en":"Invest","fr":"Investir","es":"Invertir","ar":"استثمر","zh":"投资"}.get(lang, "Invest"),
        "history": "🧾 " + {"en":"History","fr":"Historique","es":"Historial","ar":"السجل","zh":"历史"}.get(lang, "History"),
        "withdraw": "💸 " + {"en":"Withdraw","fr":"Retirer","es":"Retirar","ar":"سحب","zh":"提现"}.get(lang, "Withdraw"),
        "referrals": "👥 " + {"en":"Referrals","fr":"Fermes","es":"Referidos","ar":"الإحالات","zh":"推荐"}.get(lang, "Referrals"),
        "settings": "⚙️ " + {"en":"Settings","fr":"Paramètres","es":"Ajustes","ar":"الإعدادات","zh":"设置"}.get(lang, "Settings"),
        "information": "ℹ️ " + {"en":"Information","fr":"Information","es":"Información","ar":"المعلومات","zh":"信息"}.get(lang, "Information"),
        "help": "❓ " + {"en":"Help","fr":"Aide","es":"Ayuda","ar":"مساعدة","zh":"帮助"}.get(lang, "Help"),
        "exit": "⨉ " + {"en":"Exit","fr":"Quitter","es":"Salir","ar":"خروج","zh":"退出"}.get(lang, "Exit"),
    }

    if not full_two_column:
        rows = []
        rows.append([InlineKeyboardButton(labels["balance"], callback_data="menu_balance"),
                     InlineKeyboardButton(labels["invest"], callback_data="menu_invest")])
        rows.append([InlineKeyboardButton(labels["history"], callback_data="menu_history"),
                     InlineKeyboardButton(labels["withdraw"], callback_data="menu_withdraw")])
        rows.append([InlineKeyboardButton(labels["referrals"], callback_data="menu_referrals"),
                     InlineKeyboardButton(labels["settings"], callback_data="menu_settings")])
        rows.append([InlineKeyboardButton(labels["information"], callback_data="menu_info"),
                     InlineKeyboardButton(labels["help"], url=SUPPORT_URL)])
        rows.append([InlineKeyboardButton(labels["exit"], callback_data="menu_exit")])
        return InlineKeyboardMarkup(rows)

    tlen = 10
    left_right = [
        (labels["balance"], "menu_balance", labels["invest"], "menu_invest"),
        (labels["history"], "menu_history", labels["withdraw"], "menu_withdraw"),
        (labels["referrals"], "menu_referrals", labels["settings"], "menu_settings"),
        (labels["information"], "menu_info", labels["help"], "url"),
    ]

    rows = []
    for l_label, l_cb, r_label, r_cb in left_right:
        l = _compact_pad(l_label, target=tlen)
        r = _compact_pad(r_label, target=tlen)
        left_btn = InlineKeyboardButton(l, callback_data=l_cb)
        # Special handling for help button - use URL instead of callback
        if r_cb == "url":
            right_btn = InlineKeyboardButton(r, url=SUPPORT_URL)
        else:
            right_btn = InlineKeyboardButton(r, callback_data=r_cb)
        rows.append([left_btn, right_btn])

    exit_label = _compact_pad(labels["exit"], target=(tlen*2)//2)
    rows.append([InlineKeyboardButton(exit_label, callback_data="menu_exit")])
    return InlineKeyboardMarkup(rows)

async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str = DEFAULT_LANG):
    """Send main menu with image, falling back to text-only if image fails"""
    keyboard = build_main_menu_keyboard(MENU_FULL_TWO_COLUMN, lang=lang)
    photo_file = None
    
    try:
        # Determine if MAIN_MENU_IMAGE_URL is a URL or local file
        photo_input = MAIN_MENU_IMAGE_URL
        if not MAIN_MENU_IMAGE_URL.startswith(('http://', 'https://')):
            # It's a local file path - validate and open it
            # Ensure the path is safe (within current directory or assets)
            if '..' in MAIN_MENU_IMAGE_URL or MAIN_MENU_IMAGE_URL.startswith('/'):
                logger.warning("Invalid image path: %s", MAIN_MENU_IMAGE_URL)
                raise ValueError("Invalid image path")
            
            # Check file exists before opening
            if not os.path.exists(MAIN_MENU_IMAGE_URL):
                logger.warning("Image file not found: %s", MAIN_MENU_IMAGE_URL)
                raise FileNotFoundError(f"Image file not found: {MAIN_MENU_IMAGE_URL}")
            
            photo_file = open(MAIN_MENU_IMAGE_URL, 'rb')
            photo_input = photo_file
        
        # Try to send with photo
        if update.callback_query:
            await update.callback_query.message.reply_photo(
                photo=photo_input, 
                caption=MAIN_MENU_CAPTION, 
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        else:
            await update.message.reply_photo(
                photo=photo_input, 
                caption=MAIN_MENU_CAPTION, 
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            
    except (FileNotFoundError, ValueError, IOError, OSError) as e:
        logger.warning("Failed to send main menu image: %s", e)
        # Fallback to text-only menu
        if update.callback_query:
            await update.callback_query.message.reply_text(
                MAIN_MENU_CAPTION, 
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                MAIN_MENU_CAPTION, 
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    except Exception:
        # Catch any other unexpected errors (e.g., Telegram API errors)
        logger.exception("Unexpected error sending main menu")
        # Fallback to text-only menu
        if update.callback_query:
            await update.callback_query.message.reply_text(
                MAIN_MENU_CAPTION, 
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                MAIN_MENU_CAPTION, 
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    finally:
        # Ensure file is closed if it was opened
        if photo_file is not None:
            photo_file.close()

def is_probable_wallet(address: str) -> bool:
    address = (address or "").strip()
    if not address:
        return False
    if address.startswith("0x") and len(address) >= 40 and re.match(r"^0x[0-9a-fA-F]+$", address):
        return True
    if address.startswith("T") and 25 <= len(address) <= 35:
        return True
    if re.match(r"^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$", address):
        return True
    if 20 <= len(address) <= 100:
        return True
    return False

# -----------------------
# Admin and tx helpers (include username in admin view)
# -----------------------
def tx_card_text(tx: Transaction, username: Optional[str] = None) -> str:
    emoji = "📥" if (tx.type == 'invest') else ("💸" if tx.type == 'withdraw' else ("🤖" if tx.type == 'trade' else "💰"))
    created = tx.created_at.strftime("%Y-%m-%d %H:%M:%S") if tx.created_at else "-"
    user_line = f"User: <code>{tx.user_id}</code>"
    if username:
        user_line += f" (@{username})"
    
    base_text = (f"{emoji} <b>Ref {tx.ref}</b>\n"
                 f"Type: <b>{(tx.type or '').upper()}</b>\n"
                 f"Amount: <b>{float(tx.amount):.6f}$</b>\n"
                 f"{user_line}\n"
                 f"Status: <b>{(tx.status or '').upper()}</b>\n"
                 f"Created: {created}\n")
    
    # For withdrawal transactions, include wallet and network for admin to copy
    if tx.type == 'withdraw' and tx.wallet:
        wallet_info = (f"\n💳 <b>Withdrawal Details:</b>\n"
                      f"Wallet: <code>{tx.wallet}</code>\n"
                      f"Network: <b>{tx.network or 'N/A'}</b>\n")
        base_text += wallet_info
    
    return base_text

def admin_action_kb(tx_db_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f"admin_start_approve_{tx_db_id}"),
         InlineKeyboardButton("❌ Reject", callback_data=f"admin_start_reject_{tx_db_id}")]
    ])

def admin_confirm_kb(action: str, tx_db_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes", callback_data=f"admin_confirm_{action}_{tx_db_id}"),
         InlineKeyboardButton("❌ No", callback_data=f"admin_cancel_{tx_db_id}")]
    ])

async def send_admin_tx_notification(bot, tx: Transaction, proof_file_id: Optional[str] = None, username: Optional[str] = None):
    text = tx_card_text(tx, username=username)
    try:
        if proof_file_id and proof_file_id.startswith("photo:"):
            file_id = proof_file_id.split(":",1)[1]
            await bot.send_photo(chat_id=ADMIN_ID, photo=file_id, caption=text, parse_mode="HTML", reply_markup=admin_action_kb(tx.id))
        else:
            if proof_file_id:
                text = text + f"\nProof: <code>{proof_file_id}</code>"
            await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode="HTML", reply_markup=admin_action_kb(tx.id))
    except Exception:
        logger.exception("Failed to notify admin for transaction %s", tx.id)

async def post_admin_log(bot, message: str):
    if ADMIN_LOG_CHAT_ID:
        try:
            await bot.send_message(chat_id=ADMIN_LOG_CHAT_ID, text=message)
        except Exception:
            logger.exception("Failed to post admin log")

# -----------------------
# Helper: format price (Option A - fixed decimals, no scientific notation)
# -----------------------
def format_price(value: float, decimals: int = 12) -> str:
    """
    Convert a float to a fixed-point decimal string with `decimals` fractional digits.
    Ensures values like 3e-05 are shown as 0.000030000000 (no scientific notation).
    """
    try:
        if value is None:
            return "0"
        d = Decimal(str(value))
        q = Decimal('1').scaleb(-decimals)  # 10**-decimals
        d = d.quantize(q, rounding=ROUND_HALF_UP)
        return f"{d:.{decimals}f}"
    except Exception:
        # fallback: format with python float formatting forced to fixed decimals
        return f"{value:.{decimals}f}"

# -----------------------
# DAILY PROFIT JOB (unchanged)
# -----------------------
async def daily_profit_job():
    PROFIT_RATE = 0.015
    async with async_session() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()
        for user in users:
            try:
                total = float(user.balance or 0) + float(user.balance_in_process or 0)
                if total <= 0:
                    continue
                profit = round(total * PROFIT_RATE, 2)
                new_balance = float(user.balance or 0) + profit
                new_total_profit = float(user.total_profit or 0) + profit
                await update_user(session, user.id, balance=new_balance, daily_profit=profit, total_profit=new_total_profit)
                await log_transaction(session, user_id=user.id, ref=None, type='profit', amount=profit, status='credited', proof='', wallet='', network='', created_at=datetime.utcnow())
                logger.info("Credited daily profit %.2f to user %s", profit, user.id)
            except Exception:
                logger.exception("daily_profit_job: failed for user %s", getattr(user, "id", "<unknown>"))

# -----------------------
# Price simulation (in-memory)
# -----------------------
# Expanded list of diverse trading pairs for random selection
TRADING_PAIRS = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT',
    'DOGEUSDT', 'SOLUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
    'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'XLMUSDT',
    'ALGOUSDT', 'VETUSDT', 'FILUSDT', 'TRXUSDT', 'ETCUSDT',
    'SHIBUSDT', 'NEARUSDT', 'APTUSDT', 'ARBUSDT', 'OPUSDT',
    'INJUSDT', 'SUIUSDT', 'PEPEUSDT', 'WIFUSDT', 'BONKUSDT',
    'TAOUSDT', 'RENDERUSDT', 'STXUSDT', 'WLDUSDT', 'ICPUSDT',
    'AAVEUSDT', 'MKRUSDT', 'JUPUSDT', 'TIAUSDT', 'PYTHUSDT'
]

# Price simulation pairs (for fallback when Binance API unavailable)
PRICE_PAIRS = {
    "USDT/BTC": 0.000030,
    "USDT/ETH": 0.00045,
    "USDT/BUSD": 1.000,
    "USDT/XRP": 0.000023,
    "USDT/LTC": 0.0032,
}
_price_lock = asyncio.Lock()

def simulate_price_walk(pair: str) -> float:
    base = PRICE_PAIRS.get(pair, 1.0)
    pct = random.uniform(-0.0015, 0.0015)
    new = base * (1.0 + pct)
    PRICE_PAIRS[pair] = round(new, 8)
    return PRICE_PAIRS[pair]

def pick_random_pair() -> str:
    """Pick a random trading pair from the diverse list"""
    return random.choice(TRADING_PAIRS)

# Trading control
TRADING_ENABLED = True
TRADING_FREQ_MINUTES = 52  # Default: 52 minutes between trades
TRADES_PER_DAY = 15  # Default: 15 trades per day (every 52 minutes)
MINUTES_PER_DAY = 24 * 60  # 1440 minutes in a day
TRADING_JOB_ID = 'trading_job_scheduled'
_trading_job = None

# Trading hours configuration (New York timezone)
# Default: 5 AM - 6 PM Eastern Time (configurable via admin commands)
TRADING_START_HOUR = 5  # 5 AM ET
TRADING_END_HOUR = 18   # 6 PM ET

# -----------------------
# Trading job: fetch Binance prices with cache, use per-user config, update balances
# Supports both positive and negative trades based on admin configuration
# -----------------------
async def trading_job():
    # Check both global and database config for trading enabled status
    async with async_session() as session:
        trading_enabled_db = await get_config(session, 'trading_enabled', '1')
    
    if not TRADING_ENABLED or trading_enabled_db != '1':
        logger.debug("trading_job: Trading is disabled (TRADING_ENABLED=%s, DB=%s), skipping run", TRADING_ENABLED, trading_enabled_db)
        return
    
    # Check trading hours (New York timezone: UTC-5 EST or UTC-4 EDT)
    # Using UTC-5 as standard offset for Eastern Time
    now = datetime.utcnow()
    ny_hour = (now.hour - 5) % 24  # Convert UTC to Eastern Time (approximate)
    
    async with async_session() as session:
        # Get configured trading hours from database
        trading_start = int(await get_config(session, 'trading_start_hour', str(TRADING_START_HOUR)))
        trading_end = int(await get_config(session, 'trading_end_hour', str(TRADING_END_HOUR)))
        
        if not (trading_start <= ny_hour < trading_end):
            logger.debug("trading_job: Outside trading hours (NY time: %02d:00, window: %02d:00-%02d:00)", 
                        ny_hour, trading_start, trading_end)
            return
    
    logger.info("trading_job: starting run at %s (NY hour: %02d:00)", now.isoformat(), ny_hour)
    async with async_session() as session:
        # Get configured ranges from Config
        trade_min = float(await get_config(session, 'trade_range_min', '0.05'))
        trade_max = float(await get_config(session, 'trade_range_max', '0.25'))
        daily_min = float(await get_config(session, 'daily_range_min', '1.25'))
        daily_max = float(await get_config(session, 'daily_range_max', '1.5'))
        negative_trades_per_day = int(await get_config(session, 'negative_trades_per_day', '5'))
        
        result = await session.execute(select(User))
        users = result.scalars().all()
        if not users:
            logger.debug("trading_job: no users found")
            return
        for user in users:
            try:
                bal = float(user.balance or 0.0)
                if bal <= 1.0:
                    continue
                
                # Calculate daily profit percentage so far
                daily_profit_so_far = float(user.daily_profit or 0.0)
                starting_balance = bal - daily_profit_so_far
                if starting_balance <= 0:
                    starting_balance = bal
                current_daily_percent = (daily_profit_so_far / starting_balance) * 100 if starting_balance > 0 else 0
                
                # Determine if this should be a negative trade
                # Probability based on negative_trades_per_day / TRADES_PER_DAY
                negative_trade_probability = negative_trades_per_day / TRADES_PER_DAY if TRADES_PER_DAY > 0 else 0.15
                is_negative_trade = random.random() < negative_trade_probability
                
                if is_negative_trade:
                    # Negative trade: -0.05% to -0.25%
                    percent_per_trade = -random.uniform(0.05, 0.25)
                else:
                    # Positive trade: check if we haven't exceeded daily target
                    daily_target_percent = random.uniform(daily_min, daily_max)
                    if current_daily_percent >= daily_target_percent:
                        logger.debug(f"User {user.id} already reached daily target: {current_daily_percent:.2f}% >= {daily_target_percent:.2f}%")
                        continue
                    
                    # Generate random percent_per_trade within allowed range
                    percent_per_trade = random.uniform(trade_min, trade_max)
                    
                    # Ensure we don't exceed daily limit
                    remaining_daily_percent = daily_target_percent - current_daily_percent
                    if percent_per_trade > remaining_daily_percent:
                        percent_per_trade = remaining_daily_percent
                    
                    if percent_per_trade <= 0:
                        continue
                
                # Get per-user config or use random pair from diverse list
                user_config = await get_user_trade_config(session, user.id)
                if user_config:
                    pair = user_config['pair']
                else:
                    # Use random pair from diverse trading pairs list
                    pair = pick_random_pair()
                
                # Try to fetch Binance price, fallback to simulated
                live_price = await fetch_binance_price(pair)
                if live_price is None:
                    # Fallback: simulate price walk
                    logger.debug(f"Using simulated price for {pair}")
                    if pair not in PRICE_PAIRS:
                        PRICE_PAIRS[pair] = 50000.0 if 'BTC' in pair else 2000.0 if 'ETH' in pair else 300.0
                    async with _price_lock:
                        live_price = simulate_price_walk(pair)
                
                # Calculate profit/loss based on percent_per_trade
                profit = round((percent_per_trade / 100.0) * bal, 6)
                
                # Update balances
                new_balance = bal + profit
                new_daily_profit = float(user.daily_profit or 0.0) + profit
                new_total_profit = float(user.total_profit or 0.0) + profit
                await update_user(session, user.id, 
                                balance=new_balance, 
                                daily_profit=new_daily_profit,
                                total_profit=new_total_profit)
                
                # Log transaction
                tx_id, tx_ref = await log_transaction(
                    session,
                    user_id=user.id,
                    ref=None,
                    type='trade',
                    amount=profit,
                    status='credited',
                    proof='',
                    wallet='',
                    network='',
                    created_at=now
                )
                
                # Create spread for display
                spread = random.uniform(0.001, 0.006)
                
                # For negative trades, sell should be lower than buy (buy high, sell low)
                # For positive trades, sell should be higher than buy (buy low, sell high)
                if is_negative_trade:
                    # Negative trade: buy high, sell low
                    buy_rate_raw = live_price * (1.0 + spread/2 + random.uniform(0.0001, 0.0009))
                    sell_rate_raw = live_price * (1.0 - spread/2)
                else:
                    # Positive trade: buy low, sell high
                    buy_rate_raw = live_price * (1.0 - spread/2)
                    sell_rate_raw = live_price * (1.0 + spread/2 + random.uniform(0.0001, 0.0009))
                
                buy_rate = format_price(buy_rate_raw, decimals=8)
                sell_rate = format_price(sell_rate_raw, decimals=8)
                
                profit_percent = round((profit / bal) * 100, 2)
                display_balance = format_price(new_balance, decimals=2)
                date_str = now.strftime("%d.%m.%Y %H:%M")
                
                # Format pair for display (BTCUSDT -> USDT → BTC → USDT)
                if pair.endswith('USDT'):
                    base_asset = pair[:-4]
                    quote_asset = 'USDT'
                else:
                    base_asset = pair[:3]
                    quote_asset = pair[3:]
                trading_pair_str = f"{quote_asset} → {base_asset} → {quote_asset}"
                
                # Get user language for translated message
                lang = user.preferred_language or 'en'
                
                # Trading alert translations
                trade_alerts = {
                    'en': {
                        'title': '📢 AI trade was executed',
                        'date': 'Date',
                        'pair': 'Trading pair',
                        'buy': 'Buy rate',
                        'sell': 'Sell rate',
                        'profit': 'Profit',
                        'balance': 'Balance'
                    },
                    'fr': {
                        'title': '📢 Transaction IA exécutée',
                        'date': 'Date',
                        'pair': 'Paire de trading',
                        'buy': 'Taux d\'achat',
                        'sell': 'Taux de vente',
                        'profit': 'Profit',
                        'balance': 'Solde'
                    },
                    'es': {
                        'title': '📢 Operación IA ejecutada',
                        'date': 'Fecha',
                        'pair': 'Par de trading',
                        'buy': 'Tasa de compra',
                        'sell': 'Tasa de venta',
                        'profit': 'Ganancia',
                        'balance': 'Saldo'
                    },
                    'ar': {
                        'title': '📢 تم تنفيذ صفقة الذكاء الاصطناعي',
                        'date': 'التاريخ',
                        'pair': 'زوج التداول',
                        'buy': 'سعر الشراء',
                        'sell': 'سعر البيع',
                        'profit': 'الربح',
                        'balance': 'الرصيد'
                    }
                }
                
                t = trade_alerts.get(lang, trade_alerts['en'])
                trade_text = (
                    f"{t['title']}\n\n"
                    f"📅 {t['date']}: {date_str}\n"
                    f"💱 {t['pair']}: {trading_pair_str}\n"
                    f"📈 {t['buy']}: {buy_rate}\n"
                    f"📉 {t['sell']}: {sell_rate}\n"
                    f"📊 {t['profit']}: {profit_percent}%\n"
                    f"💰{t['balance']}: {display_balance} USDT"
                )
                # Use application instance if available, otherwise skip notification
                try:
                    if application and application.bot:
                        await application.bot.send_message(chat_id=user.id, text=trade_text)
                    else:
                        logger.warning("Application bot not available for trade notification to user %s", user.id)
                except Exception as e:
                    logger.warning("Unable to send trade alert to user %s: %s", user.id, str(e))
            except Exception:
                logger.exception("trading_job failed for user %s", getattr(user, "id", "<unknown>"))

# -----------------------
# Daily summary job: runs at 23:59 UTC to summarize daily trading
# -----------------------
async def daily_summary_job():
    """Send daily summary to users and persist records"""
    logger.info("daily_summary_job: starting daily summary")
    now = datetime.utcnow()
    today_date = now.date()
    
    async with async_session() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()
        
        for user in users:
            try:
                user_id = user.id
                balance = float(user.balance or 0.0)
                daily_profit = float(user.daily_profit or 0.0)
                
                # Skip users with no activity
                if balance <= 0 and daily_profit <= 0:
                    continue
                
                # Calculate daily percent
                starting_balance = balance - daily_profit
                if starting_balance > 0:
                    daily_percent = (daily_profit / starting_balance) * 100
                else:
                    daily_percent = 0.0
                
                # Save daily summary
                summary = DailySummary(
                    user_id=user_id,
                    date=datetime(today_date.year, today_date.month, today_date.day),
                    daily_percent=daily_percent,
                    profit_amount=daily_profit,
                    total_balance=balance,
                    created_at=now
                )
                session.add(summary)
                
                # Get user language for translated message
                lang = user.preferred_language or 'en'
                
                # Daily summary translations
                summary_translations = {
                    'en': {
                        'title': '📊 Trading work for today is completed.',
                        'total_profit': '💹 Total profit amounted to',
                        'profit_amount': '💰 Profit amount',
                        'total_balance': '📈 Total balance'
                    },
                    'fr': {
                        'title': '📊 Le travail de trading d\'aujourd\'hui est terminé.',
                        'total_profit': '💹 Le profit total s\'élève à',
                        'profit_amount': '💰 Montant du profit',
                        'total_balance': '📈 Solde total'
                    },
                    'es': {
                        'title': '📊 El trabajo de trading de hoy se ha completado.',
                        'total_profit': '💹 La ganancia total ascendió a',
                        'profit_amount': '💰 Monto de ganancia',
                        'total_balance': '📈 Saldo total'
                    },
                    'ar': {
                        'title': '📊 اكتمل عمل التداول لهذا اليوم.',
                        'total_profit': '💹 بلغ إجمالي الربح',
                        'profit_amount': '💰 مبلغ الربح',
                        'total_balance': '📈 الرصيد الإجمالي'
                    }
                }
                
                t = summary_translations.get(lang, summary_translations['en'])
                summary_text = (
                    f"{t['title']}\n"
                    f"{t['total_profit']} {daily_percent:.2f}%\n"
                    f"{t['profit_amount']}: {daily_profit:.2f} USDT\n"
                    f"{t['total_balance']}: {balance:.2f} USDT"
                )
                
                try:
                    await application.bot.send_message(chat_id=user_id, text=summary_text)
                except Exception as e:
                    logger.debug(f"Unable to send daily summary to user {user_id}: {e}")
                
                # Reset daily_profit for next day
                await update_user(session, user_id, daily_profit=0.0)
                
            except Exception as e:
                logger.exception(f"daily_summary_job failed for user {getattr(user, 'id', '<unknown>')}: {e}")
        
        await session.commit()
    logger.info("daily_summary_job: completed")

# -----------------------
# MENU CALLBACK (forwards special callbacks; handles menu items)
# -----------------------
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    
    # Answer callback immediately to remove loading state
    await query.answer()
    data = query.data or ""

    # Language settings
    if data == "lang_auto" or data.startswith("lang_"):
        await language_callback_handler(update, context)
        return

    # Settings: Set wallet
    if data == "settings_set_wallet":
        await settings_start_wallet(update, context)
        return

    # Exit/Return to main menu
    if data == "menu_exit":
        await cancel_conv(update, context)
        async with async_session() as session:
            lang = await get_user_language(session, query.from_user.id, update=update)
        await send_main_menu(update, context, lang=lang)
        return

    # Balance
    if data == "menu_balance":
        try:
            async with async_session() as session:
                await send_balance_message(query, session, query.from_user.id)
        except Exception as e:
            logger.exception(f"Error displaying balance: {e}")
            await query.message.reply_text("⚠️ Error loading balance. Please try again.")
        return

    # History
    if data == "menu_history":
        try:
            await history_command(update, context)
        except Exception as e:
            logger.exception(f"Error displaying history: {e}")
            await query.message.reply_text("⚠️ Error loading history. Please try again.")
        return

    # Referrals
    if data == "menu_referrals":
        try:
            user_id = query.from_user.id
            bot_username = (await context.bot.get_me()).username
            referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
            
            async with async_session() as session:
                lang = await get_user_language(session, user_id, update=update)
                user = await get_user(session, user_id)
                
                referral_count = int(user.get('referral_count', 0))
                referral_earnings = float(user.get('referral_earnings', 0.0))
            
            text = (
                f"<b>{t(lang, 'referral_title')}</b>\n\n"
                f"{t(lang, 'referral_intro')}\n\n"
                f"{t(lang, 'referral_link_label')}\n<code>{referral_link}</code>\n"
                f"{t(lang, 'referral_tap_to_copy')}\n\n"
                f"<b>{t(lang, 'referral_stats_title')}</b>\n"
                f"{t(lang, 'referral_total_count', count=referral_count)}\n"
                f"{t(lang, 'referral_earnings', earnings=referral_earnings)}\n\n"
                f"<b>{t(lang, 'referral_how_it_works')}</b>\n"
                f"{t(lang, 'referral_step1')}\n"
                f"{t(lang, 'referral_step2')}\n"
                f"{t(lang, 'referral_step3')}\n\n"
                f"{t(lang, 'referral_commission_info')}"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Copy Link", switch_inline_query=referral_link)],
                [InlineKeyboardButton(t(lang,"back_to_menu"), callback_data="menu_exit")]
            ])
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            logger.exception(f"Error displaying referrals: {e}")
            await query.message.reply_text("⚠️ Error loading referral info. Please try again.")
        return

    # Settings
    if data == "menu_settings":
        try:
            async with async_session() as session:
                lang = await get_user_language(session, query.from_user.id, update=update)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🌍 " + t(lang,"change_language"), callback_data="settings_language")],
                [InlineKeyboardButton(t(lang,"settings_wallet"), callback_data="settings_set_wallet")],
                [InlineKeyboardButton(t(lang,"back_to_menu"), callback_data="menu_exit")]
            ])
            text = t(lang, "settings_title") + "\n\n" + t(lang, "select_option")
            try:
                await query.edit_message_text(text, reply_markup=kb)
            except Exception:
                # If editing fails (e.g., message is a photo), send a new message
                await query.message.reply_text(text, reply_markup=kb)
        except Exception as e:
            logger.exception(f"Error displaying settings: {e}")
            await query.message.reply_text("⚠️ Error loading settings. Please try again.")
        return

    # Language settings
    if data == "settings_language":
        await settings_language_open_callback(update, context)
        return

    # Information
    if data == "menu_info":
        try:
            async with async_session() as session:
                lang = await get_user_language(session, query.from_user.id, update=update)
            # Add a back to menu button instead of showing the full menu inline
            text = t(lang, "info_text")
            kb = build_back_to_menu_keyboard(lang)
            try:
                await query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                # If editing fails (e.g., message is a photo), send a new message
                await query.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception as e:
            logger.exception(f"Error displaying info: {e}")
            await query.message.reply_text("⚠️ Error loading information. Please try again.")
        return

    # Help (legacy - now uses URL button directly)
    if data == "menu_help":
        help_button = InlineKeyboardMarkup([[InlineKeyboardButton("💬 Open Support", url=SUPPORT_URL)]])
        await query.message.reply_text(
            "Need help? Click below to chat with our support team:", 
            reply_markup=help_button
        )
        return

# -----------------------
# Balance helper (supports CallbackQuery and Message)
# -----------------------
async def send_balance_message(query_or_message, session: AsyncSession, user_id: int):
    user = await get_user(session, user_id)
    lang = await get_user_language(session, user_id)
    
    # Format balance values with proper decimals
    balance = format_price(float(user.get('balance') or 0), decimals=2)
    in_process = format_price(float(user.get('balance_in_process') or 0), decimals=2)
    daily_profit = format_price(float(user.get('daily_profit') or 0), decimals=2)
    total_profit = format_price(float(user.get('total_profit') or 0), decimals=2)
    
    text = (
        f"💰 <b>{t(lang, 'balance_title')}</b>\n\n"
        f"💵 <b>{t(lang, 'balance_available')}:</b> {balance} USDT\n"
        f"⏳ <b>{t(lang, 'balance_in_process')}:</b> {in_process} USDT\n\n"
        f"📊 <b>{t(lang, 'balance_today_profit')}:</b> {daily_profit} USDT\n"
        f"📈 <b>{t(lang, 'balance_total_profit')}:</b> {total_profit} USDT\n\n"
        f"👤 <b>{t(lang, 'balance_manager')}:</b> {SUPPORT_USER}"
    )
    
    kb = build_back_to_menu_keyboard(lang)
    
    try:
        if hasattr(query_or_message, "message") and hasattr(query_or_message, "data"):
            # It's a CallbackQuery
            try:
                await query_or_message.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
                return
            except Exception:
                # If editing fails (e.g., message is a photo), send a new message
                await query_or_message.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
        else:
            # It's a regular Message
            await query_or_message.reply_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        logger.exception("Failed to send balance message for user %s", user_id)

# -----------------------
# INVEST / WITHDRAW / ADMIN / HISTORY handlers
# -----------------------
async def invest_cmd_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    await update.effective_message.reply_text(t(lang, "invest_enter_amount"), reply_markup=None)
    return INVEST_AMOUNT

async def invest_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(t(lang, "invest_enter_amount"))
    return INVEST_AMOUNT

async def invest_amount_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    text = (msg.text or "").strip()
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError()
    except Exception:
        await msg.reply_text(t(lang, "invest_invalid_amount"))
        return INVEST_AMOUNT
    
    # Check minimum deposit amount
    if amount < MIN_DEPOSIT_AMOUNT:
        await msg.reply_text(t(lang, "invest_minimum_amount"))
        return INVEST_AMOUNT
    
    amount = round(amount, 2)
    context.user_data['invest_amount'] = amount
    
    # Show network selection keyboard
    keyboard = [
        [InlineKeyboardButton("💵 USDT (TRC20)", callback_data="invest_network_USDT")],
        [InlineKeyboardButton("₿ Bitcoin (BTC)", callback_data="invest_network_BTC")],
        [InlineKeyboardButton("◎ Solana (SOL)", callback_data="invest_network_SOLANA")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    network_msg = f"💰 Amount: {amount:.2f}$\n\nPlease select the network you want to use for deposit:"
    await msg.reply_text(network_msg, reply_markup=reply_markup)
    return INVEST_NETWORK


async def invest_network_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    try:
        async with async_session() as session:
            lang = await get_user_language(session, user_id, update)
        
        amount = context.user_data.get('invest_amount')
        if amount is None:
            await query.message.reply_text(t(lang, "invest_no_amount"))
            return ConversationHandler.END
        
        # Extract selected coin from callback data (e.g., "invest_network_USDT" -> "USDT")
        coin = query.data.replace("invest_network_", "")
        context.user_data['invest_coin'] = coin
        
        # Map SOLANA to SOL for consistency
        coin_lookup = coin if coin != "SOLANA" else "SOL"
        
        # Get deposit wallet for selected coin
        async with async_session() as session:
            deposit_wallet = await get_primary_deposit_wallet(session, coin_lookup)
        
        # Fall back to MASTER_WALLET if no wallet configured (only for USDT)
        if deposit_wallet:
            wallet = deposit_wallet['address']
            network = deposit_wallet['network']
        else:
            if coin == "USDT":
                wallet = MASTER_WALLET
                network = MASTER_NETWORK
            else:
                await query.message.reply_text(
                    f"❌ No deposit wallet configured for {coin}.\n"
                    f"Please contact admin or choose a different network.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("« Back to network selection", callback_data="invest_back_to_network")
                    ]])
                )
                return INVEST_NETWORK
        
        # Display network name based on coin
        network_display_name = {
            "USDT": "USDT (TRC20)",
            "BTC": "Bitcoin (BTC)",
            "SOLANA": "Solana (SOL)",
            "SOL": "Solana (SOL)"
        }.get(coin, coin)
        
        wallet_msg = (
            f"📥 Deposit {amount:.2f}$ using {network_display_name}\n\n"
            f"Send to wallet:\n"
            f"Wallet: <code>{wallet}</code>\n"
            f"Network: <b>{network}</b>\n\n"
            f"After sending, upload a screenshot OR send the transaction hash (txid)."
        )
        
        try:
            await query.message.edit_text(wallet_msg, parse_mode="HTML")
        except Exception:
            await query.message.reply_text(wallet_msg, parse_mode="HTML")
        
        # Store wallet and network in user_data for later use
        context.user_data['invest_wallet'] = wallet
        context.user_data['invest_network'] = network
        
        return INVEST_PROOF
    except Exception as e:
        logger.exception("Error in invest_network_selected")
        await query.message.reply_text(f"Error selecting network: {str(e)}\n\nPlease try again or contact support.")
        return ConversationHandler.END


async def invest_proof_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    amount = context.user_data.get('invest_amount')
    if amount is None:
        await msg.reply_text(t(lang, "invest_no_amount"))
        return ConversationHandler.END
    proof_label = None
    if msg.photo:
        file_id = msg.photo[-1].file_id
        proof_label = f"photo:{file_id}"
    else:
        text = (msg.text or "").strip()
        if text:
            proof_label = text
    if not proof_label:
        await msg.reply_text(t(lang, "invest_upload_proof"))
        return INVEST_PROOF
    context.user_data['invest_proof'] = proof_label
    await msg.reply_text(
        t(lang, "invest_confirm_prompt", proof=proof_label, amount=amount),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(t(lang, "invest_confirm_yes"), callback_data="invest_confirm_yes"),
            InlineKeyboardButton(t(lang, "invest_confirm_no"), callback_data="invest_confirm_no")
        ]])
    )
    return INVEST_CONFIRM

async def invest_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
    user_id = query.from_user.id if query else update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    amount = context.user_data.get('invest_amount')
    proof = context.user_data.get('invest_proof')
    if amount is None or proof is None:
        target = query.message if query else update.effective_message
        await target.reply_text(t(lang, "invest_missing_data"), reply_markup=build_main_menu_keyboard(MENU_FULL_TWO_COLUMN))
        context.user_data.pop('invest_amount', None)
        context.user_data.pop('invest_proof', None)
        context.user_data.pop('invest_coin', None)
        context.user_data.pop('invest_wallet', None)
        context.user_data.pop('invest_network', None)
        return ConversationHandler.END

    # Get wallet and network from user_data (stored during network selection)
    wallet = context.user_data.get('invest_wallet')
    network = context.user_data.get('invest_network')
    
    # If not in user_data (backward compatibility), fall back to USDT
    if not wallet or not network:
        async with async_session() as session:
            deposit_wallet = await get_primary_deposit_wallet(session, 'USDT')
            
            if deposit_wallet:
                wallet = deposit_wallet['address']
                network = deposit_wallet['network']
            else:
                wallet = MASTER_WALLET
                network = MASTER_NETWORK
    
    async with async_session() as session:
        tx_db_id, tx_ref = await log_transaction(
            session,
            user_id=user_id,
            ref=None,
            type='invest',
            amount=amount,
            status='pending',
            proof=str(proof),
            wallet=wallet,
            network=network,
            created_at=datetime.utcnow()
        )

    now = datetime.utcnow()
    pdt_str = (now.replace(tzinfo=timezone.utc) - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M (PDT)")
    deposit_request_text = t(lang, "invest_request_success", ref=tx_ref, amount=amount, network=network, wallet=wallet, date=pdt_str)

    try:
        if query:
            await query.message.reply_text(deposit_request_text, parse_mode="HTML")
        else:
            await update.effective_message.reply_text(deposit_request_text, parse_mode="HTML")
    except Exception:
        logger.exception("Failed to send deposit request message to user %s", user_id)

    async with async_session() as session:
        result = await session.execute(select(Transaction).where(Transaction.id == tx_db_id))
        tx = result.scalar_one_or_none()

    username = None
    if query and getattr(query, "from_user", None):
        username = (query.from_user.username or "").strip()
    elif update and getattr(update, "effective_user", None):
        username = (update.effective_user.username or "").strip()
    if username == "":
        username = None

    # notify admin and log; if admin send failed, will be logged
    try:
        await send_admin_tx_notification(context.application.bot, tx, proof_file_id=proof, username=username)
    except Exception:
        logger.exception("Failed sending admin notification for invest %s", tx_db_id)
    await post_admin_log(context.application.bot, f"New INVEST #{tx_db_id} ref {tx_ref} user {user_id} username @{username or 'N/A'} amount {amount:.2f}$")

    context.user_data.pop('invest_amount', None)
    context.user_data.pop('invest_proof', None)
    context.user_data.pop('invest_coin', None)
    context.user_data.pop('invest_wallet', None)
    context.user_data.pop('invest_network', None)
    return ConversationHandler.END

# Withdraw handlers with full multilingual support
async def withdraw_cmd_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    await update.effective_message.reply_text(t(lang, "withdraw_enter_amount"))
    return WITHDRAW_AMOUNT

async def withdraw_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(t(lang, "withdraw_enter_amount"))
    return WITHDRAW_AMOUNT

async def withdraw_amount_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    text = (msg.text or "").strip()
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError()
    except Exception:
        await msg.reply_text(t(lang, "withdraw_invalid_amount"))
        return WITHDRAW_AMOUNT
    amount = round(amount, 2)
    context.user_data['withdraw_amount'] = amount
    async with async_session() as session:
        user = await get_user(session, user_id)
    balance = float(user.get('balance') or 0)
    if amount > balance:
        await msg.reply_text(t(lang, "withdraw_insufficient", balance=balance))
        context.user_data.pop('withdraw_amount', None)
        return WITHDRAW_AMOUNT
    saved_wallet = user.get('wallet_address')
    saved_network = user.get('wallet_network')
    if saved_wallet:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(t(lang, "withdraw_use_saved"), callback_data="withdraw_use_saved")]])
        await msg.reply_text(t(lang, "withdraw_saved_wallet", wallet=saved_wallet, network=saved_network), parse_mode="HTML", reply_markup=kb)
    else:
        await msg.reply_text(t(lang, "withdraw_no_saved"))
    return WITHDRAW_WALLET

async def withdraw_wallet_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)

    if update.callback_query and update.callback_query.data == "withdraw_use_saved":
        await update.callback_query.answer()
        async with async_session() as session:
            user = await get_user(session, user_id)
        wallet_address = user.get('wallet_address')
        wallet_network = user.get('wallet_network')
        if not wallet_address:
            await msg.reply_text(t(lang, "withdraw_no_saved_found"))
            return WITHDRAW_WALLET
    else:
        text = (msg.text or "").strip()
        if not text:
            await msg.reply_text(t(lang, "withdraw_send_wallet"))
            return WITHDRAW_WALLET
        parts = text.split()
        wallet_address = parts[0]
        wallet_network = parts[1] if len(parts) > 1 else ''
        if wallet_address.startswith('/'):
            await msg.reply_text(t(lang, "withdraw_looks_command"))
            return WITHDRAW_WALLET
        if not is_probable_wallet(wallet_address):
            await msg.reply_text(t(lang, "withdraw_invalid_wallet"))
            context.user_data['pending_wallet_candidate'] = (wallet_address, wallet_network)
            return WITHDRAW_WALLET
        async with async_session() as session:
            await update_user(session, user_id, wallet_address=wallet_address, wallet_network=wallet_network)

    context.user_data['withdraw_wallet'] = wallet_address
    context.user_data['withdraw_network'] = wallet_network
    amount = context.user_data.get('withdraw_amount')
    if amount:
        async with async_session() as session:
            user = await get_user(session, user_id)
            balance = float(user.get('balance') or 0)
            if amount > balance:
                await msg.reply_text(t(lang, "withdraw_insufficient", balance=balance), reply_markup=build_main_menu_keyboard())
                context.user_data.pop('withdraw_amount', None)
                return ConversationHandler.END
            new_balance = balance - amount
            new_in_process = float(user.get('balance_in_process') or 0) + amount
            await update_user(session, user_id, balance=new_balance, balance_in_process=new_in_process)

        await msg.reply_text(
            t(lang, "withdraw_confirm_prompt", amount=amount, wallet=wallet_address, network=wallet_network),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(t(lang, "withdraw_confirm_yes"), callback_data="withdraw_confirm_yes"),
                InlineKeyboardButton(t(lang, "withdraw_confirm_no"), callback_data="withdraw_confirm_no")
            ]])
        )
        return WITHDRAW_CONFIRM
    else:
        await msg.reply_text(t(lang, "withdraw_wallet_saved", wallet=wallet_address, network=wallet_network), parse_mode="HTML", reply_markup=build_main_menu_keyboard(MENU_FULL_TWO_COLUMN))
        context.user_data.pop('withdraw_wallet', None)
        context.user_data.pop('withdraw_network', None)
        return ConversationHandler.END

async def withdraw_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    amount = context.user_data.get('withdraw_amount')
    wallet = context.user_data.get('withdraw_wallet')
    network = context.user_data.get('withdraw_network', '')

    if amount is None or not wallet:
        await query.message.reply_text(t(lang, "withdraw_missing_data"), reply_markup=build_main_menu_keyboard(MENU_FULL_TWO_COLUMN))
        context.user_data.pop('withdraw_amount', None)
        context.user_data.pop('withdraw_wallet', None)
        context.user_data.pop('withdraw_network', None)
        return ConversationHandler.END

    async with async_session() as session:
        tx_db_id, tx_ref = await log_transaction(
            session,
            user_id=user_id,
            ref=None,
            type='withdraw',
            amount=amount,
            status='pending',
            proof='',
            wallet=wallet,
            network=network,
            created_at=datetime.utcnow()
        )

    now = datetime.utcnow()
    pdt_str = (now.replace(tzinfo=timezone.utc) - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M (PDT)")
    withdraw_request_text = t(lang, "withdraw_request_success", ref=tx_ref, amount=amount, network=network or 'N/A', wallet=wallet, date=pdt_str)

    try:
        await query.message.reply_text(withdraw_request_text, parse_mode="HTML")
    except Exception:
        await query.message.reply_text(withdraw_request_text)

    async with async_session() as session:
        result = await session.execute(select(Transaction).where(Transaction.id == tx_db_id))
        tx = result.scalar_one_or_none()

    username = (query.from_user.username or "").strip() if getattr(query, "from_user", None) else None
    if username == "":
        username = None

    try:
        await send_admin_tx_notification(context.application.bot, tx, proof_file_id=None, username=username)
    except Exception:
        logger.exception("Failed sending admin notification for withdraw %s", tx_db_id)
    await post_admin_log(context.application.bot, f"New WITHDRAW #{tx_db_id} ref {tx_ref} user {user_id} username @{username or 'N/A'} amount {amount:.2f}$")

    context.user_data.pop('withdraw_amount', None)
    context.user_data.pop('withdraw_wallet', None)
    context.user_data.pop('withdraw_network', None)
    return ConversationHandler.END

# -----------------------
# ADMIN flows (approve/reject)
# -----------------------
async def admin_start_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("admin_start_action_callback invoked without callback_query")
        return
    await query.answer()
    if not _is_admin(query.from_user.id):
        logger.warning("admin_start_action_callback: user %s is not admin", query.from_user.id)
        await query.message.reply_text("Forbidden: admin only.")
        return
    logger.info("admin_start_action_callback: admin %s requested action %s", query.from_user.id, query.data)
    data = query.data
    parts = data.split("_")
    if len(parts) < 4:
        await query.message.reply_text("Invalid admin action callback.")
        return
    action = parts[2]
    tx_db_id = int(parts[3])
    await query.message.reply_text(f"Are you sure you want to {action} transaction {tx_db_id}?", reply_markup=admin_confirm_kb(action, tx_db_id))

async def admin_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.warning("admin_confirm_callback invoked without callback_query")
        return
    await query.answer()
    if not _is_admin(query.from_user.id):
        logger.warning("admin_confirm_callback: user %s is not admin", query.from_user.id)
        await query.message.reply_text("Forbidden: admin only.")
        return
    logger.info("admin_confirm_callback: admin %s confirmed %s", query.from_user.id, query.data)
    data = query.data
    parts = data.split("_")
    if len(parts) < 4:
        await query.message.reply_text("Invalid confirmation data.")
        return
    action = parts[2]
    tx_db_id = int(parts[3])

    try:
        async with async_session() as session:
            result = await session.execute(select(Transaction).where(Transaction.id == tx_db_id))
            tx = result.scalar_one_or_none()
            if not tx:
                await query.message.reply_text("Transaction not found.")
                return
            if tx.status != 'pending':
                await query.message.reply_text(f"Transaction already processed (status: {tx.status}).")
                return

            if action == 'approve':
                if tx.type == 'invest':
                    user = await get_user(session, tx.user_id)
                    new_balance = float(user.get('balance') or 0) + float(tx.amount or 0)
                    await update_user(session, tx.user_id, balance=new_balance)
                    await session.execute(sa_update(Transaction).where(Transaction.id == tx_db_id).values(status='credited'))
                    await session.commit()
                    
                    # Process referral commission (2% of first deposit only)
                    referrer_id = user.get('referrer_id')
                    if referrer_id:
                        # Check if this is the user's first credited deposit
                        result_check = await session.execute(
                            select(Transaction).where(
                                Transaction.user_id == tx.user_id,
                                Transaction.type == 'invest',
                                Transaction.status == 'credited',
                                Transaction.id != tx_db_id  # Exclude current transaction
                            )
                        )
                        previous_deposits = result_check.scalars().all()
                        
                        # Only give commission on first deposit
                        if not previous_deposits:
                            commission_rate = 0.02  # 2% commission on first deposit
                            commission = float(tx.amount or 0) * commission_rate
                            
                            # Get referrer and update their earnings
                            referrer = await get_user(session, referrer_id)
                            referrer_balance = float(referrer.get('balance') or 0) + commission
                            referrer_earnings = float(referrer.get('referral_earnings') or 0) + commission
                            
                            await update_user(session, referrer_id, 
                                            balance=referrer_balance,
                                            referral_earnings=referrer_earnings)
                            
                            # Log commission transaction
                            await log_transaction(
                                session,
                                user_id=referrer_id,
                                ref=None,
                                type='referral_commission',
                                amount=commission,
                                status='credited',
                                proof=f'Commission from user {tx.user_id} first deposit',
                                wallet='',
                                network='',
                                created_at=datetime.utcnow()
                            )
                            
                            # Notify referrer
                            try:
                                await context.application.bot.send_message(
                                    chat_id=referrer_id,
                                    text=f"💰 Referral Commission Earned!\n\n"
                                         f"Amount: {commission:.2f} USDT (2%)\n"
                                         f"From: User {tx.user_id}'s first deposit\n"
                                         f"New Balance: {referrer_balance:.2f} USDT"
                                )
                            except Exception:
                                logger.exception("Failed to notify referrer")

                    receipt_text = (
                        "  **Deposit Receipt **\n"
                        "✅ Your deposit has been approved and credited\n"
                        f"Transaction ID, D-{tx.ref}\n"
                        f"Amount, {float(tx.amount):.2f} USDT\n"
                        f"Date, {(datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=7)).strftime('%Y-%m-%d %H:%M (PDT)')}\n"
                        f"New balance: ${new_balance:.2f}"
                    )
                    try:
                        await context.application.bot.send_message(chat_id=tx.user_id, text=receipt_text, parse_mode="HTML")
                    except Exception:
                        logger.exception("Notify user fail invest approve")
                    await query.message.reply_text(f"Invest #{tx_db_id} credited.")
                    await post_admin_log(context.application.bot, f"Admin approved INVEST #{tx_db_id} ref {tx.ref}")

                elif tx.type == 'withdraw':
                    user = await get_user(session, tx.user_id)
                    new_in_process = max(0.0, float(user.get('balance_in_process') or 0) - float(tx.amount or 0))
                    await update_user(session, tx.user_id, balance_in_process=new_in_process)
                    await session.execute(sa_update(Transaction).where(Transaction.id == tx_db_id).values(status='completed'))
                    await session.commit()

                    receipt_text = (
                        "  **Withdrawal Receipt **\n"
                        "✅ Your withdrawal has been approved and processed\n"
                        f"Transaction ID, W-{tx.ref}\n"
                        f"Amount, {float(tx.amount):.2f} USDT\n"
                        f"Date, {(datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=7)).strftime('%Y-%m-%d %H:%M (PDT)')}\n"
                        f"Wallet: {tx.wallet}\n"
                        f"Network: {tx.network}\n"
                        "If you don't see the funds in your wallet within a few minutes, please contact support."
                    )
                    try:
                        await context.application.bot.send_message(chat_id=tx.user_id, text=receipt_text, parse_mode="HTML")
                    except Exception:
                        logger.exception("Notify user fail withdraw complete")
                    await query.message.reply_text(f"Withdraw #{tx_db_id} completed.")
                    await post_admin_log(context.application.bot, f"Admin approved WITHDRAW #{tx_db_id} ref {tx.ref}")

            else:
                # reject
                if tx.type == 'invest':
                    await session.execute(sa_update(Transaction).where(Transaction.id == tx_db_id).values(status='rejected'))
                    await session.commit()
                    try:
                        await context.application.bot.send_message(chat_id=tx.user_id, text=f"❌ Your deposit (ref D-{tx.ref}) was rejected by admin.")
                    except Exception:
                        logger.exception("Notify user invest reject fail")
                    await query.message.reply_text(f"Invest #{tx_db_id} rejected.")
                    await post_admin_log(context.application.bot, f"Admin rejected INVEST #{tx_db_id} ref {tx.ref}")

                elif tx.type == 'withdraw':
                    user = await get_user(session, tx.user_id)
                    new_in_process = max(0.0, float(user.get('balance_in_process') or 0) - float(tx.amount or 0))
                    new_balance = float(user.get('balance') or 0) + float(tx.amount or 0)
                    await update_user(session, tx.user_id, balance=new_balance, balance_in_process=new_in_process)
                    await session.execute(sa_update(Transaction).where(Transaction.id == tx_db_id).values(status='rejected'))
                    await session.commit()
                    try:
                        await context.application.bot.send_message(chat_id=tx.user_id, text=f"❌ Your withdrawal (ref W-{tx.ref}) was rejected by admin. Funds restored.")
                    except Exception:
                        logger.exception("Notify user withdraw reject fail")
                    await query.message.reply_text(f"Withdraw #{tx_db_id} rejected and funds restored.")
                    await post_admin_log(context.application.bot, f"Admin rejected WITHDRAW #{tx_db_id} ref {tx.ref}")

    except Exception:
        logger.exception("Error handling admin confirmation for tx %s", tx_db_id)

    return

# Admin cancel handler
async def admin_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("Action cancelled.")

# -----------------------
# Admin commands to control trading simulation
# -----------------------
def _is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID and ADMIN_ID != 0

async def cmd_trade_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    global TRADING_ENABLED
    TRADING_ENABLED = True
    
    # Persist to database
    async with async_session() as session:
        await set_config(session, 'trading_enabled', '1')
    
    await update.effective_message.reply_text("✅ Trading simulation ENABLED.")
    await post_admin_log(context.bot, "Admin enabled trading simulation.")

async def cmd_trade_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    global TRADING_ENABLED
    TRADING_ENABLED = False
    
    # Persist to database
    async with async_session() as session:
        await set_config(session, 'trading_enabled', '0')
    
    await update.effective_message.reply_text("❌ Trading simulation DISABLED.")
    await post_admin_log(context.bot, "Admin disabled trading simulation.")

async def cmd_trade_freq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    args = context.args
    if not args or not args[0].isdigit():
        await update.effective_message.reply_text("Usage: /trade_freq <minutes> (integer)")
        return
    minutes = max(1, int(args[0]))
    global TRADING_FREQ_MINUTES
    TRADING_FREQ_MINUTES = minutes
    await update.effective_message.reply_text(f"Trading frequency set to {minutes} minutes. Will apply after restart or when triggered with /trade_now.")
    await post_admin_log(context.bot, f"Admin set trading frequency to {minutes} minutes.")

async def cmd_trade_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    await update.effective_message.reply_text("Running trading job now...")
    await trading_job()
    await update.effective_message.reply_text("Trading run completed.")
    await post_admin_log(context.bot, "Admin triggered immediate trading run.")

async def cmd_trade_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    await update.effective_message.reply_text(f"Trading: {'ENABLED' if TRADING_ENABLED else 'DISABLED'}\nFrequency: {TRADING_FREQ_MINUTES} minutes\nTrades per day: {TRADES_PER_DAY}\nSimulated pairs: {', '.join(PRICE_PAIRS.keys())}")

async def cmd_set_trades_per_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    args = context.args
    if not args:
        await update.effective_message.reply_text("Usage: /set_trades_per_day <number> (positive integer, e.g., 144 for every 10 minutes)")
        return
    
    try:
        trades_per_day = int(args[0])
    except ValueError:
        await update.effective_message.reply_text("Invalid number. Usage: /set_trades_per_day <number> (positive integer)")
        return
    
    if trades_per_day <= 0:
        await update.effective_message.reply_text("Trades per day must be a positive integer.")
        return
    
    # Calculate frequency in minutes (use round for accuracy)
    freq_minutes = max(1, round(MINUTES_PER_DAY / trades_per_day))
    
    global TRADES_PER_DAY, TRADING_FREQ_MINUTES
    TRADES_PER_DAY = trades_per_day
    TRADING_FREQ_MINUTES = freq_minutes
    
    # Reschedule the trading job
    global _scheduler
    if _scheduler:
        # Remove existing trading job by ID
        try:
            _scheduler.remove_job(TRADING_JOB_ID)
            logger.info("Removed existing trading job with id: %s", TRADING_JOB_ID)
        except JobLookupError:
            logger.info("Trading job does not exist yet, will create new one")
        
        # Add new job with updated frequency and explicit ID
        _scheduler.add_job(
            trading_job, 
            'interval', 
            minutes=freq_minutes, 
            id=TRADING_JOB_ID,
            next_run_time=datetime.utcnow() + timedelta(seconds=5)
        )
        logger.info("Rescheduled trading job with frequency: %d minutes (trades per day: %d)", freq_minutes, trades_per_day)
    
    await update.effective_message.reply_text(
        f"✅ Trades per day set to {trades_per_day}.\n"
        f"Trading frequency: {freq_minutes} minutes.\n"
        f"Changes applied immediately."
    )
    await post_admin_log(context.bot, f"Admin set trades per day to {trades_per_day} (frequency: {freq_minutes} minutes).")

async def cmd_set_negative_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_negative_trades <number> - Set how many trades per day should be negative"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    args = context.args
    if not args:
        await update.effective_message.reply_text("Usage: /set_negative_trades <number>\nExample: /set_negative_trades 5\n\nSets how many trades per day should result in losses (-0.05% to -0.25%).")
        return
    
    try:
        negative_trades = int(args[0])
    except ValueError:
        await update.effective_message.reply_text("Invalid number. Usage: /set_negative_trades <number> (positive integer)")
        return
    
    if negative_trades < 0:
        await update.effective_message.reply_text("Number must be non-negative (0 or more).")
        return
    
    if negative_trades > TRADES_PER_DAY:
        await update.effective_message.reply_text(f"⚠️ Warning: Negative trades ({negative_trades}) exceeds total trades per day ({TRADES_PER_DAY}).\nSetting anyway, but consider adjusting trades per day.")
    
    # Store in Config
    async with async_session() as session:
        await set_config(session, 'negative_trades_per_day', str(negative_trades))
    
    await update.effective_message.reply_text(
        f"✅ Negative trades per day set to {negative_trades}.\n"
        f"Each negative trade will result in a loss of -0.05% to -0.25%."
    )
    await post_admin_log(context.bot, f"Admin set negative trades per day to {negative_trades}.")

async def cmd_set_daily_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_daily_percent <percent>"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    args = context.args
    if not args:
        await update.effective_message.reply_text("Usage: /set_daily_percent <percent> (e.g., 1.5 for 1.5%)")
        return
    
    try:
        percent = float(args[0])
    except ValueError:
        await update.effective_message.reply_text("Invalid number. Usage: /set_daily_percent <percent>")
        return
    
    if percent <= 0:
        await update.effective_message.reply_text("Daily percent must be positive.")
        return
    
    global GLOBAL_DAILY_PERCENT
    GLOBAL_DAILY_PERCENT = percent
    
    await update.effective_message.reply_text(f"✅ Global daily percent set to {percent}%")
    await post_admin_log(context.bot, f"Admin set global daily percent to {percent}%")

async def cmd_set_trade_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_trade_percent <percent>"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    args = context.args
    if not args:
        await update.effective_message.reply_text("Usage: /set_trade_percent <percent> (e.g., 0.5 for 0.5% per trade)")
        return
    
    try:
        percent = float(args[0])
    except ValueError:
        await update.effective_message.reply_text("Invalid number. Usage: /set_trade_percent <percent>")
        return
    
    if percent <= 0:
        await update.effective_message.reply_text("Trade percent must be positive.")
        return
    
    global GLOBAL_TRADE_PERCENT
    GLOBAL_TRADE_PERCENT = percent
    
    await update.effective_message.reply_text(f"✅ Global trade percent set to {percent}%")
    await post_admin_log(context.bot, f"Admin set global trade percent to {percent}%")

async def cmd_set_user_trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_user_trade <user_id> <pair> <percent>"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    args = context.args
    if len(args) < 3:
        await update.effective_message.reply_text("Usage: /set_user_trade <user_id> <pair> <percent>\nExample: /set_user_trade 123456 BTCUSDT 0.5")
        return
    
    try:
        target_user_id = int(args[0])
        pair = args[1].upper()
        percent = float(args[2])
    except ValueError:
        await update.effective_message.reply_text("Invalid arguments. Usage: /set_user_trade <user_id> <pair> <percent>")
        return
    
    if percent <= 0:
        await update.effective_message.reply_text("Percent must be positive.")
        return
    
    async with async_session() as session:
        await set_user_trade_config(session, target_user_id, pair, percent)
    
    await update.effective_message.reply_text(
        f"✅ User {target_user_id} trade config set:\n"
        f"Pair: {pair}\n"
        f"Percent per trade: {percent}%"
    )
    await post_admin_log(context.bot, f"Admin set user {target_user_id} trade config: {pair} @ {percent}%")

async def cmd_trading_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /trading_status - show current config"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    # Get configured ranges from Config
    async with async_session() as session:
        trade_min = await get_config(session, 'trade_range_min', '0.05')
        trade_max = await get_config(session, 'trade_range_max', '0.25')
        daily_min = await get_config(session, 'daily_range_min', '1.25')
        daily_max = await get_config(session, 'daily_range_max', '1.5')
        negative_trades = await get_config(session, 'negative_trades_per_day', '5')
        
        result = await session.execute(select(UserTradeConfig))
        user_configs = result.scalars().all()
        override_count = len(user_configs)
        
        override_text = ""
        if user_configs:
            override_text = "\n\n👥 Per-user overrides:\n"
            for cfg in user_configs[:10]:  # Show max 10
                override_text += f"  User {cfg.user_id}: {cfg.pair} @ {float(cfg.percent_per_trade)}%\n"
            if len(user_configs) > 10:
                override_text += f"  ... and {len(user_configs) - 10} more\n"
    
    status_text = (
        "⚙️ Trading Configuration Status\n\n"
        f"🔄 Trading: {'ENABLED' if TRADING_ENABLED else 'DISABLED'}\n"
        f"⏱ Frequency: {TRADING_FREQ_MINUTES} minutes (determined by trades per day)\n"
        f"📊 Trades per day: {TRADES_PER_DAY} (determined by frequency)\n"
        f"💹 Global daily percent: {daily_min}% to {daily_max}%\n"
        f"📈 Global trade percent: {trade_min}% to {trade_max}%\n"
        f"📉 Negative trades per day: {negative_trades} (loss: -0.05% to -0.25%)\n"
        f"🪙 Trading pairs: Random from 20 diverse coins\n"
        f"👤 User overrides: {override_count}"
        f"{override_text}"
    )
    
    await update.effective_message.reply_text(status_text)

async def cmd_trading_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /trading_summary [YYYY-MM-DD] - show aggregated daily summary"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    # Parse date argument or use today
    args = context.args
    if args:
        try:
            target_date = datetime.strptime(args[0], "%Y-%m-%d").date()
        except ValueError:
            await update.effective_message.reply_text("Invalid date format. Use YYYY-MM-DD")
            return
    else:
        target_date = datetime.utcnow().date()
    
    async with async_session() as session:
        # Get all summaries for the date
        start_dt = datetime(target_date.year, target_date.month, target_date.day)
        end_dt = start_dt + timedelta(days=1)
        
        result = await session.execute(
            select(DailySummary).where(
                DailySummary.date >= start_dt,
                DailySummary.date < end_dt
            )
        )
        summaries = result.scalars().all()
        
        if not summaries:
            await update.effective_message.reply_text(f"No trading summaries found for {target_date}")
            return
        
        # Aggregate
        total_users = len(summaries)
        total_profit = sum(float(s.profit_amount) for s in summaries)
        total_balance = sum(float(s.total_balance) for s in summaries)
        avg_percent = sum(float(s.daily_percent) for s in summaries) / total_users if total_users > 0 else 0
        
        summary_text = (
            f"📊 Trading Summary for {target_date}\n\n"
            f"👥 Active users: {total_users}\n"
            f"💰 Total profit: {total_profit:.2f} USDT\n"
            f"📈 Total balance: {total_balance:.2f} USDT\n"
            f"💹 Average daily %: {avg_percent:.2f}%"
        )
        
        await update.effective_message.reply_text(summary_text)

async def cmd_use_binance_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /use_binance_on - Enable Binance API"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    global USE_BINANCE
    USE_BINANCE = True
    
    # Persist to database
    async with async_session() as session:
        await set_config(session, 'use_binance_api', '1')
    
    await update.effective_message.reply_text("✅ Binance API enabled")
    await post_admin_log(context.bot, "Admin enabled Binance API")

async def cmd_use_binance_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /use_binance_off - Disable Binance API"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    global USE_BINANCE
    USE_BINANCE = False
    
    # Persist to database
    async with async_session() as session:
        await set_config(session, 'use_binance_api', '0')
    
    await update.effective_message.reply_text("✅ Binance API disabled (will use simulated prices)")
    await post_admin_log(context.bot, "Admin disabled Binance API")

async def cmd_binance_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /binance_status - Show Binance API status"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    status_text = (
        "🔗 Binance API Status\n\n"
        f"Status: {'✅ ENABLED' if USE_BINANCE else '❌ DISABLED'}\n"
        f"API URL: {BINANCE_API_URL}\n"
        f"Cache TTL: {BINANCE_CACHE_TTL}s\n"
        f"Cache entries: {len(_binance_price_cache)}"
    )
    
    await update.effective_message.reply_text(status_text)

async def cmd_set_daily_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_daily_range <min> <max> - Set allowed daily percent range (e.g., 1.25 1.5)"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text(
            "Usage: /set_daily_range <min> <max>\n"
            "Example: /set_daily_range 1.25 1.5\n"
            "Sets the allowed daily percent range."
        )
        return
    
    try:
        min_percent = float(args[0])
        max_percent = float(args[1])
    except ValueError:
        await update.effective_message.reply_text("Invalid numbers. Usage: /set_daily_range <min> <max>")
        return
    
    # Validate bounds: daily must be 1.25% - 1.5%
    ALLOWED_DAILY_MIN = 1.25
    ALLOWED_DAILY_MAX = 1.5
    
    if min_percent < ALLOWED_DAILY_MIN or min_percent > ALLOWED_DAILY_MAX:
        await update.effective_message.reply_text(
            f"❌ Min daily percent must be between {ALLOWED_DAILY_MIN}% and {ALLOWED_DAILY_MAX}%"
        )
        return
    
    if max_percent < ALLOWED_DAILY_MIN or max_percent > ALLOWED_DAILY_MAX:
        await update.effective_message.reply_text(
            f"❌ Max daily percent must be between {ALLOWED_DAILY_MIN}% and {ALLOWED_DAILY_MAX}%"
        )
        return
    
    if min_percent > max_percent:
        await update.effective_message.reply_text("❌ Min percent cannot be greater than max percent")
        return
    
    # Store in Config
    async with async_session() as session:
        await set_config(session, 'daily_range_min', str(min_percent))
        await set_config(session, 'daily_range_max', str(max_percent))
    
    await update.effective_message.reply_text(
        f"✅ Daily percent range set to {min_percent}% - {max_percent}%"
    )
    await post_admin_log(context.bot, f"Admin set daily range to {min_percent}% - {max_percent}%")

async def cmd_set_trade_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_trade_range <min> <max> - Set allowed per-trade percent range (e.g., 0.05 0.25)"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    args = context.args
    if len(args) < 2:
        await update.effective_message.reply_text(
            "Usage: /set_trade_range <min> <max>\n"
            "Example: /set_trade_range 0.05 0.25\n"
            "Sets the allowed per-trade percent range."
        )
        return
    
    try:
        min_percent = float(args[0])
        max_percent = float(args[1])
    except ValueError:
        await update.effective_message.reply_text("Invalid numbers. Usage: /set_trade_range <min> <max>")
        return
    
    # Validate bounds: trade must be 0.05% - 0.25%
    ALLOWED_TRADE_MIN = 0.05
    ALLOWED_TRADE_MAX = 0.25
    
    if min_percent < ALLOWED_TRADE_MIN or min_percent > ALLOWED_TRADE_MAX:
        await update.effective_message.reply_text(
            f"❌ Min trade percent must be between {ALLOWED_TRADE_MIN}% and {ALLOWED_TRADE_MAX}%"
        )
        return
    
    if max_percent < ALLOWED_TRADE_MIN or max_percent > ALLOWED_TRADE_MAX:
        await update.effective_message.reply_text(
            f"❌ Max trade percent must be between {ALLOWED_TRADE_MIN}% and {ALLOWED_TRADE_MAX}%"
        )
        return
    
    if min_percent > max_percent:
        await update.effective_message.reply_text("❌ Min percent cannot be greater than max percent")
        return
    
    # Store in Config
    async with async_session() as session:
        await set_config(session, 'trade_range_min', str(min_percent))
        await set_config(session, 'trade_range_max', str(max_percent))
    
    await update.effective_message.reply_text(
        f"✅ Per-trade percent range set to {min_percent}% - {max_percent}%"
    )
    await post_admin_log(context.bot, f"Admin set trade range to {min_percent}% - {max_percent}%")

async def cmd_set_trading_hours(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_trading_hours <start_hour> <end_hour> - Set trading hours in NY timezone (0-23)"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    if not context.args or len(context.args) != 2:
        await update.effective_message.reply_text(
            "Usage: /set_trading_hours <start_hour> <end_hour>\n"
            "Example: /set_trading_hours 5 18\n"
            "(Sets trading window to 5 AM - 6 PM NY time)\n"
            "Hours are in 24-hour format (0-23)"
        )
        return
    
    try:
        start_hour = int(context.args[0])
        end_hour = int(context.args[1])
    except ValueError:
        await update.effective_message.reply_text("❌ Invalid hours. Both start and end must be integers (0-23)")
        return
    
    if not (0 <= start_hour <= 23) or not (0 <= end_hour <= 23):
        await update.effective_message.reply_text("❌ Hours must be between 0 and 23")
        return
    
    if start_hour >= end_hour:
        await update.effective_message.reply_text("❌ Start hour must be less than end hour")
        return
    
    # Store in Config
    async with async_session() as session:
        await set_config(session, 'trading_start_hour', str(start_hour))
        await set_config(session, 'trading_end_hour', str(end_hour))
    
    duration = end_hour - start_hour
    await update.effective_message.reply_text(
        f"✅ Trading hours set to {start_hour:02d}:00 - {end_hour:02d}:00 NY time\n"
        f"({duration} hour window)"
    )
    await post_admin_log(context.bot, f"Admin set trading hours to {start_hour:02d}:00 - {end_hour:02d}:00 NY time")

async def cmd_trading_hours_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /trading_hours_status - Show current trading hours configuration"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    async with async_session() as session:
        start_hour = int(await get_config(session, 'trading_start_hour', str(TRADING_START_HOUR)))
        end_hour = int(await get_config(session, 'trading_end_hour', str(TRADING_END_HOUR)))
    
    now = datetime.utcnow()
    ny_hour = (now.hour - 5) % 24
    duration = end_hour - start_hour
    is_trading_time = start_hour <= ny_hour < end_hour
    
    status = "🟢 ACTIVE" if is_trading_time else "🔴 INACTIVE"
    
    await update.effective_message.reply_text(
        f"⏰ <b>Trading Hours Configuration</b>\n\n"
        f"Window: {start_hour:02d}:00 - {end_hour:02d}:00 NY time\n"
        f"Duration: {duration} hours\n"
        f"Current NY time: {ny_hour:02d}:{now.minute:02d}\n"
        f"Status: {status}\n\n"
        f"Use /set_trading_hours to change",
        parse_mode='HTML'
    )

# Notification/Broadcast commands
async def cmd_set_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_broadcast_message <message> - Set broadcast message for all users"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    # Get message from command args
    message = ' '.join(context.args) if context.args else None
    
    if not message:
        await update.effective_message.reply_text(
            "Usage: /set_broadcast_message <your message>\n\n"
            "Example: /set_broadcast_message 🎉 Great news! AI just completed a profitable trade. Check your balance!"
        )
        return
    
    async with async_session() as session:
        await set_config(session, 'broadcast_message', message)
    
    await update.effective_message.reply_text(
        f"✅ Broadcast message set:\n\n{message}\n\n"
        f"Use /send_broadcast to send this to all users."
    )
    await post_admin_log(context.bot, f"Admin set broadcast message")

async def cmd_set_new_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_new_user_message <message> - Set message for users who haven't invested"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    message = ' '.join(context.args) if context.args else None
    
    if not message:
        await update.effective_message.reply_text(
            "Usage: /set_new_user_message <your message>\n\n"
            "Example: /set_new_user_message 🤖 AI performed a profitable trade! Ready to start earning? Make your first deposit now!"
        )
        return
    
    async with async_session() as session:
        await set_config(session, 'new_user_message', message)
    
    await update.effective_message.reply_text(
        f"✅ New user message set:\n\n{message}\n\n"
        f"Use /send_new_user_alert to send this to users who haven't invested."
    )
    await post_admin_log(context.bot, f"Admin set new user message")

async def cmd_send_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /send_broadcast - Send broadcast message to all users"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    async with async_session() as session:
        message = await get_config(session, 'broadcast_message')
        
        if not message:
            await update.effective_message.reply_text(
                "❌ No broadcast message set. Use /set_broadcast_message first."
            )
            return
        
        # Get all users
        result = await session.execute(select(User))
        users = result.scalars().all()
    
    sent_count = 0
    failed_count = 0
    
    status_msg = await update.effective_message.reply_text(
        f"📤 Sending broadcast to {len(users)} users..."
    )
    
    for user in users:
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=message,
                parse_mode="HTML"
            )
            sent_count += 1
            
            # Rate limiting
            if sent_count % 20 == 0:
                await asyncio.sleep(1)
        except Exception as e:
            logger.exception(f"Failed to send broadcast to user {user.id}")
            failed_count += 1
    
    await status_msg.edit_text(
        f"✅ Broadcast complete!\n\n"
        f"✔️ Sent: {sent_count}\n"
        f"❌ Failed: {failed_count}\n\n"
        f"Message:\n{message}"
    )
    await post_admin_log(context.bot, f"Admin sent broadcast to {sent_count} users")

async def cmd_send_new_user_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /send_new_user_alert - Send message to users who haven't invested"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    async with async_session() as session:
        message = await get_config(session, 'new_user_message')
        
        if not message:
            await update.effective_message.reply_text(
                "❌ No new user message set. Use /set_new_user_message first."
            )
            return
        
        # Get users who have no credited investments
        result = await session.execute(
            select(User).where(
                ~User.id.in_(
                    select(Transaction.user_id).where(
                        Transaction.type == 'invest',
                        Transaction.status == 'credited'
                    ).distinct()
                )
            )
        )
        users = result.scalars().all()
    
    sent_count = 0
    failed_count = 0
    
    status_msg = await update.effective_message.reply_text(
        f"📤 Sending new user alert to {len(users)} users who haven't invested..."
    )
    
    for user in users:
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=message,
                parse_mode="HTML"
            )
            sent_count += 1
            
            # Rate limiting
            if sent_count % 20 == 0:
                await asyncio.sleep(1)
        except Exception as e:
            logger.exception(f"Failed to send new user alert to user {user.id}")
            failed_count += 1
    
    await status_msg.edit_text(
        f"✅ New user alert complete!\n\n"
        f"✔️ Sent: {sent_count}\n"
        f"❌ Failed: {failed_count}\n\n"
        f"Message:\n{message}"
    )
    await post_admin_log(context.bot, f"Admin sent new user alert to {sent_count} users")

async def cmd_view_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /view_notifications - View current notification messages"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    async with async_session() as session:
        broadcast_msg = await get_config(session, 'broadcast_message', 'Not set')
        new_user_msg = await get_config(session, 'new_user_message', 'Not set')
    
    text = (
        "📋 <b>Current Notification Messages</b>\n\n"
        "<b>Broadcast Message (All Users):</b>\n"
        f"{broadcast_msg}\n\n"
        "<b>New User Message (Non-Investors):</b>\n"
        f"{new_user_msg}\n\n"
        "<i>Use /set_broadcast_message or /set_new_user_message to update.</i>"
    )
    
    await update.effective_message.reply_text(text, parse_mode="HTML")

async def cmd_admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /admin_stats - Show analytics dashboard"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    async with async_session() as session:
        # Total users
        result = await session.execute(select(User))
        all_users = result.scalars().all()
        total_users = len(all_users)
        
        # Active investors (users with at least one credited investment)
        result = await session.execute(
            select(Transaction.user_id).where(
                Transaction.type == 'invest',
                Transaction.status == 'credited'
            ).distinct()
        )
        active_investors = len(result.scalars().all())
        
        # Pending transactions
        result = await session.execute(
            select(Transaction).where(Transaction.status == 'pending')
        )
        pending_txs = result.scalars().all()
        pending_count = len(pending_txs)
        pending_invest = sum(float(tx.amount) for tx in pending_txs if tx.type == 'invest')
        pending_withdraw = sum(float(tx.amount) for tx in pending_txs if tx.type == 'withdraw')
        
        # Today's volume
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        result = await session.execute(
            select(Transaction).where(
                Transaction.created_at >= today_start,
                Transaction.status == 'credited'
            )
        )
        today_txs = result.scalars().all()
        today_deposits = sum(float(tx.amount) for tx in today_txs if tx.type == 'invest')
        today_withdrawals = sum(float(tx.amount) for tx in today_txs if tx.type == 'withdraw')
        
        # Total platform balances
        total_balance = sum(float(u.balance or 0) for u in all_users)
        total_in_process = sum(float(u.balance_in_process or 0) for u in all_users)
        total_profit_paid = sum(float(u.total_profit or 0) for u in all_users)
        
        # New users today
        new_today = sum(1 for u in all_users if u.joined_at and u.joined_at >= today_start)
    
    stats_text = (
        f"📊 <b>Admin Analytics Dashboard</b>\n\n"
        f"👥 <b>Users:</b>\n"
        f"Total Users: {total_users}\n"
        f"Active Investors: {active_investors}\n"
        f"New Today: {new_today}\n\n"
        f"⏳ <b>Pending Transactions:</b>\n"
        f"Count: {pending_count}\n"
        f"💰 Deposits: ${pending_invest:.2f}\n"
        f"💸 Withdrawals: ${pending_withdraw:.2f}\n\n"
        f"📈 <b>Today's Volume:</b>\n"
        f"💰 Deposits: ${today_deposits:.2f}\n"
        f"💸 Withdrawals: ${today_withdrawals:.2f}\n"
        f"📊 Net: ${today_deposits - today_withdrawals:.2f}\n\n"
        f"💼 <b>Platform Totals:</b>\n"
        f"Available Balance: ${total_balance:.2f}\n"
        f"In Process: ${total_in_process:.2f}\n"
        f"Total Profits Paid: ${total_profit_paid:.2f}\n\n"
        f"<i>Use /pending to manage transactions</i>"
    )
    
    await update.effective_message.reply_text(stats_text, parse_mode="HTML")

async def cmd_system_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /system_status - Show system health check"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    # Calculate bot uptime
    if _scheduler and _scheduler.running:
        scheduler_status = "✅ Running"
    else:
        scheduler_status = "❌ Not Running"
    
    # Check database connection
    try:
        async with async_session() as session:
            await session.execute(select(User).limit(1))
        db_status = "✅ Connected"
    except Exception as e:
        db_status = f"❌ Error: {str(e)[:50]}"
    
    # Check trading system
    async with async_session() as session:
        trading_enabled = await get_config(session, 'trading_enabled', '1')
        use_binance = await get_config(session, 'use_binance_api', '1')
    
    trading_status = "✅ Enabled" if trading_enabled == '1' else "❌ Disabled"
    binance_status = "✅ Enabled" if use_binance == '1' else "❌ Disabled"
    
    # Get recent error count
    try:
        async with async_session() as session:
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            result = await session.execute(
                select(ErrorLog).where(ErrorLog.created_at >= one_hour_ago)
            )
            recent_errors = len(result.scalars().all())
    except:
        recent_errors = "N/A"
    
    status_text = (
        f"🔧 <b>System Status</b>\n\n"
        f"⚙️ <b>Core Systems:</b>\n"
        f"Bot: ✅ Online\n"
        f"Scheduler: {scheduler_status}\n"
        f"Database: {db_status}\n\n"
        f"🤖 <b>Trading System:</b>\n"
        f"Trading: {trading_status}\n"
        f"Binance API: {binance_status}\n\n"
        f"⚠️ <b>Errors:</b>\n"
        f"Last Hour: {recent_errors}\n\n"
        f"<i>Use /error_logs to view recent errors</i>"
    )
    
    await update.effective_message.reply_text(status_text, parse_mode="HTML")

async def cmd_error_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /error_logs - View recent error logs"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    try:
        async with async_session() as session:
            # Get last 10 errors
            result = await session.execute(
                select(ErrorLog).order_by(ErrorLog.created_at.desc()).limit(10)
            )
            errors = result.scalars().all()
        
        if not errors:
            await update.effective_message.reply_text("No errors logged recently.")
            return
        
        lines = ["🔴 <b>Recent Errors (Last 10)</b>\n"]
        for err in errors:
            time_str = err.created_at.strftime("%Y-%m-%d %H:%M:%S")
            user_info = f"User: {err.user_id}" if err.user_id else "System"
            cmd_info = f"Cmd: {err.command}" if err.command else ""
            lines.append(
                f"\n<b>{time_str}</b>\n"
                f"Type: {err.error_type}\n"
                f"Message: {err.error_message[:100]}\n"
                f"{user_info} {cmd_info}"
            )
        
        error_text = "\n".join(lines)
        await update.effective_message.reply_text(error_text, parse_mode="HTML")
    except Exception as e:
        await update.effective_message.reply_text(f"Error retrieving logs: {str(e)}")

async def cmd_send_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /send_reminders - Send reminders for pending transactions over 24h"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    try:
        async with async_session() as session:
            # Find pending transactions older than 24 hours
            twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24)
            result = await session.execute(
                select(Transaction).where(
                    Transaction.status == 'pending',
                    Transaction.created_at < twenty_four_hours_ago
                )
            )
            old_pending = result.scalars().all()
        
        if not old_pending:
            await update.effective_message.reply_text("No pending transactions older than 24 hours.")
            return
        
        sent_count = 0
        for tx in old_pending:
            try:
                hours_old = int((datetime.utcnow() - tx.created_at).total_seconds() / 3600)
                if tx.type == 'invest':
                    message = (
                        f"⏰ <b>Deposit Reminder</b>\n\n"
                        f"Your deposit of ${float(tx.amount):.2f} has been pending for {hours_old} hours.\n"
                        f"Transaction ID: D-{tx.ref}\n\n"
                        f"If you've completed the payment, please wait for admin approval.\n"
                        f"If not, please complete your deposit to start earning!"
                    )
                else:  # withdraw
                    message = (
                        f"⏰ <b>Withdrawal Reminder</b>\n\n"
                        f"Your withdrawal of ${float(tx.amount):.2f} has been pending for {hours_old} hours.\n"
                        f"Transaction ID: W-{tx.ref}\n\n"
                        f"Our team is processing your request. You'll be notified once completed."
                    )
                
                await context.bot.send_message(
                    chat_id=tx.user_id,
                    text=message,
                    parse_mode="HTML"
                )
                sent_count += 1
            except Exception as e:
                logger.exception(f"Failed to send reminder to user {tx.user_id}")
        
        await update.effective_message.reply_text(
            f"✅ Sent {sent_count} reminder(s) for {len(old_pending)} pending transaction(s)."
        )
        await post_admin_log(context.bot, f"Admin sent {sent_count} reminders")
    except Exception as e:
        await update.effective_message.reply_text(f"Error sending reminders: {str(e)}")

async def cmd_set_deposit_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /set_deposit_wallet <coin> <network> <address> [primary]"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    try:
        args = context.args if hasattr(context, 'args') and context.args else []
        if len(args) < 3:
            await update.effective_message.reply_text(
                "Usage: /set_deposit_wallet <coin> <network> <address> [primary]\n"
                "Example: /set_deposit_wallet BTC BTC bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh primary\n"
                "Example: /set_deposit_wallet USDT TRC20 TAbc123... primary\n"
                "Example: /set_deposit_wallet SOLANA SOL <sol-address> primary"
            )
            return
        
        coin = args[0].upper()
        network = args[1].upper()
        address = args[2]
        is_primary = len(args) > 3 and args[3].lower() == 'primary'
        
        async with async_session() as session:
            await set_deposit_wallet(session, coin, network, address, is_primary)
        
        primary_text = " (marked as primary)" if is_primary else ""
        await update.effective_message.reply_text(
            f"✅ Deposit wallet added{primary_text}:\n"
            f"Coin: {coin}\n"
            f"Network: {network}\n"
            f"Address: <code>{address}</code>",
            parse_mode="HTML"
        )
        await post_admin_log(context.bot, f"Admin set deposit wallet: {coin}/{network} = {address}{primary_text}")
    except Exception as e:
        logger.exception("Error in cmd_set_deposit_wallet")
        await update.effective_message.reply_text(f"Error setting wallet: {str(e)}")


async def cmd_list_deposit_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /list_deposit_wallets [coin]"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    try:
        args = context.args if hasattr(context, 'args') and context.args else []
        coin = args[0].upper() if args else None
        
        async with async_session() as session:
            wallets = await get_deposit_wallets(session, coin)
        
        if not wallets:
            coin_text = f" for {coin}" if coin else ""
            await update.effective_message.reply_text(f"No deposit wallets configured{coin_text}.")
            return
        
        lines = ["💳 Deposit Wallets:\n"]
        for w in wallets:
            primary_mark = " ⭐ PRIMARY" if w['is_primary'] else ""
            lines.append(
                f"ID: {w['id']}{primary_mark}\n"
                f"  Coin: {w['coin']}\n"
                f"  Network: {w['network']}\n"
                f"  Address: <code>{w['address']}</code>\n"
            )
        
        await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.exception("Error in cmd_list_deposit_wallets")
        await update.effective_message.reply_text(f"Error listing wallets: {str(e)}")

async def cmd_mark_primary_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /mark_primary_wallet <wallet_id>"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    try:
        args = context.args if hasattr(context, 'args') and context.args else []
        if not args or not args[0].isdigit():
            await update.effective_message.reply_text(
                "Usage: /mark_primary_wallet <wallet_id>\n"
                "Use /list_deposit_wallets to see wallet IDs"
            )
            return
        
        wallet_id = int(args[0])
        
        async with async_session() as session:
            success = await mark_primary_deposit_wallet(session, wallet_id)
        
        if success:
            await update.effective_message.reply_text(f"✅ Wallet {wallet_id} marked as primary")
            await post_admin_log(context.bot, f"Admin marked wallet {wallet_id} as primary")
        else:
            await update.effective_message.reply_text(f"❌ Wallet {wallet_id} not found")
    except Exception as e:
        logger.exception("Error in cmd_mark_primary_wallet")
        await update.effective_message.reply_text(f"Error marking wallet as primary: {str(e)}")

async def cmd_remove_deposit_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /remove_deposit_wallet <wallet_id>"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    try:
        args = context.args if hasattr(context, 'args') and context.args else []
        if not args or not args[0].isdigit():
            await update.effective_message.reply_text(
                "Usage: /remove_deposit_wallet <wallet_id>\n"
                "Use /list_deposit_wallets to see wallet IDs"
            )
            return
        
        wallet_id = int(args[0])
        
        async with async_session() as session:
            success = await delete_deposit_wallet(session, wallet_id)
        
        if success:
            await update.effective_message.reply_text(f"✅ Wallet {wallet_id} removed")
            await post_admin_log(context.bot, f"Admin removed wallet {wallet_id}")
        else:
            await update.effective_message.reply_text(f"❌ Wallet {wallet_id} not found")
    except Exception as e:
        logger.exception("Error in cmd_remove_deposit_wallet")
        await update.effective_message.reply_text(f"Error removing wallet: {str(e)}")

async def cmd_admin_cmds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /admin_cmds - Show all admin commands"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    commands_text = (
        "🛠 Admin Commands List\n\n"
        "**Trading Control:**\n"
        "/trade_on - Enable trading\n"
        "/trade_off - Disable trading\n"
        "/trade_status - Show trading status\n"
        "/trade_now - Trigger trading job now\n"
        "/trade_freq [minutes] - Set trading frequency\n"
        "/list_trading_vars - List all trading configuration\n\n"
        "**Binance Control:**\n"
        "/use_binance_on - Enable Binance API\n"
        "/use_binance_off - Disable Binance API\n"
        "/binance_status - Show Binance status\n\n"
        "**Configuration:**\n"
        "/set_trades_per_day <num> - Set trades per day\n"
        "/set_negative_trades <num> - Set negative trades per day\n"
        "/set_daily_percent <percent> - Set daily profit target\n"
        "/set_trade_percent <percent> - Set trade percent\n"
        "/set_daily_range <min> <max> - Set daily percent range (1.25-1.5%)\n"
        "/set_trade_range <min> <max> - Set trade percent range (0.05-0.25%)\n"
        "/set_trading_hours <start> <end> - Set trading hours (NY time, 0-23)\n"
        "/trading_hours_status - Show current trading hours\n"
        "/set_user_trade <user_id> <pair> <percent> - Set user config\n"
        "/trading_status - Show trading config\n"
        "/trading_summary [date] - View daily summary\n\n"
        "**Deposit Wallets:**\n"
        "/set_deposit_wallet <coin> <network> <address> [primary] - Add/update wallet\n"
        "/list_deposit_wallets [coin] - List all deposit wallets\n"
        "/mark_primary_wallet <id> - Mark wallet as primary\n"
        "/remove_deposit_wallet <id> - Remove a wallet\n\n"
        "**Notifications:**\n"
        "/set_broadcast_message <message> - Set broadcast for all users\n"
        "/set_new_user_message <message> - Set message for non-investors\n"
        "/send_broadcast - Send broadcast to all users\n"
        "/send_new_user_alert - Send alert to non-investors\n"
        "/view_notifications - View current messages\n\n"
        "**Analytics:**\n"
        "/admin_stats - View analytics dashboard\n"
        "/system_status - System health check\n"
        "/error_logs - View recent errors\n"
        "/send_reminders - Send reminders for old pending txs\n\n"
        "**Admin:**\n"
        "/admin_cmds - Show this message\n"
        "/pending - Show pending transactions"
    )
    
    await update.effective_message.reply_text(commands_text)

async def cmd_list_trading_vars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: /list_trading_vars - List all trading configuration variables"""
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await update.effective_message.reply_text("Forbidden: admin only.")
        return
    
    async with async_session() as session:
        # Get all config values from database
        trading_enabled_db = await get_config(session, 'trading_enabled', '1')
        use_binance_db = await get_config(session, 'use_binance_api', '1')
        trade_min = await get_config(session, 'trade_range_min', '0.05')
        trade_max = await get_config(session, 'trade_range_max', '0.25')
        daily_min = await get_config(session, 'daily_range_min', '1.25')
        daily_max = await get_config(session, 'daily_range_max', '1.5')
        negative_trades = await get_config(session, 'negative_trades_per_day', '5')
    
    # Check scheduler status
    scheduler_running = "✅ Running" if _scheduler and _scheduler.running else "❌ Not Running"
    
    # Check if trading job exists in scheduler
    trading_job_scheduled = "❌ Not Scheduled"
    next_run = "N/A"
    if _scheduler:
        try:
            job = _scheduler.get_job(TRADING_JOB_ID)
            if job:
                trading_job_scheduled = "✅ Scheduled"
                if job.next_run_time:
                    next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            pass
    
    # Build comprehensive status report
    status_text = (
        "📊 <b>Trading Configuration &amp; Status</b>\n\n"
        "<b>🔧 System Status:</b>\n"
        f"Scheduler: {scheduler_running}\n"
        f"Trading Job: {trading_job_scheduled}\n"
        f"Next Run: {next_run}\n\n"
        "<b>⚙️ Trading Control:</b>\n"
        f"Trading Enabled (Global): {'✅ Yes' if TRADING_ENABLED else '❌ No'}\n"
        f"Trading Enabled (Database): {'✅ Yes' if trading_enabled_db == '1' else '❌ No'}\n"
        f"Binance API (Global): {'✅ Enabled' if USE_BINANCE else '❌ Disabled'}\n"
        f"Binance API (Database): {'✅ Enabled' if use_binance_db == '1' else '❌ Disabled'}\n\n"
        "<b>📈 Trading Parameters:</b>\n"
        f"Trades Per Day: {TRADES_PER_DAY}\n"
        f"Trading Frequency: {TRADING_FREQ_MINUTES} minutes\n"
        f"Negative Trades/Day: {negative_trades}\n\n"
        "<b>📊 Profit Ranges:</b>\n"
        f"Daily Profit Range: {daily_min}% - {daily_max}%\n"
        f"Trade Profit Range: {trade_min}% - {trade_max}%\n"
        f"Negative Trade Range: -0.05% - -0.25%\n\n"
        "<b>💱 Trading Pairs:</b>\n"
        f"Available Pairs: {len(TRADING_PAIRS)} pairs\n"
        f"Pairs: {', '.join(TRADING_PAIRS[:5])}" + 
        ("..." if len(TRADING_PAIRS) > 5 else "") + "\n\n"
        "<b>🔄 Global Defaults:</b>\n"
        f"Daily Percent: {GLOBAL_DAILY_PERCENT}%\n"
        f"Trade Percent: {GLOBAL_TRADE_PERCENT}%\n\n"
        "<i>Use /trade_on or /trade_off to enable/disable trading\n"
        "Use /trade_now to trigger an immediate trade cycle</i>"
    )
    
    await update.effective_message.reply_text(status_text, parse_mode="HTML")


# -----------------------
# HISTORY handlers (unchanged)
# -----------------------
async def admin_pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ef_msg = update.effective_message
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await ef_msg.reply_text("Forbidden: admin only.")
        return
    async with async_session() as session:
        result = await session.execute(select(Transaction).where(Transaction.status == 'pending').order_by(Transaction.created_at.asc()))
        pending: List[Transaction] = result.scalars().all()
    if not pending:
        await ef_msg.reply_text("No pending transactions.")
        return
    for tx in pending:
        proof = tx.proof or ""
        username = None
        try:
            tg_user = await application.bot.get_chat(tx.user_id)
            username = getattr(tg_user, "username", None)
        except Exception:
            username = None
        try:
            if proof.startswith("photo:"):
                file_id = proof.split(":",1)[1]
                await context.application.bot.send_photo(chat_id=user_id, photo=file_id, caption=tx_card_text(tx, username=username), parse_mode="HTML", reply_markup=admin_action_kb(tx.id))
            else:
                caption = tx_card_text(tx, username=username) + (f"\nProof: <code>{proof}</code>" if proof else "")
                await context.application.bot.send_message(chat_id=user_id, text=caption, parse_mode="HTML", reply_markup=admin_action_kb(tx.id))
        except Exception:
            logger.exception("Failed to send pending tx %s to admin", tx.id)

def history_list_item_text(tx: Transaction) -> str:
    created = tx.created_at.strftime("%Y-%m-%d") if tx.created_at else "-"
    ttype_raw = (tx.type or "").lower()
    if ttype_raw.startswith("with"):
        ttype = "WITHDRAW"
    elif ttype_raw.startswith("invest") or ttype_raw == "profit" or ttype_raw == "deposit":
        ttype = "DEPOSIT"
    elif ttype_raw == "trade":
        ttype = "TRADE"
    else:
        ttype = (tx.type or "UNKNOWN").upper()
    amount = f"{float(tx.amount):.6f}$" if tx.amount is not None else "-"
    return f"{created}  |  {amount}  |  {ttype}"

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ef_msg = update.effective_message
    user_id = update.effective_user.id
    args = context.args if hasattr(context, "args") else []
    is_admin = _is_admin(user_id)

    if args and args[0].lower() == "all":
        if not is_admin:
            await ef_msg.reply_text("Forbidden: admin only.")
            return
        limit = 200
        async with async_session() as session:
            result = await session.execute(select(Transaction).order_by(Transaction.created_at.desc()).limit(limit))
            txs: List[Transaction] = result.scalars().all()
        if not txs:
            await ef_msg.reply_text("No transactions found.")
            return
        lines = []
        for tx in txs:
            created = tx.created_at.strftime("%Y-%m-%d %H:%M:%S") if tx.created_at else ""
            lines.append(f"DB:{tx.id} Ref:{tx.ref} {tx.type.upper()} {float(tx.amount):.6f}$ {tx.status} {created}")
        for i in range(0, len(lines), 50):
            await ef_msg.reply_text("\n".join(lines[i:i+50]))
        return

    page = 1
    if args and args[0].isdigit():
        page = max(1, int(args[0]))
    per_page = 10
    
    # Get user language
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
        result = await session.execute(select(Transaction).where(Transaction.user_id == user_id).order_by(Transaction.created_at.desc()))
        txs: List[Transaction] = result.scalars().all()
    
    if not txs:
        await ef_msg.reply_text(t(lang, "history_no_transactions"))
        return
    total = len(txs)
    total_pages = (total + per_page - 1) // per_page
    page = min(page, total_pages)
    start = (page-1)*per_page
    page_items = txs[start:start+per_page]

    kb_rows = []
    for tx in page_items:
        kb_rows.append([InlineKeyboardButton(history_list_item_text(tx), callback_data=f"history_details_{tx.id}_{page}_{user_id}")])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(t(lang, "history_prev"), callback_data=f"history_page_{page-1}_{user_id}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(t(lang, "history_next"), callback_data=f"history_page_{page+1}_{user_id}"))
    nav.append(InlineKeyboardButton(t(lang, "history_exit"), callback_data="menu_exit"))
    if nav:
        kb_rows.append(nav)

    header = f"🧾 Transactions (page {page}/{total_pages}) — Tap an item for details\n\n"
    await ef_msg.reply_text(header, reply_markup=InlineKeyboardMarkup(kb_rows))

async def history_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    if len(parts) < 4:
        await query.message.reply_text("Invalid pagination data.")
        return
    page = int(parts[2])
    uid = int(parts[3])
    if update.effective_user.id != uid and not _is_admin(update.effective_user.id):
        await query.message.reply_text("Forbidden: cannot view other user's history.")
        return
    context.args = [str(page)]
    await history_command(update, context)

async def history_details_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    if len(parts) < 5:
        await query.message.reply_text("Invalid details callback.")
        return
    tx_db_id = int(parts[2])
    page = int(parts[3])
    owner_id = int(parts[4])

    if update.effective_user.id != owner_id and not _is_admin(update.effective_user.id):
        await query.message.reply_text("Forbidden: cannot view other user's transaction.")
        return

    async with async_session() as session:
        result = await session.execute(select(Transaction).where(Transaction.id == tx_db_id))
        tx = result.scalar_one_or_none()
    if not tx:
        await query.message.reply_text("Transaction not found.")
        return

    created = tx.created_at.strftime("%Y-%m-%d %H:%M:%S") if tx.created_at else ""
    amount = f"{float(tx.amount):.6f}$" if tx.amount is not None else ""
    tx_type = (tx.type or "").upper()
    status = (tx.status or "").upper()
    ref = tx.ref or "-"
    proof = tx.proof or ""
    wallet = tx.wallet or ""
    network = tx.network or "-"

    detail_text = (
        f"📄 <b>Transaction Details</b>\n\n"
        f"Ref: <code>{ref}</code>\n"
        f"Type: <b>{tx_type}</b>\n"
        f"Amount: <b>{amount}</b>\n"
        f"Status: <b>{status}</b>\n"
        f"Date: {created}\n"
        f"Wallet: <code>{wallet}</code>\n"
        f"Network: {network}\n"
    )

    back_cb = f"history_back_{page}_{owner_id}"
    kb = []
    if _is_admin(query.from_user.id) and tx.status == 'pending':
        kb.append([InlineKeyboardButton("✅ Approve", callback_data=f"admin_start_approve_{tx.id}"),
                   InlineKeyboardButton("❌ Reject", callback_data=f"admin_start_reject_{tx.id}")])
    kb.append([InlineKeyboardButton("◀ Back to History", callback_data=back_cb),
               InlineKeyboardButton("Exit ❌", callback_data="menu_exit")])

    if proof and proof.startswith("photo:"):
        file_id = proof.split(":", 1)[1]
        try:
            await context.application.bot.send_photo(chat_id=query.from_user.id, photo=file_id, caption=detail_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            return
        except Exception:
            pass

    if proof and proof != "-":
        detail_text += f"\nProof: <code>{proof}</code>"

    await query.message.reply_text(detail_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

async def history_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    if len(parts) < 4:
        await query.message.reply_text("Invalid back callback.")
        return
    page = int(parts[2])
    uid = int(parts[3])
    if update.effective_user.id != uid and not _is_admin(update.effective_user.id):
        await query.message.reply_text("Forbidden: cannot view other user's history.")
        return
    context.args = [str(page)]
    await history_command(update, context)

# -----------------------
# LANGUAGE, start, help, wallet, balance handlers
# -----------------------
def build_language_kb(current_lang: str) -> InlineKeyboardMarkup:
    rows = []
    rows.append([InlineKeyboardButton(TRANSLATIONS.get(current_lang, TRANSLATIONS[DEFAULT_LANG])["lang_auto"], callback_data="lang_auto")])
    for code in SUPPORTED_LANGS:
        label = TRANSLATIONS[DEFAULT_LANG].get(f"lang_{code}", LANG_DISPLAY.get(code, code))
        rows.append([InlineKeyboardButton(label, callback_data=f"lang_{code}")])
    rows.append([InlineKeyboardButton("◀ Back", callback_data="menu_settings")])
    return InlineKeyboardMarkup(rows)

async def settings_language_open_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with async_session() as session:
        lang = await get_user_language(session, query.from_user.id, update=update)
    await query.message.edit_text(t(lang, "settings_title") + "\n\n" + t(lang, "settings_language"), reply_markup=build_language_kb(lang))

async def language_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    selected = None
    if data == "lang_auto":
        selected = "auto"
    elif data and data.startswith("lang_"):
        selected = data.split("_",1)[1]

    async with async_session() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            user = User(id=user_id, preferred_language=selected)
            session.add(user)
            await session.commit()
        else:
            await session.execute(sa_update(User).where(User.id == user_id).values(preferred_language=selected))
            await session.commit()

        effective_lang = await get_user_language(session, user_id, update=update)

    # Show clear success message
    success_msg = t(effective_lang, "lang_set_success")
    
    try:
        await query.message.edit_text(
            success_msg,
            parse_mode="HTML"
        )
    except Exception:
        await query.message.reply_text(
            success_msg,
            parse_mode="HTML"
        )
    
    # Wait a moment for user to see the message
    await asyncio.sleep(1)
    
    # Now show the main menu in the new language
    welcome_text = t(effective_lang, "welcome_text")
    full_text = welcome_text + "\n\n" + t(effective_lang, "main_menu_title")
    
    try:
        await query.message.edit_text(
            full_text,
            reply_markup=build_main_menu_keyboard(MENU_FULL_TWO_COLUMN, lang=effective_lang),
            parse_mode="HTML"
        )
    except Exception:
        await query.message.reply_text(
            full_text,
            reply_markup=build_main_menu_keyboard(MENU_FULL_TWO_COLUMN, lang=effective_lang),
            parse_mode="HTML"
        )

async def cancel_conv(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE):
    if context and getattr(context, "user_data", None):
        context.user_data.clear()
    if update and getattr(update, "callback_query", None):
        await update.callback_query.answer()
    return ConversationHandler.END

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User command: /stats - Show personal statistics"""
    user_id = update.effective_user.id
    
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
        user = await get_user(session, user_id)
        
        # Get user's transaction history
        result = await session.execute(
            select(Transaction).where(
                Transaction.user_id == user_id,
                Transaction.type == 'invest',
                Transaction.status == 'credited'
            ).order_by(Transaction.created_at)
        )
        investments = result.scalars().all()
        
        # Calculate stats
        total_invested = sum(float(inv.amount) for inv in investments)
        total_earned = float(user.get('total_profit', 0))
        current_balance = float(user.get('balance', 0))
        balance_in_process = float(user.get('balance_in_process', 0))
        referral_earnings = float(user.get('referral_earnings', 0))
        referral_count = int(user.get('referral_count', 0))
        
        # Calculate days active
        joined_at = user.get('joined_at')
        if joined_at:
            days_active = (datetime.utcnow() - joined_at).days
        else:
            days_active = 0
        
        # Calculate ROI
        if total_invested > 0:
            roi_percent = (total_earned / total_invested) * 100
        else:
            roi_percent = 0.0
    
    stats_text = (
        f"📊 <b>Your Statistics</b>\n\n"
        f"💼 <b>Investment Overview:</b>\n"
        f"💰 Total Invested: ${total_invested:.2f}\n"
        f"📈 Total Earned: ${total_earned:.2f}\n"
        f"📊 ROI: {roi_percent:.2f}%\n\n"
        f"💵 <b>Balances:</b>\n"
        f"✅ Available: ${current_balance:.2f}\n"
        f"⏳ In Process: ${balance_in_process:.2f}\n\n"
        f"👥 <b>Referrals:</b>\n"
        f"👤 Total Referrals: {referral_count}\n"
        f"💸 Referral Earnings: ${referral_earnings:.2f}\n\n"
        f"📅 <b>Activity:</b>\n"
        f"🗓 Days Active: {days_active}\n"
        f"📥 Total Deposits: {len(investments)}\n\n"
        f"<i>Keep growing your portfolio! 🚀</i>"
    )
    
    await update.effective_message.reply_text(stats_text, parse_mode="HTML")

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with async_session() as session:
        await send_balance_message(update.effective_message, session, update.effective_user.id)

async def balance_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with async_session() as session:
        await send_balance_message(update.effective_message, session, update.effective_user.id)

async def wallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    args = context.args
    if args:
        wallet_address = args[0]
        wallet_network = args[1] if len(args) > 1 else ''
        async with async_session() as session:
            await update_user(session, user_id, wallet_address=wallet_address, wallet_network=wallet_network)
        await update.effective_message.reply_text(t(lang, "withdraw_wallet_saved", wallet=wallet_address, network=wallet_network), parse_mode="HTML")
    else:
        async with async_session() as session:
            user = await get_user(session, user_id)
        wallet_address = user.get('wallet_address')
        wallet_network = user.get('wallet_network')
        if wallet_address:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(t(lang, "wallet_use_button"), callback_data="withdraw_use_saved")]])
            await update.effective_message.reply_text(t(lang, "wallet_saved", wallet=wallet_address, network=wallet_network), parse_mode="HTML", reply_markup=kb)
        else:
            await update.effective_message.reply_text(t(lang, "wallet_not_saved"))

async def information_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with async_session() as session:
        lang = await get_user_language(session, update.effective_user.id, update=update)
    # Add a back to menu button instead of showing the full menu inline
    await update.effective_message.reply_text(t(lang, "info_text"), reply_markup=build_back_to_menu_keyboard(lang), parse_mode="HTML")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    help_button = InlineKeyboardMarkup([[InlineKeyboardButton(t(lang, "help_button"), url=SUPPORT_URL)]])
    await update.effective_message.reply_text(t(lang, "help_message"), reply_markup=help_button)

async def settings_start_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with async_session() as session:
        lang = await get_user_language(session, user_id, update)
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(t(lang, "wallet_send_address"))
    else:
        await update.effective_message.reply_text(t(lang, "wallet_send_address"))
    return WITHDRAW_WALLET

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Check for referral code in arguments
    referrer_id = None
    if context.args:
        arg = context.args[0]
        if arg.startswith('ref_'):
            try:
                referrer_id = int(arg.replace('ref_', ''))
                # Don't allow self-referral
                if referrer_id == user_id:
                    referrer_id = None
            except ValueError:
                referrer_id = None
    
    async with async_session() as session:
        # Get or create user
        result = await session.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        
        # If new user and has referrer, link them
        if not user and referrer_id:
            # Verify referrer exists
            referrer_result = await session.execute(select(User).where(User.id == referrer_id))
            referrer = referrer_result.scalar_one_or_none()
            
            if referrer:
                # Create new user with referrer
                user = User(id=user_id, referrer_id=referrer_id)
                session.add(user)
                await session.commit()
                
                # Increment referrer's count
                await update_user(session, referrer_id, 
                                referral_count=int(referrer.referral_count or 0) + 1)
                
                # Notify referrer
                try:
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=f"🎉 New referral! User {user_id} joined using your link."
                    )
                except Exception:
                    pass
        
        lang = await get_user_language(session, user_id, update=update)
    
    await send_main_menu(update, context, lang=lang)

# -----------------------
# MAIN wiring: schedule trading_job and wire admin commands
# -----------------------
application: Optional[Application] = None
_scheduler: Optional[AsyncIOScheduler] = None

def main():
    global application, _scheduler
    application = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('invest', invest_cmd_handler),
            CommandHandler('withdraw', withdraw_cmd_handler),
            CallbackQueryHandler(invest_start_cmd, pattern='^menu_invest$'),
            CallbackQueryHandler(withdraw_start_cmd, pattern='^menu_withdraw$'),
            CallbackQueryHandler(settings_start_wallet, pattern='^settings_set_wallet$'),
        ],
        states={
            INVEST_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, invest_amount_received)],
            INVEST_NETWORK: [CallbackQueryHandler(invest_network_selected, pattern='^invest_network_')],
            INVEST_PROOF: [MessageHandler(filters.PHOTO | (filters.TEXT & ~filters.COMMAND), invest_proof_received)],
            INVEST_CONFIRM: [CallbackQueryHandler(invest_confirm_callback, pattern='^invest_confirm_yes$'), CallbackQueryHandler(invest_confirm_callback, pattern='^invest_confirm_no$')],
            WITHDRAW_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_amount_received)],
            WITHDRAW_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_wallet_received), CallbackQueryHandler(withdraw_wallet_received, pattern='^withdraw_use_saved$')],
            WITHDRAW_CONFIRM: [CallbackQueryHandler(withdraw_confirm_callback, pattern='^withdraw_confirm_yes$'), CallbackQueryHandler(withdraw_confirm_callback, pattern='^withdraw_confirm_no$')],
        },
        fallbacks=[CommandHandler('cancel', lambda u,c: cancel_conv(u,c))],
        allow_reentry=True,
    )

    application.add_handler(conv_handler)

    # language & settings handlers
    application.add_handler(CallbackQueryHandler(settings_language_open_callback, pattern='^settings_language$'))
    application.add_handler(CallbackQueryHandler(language_callback_handler, pattern='^lang_'))
    application.add_handler(CallbackQueryHandler(language_callback_handler, pattern='^lang_auto$'))

    # admin handlers (must be registered before generic menu handler so patterns match)
    application.add_handler(CallbackQueryHandler(admin_start_action_callback, pattern='^admin_start_(approve|reject)_\\d+$'))
    application.add_handler(CallbackQueryHandler(admin_confirm_callback, pattern='^admin_confirm_(approve|reject)_\\d+$'))
    application.add_handler(CallbackQueryHandler(admin_cancel_callback, pattern='^admin_cancel_\\d+$'))

    # history callbacks
    application.add_handler(CallbackQueryHandler(history_page_callback, pattern='^history_page_\\d+_\\d+$'))
    application.add_handler(CallbackQueryHandler(history_details_callback, pattern='^history_details_\\d+_\\d+_\\d+$'))
    application.add_handler(CallbackQueryHandler(history_back_callback, pattern='^history_back_\\d+_\\d+$'))

    # generic menu handler should come after specific handlers
    application.add_handler(CallbackQueryHandler(menu_callback))

    # commands
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("wallet", wallet_command))
    # use history_command (defined above)
    application.add_handler(CommandHandler("history", history_command))
    application.add_handler(CommandHandler("information", information_command))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("pending", admin_pending_command))

    # admin trade control commands
    application.add_handler(CommandHandler("trade_on", cmd_trade_on))
    application.add_handler(CommandHandler("trade_off", cmd_trade_off))
    application.add_handler(CommandHandler("trade_freq", cmd_trade_freq))
    application.add_handler(CommandHandler("trade_now", cmd_trade_now))
    application.add_handler(CommandHandler("trade_status", cmd_trade_status))
    application.add_handler(CommandHandler("set_trades_per_day", cmd_set_trades_per_day))
    application.add_handler(CommandHandler("set_negative_trades", cmd_set_negative_trades))
    application.add_handler(CommandHandler("set_daily_percent", cmd_set_daily_percent))
    application.add_handler(CommandHandler("set_trade_percent", cmd_set_trade_percent))
    application.add_handler(CommandHandler("set_daily_range", cmd_set_daily_range))
    application.add_handler(CommandHandler("set_trade_range", cmd_set_trade_range))
    application.add_handler(CommandHandler("set_user_trade", cmd_set_user_trade))
    application.add_handler(CommandHandler("set_trading_hours", cmd_set_trading_hours))
    application.add_handler(CommandHandler("trading_hours_status", cmd_trading_hours_status))
    application.add_handler(CommandHandler("trading_status", cmd_trading_status))
    application.add_handler(CommandHandler("trading_summary", cmd_trading_summary))
    
    # Binance control commands
    application.add_handler(CommandHandler("use_binance_on", cmd_use_binance_on))
    application.add_handler(CommandHandler("use_binance_off", cmd_use_binance_off))
    application.add_handler(CommandHandler("binance_status", cmd_binance_status))
    
    # Deposit wallet commands
    application.add_handler(CommandHandler("set_deposit_wallet", cmd_set_deposit_wallet))
    application.add_handler(CommandHandler("list_deposit_wallets", cmd_list_deposit_wallets))
    application.add_handler(CommandHandler("mark_primary_wallet", cmd_mark_primary_wallet))
    application.add_handler(CommandHandler("remove_deposit_wallet", cmd_remove_deposit_wallet))
    
    # Notification/Broadcast commands
    application.add_handler(CommandHandler("set_broadcast_message", cmd_set_broadcast_message))
    application.add_handler(CommandHandler("set_new_user_message", cmd_set_new_user_message))
    application.add_handler(CommandHandler("send_broadcast", cmd_send_broadcast))
    application.add_handler(CommandHandler("send_new_user_alert", cmd_send_new_user_alert))
    application.add_handler(CommandHandler("view_notifications", cmd_view_notifications))
    
    # Analytics and System commands
    application.add_handler(CommandHandler("admin_stats", cmd_admin_stats))
    application.add_handler(CommandHandler("system_status", cmd_system_status))
    application.add_handler(CommandHandler("error_logs", cmd_error_logs))
    application.add_handler(CommandHandler("send_reminders", cmd_send_reminders))
    
    # User stats command
    application.add_handler(CommandHandler("stats", stats_command))
    
    # Admin helper commands
    application.add_handler(CommandHandler("admin_cmds", cmd_admin_cmds))
    application.add_handler(CommandHandler("list_trading_vars", cmd_list_trading_vars))

    application.add_handler(MessageHandler(filters.Regex("^Balance$"), balance_text_handler))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Initialize trading state from database with error handling
    async def init_trading_state():
        global TRADING_ENABLED, USE_BINANCE
        try:
            async with async_session() as session:
                trading_enabled_db = await get_config(session, 'trading_enabled', '1')
                use_binance_db = await get_config(session, 'use_binance_api', '1')
                TRADING_ENABLED = (trading_enabled_db == '1')
                USE_BINANCE = (use_binance_db == '1')
                logger.info("Initialized trading state from DB: TRADING_ENABLED=%s, USE_BINANCE=%s", TRADING_ENABLED, USE_BINANCE)
        except Exception as e:
            logger.warning("Failed to initialize trading state from DB, using defaults: %s", e)
            # Keep current global defaults if DB read fails
    
    try:
        loop.run_until_complete(init_trading_state())
    except Exception as e:
        logger.error("Error initializing trading state: %s", e)
    
    try:
        _scheduler = AsyncIOScheduler(event_loop=loop)
    except TypeError:
        _scheduler = AsyncIOScheduler()
    # daily profit
    _scheduler.add_job(daily_profit_job, 'cron', hour=0, minute=0)
    # daily summary job at 23:59 UTC
    _scheduler.add_job(daily_summary_job, 'cron', hour=23, minute=59)
    # SCHEDULE trading_job directly as coroutine — not via lambda
    _scheduler.add_job(
        trading_job, 
        'interval', 
        minutes=TRADING_FREQ_MINUTES, 
        id=TRADING_JOB_ID,
        next_run_time=datetime.utcnow() + timedelta(seconds=15)
    )
    _scheduler.start()

    logger.info("Nexo Trading Bot STARTED")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    try:
        asyncio.run(init_db(retries=5, backoff=2.0, fallback_to_sqlite=True))
    except Exception as e:
        logger.exception("DB init failed: %s", e)
        raise
    main()
