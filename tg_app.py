# tg_app.py
# -*- coding: utf-8 -*-
import sys
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import os
import asyncio
import logging
import json
import base64
import time
import re
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)
TRADES_FILE = os.getenv("TRADES_FILE", "./trades.json")
SYMBOLS_ENV = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]

# aiogram
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# local DB helper (expected methods used in this file)
import db_json as db  # create_default_user, get_user, set_api_keys, update_setting, load_users, set_subscription, get_trades_for_user

# optional Bybit client module (may be None if not present)
try:
    import client as client_module
except Exception:
    client_module = None

# optional encryption for storing API keys
try:
    from cryptography.fernet import Fernet
    KEY_FILE = ".fernet.key"
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            FERNET_KEY = f.read()
    else:
        FERNET_KEY = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(FERNET_KEY)
    fernet = Fernet(FERNET_KEY)
    HAVE_CRYPTO = True
except Exception:
    fernet = None
    HAVE_CRYPTO = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# create requests session with simple retry/backoff (used by CryptoPay / fallback symbol checks)
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.4, status_forcelist=(500, 502, 503, 504))
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Payment / CryptoBot settings
PAYMENT_AMOUNT = float(os.getenv("PAYMENT_AMOUNT_USDT", "5"))
PAYMENT_ASSET = os.getenv("PAYMENT_ASSET", "USDT")
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")  # must be in .env to use CryptoBot API
CRYPTO_CREATE_INVOICE_URL = "https://pay.crypt.bot/api/createInvoice"
CRYPTO_GET_INVOICES_URL = "https://pay.crypt.bot/api/getInvoices"
CRYPTO_HEADERS = {"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN} if CRYPTOBOT_TOKEN else {}

# small rate-limit for notifying admin about external-service errors
ERROR_NOTIFY_INTERVAL = 300  # seconds
_LAST_ERROR_NOTIFY: Dict[str, float] = {}

def _should_notify(key: str) -> bool:
    now = time.time()
    last = _LAST_ERROR_NOTIFY.get(key, 0)
    if now - last > ERROR_NOTIFY_INTERVAL:
        _LAST_ERROR_NOTIFY[key] = now
        return True
    return False

async def _async_send_admin(text: str):
    try:
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, text)
    except Exception:
        logger.exception("Failed to send admin notification (async)")

def notify_admin_rate_limited_sync(text: str, key: str = "default_notify"):
    if not ADMIN_ID:
        return
    if not _should_notify(key):
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_async_send_admin(text))
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            session.post(url, json={"chat_id": ADMIN_ID, "text": text}, timeout=5)
    except Exception:
        logger.exception("notify_admin_rate_limited_sync failed")

# Localization (RU / EN / ES)
LOCALE = {
    "ru": {
        # core
        "choose_lang": "Выберите язык / Choose language:",
        "welcome": "👋 Привет! Это JamesTrade.\nВыберите пункт меню ниже:",
        # menu labels
        "menu_subscription": "📊 Подписка",
        "menu_settings": "⚙️ Настройки",
        "menu_trades": "💹 Мои сделки",
        "menu_bot_on": "🤖 Бот: ВКЛ",
        "menu_bot_off": "🤖 Бот: ВЫКЛ",
        "menu_support": "🆘 Поддержка",
        "menu_info": "ℹ️ ИНФО",
        # API keys
        "enter_api_key": "Введите API Key (в следующем сообщении):",
        "enter_api_secret": "Теперь введите API Secret (в следующем сообщении):",
        "keys_saved_ok": "✅ API ключи сохранены и успешно проверены.",
        "keys_saved_warn": "❗️ Ключи сохранены, но проверка не прошла: {info}\nПроверьте права ключей (read/balance/trade) и флаг TESTNET.",
        "keys_saved_no_client": "Ключи сохранены, но сервер не настроен для проверки ключей.",
        "no_keys": "❌ У вас не добавлены API ключи. Добавьте их в ⚙️ Настройки -> API ключи.",
        "invalid_keys": "❌ Неверные или недостаточные права API ключей: {info}\nПроверьте ключи и права (баланс/торговля).",
        "save_failed": "❌ Не удалось сохранить ключи. Попробуйте позже.",
        # subscription / trading
        "subscribe_required": "🔒 У вас нет активной подписки. Купите подписку через меню или /buy.",
        "trading_on": "▶️ Торговля включена.",
        "trading_off": "⏸️ Торговля отключена.",
        "buy_success": "👉 Ссылка на оплату: {url}\nПосле оплаты подписка активируется автоматически.",
        "buy_fail": "❌ Не удалось создать счёт. Попробуйте позже.",
        "invoice_paid": "✅ Оплата получена! Ваша подписка активирована на 30 дней.",
        # settings
        "settings_menu_title": "⚙️ Меню настроек — выберите раздел:",
        "settings_testnet_status": "🌐 TESTNET переключён {status}.",
        "settings_back": "⬅ Назад",
        "settings_lang": "🌐 Язык / Language",
        # pairs
        "pairs_title": "Выберите торговые пары (нажмите, чтобы переключить) или введите свои:",
        "pairs_saved": "✅ Выбранные пары сохранены: {pairs}",
        "pairs_input_prompt": "Введите пары через запятую или пробелы (например: BTCUSDT, ETHUSDT или BTC/USDT):",
        "pairs_saved_partial": "✅ Сохранены: {valid}. Необработаны/недействительны: {invalid}",
        "pairs_invalid_none": "❌ Никакие введённые пары не были распознаны как действительные: {invalid}",
        "pairs_manual_saved": "✅ Ваши пары сохранены: {pairs}",
        "pairs_manual_button": "✏️ Ввести свои",
        "pairs_done_button": "✅ Готово",
        # indicators / risk
        "risk_title": "Текущие risk-настройки:\n{fields}\n\nИзменить: SET KEY VALUE",
        "indicators_menu_title": "⚙️ Настройки индикаторов — выберите раздел:",
        "indicators_global_title": "🌐 Глобальные настройки индикаторов:\nНажмите кнопку, чтобы переключить индикатор.",
        "indicators_advanced_text": "🔧 Расширенные настройки (текущие):\n\n{settings}\n\nЧтобы изменить значение используйте команду:\nSET KEY VALUE\n\nПример: SET RSI_PERIOD 14",
        # support
        "support_prompt": "Опишите проблему — ваше сообщение будет отправлено админу. Для отмены введите /cancel",
        "support_sent": "✅ Сообщение отправлено в поддержку. Ожидайте ответа.",
        "support_failed": "Ошибка при отправке в поддержку. Попробуйте позже.",
        # trades
        "no_trades": "💤 Сделок пока нет.",
        "trades_end": "— Конец —",
        "trade_notification": "⚡️ Сделка: {symbol} {side}\nЦена: {price}\nОбъём: {qty}\nPnL: {pnl}\n{ts}",
        # admin
        "admin_only": "❌ Только админ.",
        "invalid_user_id": "❌ Некорректный идентификатор пользователя.",
        "enter_reply_prompt": "Введите ответ пользователю {target}. Для отмены: /cancel",
        "reply_sent": "✅ Ответ отправлен пользователю.",
        # generic
        "action_cancelled": "Действие отменено.",
        "set_usage": "Ошибка. Формат: SET KEY VALUE",
        "error_data": "Ошибка данных.",
        "welcome_short": "🤖 Команды: /buy — оплатить подписку; SET KEY VALUE — изменить настройку.",
    },
    "en": {
        # core
        "choose_lang": "Choose language / Выберите язык:",
        "welcome": "👋 Hi! This is JamesTrade.\nChoose an item from the menu:",
        # menu labels
        "menu_subscription": "📊 Subscription",
        "menu_settings": "⚙️ Settings",
        "menu_trades": "💹 My trades",
        "menu_bot_on": "🤖 Bot: ON",
        "menu_bot_off": "🤖 Bot: OFF",
        "menu_support": "🆘 Support",
        "menu_info": "ℹ️ INFO",
        # API keys
        "enter_api_key": "Enter API Key (in the next message):",
        "enter_api_secret": "Now enter API Secret (in the next message):",
        "keys_saved_ok": "✅ API keys saved and validated successfully.",
        "keys_saved_warn": "❗️ Keys saved but validation failed: {info}\nCheck key permissions (read/balance/trade) and TESTNET flag.",
        "keys_saved_no_client": "Keys saved but server cannot validate keys (client.py missing).",
        "no_keys": "❌ You haven't added API keys. Add them in ⚙️ Settings -> API keys.",
        "invalid_keys": "❌ Invalid or insufficient API key permissions: {info}\nCheck keys and permissions (balance/trade).",
        "save_failed": "❌ Failed to save API keys. Try again later.",
        # subscription / trading
        "subscribe_required": "🔒 You don't have an active subscription. Buy it in menu or /buy.",
        "trading_on": "▶️ Trading enabled.",
        "trading_off": "⏸️ Trading disabled.",
        "buy_success": "👉 Payment link: {url}\nAfter payment your subscription will be activated automatically.",
        "buy_fail": "❌ Failed to create invoice. Try later.",
        "invoice_paid": "✅ Payment received! Your subscription is activated for 30 days.",
        # settings
        "settings_menu_title": "⚙️ Settings menu — choose section:",
        "settings_testnet_status": "🌐 TESTNET toggled {status}.",
        "settings_back": "⬅ Back",
        "settings_lang": "🌐 Language",
        # pairs
        "pairs_title": "Choose trading pairs (tap to toggle) or input your own:",
        "pairs_saved": "✅ Selected pairs saved: {pairs}",
        "pairs_input_prompt": "Enter pairs separated by comma or spaces (e.g. BTCUSDT, ETHUSDT or BTC/USDT):",
        "pairs_saved_partial": "✅ Saved: {valid}. Unrecognized/invalid: {invalid}",
        "pairs_invalid_none": "❌ None of the entered pairs were recognized as valid: {invalid}",
        "pairs_manual_saved": "✅ Your pairs saved: {pairs}",
        "pairs_manual_button": "✏️ Input your own",
        "pairs_done_button": "✅ Done",
        # indicators / risk
        "risk_title": "Current risk settings:\n{fields}\n\nChange with: SET KEY VALUE",
        "indicators_menu_title": "⚙️ Indicator settings — choose:",
        "indicators_global_title": "🌐 Global indicator toggles:\nPress button to toggle an indicator.",
        "indicators_advanced_text": "🔧 Advanced settings (current):\n\n{settings}\n\nTo change use:\nSET KEY VALUE\n\nExample: SET RSI_PERIOD 14",
        # support
        "support_prompt": "Describe the issue — your message will be sent to admin. To cancel, use /cancel",
        "support_sent": "✅ Message sent to support. Wait for reply.",
        "support_failed": "Failed to forward to support. Try later.",
        # trades
        "no_trades": "💤 No trades yet.",
        "trades_end": "— End —",
        "trade_notification": "⚡️ Trade: {symbol} {side}\nPrice: {price}\nQty: {qty}\nPnL: {pnl}\n{ts}",
        # admin
        "admin_only": "❌ Admin only.",
        "invalid_user_id": "❌ Invalid user id.",
        "enter_reply_prompt": "Enter reply to user {target}. To cancel: /cancel",
        "reply_sent": "✅ Reply sent to the user.",
        # generic
        "action_cancelled": "Action cancelled.",
        "set_usage": "Error. Format: SET KEY VALUE",
        "error_data": "Bad data.",
        "welcome_short": "🤖 Commands: /buy — pay subscription; SET KEY VALUE — change setting.",
    },
    "es": {
        # core
        "choose_lang": "Elige idioma / Choose language:",
        "welcome": "👋 ¡Hola! Esto es JamesTrade.\nElige una opción del menú:",
        # menu labels
        "menu_subscription": "📊 Suscripción",
        "menu_settings": "⚙️ Ajustes",
        "menu_trades": "💹 Mis operaciones",
        "menu_bot_on": "🤖 Bot: ON",
        "menu_bot_off": "🤖 Bot: OFF",
        "menu_support": "🆘 Soporte",
        "menu_info": "ℹ️ INFO",
        # API keys
        "enter_api_key": "Introduce API Key (en el siguiente mensaje):",
        "enter_api_secret": "Ahora introduce API Secret (en el siguiente mensaje):",
        "keys_saved_ok": "✅ Claves API guardadas y validadas con éxito.",
        "keys_saved_warn": "❗️ Claves guardadas, pero la validación falló: {info}\nVerifica permisos (read/balance/trade) y TESTNET.",
        "keys_saved_no_client": "Claves guardadas, pero el servidor no puede validar (client.py ausente).",
        "no_keys": "❌ No has añadido claves API. Añádelas en ⚙️ Ajustes -> API keys.",
        "invalid_keys": "❌ Claves inválidas o permisos insuficientes: {info}\nVerifica las claves y permisos (balance/trade).",
        "save_failed": "❌ No se pudieron guardar las claves. Intenta más tarde.",
        # subscription / trading
        "subscribe_required": "🔒 No tienes una suscripción activa. Cómprala en el menú o /buy.",
        "trading_on": "▶️ Trading activado.",
        "trading_off": "⏸️ Trading desactivado.",
        "buy_success": "👉 Enlace de pago: {url}\nTras el pago, la suscripción se activará automáticamente.",
        "buy_fail": "❌ No se pudo crear la factura. Intenta más tarde.",
        "invoice_paid": "✅ ¡Pago recibido! Tu suscripción está activada por 30 días.",
        # settings
        "settings_menu_title": "⚙️ Menú de ajustes — elige sección:",
        "settings_testnet_status": "🌐 TESTNET cambiado a {status}.",
        "settings_back": "⬅ Volver",
        "settings_lang": "🌐 Idioma",
        # pairs
        "pairs_title": "Elige pares de trading (toca para alternar) o introduce los tuyos:",
        "pairs_input_prompt": "Introduce pares separados por comas o espacios (p. ej.: BTCUSDT, ETHUSDT o BTC/USDT):",
        "pairs_saved_partial": "✅ Guardados: {valid}. No reconocidos/invalidos: {invalid}",
        "pairs_invalid_none": "❌ Ninguno de los pares introducidos fue reconocido como válido: {invalid}",
        "pairs_manual_saved": "✅ Tus pares guardados: {pairs}",
        "pairs_manual_button": "✏️ Introducir propios",
        "pairs_done_button": "✅ Hecho",
        # indicators / risk
        "risk_title": "Ajustes de riesgo actuales:\n{fields}\n\nCambiar: SET KEY VALUE",
        "indicators_menu_title": "⚙️ Ajustes de indicadores — elige:",
        "indicators_global_title": "🌐 Indicadores globales:\nPulsa para alternar un indicador.",
        "indicators_advanced_text": "🔧 Ajustes avanzados (actuales):\n\n{settings}\n\nPara cambiar usa:\nSET KEY VALUE\n\nEjemplo: SET RSI_PERIOD 14",
        # support
        "support_prompt": "Describe el problema — tu mensaje se enviará al administrador. Para cancelar usa /cancel",
        "support_sent": "✅ Mensaje enviado al soporte. Espera respuesta.",
        "support_failed": "Error al enviar al soporte. Intenta más tarde.",
        # trades
        "no_trades": "💤 Aún no hay operaciones.",
        "trades_end": "— Fin —",
        "trade_notification": "⚡️ Operación: {symbol} {side}\nPrecio: {price}\nCantidad: {qty}\nPnL: {pnl}\n{ts}",
        # admin
        "admin_only": "❌ Solo administrador.",
        "invalid_user_id": "❌ Id de usuario inválido.",
        "enter_reply_prompt": "Introduce la respuesta al usuario {target}. Para cancelar: /cancel",
        "reply_sent": "✅ Respuesta enviada al usuario.",
        # generic
        "action_cancelled": "Acción cancelada.",
        "set_usage": "Error. Formato: SET KEY VALUE",
        "error_data": "Datos erróneos.",
        "welcome_short": "🤖 Comandos: /buy — pagar suscripción; SET KEY VALUE — cambiar ajuste.",
    },
}

# FSM
class Form(StatesGroup):
    api_key = State()
    api_secret = State()
    support_user = State()
    admin_reply = State()
    pairs_input = State()  # new state: manual pairs input
    # optional: add language selection state if needed later

# Helpers encryption
def encrypt(text: str) -> str:
    if not text:
        return ""
    if HAVE_CRYPTO and fernet:
        return fernet.encrypt(text.encode()).decode()
    return base64.b64encode(text.encode()).decode()

def decrypt(text: str) -> str:
    if not text:
        return ""
    if HAVE_CRYPTO and fernet:
        try:
            return fernet.decrypt(text.encode()).decode()
        except Exception:
            return text
    try:
        return base64.b64decode(text.encode()).decode()
    except Exception:
        return text

# Localization helper
def t(uid: Optional[int], key: str, **kwargs) -> str:
    lang = "ru"
    try:
        if uid is not None:
            u = db.get_user(uid) or {}
            settings = u.get("settings") or {}
            lang = settings.get("lang", settings.get("language", "ru")) or "ru"
            if lang not in LOCALE:
                lang = "ru"
    except Exception:
        lang = "ru"
    s = LOCALE.get(lang, LOCALE["ru"]).get(key, key)
    if kwargs:
        try:
            return s.format(**kwargs)
        except Exception:
            return s
    return s

# Normalize symbol string
def normalize_symbol(sym: str) -> str:
    if not sym:
        return ""
    s = sym.strip().upper()
    # replace separators with nothing
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s

# Validate symbols best-effort:
def validate_symbols(uid: int, symbols: List[str]) -> Tuple[List[str], List[str]]:
    """
    Return (valid_list, invalid_list).
    Uses client_module.BybitClient.get_symbol_info or public REST fallback.
    """
    valid = []
    invalid = []
    # get user's testnet flag if any
    u = db.get_user(uid) or {}
    settings = u.get("settings") or {}
    testnet = bool(settings.get("TESTNET", False))

    # try to instantiate a client without user API keys (works for public endpoints in client.py)
    client = None
    if client_module is not None:
        try:
            client = client_module.BybitClient(api_key=None, api_secret=None, testnet=testnet)
        except Exception:
            client = None

    base_public = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"

    for s in symbols:
        ns = normalize_symbol(s)
        if not ns:
            continue
        ok = False
        # 1) try client.get_symbol_info
        try:
            if client is not None and hasattr(client, "get_symbol_info"):
                info = client.get_symbol_info(ns)
                if info and isinstance(info, dict) and info:
                    ok = True
            # 2) try client.fetch_ohlcv_df -> check not empty DataFrame
            if not ok and client is not None and hasattr(client, "fetch_ohlcv_df"):
                try:
                    df = client.fetch_ohlcv_df(ns, interval="5", limit=1)
                    if hasattr(df, "empty"):
                        if not df.empty:
                            ok = True
                    else:
                        if df:
                            ok = True
                except Exception:
                    pass
            # 3) public REST fallback: instruments-info
            if not ok:
                try:
                    params = {"category": getattr(client, "category", "linear"), "symbol": ns}
                    url = base_public + "/v5/market/instruments-info"
                    r = session.get(url, params=params, timeout=6)
                    j = r.json() if r is not None else {}
                    items = None
                    if isinstance(j, dict):
                        res = j.get("result") or j
                        if isinstance(res, dict):
                            items = res.get("list") or []
                        elif isinstance(res, list):
                            items = res
                    if items:
                        for it in items:
                            if isinstance(it, dict) and (it.get("symbol") == ns or it.get("name") == ns):
                                ok = True
                                break
                except Exception:
                    pass
        except Exception:
            pass

        if ok:
            valid.append(ns)
        else:
            invalid.append(ns)
    # deduplicate preserving order
    def uniq(seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out
    return uniq(valid), uniq(invalid)

# --- New: validation of user keys (unchanged semantics) ---
def validate_user_keys(uid: int) -> Tuple[bool, str]:
    """
    Try to validate user's stored API keys.
    Returns (True, info) on success, (False, error_message) on failure.
    """
    try:
        u = db.get_user(uid)
        if not u:
            return False, "User not found"
        api_key_enc = u.get("api_key") or ""
        api_secret_enc = u.get("api_secret") or ""
        if not api_key_enc or not api_secret_enc:
            return False, "missing_keys"
        api_key = decrypt(api_key_enc)
        api_secret = decrypt(api_secret_enc)
        settings = u.get("settings") or {}
        testnet = bool(settings.get("TESTNET", False))

        if client_module is None:
            return False, "no_client"

        # create client (client constructor doesn't perform network calls)
        try:
            client = client_module.BybitClient(api_key=api_key, api_secret=api_secret, testnet=testnet)
        except Exception as e:
            logger.exception("Failed to create BybitClient for validation")
            return False, f"client_init_error: {e}"

        # prefer lightweight non-destructive call - balance or account info
        try:
            if hasattr(client, "get_balance_usdt"):
                bal = client.get_balance_usdt()
                # balance None => likely auth error / insufficient rights -> treat as fail
                if bal is None:
                    return False, "auth_or_rights"
                return True, "ok_balance"
            if hasattr(client, "get_account_info"):
                info = client.get_account_info()
                if info is None:
                    return False, "auth_or_rights"
                return True, "ok_account"
        except Exception as e:
            msg = str(e).lower()
            logger.exception("Key validation exception for user %s: %s", uid, e)
            if "401" in msg or "unauthorized" in msg or "invalid" in msg:
                return False, "auth_or_rights"
            return False, f"exception: {e}"
        return False, "no_validation_method"
    except Exception as e:
        logger.exception("validate_user_keys generic error")
        return False, f"internal_error: {e}"

# Subscription/trading helpers
def has_active_sub(user_id: int) -> bool:
    u = db.get_user(user_id)
    if not u:
        return False
    sub_until = u.get("sub_until")
    if not sub_until:
        return False
    try:
        dt = datetime.fromisoformat(str(sub_until))
        return dt > datetime.utcnow()
    except Exception:
        return False

def is_trading_active(user_id: int) -> bool:
    u = db.get_user(user_id)
    if not u:
        return False
    settings = u.get("settings", {}) or {}
    return bool(settings.get("active"))

# Reply keyboard builder (localized)
def main_reply_kb(user_id: Optional[int] = None, resize: bool = True) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text=t(user_id, "menu_subscription"))
    builder.button(text=t(user_id, "menu_settings"))
    builder.button(text=t(user_id, "menu_trades"))
    bot_label = t(user_id, "menu_bot_off")
    if user_id is not None and is_trading_active(user_id):
        bot_label = t(user_id, "menu_bot_on")
    builder.button(text=bot_label)
    builder.button(text=t(user_id, "menu_support"))
    builder.button(text=t(user_id, "menu_info"))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=resize)

def admin_reply_kb_for_user(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Ответить", callback_data=f"admin_reply:{user_id}")
    kb.adjust(1)
    return kb.as_markup()

# ---------- CryptoPay helpers (unchanged) ----------
def create_invoice(user_id: int, amount: Optional[float] = None) -> Tuple[Optional[str], Optional[str]]:
    if not CRYPTOBOT_TOKEN:
        logger.warning("CRYPTOBOT_TOKEN not set; cannot create invoice")
        return None, None
    amt = PAYMENT_AMOUNT if amount is None else amount
    payload = {
        "amount": amt,
        "asset": PAYMENT_ASSET,
        "description": f"Подписка JamesTrade.ai для {user_id}",
        "payload": str(user_id),
        "allow_comments": False,
        "allow_anonymous": False,
    }
    try:
        r = session.post(CRYPTO_CREATE_INVOICE_URL, headers=CRYPTO_HEADERS, json=payload, timeout=15)
        j = r.json()
        if j.get("ok") and isinstance(j.get("result"), dict):
            inv = j["result"]
            pay_url = inv.get("pay_url")
            invoice_id = inv.get("invoice_id") or inv.get("id") or inv.get("uid")
            return pay_url, str(invoice_id) if invoice_id is not None else None
        logger.warning("create_invoice failed response: %s", j)
    except Exception as e:
        logger.exception("create_invoice error: %s", e)
        notify_admin_rate_limited_sync(f"CryptoPay create_invoice unexpected error: {e}", key="cryptobot_create")
    return None, None

def fetch_invoice_status_with_retry(invoice_id: str, retries: int = 3, backoff: float = 2.0) -> Optional[dict]:
    if not CRYPTOBOT_TOKEN:
        return None
    params = {"invoice_ids": invoice_id}
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            r = session.get(CRYPTO_GET_INVOICES_URL, headers=CRYPTO_HEADERS, params=params, timeout=12)
            if r.status_code == 502:
                time.sleep(delay)
                delay *= backoff
                continue
            j = r.json()
            if j.get("ok"):
                items = j.get("result", {}).get("items") or j.get("result") or []
                if isinstance(items, list) and items:
                    return items[0]
                if isinstance(j.get("result"), dict) and j["result"].get("items") is None:
                    return j["result"]
                return None
            return None
        except Exception as e:
            logger.exception("fetch_invoice_status error: %s", e)
            time.sleep(delay)
            delay *= backoff
    return None

fetch_invoice_status = fetch_invoice_status_with_retry

# ---------- Handlers ----------

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    db.create_default_user(m.from_user.id, m.from_user.username)
    u = db.get_user(m.from_user.id) or {}
    s = (u.get("settings") or {})
    lang = s.get("lang") or s.get("language")
    if not lang:
        kb = InlineKeyboardBuilder()
        kb.button(text="🇷🇺 Русский", callback_data="lang:ru")
        kb.button(text="🇬🇧 English", callback_data="lang:en")
        kb.button(text="🇪🇸 Español", callback_data="lang:es")
        kb.adjust(3)
        await m.answer(LOCALE["ru"]["choose_lang"], reply_markup=kb.as_markup())
        return
    await m.answer(t(m.from_user.id, "welcome"), reply_markup=main_reply_kb(m.from_user.id))

@dp.callback_query(lambda c: c.data and c.data.startswith("lang:"))
async def cb_lang_set(c: types.CallbackQuery):
    await c.answer()
    try:
        _, lang = c.data.split(":", 1)
    except Exception:
        lang = "ru"
    db.create_default_user(c.from_user.id, c.from_user.username)
    db.update_setting(c.from_user.id, "lang", lang)
    await c.message.answer(t(c.from_user.id, "welcome"), reply_markup=main_reply_kb(c.from_user.id))

# API keys flow
@dp.callback_query(lambda c: c.data == "settings_api")
async def cb_settings_api(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    await c.message.answer(t(c.from_user.id, "enter_api_key"), reply_markup=main_reply_kb(c.from_user.id))
    await state.set_state(Form.api_key)

@dp.message(Form.api_key)
async def process_api_key(m: types.Message, state: FSMContext):
    await state.update_data(api_key=m.text.strip())
    await m.answer(t(m.from_user.id, "enter_api_secret"), reply_markup=main_reply_kb(m.from_user.id))
    await state.set_state(Form.api_secret)

@dp.message(Form.api_secret)
async def process_api_secret(m: types.Message, state: FSMContext):
    data = await state.get_data()
    key_plain = data.get("api_key", "").strip()
    secret_plain = m.text.strip()
    # store encrypted values in DB
    try:
        enc_key = encrypt(key_plain)
        enc_secret = encrypt(secret_plain)
        db.set_api_keys(m.from_user.id, enc_key, enc_secret)
    except Exception:
        logger.exception("Failed to save api keys to DB")
        await m.answer(t(m.from_user.id, "save_failed"), reply_markup=main_reply_kb(m.from_user.id))
        await state.clear()
        return

    # validate keys on save (on-demand)
    ok, info = validate_user_keys(m.from_user.id)
    if ok:
        await m.answer(t(m.from_user.id, "keys_saved_ok"), reply_markup=main_reply_kb(m.from_user.id))
    else:
        # translate some special internal codes to localized messages
        if info == "no_client":
            await m.answer(t(m.from_user.id, "keys_saved_no_client"), reply_markup=main_reply_kb(m.from_user.id))
        elif info == "missing_keys":
            await m.answer(t(m.from_user.id, "no_keys"), reply_markup=main_reply_kb(m.from_user.id))
        elif info == "auth_or_rights":
            await m.answer(t(m.from_user.id, "keys_saved_warn", info="401/unauthorized or insufficient rights"), reply_markup=main_reply_kb(m.from_user.id))
        else:
            await m.answer(t(m.from_user.id, "keys_saved_warn", info=str(info)), reply_markup=main_reply_kb(m.from_user.id))
        notify_admin_rate_limited_sync(f"User {m.from_user.id} saved API keys but validation failed: {info}", key="user_key_invalid")
    await state.clear()

# Toggle trading via keyboard button
@dp.message(lambda m: (m.text and m.text.startswith("🤖 Бот:")) or (m.text and m.text.startswith("🤖 Bot:")))
async def toggle_bot_via_button(m: types.Message):
    uid = m.from_user.id
    db.create_default_user(uid)
    current = is_trading_active(uid)
    # turning ON -> must have valid API keys and subscription
    if not current:
        if not has_active_sub(uid):
            await m.reply(t(uid, "subscribe_required"), reply_markup=main_reply_kb(uid))
            return
        # ensure keys exist
        u = db.get_user(uid) or {}
        api_key_enc = u.get("api_key") or ""
        api_secret_enc = u.get("api_secret") or ""
        if not api_key_enc or not api_secret_enc:
            await m.reply(t(uid, "no_keys"), reply_markup=main_reply_kb(uid))
            return
        ok, info = validate_user_keys(uid)
        if not ok:
            # map internal codes
            if info == "no_client":
                await m.reply(t(uid, "keys_saved_no_client"), reply_markup=main_reply_kb(uid))
            elif info == "auth_or_rights":
                await m.reply(t(uid, "invalid_keys", info="401/unauthorized"), reply_markup=main_reply_kb(uid))
            else:
                await m.reply(t(uid, "invalid_keys", info=str(info)), reply_markup=main_reply_kb(uid))
            db.update_setting(uid, "active", False)
            notify_admin_rate_limited_sync(f"User {uid} tried to enable trading but key validation failed: {info}", key="user_enable_fail")
            return
        db.update_setting(uid, "active", True)
        await m.reply(t(uid, "trading_on"), reply_markup=main_reply_kb(uid))
    else:
        db.update_setting(uid, "active", False)
        await m.reply(t(uid, "trading_off"), reply_markup=main_reply_kb(uid))

# /buy and pay flows
@dp.message(Command("buy"))
async def cmd_buy(m: types.Message):
    pay_url, invoice_id = create_invoice(m.from_user.id)
    if pay_url:
        db.update_setting(m.from_user.id, "last_invoice_id", invoice_id)
        await m.reply(t(m.from_user.id, "buy_success", url=pay_url), reply_markup=main_reply_kb(m.from_user.id))
        if ADMIN_ID:
            try:
                await bot.send_message(ADMIN_ID, f"Пользователь @{m.from_user.username} (id={m.from_user.id}) создал инвойс {invoice_id}.")
            except Exception:
                pass
    else:
        await m.reply(t(m.from_user.id, "buy_fail"), reply_markup=main_reply_kb(m.from_user.id))

# Settings menu
@dp.message(lambda m: m.text == t(m.from_user.id, "menu_settings"))
async def menu_settings_main(m: types.Message):
    if not has_active_sub(m.from_user.id):
        await m.reply(t(m.from_user.id, "subscribe_required"), reply_markup=main_reply_kb(m.from_user.id))
        return
    db.create_default_user(m.from_user.id, m.from_user.username)
    kb = InlineKeyboardBuilder()
    # localized labels where appropriate
    kb.button(text="🔑 API keys", callback_data="settings_api")
    kb.button(text="🌐 TESTNET (ON/OFF)", callback_data="settings_testnet")
    kb.button(text="💱 Pairs", callback_data="settings_pairs")
    kb.button(text="💰 Risk management", callback_data="settings_risk")
    kb.button(text="📊 Indicators", callback_data="settings_indicators")
    kb.button(text=t(m.from_user.id, "settings_lang"), callback_data="settings_lang")
    kb.button(text=t(m.from_user.id, "settings_back"), callback_data="settings_back")
    kb.adjust(1)
    await m.reply(t(m.from_user.id, "settings_menu_title"), reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "settings_testnet")
async def cb_settings_testnet(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    db.create_default_user(uid, c.from_user.username)
    user = db.get_user(uid) or {}
    settings = user.get("settings", {}) or {}
    cur = bool(settings.get("TESTNET", False))
    new = not cur
    db.update_setting(uid, "TESTNET", new)
    status = "ON" if new else "OFF"
    # localized status output
    await c.message.answer(t(uid, "settings_testnet_status", status=status), reply_markup=main_reply_kb(uid))

# Language selection from settings
@dp.callback_query(lambda c: c.data == "settings_lang")
async def cb_settings_lang(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    kb = InlineKeyboardBuilder()
    kb.button(text="🇷🇺 Русский", callback_data="lang:ru")
    kb.button(text="🇬🇧 English", callback_data="lang:en")
    kb.button(text="🇪🇸 Español", callback_data="lang:es")
    kb.adjust(3)
    await c.message.answer(t(uid, "choose_lang"), reply_markup=kb.as_markup())

# Pairs selection menu (with toggle + manual input)
@dp.callback_query(lambda c: c.data == "settings_pairs")
async def cb_settings_pairs(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    db.create_default_user(uid, c.from_user.username)
    user = db.get_user(uid) or {}
    settings = user.get("settings", {}) or {}
    selected = set([p.upper() for p in settings.get("symbols", SYMBOLS_ENV)])
    kb = InlineKeyboardBuilder()
    # show list from ENV (SYMBOLS_ENV)
    for sym in SYMBOLS_ENV:
        label = f"{'✅' if sym in selected else '▫️'} {sym}"
        kb.button(text=label, callback_data=f"pairs_toggle:{sym}")
    kb.button(text=t(uid, "pairs_manual_button"), callback_data="pairs_input")
    kb.button(text=t(uid, "pairs_done_button"), callback_data="pairs_done")
    kb.adjust(2)
    await c.message.answer(t(uid, "pairs_title"), reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith("pairs_toggle:"))
async def cb_pairs_toggle(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    try:
        _, sym = c.data.split(":", 1)
        sym = sym.upper()
    except Exception:
        await c.answer(t(uid, "error_data"))
        return
    db.create_default_user(uid, c.from_user.username)
    user = db.get_user(uid) or {}
    s = set([p.upper() for p in (user.get("settings", {}) or {}).get("symbols", SYMBOLS_ENV)])
    if sym in s:
        s.remove(sym)
    else:
        s.add(sym)
    # save back as list
    db.update_setting(uid, "symbols", list(s))
    # refresh menu
    await cb_settings_pairs(c)

@dp.callback_query(lambda c: c.data == "pairs_done")
async def cb_pairs_done(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    user = db.get_user(uid) or {}
    symbols = user.get("settings", {}).get("symbols", SYMBOLS_ENV)
    await c.message.answer(t(uid, "pairs_saved", pairs=",".join(symbols)), reply_markup=main_reply_kb(uid))

@dp.callback_query(lambda c: c.data == "pairs_input")
async def cb_pairs_input(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    uid = c.from_user.id
    await c.message.answer(t(uid, "pairs_input_prompt"), reply_markup=main_reply_kb(uid))
    await state.set_state(Form.pairs_input)

@dp.message(Form.pairs_input)
async def process_pairs_input(m: types.Message, state: FSMContext):
    uid = m.from_user.id
    raw = m.text or ""
    # split by comma, semicolon, newline; also handle whitespace
    parts = re.split(r'[,;\n]+', raw)
    tokens = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # if there's whitespace-separated list (no commas), split
        if ("," not in raw and ";" not in raw and "\n" not in raw) and " " in p:
            tokens.extend([x.strip() for x in p.split() if x.strip()])
        else:
            tokens.append(p)
    tokens = [normalize_symbol(x) for x in tokens if x and normalize_symbol(x)]
    if not tokens:
        await m.reply(t(uid, "pairs_invalid_none", invalid=raw), reply_markup=main_reply_kb(uid))
        await state.clear()
        return
    valid, invalid = validate_symbols(uid, tokens)
    if valid:
        db.update_setting(uid, "symbols", valid)
        if invalid:
            await m.reply(t(uid, "pairs_saved_partial", valid=",".join(valid), invalid=",".join(invalid)), reply_markup=main_reply_kb(uid))
        else:
            await m.reply(t(uid, "pairs_manual_saved", pairs=",".join(valid)), reply_markup=main_reply_kb(uid))
    else:
        await m.reply(t(uid, "pairs_invalid_none", invalid=",".join(invalid)), reply_markup=main_reply_kb(uid))
    await state.clear()

# Indicators / trades / support / admin handlers (kept but localized)
@dp.callback_query(lambda c: c.data == "settings_risk")
async def cb_settings_risk(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    db.create_default_user(uid, c.from_user.username)
    user = db.get_user(uid) or {}
    s = user.get("settings", {}) or {}
    fields = {k: s.get(k) for k in ("ORDER_PERCENT", "ORDER_SIZE_USD", "TP_PCT", "SL_PCT", "MIN_NOTIONAL") if k in s}
    txt = t(uid, "risk_title", fields=json.dumps(fields, indent=2, ensure_ascii=False))
    kb = InlineKeyboardBuilder(); kb.button(text=t(uid, "settings_back"), callback_data="settings_back"); kb.adjust(1)
    try:
        await c.message.edit_text(txt, reply_markup=kb.as_markup())
    except Exception:
        await c.message.answer(txt, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "settings_indicators")
async def cb_settings_indicators(c: types.CallbackQuery):
    await c.answer()
    kb = InlineKeyboardBuilder()
    kb.button(text="🌐 " + ("Global" if False else "Глобальные"), callback_data="ind_global")
    kb.button(text="🔧 " + ("Advanced" if False else "Расширенные"), callback_data="ind_advanced")
    kb.button(text=t(c.from_user.id, "settings_back"), callback_data="settings_back")
    kb.adjust(1)
    try:
        await c.message.edit_text(t(c.from_user.id, "indicators_menu_title"), reply_markup=kb.as_markup())
    except Exception:
        await c.message.answer(t(c.from_user.id, "indicators_menu_title"), reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "ind_global")
async def cb_ind_global(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    db.create_default_user(uid, c.from_user.username)
    settings = (db.get_user(uid) or {}).get("settings", {}) or {}
    kb = InlineKeyboardBuilder()
    for ind in ("RSI", "MACD", "EMA", "OI"):
        key = f"{ind}_ENABLED"
        cur = bool(settings.get(key, True))
        label = f"{ind}: {'ВКЛ' if cur else 'ВЫКЛ'}"
        kb.button(text=label, callback_data=f"ind_toggle:{ind}")
    kb.adjust(2)
    kb.button(text=t(uid, "settings_back"), callback_data="settings_indicators")
    try:
        await c.message.edit_text(t(uid, "indicators_global_title"), reply_markup=kb.as_markup())
    except Exception:
        await c.message.answer(t(uid, "indicators_global_title"), reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith("ind_toggle:"))
async def cb_ind_toggle(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    try:
        _, ind = c.data.split(":", 1)
    except Exception:
        await c.message.answer(t(uid, "error_data"))
        return
    key = f"{ind}_ENABLED"
    db.create_default_user(uid, c.from_user.username)
    user = db.get_user(uid) or {}
    settings = user.get("settings", {}) or {}
    cur = bool(settings.get(key, True))
    new = not cur
    db.update_setting(uid, key, new)
    await cb_ind_global(c)

@dp.callback_query(lambda c: c.data == "ind_advanced")
async def cb_ind_advanced(c: types.CallbackQuery):
    await c.answer()
    uid = c.from_user.id
    user = db.get_user(uid) or {}
    settings = user.get("settings", {}) or {}
    txt = t(uid, "indicators_advanced_text", settings=json.dumps(settings, indent=2, ensure_ascii=False))
    kb = InlineKeyboardBuilder(); kb.button(text=t(uid, "settings_back"), callback_data="settings_indicators"); kb.adjust(1)
    try:
        await c.message.edit_text(txt, reply_markup=kb.as_markup())
    except Exception:
        await c.message.answer(txt, reply_markup=kb.as_markup())

@dp.message(lambda m: m.text == t(m.from_user.id, "menu_trades"))
async def menu_trades(m: types.Message):
    if not has_active_sub(m.from_user.id):
        await m.reply(t(m.from_user.id, "subscribe_required"), reply_markup=main_reply_kb(m.from_user.id))
        return
    rows = db.get_trades_for_user(m.from_user.id, limit=50)
    if not rows:
        await m.reply(t(m.from_user.id, "no_trades"), reply_markup=main_reply_kb(m.from_user.id))
        return
    lines = []
    for r in rows[-20:]:
        ts = r.get("ts", "")
        symbol = r.get("symbol", "")
        side = r.get("side", "")
        qty = r.get("qty", "")
        price = r.get("price", "")
        pnl = r.get("pnl", "")
        lines.append(t(m.from_user.id, "trade_notification", symbol=symbol, side=side, price=price, qty=qty, pnl=pnl, ts=ts))
    chunk_size = 5
    for i in range(0, len(lines), chunk_size):
        await m.reply("\n\n".join(lines[i : i + chunk_size]))
    await m.reply(t(m.from_user.id, "trades_end"), reply_markup=main_reply_kb(m.from_user.id))

@dp.message(lambda m: m.text == t(m.from_user.id, "menu_support"))
async def menu_support(m: types.Message, state: FSMContext):
    await m.reply(t(m.from_user.id, "support_prompt"), reply_markup=main_reply_kb(m.from_user.id))
    await state.set_state(Form.support_user)

@dp.message(Form.support_user)
async def process_support_user(m: types.Message, state: FSMContext):
    txt = m.text or "<non-text>"
    uname = m.from_user.username or m.from_user.full_name or str(m.from_user.id)
    admin_text = f"📩 Support from @{uname} (id={m.from_user.id}):\n{txt}"
    try:
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, admin_text, reply_markup=admin_reply_kb_for_user(m.from_user.id))
        else:
            logger.warning("ADMIN_ID not configured - support message not forwarded to admin")
        await m.answer(t(m.from_user.id, "support_sent"), reply_markup=main_reply_kb(m.from_user.id))
    except Exception:
        logger.exception("Failed to forward support to admin")
        await m.answer(t(m.from_user.id, "support_failed"), reply_markup=main_reply_kb(m.from_user.id))
    await state.clear()

# Admin reply/callbacks / utilities (kept same but localized)
@dp.callback_query(lambda c: c.data and c.data.startswith("admin_reply:"))
async def cb_admin_reply(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    if c.from_user.id != ADMIN_ID:
        await c.message.answer(t(c.from_user.id, "admin_only"))
        return
    try:
        _, uid_s = c.data.split(":", 1)
        uid = int(uid_s)
    except Exception:
        await c.message.answer(t(c.from_user.id, "invalid_user_id"))
        return
    # call with target=uid to avoid t(...) positional/keyword clash
    await c.message.answer(t(c.from_user.id, "enter_reply_prompt", target=uid))
    await state.update_data(reply_to=uid)
    await state.set_state(Form.admin_reply)

@dp.message(Form.admin_reply)
async def process_admin_reply(m: types.Message, state: FSMContext):
    data = await state.get_data()
    target = data.get("reply_to")
    if not target:
        await m.reply(t(m.from_user.id, "error_data"))
        await state.clear()
        return
    text = m.text or ""
    try:
        await bot.send_message(int(target), f"📩 {t(m.from_user.id, 'reply_sent')}\n\n{text}")
        await m.reply(t(m.from_user.id, "reply_sent"), reply_markup=main_reply_kb(m.from_user.id))
        if ADMIN_ID and ADMIN_ID != m.from_user.id:
            try:
                await bot.send_message(ADMIN_ID, f"Админ @{m.from_user.username} ответил пользователю {target}.")
            except Exception:
                pass
    except Exception:
        logger.exception("Failed to send admin reply to user %s", target)
        await m.reply(t(m.from_user.id, "support_failed"))
    await state.clear()

# New admin command: broadcast (background)
@dp.message(lambda m: m.text and m.text.startswith("/broadcast ") or m.text and m.text == "/broadcast")
async def cmd_broadcast(m: types.Message):
    if m.from_user.id != ADMIN_ID:
        await m.reply(t(m.from_user.id, "admin_only"))
        return

    text = (m.text or "").partition(" ")[2].strip()
    if not text:
        await m.reply("Usage: /broadcast <text_to_send>")
        return

    await m.reply(f"Starting broadcast to all users... ({len(db.load_users())} users). I'll report back when finished.")
    # run in background to avoid blocking
    async def _broadcast_task(message_text: str):
        users = db.load_users() if hasattr(db, "load_users") else {}
        total = 0
        success = 0
        failed = 0
        for uid_str in list(users.keys()):
            try:
                uid = int(uid_str)
            except Exception:
                continue
            total += 1
            try:
                await bot.send_message(uid, message_text)
                success += 1
            except Exception:
                failed += 1
            # light pacing to reduce rate-limit issues
            await asyncio.sleep(0.05)
        # send summary to admin
        await bot.send_message(ADMIN_ID, f"Broadcast finished: total={total}, success={success}, failed={failed}")
    asyncio.create_task(_broadcast_task(text))

# New admin command: give_sub <id> <days|forever>
@dp.message(lambda m: m.text and m.text.startswith("/give_sub "))
async def cmd_give_sub(m: types.Message):
    if m.from_user.id != ADMIN_ID:
        await m.reply(t(m.from_user.id, "admin_only"))
        return
    parts = m.text.strip().split()
    if len(parts) < 3:
        await m.reply("Usage: /give_sub <user_id> <days|forever>")
        return
    try:
        target_id = int(parts[1])
    except Exception:
        await m.reply(t(m.from_user.id, "invalid_user_id"))
        return
    days_token = parts[2].lower()
    if days_token == "forever":
        days = 36500  # pseudo-forever: ~100 years
    else:
        try:
            days = int(days_token)
            if days <= 0:
                raise ValueError()
        except Exception:
            await m.reply("Days must be a positive integer or 'forever'.")
            return
    try:
        # use db.set_subscription which expects days param
        db.set_subscription(target_id, days=days)
        # notify target and admin
        try:
            await bot.send_message(target_id, f"✅ Ваша подписка обновлена администратором на {days} дней.")
        except Exception:
            pass
        await m.reply(f"✅ Subscription granted to {target_id} for {days} days.")
    except Exception as e:
        logger.exception("Failed to give subscription")
        await m.reply(f"Failed to set subscription: {e}")

@dp.message(Command("cancel"))
async def cmd_cancel(m: types.Message, state: FSMContext):
    await state.clear()
    await m.reply(t(m.from_user.id, "action_cancelled"), reply_markup=main_reply_kb(m.from_user.id))

@dp.message(Command("help"))
async def cmd_help(m: types.Message):
    await m.reply(t(m.from_user.id, "welcome_short"), reply_markup=main_reply_kb(m.from_user.id))

# Info handler (static English text as requested)
@dp.message(lambda m: m.text == t(m.from_user.id, "menu_info"))
async def menu_info(m: types.Message):
    # English info text (translated from the supplied Russian text)
    info_text = (
        "🤖 HOW THE BOT WORKS\n"
        "This bot connects directly to Bybit. You add your API keys and when the bot is enabled, "
        "it trades using the funds in your Unified Trading Account. The bot analyzes the market and "
        "executes trades while you do other things.\n\n"
        "💎 WHY SUBSCRIBE?\n"
        "Subscription is needed for project support and maintenance and to get access to the bot and its features.\n\n"
        "⚡️ The bot only trades your funds on your Bybit spot account — no other fees.\n\n"
        "📊 INDICATORS USED\n"
        "1. OPEN INTEREST — shows interest of buyers and sellers (informational, not configurable).\n"
        "2. RSI — identifies overbought/oversold conditions.\n"
        "3. MACD — signals trend changes.\n"
        "EMA is also used for price trend tracking.\n\n"
        "⚠️ ADDITIONAL INFORMATION\n"
        "Performance depends on indicator settings and market conditions. The bot does not guarantee profits — "
        "typical monthly returns may vary widely (example: 15–50% in some setups), depending on market and settings. "
        "Trade responsibly and adjust settings to your strategy.\n\n"
        "Currently the bot works only with Bybit.\n\n"
        "📚 Basic commands:\n"
        "/buy — create a subscription invoice\n"
        "SET KEY VALUE — change a numeric or boolean setting (example: SET RSI_PERIOD 14)\n"
        "SETKEY <api_key> <api_secret> — quick set of API keys\n"
    )
    await m.reply(info_text, reply_markup=main_reply_kb(m.from_user.id))

# Subscription menu handler
@dp.message(lambda m: m.text == t(m.from_user.id, "menu_subscription"))
async def menu_subscription(m: types.Message):
    uid = m.from_user.id
    # show subscription status and quick buy instruction
    u = db.get_user(uid) or {}
    sub_until = u.get("sub_until")
    status = "No active subscription"
    if sub_until:
        try:
            if datetime.fromisoformat(sub_until) > datetime.utcnow():
                status = f"Active until {sub_until}"
            else:
                status = "No active subscription"
        except Exception:
            status = "No active subscription"
    text = f"Subscription status: {status}\n\nTo buy or renew subscription use /buy or press the command in the menu."
    await m.reply(text, reply_markup=main_reply_kb(uid))

# workers: trades_worker and check_invoices_worker (only read/write db & trades.json; no Bybit calls at startup)
async def trades_worker():
    last_index = 0
    try:
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
                last_index = len(arr)
    except Exception:
        last_index = 0

    try:
        await bot.get_me()
    except Exception:
        logger.warning("Bot.get_me failed at trades_worker startup")

    while True:
        try:
            if not os.path.exists(TRADES_FILE):
                await asyncio.sleep(2)
                continue
            with open(TRADES_FILE, "r", encoding="utf-8") as f:
                trades = json.load(f)
            if len(trades) > last_index:
                for t in trades[last_index:]:
                    try:
                        uid = int(t.get("user_id"))
                        if not has_active_sub(uid) or not is_trading_active(uid):
                            continue
                        # localized trade message
                        user = db.get_user(uid) or {}
                        lang = (user.get("settings", {}) or {}).get("lang", "ru")
                        fmt = LOCALE.get(lang, LOCALE["ru"])["trade_notification"]
                        await bot.send_message(uid, fmt.format(
                            symbol=t.get('symbol'), side=t.get('side'), price=t.get('price'), qty=t.get('qty'), pnl=t.get('pnl'), ts=t.get('ts')
                        ))
                    except Exception:
                        logger.exception("Failed to send trade notification")
                last_index = len(trades)
        except Exception:
            logger.exception("trades_worker error")
        await asyncio.sleep(3)

async def check_invoices_worker():
    try:
        await bot.get_me()
    except Exception:
        logger.warning("Bot.get_me failed at invoices_worker startup")

    while True:
        try:
            users = db.load_users() if hasattr(db, "load_users") else {}
            for uid_str, u in users.items():
                try:
                    uid = int(uid_str)
                except Exception:
                    continue
                settings = (u.get("settings") or {})
                inv_id = settings.get("last_invoice_id")
                if not inv_id:
                    continue
                inv = fetch_invoice_status(str(inv_id))
                if not inv:
                    continue
                status_val = ""
                if isinstance(inv, dict):
                    status_val = str(inv.get("status") or inv.get("state") or inv.get("result") or "").lower()
                if any(k in status_val for k in ("paid", "confirmed", "success")):
                    try:
                        db.set_subscription(uid, days=30)
                        db.update_setting(uid, "last_invoice_id", None)
                        try:
                            await bot.send_message(uid, t(uid, "invoice_paid"))
                        except Exception:
                            pass
                        if ADMIN_ID:
                            try:
                                await bot.send_message(ADMIN_ID, f"💰 Пользователь {uid} успешно оплатил подписку (invoice {inv_id}).")
                            except Exception:
                                pass
                    except Exception:
                        logger.exception("Failed to set subscription for paid invoice")
        except Exception:
            logger.exception("check_invoices_worker error")
        await asyncio.sleep(8)

# Global error handler - flexible signature to handle different aiogram versions
async def _global_errors_handler(update_or_exception, exception=None):
    """
    Compatible error handler for aiogram variations:
    - may be called as _global_errors_handler(update, exception)
    - or as _global_errors_handler(exception)
    We normalize arguments.
    """
    try:
        if exception is None and isinstance(update_or_exception, Exception):
            exc = update_or_exception
            update = None
        else:
            update = update_or_exception
            exc = exception
        logger.exception("Unhandled exception for update %s: %s", update, exc)
        if ADMIN_ID and _should_notify("dp_unhandled"):
            msg = f"❗️Unhandled error: {type(exc).__name__}\n{str(exc)[:800]}"
            try:
                await _async_send_admin(msg)
            except Exception:
                logger.exception("Failed to send admin notification from error handler")
    except Exception:
        logger.exception("Error in global error handler")
    return True

dp.errors.register(_global_errors_handler)

# main runner
async def main():
    tasks = [
        asyncio.create_task(trades_worker(), name="trades_worker"),
        asyncio.create_task(check_invoices_worker(), name="check_invoices_worker"),
        asyncio.create_task(dp.start_polling(bot), name="telegram_poller"),
    ]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            if t.exception():
                raise t.exception()
    except asyncio.CancelledError:
        logger.info("Main cancelled")
    except Exception:
        logger.exception("Unhandled exception in main tasks")
        notify_admin_rate_limited_sync("Main loop crashed: check logs", key="main_crash")
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        try:
            await bot.session.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down (KeyboardInterrupt)...")
    except Exception:
        logger.exception("Unhandled exception in __main__")
