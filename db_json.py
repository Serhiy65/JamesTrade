# db_json.py — простая версия без шифрования API ключей
import sys
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import os, json, threading, traceback
from datetime import datetime, timedelta

LOCK = threading.Lock()
USERS_FILE = os.getenv('USERS_FILE', './users.json')
TRADES_FILE = os.getenv('TRADES_FILE', './trades.json')


def _ensure_files():
    """Создает users.json и trades.json, если их нет"""
    for path, default in [(USERS_FILE, {}), (TRADES_FILE, [])]:
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default, f, indent=4, ensure_ascii=False)


def _read(path, default):
    """Безопасное чтение JSON"""
    try:
        if not os.path.exists(path):
            _ensure_files()
            return default
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[DB_JSON] Ошибка чтения {path}: {e}")
        traceback.print_exc()
        return default


def _write(path, data):
    """Безопасная запись JSON"""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"[DB_JSON] Ошибка записи {path}: {e}")
        traceback.print_exc()


# ------------------------
# Основная логика работы с пользователями
# ------------------------

def _ensure_user_defaults(users, uid, username=None):
    """Добавляет все недостающие поля и настройки пользователю"""
    uid = str(uid)
    if uid not in users:
        users[uid] = {}

    u = users[uid]
    u.setdefault('username', username or f"user_{uid}")
    u.setdefault('api_key', '')
    u.setdefault('api_secret', '')
    u.setdefault('sub_until', None)

    # стандартные настройки торговли
    defaults = {
        'USE_RSI': True, 'RSI_PERIOD': 14, 'RSI_OVERSOLD': 40, 'RSI_OVERBOUGHT': 60,
        'USE_EMA': True, 'FAST_MA': 50, 'SLOW_MA': 200,
        'USE_MACD': True, 'MACD_FAST': 8, 'MACD_SLOW': 21, 'MACD_SIGNAL': 5,
        'USE_OI': False, 'OI_WINDOW': 3, 'OI_MIN_CHANGE_PCT': 5.0, 'OI_DIRECTION': 'up',
        'BUY_CONFIRMATION_RATIO': 0.66, 'SELL_CONFIRMATION_RATIO': 0.33,
        'ORDER_PERCENT': 10.0, 'ORDER_SIZE_USD': 0.0,
        'TP_PCT': 1.0, 'SL_PCT': 0.5,
        'QTY_PRECISION': 6,
        'MIN_NOTIONAL': 5.0,
        'SYMBOLS': ['BTCUSDT'],
        'TESTNET': True,
        'DRY_RUN': True,
        'DISABLED_AUTH': False
    }

    if 'settings' not in u or not isinstance(u['settings'], dict):
        u['settings'] = {}
    for k, v in defaults.items():
        u['settings'].setdefault(k, v)

    u.setdefault('_positions', {})
    users[uid] = u
    return users


def load_users(path=None):
    _ensure_files()
    return _read(path or USERS_FILE, {})


def save_users(data, path=None):
    with LOCK:
        _write(path or USERS_FILE, data)


def get_user(uid, path=None):
    users = load_users(path)
    users = _ensure_user_defaults(users, uid)
    save_users(users, path)
    return users.get(str(uid))


def create_default_user(uid, username=None, path=None):
    users = load_users(path)
    users = _ensure_user_defaults(users, uid, username)
    save_users(users, path)
    return users[str(uid)]


def set_api_keys(uid, api_key, api_secret, path=None):
    """Сохраняет ключи в обычном виде"""
    users = load_users(path)
    users = _ensure_user_defaults(users, uid)
    u = users[str(uid)]
    u['api_key'] = api_key.strip()
    u['api_secret'] = api_secret.strip()
    save_users(users, path)


def set_subscription(uid, days=30, path=None):
    """Выдает подписку пользователю"""
    users = load_users(path)
    users = _ensure_user_defaults(users, uid)
    users[str(uid)]['sub_until'] = (datetime.utcnow() + timedelta(days=days)).isoformat()
    save_users(users, path)


def is_subscribed(uid, path=None):
    """Проверяет активна ли подписка"""
    u = get_user(uid, path)
    if not u or not u.get('sub_until'):
        return False
    try:
        return datetime.fromisoformat(u['sub_until']) > datetime.utcnow()
    except Exception:
        return False


def update_setting(uid, key, value, path=None):
    """Обновляет настройку конкретного пользователя"""
    users = load_users(path)
    users = _ensure_user_defaults(users, uid)
    users[str(uid)]['settings'][key] = value
    save_users(users, path)
    return users[str(uid)]['settings']


def append_trade(trade, path=None):
    """Добавляет новую запись о сделке"""
    path = path or TRADES_FILE
    with LOCK:
        trades = _read(path, [])
        trades.append(trade)
        _write(path, trades)


def get_trades_for_user(uid, limit=100, path=None):
    """Возвращает последние сделки пользователя"""
    trades = _read(path or TRADES_FILE, [])
    uid = str(uid)
    user_trades = [t for t in trades if str(t.get('user_id')) == uid]
    return user_trades[-limit:]


# ------------------------
# Автозапуск при импорте
# ------------------------
_ensure_files()
users = load_users()
changed = False
for uid in list(users.keys()):
    before = dict(users.get(uid, {}))
    users = _ensure_user_defaults(users, uid)
    if users.get(uid) != before:
        changed = True
if changed:
    save_users(users)

print("[DB_JSON] Готово — все пользователи нормализованы, ключи сохраняются в обычном виде ✅")
