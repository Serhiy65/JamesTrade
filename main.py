"""
JamesTrade — Главный запускатель проекта
----------------------------------------
Этот файл одновременно запускает:
  • trading_core.py — торговое ядро
  • tg_app.py — Telegram-бот
"""

import sys
import subprocess
import threading
import time
import os
import importlib

# === 1. Исправляем кодировку консоли ===
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

print("[Main] ✅ Кодировка UTF-8 установлена.")

# === 2. Проверяем зависимости ===
REQUIRED_LIBS = [
    "requests",
    "pandas",
    "numpy",
    "python-dotenv",
    "telebot",
    "ta",
]

def install_missing():
    missing = []
    for lib in REQUIRED_LIBS:
        try:
            importlib.import_module(lib)
        except ImportError:
            missing.append(lib)
    if missing:
        print(f"[Main] ⚙️ Устанавливаю недостающие библиотеки: {', '.join(missing)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    else:
        print("[Main] ✅ Все зависимости установлены.")

install_missing()

# === 3. Загружаем .env переменные ===
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("[Main] ✅ Переменные окружения (.env) загружены.")
except Exception as e:
    print(f"[Main] ⚠️ Не удалось загрузить .env: {e}")

# === 4. Функции для запуска ботов ===
def run_trading_core():
    while True:
        try:
            print("\n[Main] 🔥 Запуск trading_core.py ...")
            subprocess.run([sys.executable, "trading_core.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"[Main] ⚠️ Trading core упал: {e}. Перезапуск через 5 сек...")
            time.sleep(5)
        except Exception as e:
            print(f"[Main] ❌ Ошибка trading_core: {e}")
            time.sleep(5)

def run_tg_app():
    while True:
        try:
            print("\n[Main] 💬 Запуск tg_app.py ...")
            subprocess.run([sys.executable, "tg_app.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"[Main] ⚠️ Telegram бот упал: {e}. Перезапуск через 5 сек...")
            time.sleep(5)
        except Exception as e:
            print(f"[Main] ❌ Ошибка tg_app: {e}")
            time.sleep(5)

            def run_bot_process():
                while True:
                    try:
                        print("\n[Main] 🔥 Запуск bot.py ...")
                        subprocess.run([sys.executable, "bot.py"], check=True)
                    except subprocess.CalledProcessError as e:
                        print(f"[Main] ⚠️ Bot упал: {e}. Перезапуск через 5 сек...")
                        time.sleep(5)
                    except Exception as e:
                        print(f"[Main] ❌ Ошибка bot : {e}")
                        time.sleep(5)

            print(f"[Main] ⚠️ Trading core упал: {e}. Перезапуск через 5 сек...")
            time.sleep(5)
        except Exception as e:
            print(f"[Main] ❌ Ошибка trading_core: {e}")
            time.sleep(5)

# === 5. Запуск потоков ===
t1 = threading.Thread(target=run_trading_core, daemon=True)
t2 = threading.Thread(target=run_tg_app, daemon=True)

t1.start()
t2.start()

print("\n[Main] 🚀 Проект запущен! Работают оба модуля (торговля + Telegram).")

# === 6. Основной цикл удержания ===
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[Main] 📴 Остановка вручную. Завершаем все процессы...")
    sys.exit(0)
