Full Trade Bot MVP
------------------

Files:
- tg_app.py        Telegram bot for user management, settings, support, subscription
- trading_core.py  Trading loop using per-user settings, writes trades to trades.json
- client.py        Bybit client wrapper (included)
- db_json.py       JSON DB helpers for users and trades
- users.json       Per-user settings (initially empty)
- trades.json      Trade log (initially empty)
- .env.example     Example env file

Security:
- Do not commit .env with secrets.
- For production, move to a proper DB and secure key storage.
