# client.py
# -*- coding: utf-8 -*-
"""
Lightweight Bybit client wrapper used by tg_app.py

- Конструктор НЕ выполняет сетевых запросов.
- Методы делают сетевые вызовы только при явном вызове.
- Предназначен для простой валидации ключей (get_balance_usdt / get_account_info) и place_order.
- Если у тебя уже был продвинутый client.py (pybit), можно подсунуть его под тем же именем — tg_app совместим.
"""

import time
import hmac
import hashlib
import requests
import math
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAINNET = "https://api.bybit.com"
TESTNET = "https://api-testnet.bybit.com"

class BybitClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.base_url = TESTNET if testnet else MAINNET
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # IMPORTANT: do NOT perform network calls here (no ping/time sync).
        logger.info("[BybitClient] initialized (no network calls in ctor). Testnet=%s", testnet)

    def _sign(self, params: Dict[str, Any]) -> str:
        # lexicographic order
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
        return hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()

    def _get(self, path: str, params: Optional[dict] = None, auth: bool = False) -> Optional[dict]:
        url = self.base_url + path
        params = params.copy() if params else {}
        try:
            if auth:
                params.setdefault("api_key", self.api_key)
                params.setdefault("timestamp", int(time.time() * 1000))
                params["sign"] = self._sign(params)
            r = self.session.get(url, params=params, timeout=12)
            if r.status_code != 200:
                logger.warning("GET %s status %s body=%s", path, r.status_code, r.text[:300])
                return None
            return r.json()
        except Exception as e:
            logger.exception("GET %s error: %s", path, e)
            return None

    def _post(self, path: str, params: Optional[dict] = None, auth: bool = False) -> Optional[dict]:
        url = self.base_url + path
        params = params.copy() if params else {}
        try:
            if auth:
                params.setdefault("api_key", self.api_key)
                params.setdefault("timestamp", int(time.time() * 1000))
                params["sign"] = self._sign(params)
            r = self.session.post(url, json=params, timeout=12)
            if r.status_code != 200:
                logger.warning("POST %s status %s body=%s", path, r.status_code, r.text[:300])
                return None
            return r.json()
        except Exception as e:
            logger.exception("POST %s error: %s", path, e)
            return None

    # --- lightweight ping (public) ---
    def ping(self) -> bool:
        try:
            r = self._get("/v5/market/time", params=None, auth=False)
            return r is not None
        except Exception:
            return False

    # --- get balance (non-destructive, requires auth) ---
    def get_balance_usdt(self) -> Optional[float]:
        """
        Returns float balance or None on error (including auth errors).
        """
        try:
            res = self._get("/v5/account/wallet-balance", params={"accountType": "UNIFIED"}, auth=True)
            if not res or not isinstance(res, dict):
                return None
            # parse result
            result = res.get("result") or {}
            lst = result.get("list") or []
            if not lst and isinstance(result, list):
                lst = result
            if not lst:
                # may contain direct coin info
                return 0.0
            for acc in lst:
                # each acc may contain 'coin' list
                coins = acc.get("coin") or acc.get("coins") or []
                if isinstance(coins, list):
                    for c in coins:
                        if c.get("coin") == "USDT":
                            for k in ("availableToTrade", "availableBalance", "walletBalance", "balance"):
                                if k in c:
                                    try:
                                        return float(c.get(k) or 0.0)
                                    except Exception:
                                        continue
                if acc.get("coin") == "USDT":
                    for k in ("availableToTrade", "availableBalance", "walletBalance", "balance"):
                        if k in acc:
                            try:
                                return float(acc.get(k) or 0.0)
                            except Exception:
                                continue
            return 0.0
        except Exception as e:
            logger.exception("get_balance_usdt error: %s", e)
            return None

    def get_account_info(self) -> Optional[dict]:
        # lightweight account info (requires auth)
        try:
            res = self._get("/v5/account/info", params={}, auth=True)
            if not res:
                return None
            return res.get("result") or res
        except Exception as e:
            logger.exception("get_account_info error: %s", e)
            return None

    def fetch_ohlcv_df(self, symbol: str, interval: str = "5", limit: int = 200):
        # This method is left intentionally simple — if user has pybit wrapper prefer using it.
        # For now, make a public call to /v5/market/kline (no auth)
        try:
            params = {"category": "linear", "symbol": symbol, "interval": str(interval), "limit": int(limit)}
            res = self._get("/v5/market/kline", params=params, auth=False)
            if not res or not isinstance(res, dict):
                return None
            # the tg_app expects a pandas DataFrame in original code; here we return raw dict for caller to parse
            return res
        except Exception as e:
            logger.exception("fetch_ohlcv_df error: %s", e)
            return None

    def place_order(self, side: str, qty: float, symbol: str) -> Dict[str, Any]:
        """
        Simple wrapper for placing a MARKET order. Returns API response dict or {'error': '...'}.
        """
        try:
            side_norm = "Buy" if str(side).lower().startswith("b") else "Sell"
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side_norm,
                "orderType": "Market",
                "qty": str(qty),
            }
            res = self._post("/v5/order/create", params=params, auth=True)
            if not res:
                return {"error": "no_response"}
            return res
        except Exception as e:
            logger.exception("place_order error: %s", e)
            return {"error": str(e)}
