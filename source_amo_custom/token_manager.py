"""Управление OAuth токенами через PostgreSQL"""

import logging
import requests
import psycopg2
from datetime import datetime, timedelta

logger = logging.getLogger("airbyte")


class DatabaseTokenManager:
    """Менеджер токенов с хранением в PostgreSQL и автоматическим обновлением"""
    
    def __init__(self, db_config, domain):
        self.db_config = db_config
        self.domain = domain
        self._memory_access_token = None
    
    def invalidate_memory_token(self):
        """Инвалидирует токен в памяти при 401 ошибке"""
        logger.warning(f"Invalidating memory token for {self.domain} due to 401.")
        self._memory_access_token = None

    def get_valid_token(self):
        """Получает валидный access token, обновляя при необходимости"""
        if self._memory_access_token:
            return self._memory_access_token

        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT access_token, refresh_token, client_id, client_secret, expires_at "
                    "FROM amo_tokens WHERE domain = %s FOR UPDATE",
                    (self.domain,)
                )
                row = cur.fetchone()
                if not row:
                    raise Exception(f"Domain {self.domain} not found in amo_tokens.")
                
                access_token, refresh_token, client_id, client_secret, expires_at = row
                now = datetime.now()
                is_expired = True if not expires_at else (now > (expires_at - timedelta(minutes=5)))
                
                if is_expired:
                    logger.info(f"Token expired. Refreshing...")
                    try:
                        new_access, new_refresh, expires_in = self._refresh_in_amo(
                            self.domain, refresh_token, client_id, client_secret
                        )
                        new_expires_at = datetime.now() + timedelta(seconds=expires_in)
                        cur.execute(
                            "UPDATE amo_tokens SET access_token = %s, refresh_token = %s, "
                            "expires_at = %s, updated_at = NOW() WHERE domain = %s",
                            (new_access, new_refresh, new_expires_at, self.domain)
                        )
                        conn.commit()
                        self._memory_access_token = new_access
                        return new_access
                    except Exception as e:
                        conn.rollback()
                        raise e
                else:
                    conn.commit()
                    self._memory_access_token = access_token
                    return access_token
        finally:
            conn.close()

    def _refresh_in_amo(self, domain, refresh_token, client_id, client_secret):
        """Обновляет токен через AmoCRM OAuth API"""
        url = f"https://{domain}.amocrm.ru/oauth2/access_token"
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": "https://airbyte.io"
        }
        resp = requests.post(url, json=payload)
        if not resp.ok:
            logger.error(f"Refresh failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        data = resp.json()
        return data['access_token'], data['refresh_token'], data.get('expires_in', 86400)
