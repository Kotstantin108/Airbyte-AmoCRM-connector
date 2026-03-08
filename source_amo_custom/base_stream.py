"""Базовые классы для потоков данных"""

import logging
import requests
import time
from urllib.parse import urlparse, parse_qs
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream

from .constants import MAX_RECORDS_PER_PAGE, RATE_LIMIT_DELAY_SECONDS
from .constants import BACKOFF_RATE_LIMIT, BACKOFF_SERVER_ERROR, BACKOFF_UNAUTHORIZED

logger = logging.getLogger("airbyte")


class AmoStream(HttpStream):
    """Базовый класс для всех потоков AmoCRM"""
    
    primary_key = "id"
    
    def __init__(self, token_manager, domain, start_date, **kwargs):
        super().__init__(**kwargs)
        self.token_manager = token_manager
        self.domain = domain
        self.start_date = start_date

    @property
    def url_base(self) -> str:
        return f"https://{self.domain}.amocrm.ru/api/v4/"

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        """Добавляет Bearer токен в заголовки"""
        token = self.token_manager.get_valid_token()
        return {"Authorization": f"Bearer {token}"}

    def should_retry(self, response: requests.Response) -> bool:
        """Определяет, нужно ли повторить запрос при ошибке"""
        if response.status_code == 401:
            self.token_manager.invalidate_memory_token()
            return True
        if response.status_code == 429:
            return True
        if response.status_code >= 500:
            return True
        return False
        
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """Возвращает время ожидания перед повтором запроса"""
        if response.status_code == 429:
            return BACKOFF_RATE_LIMIT
        if response.status_code >= 500:
            return BACKOFF_SERVER_ERROR
        if response.status_code == 401:
            return BACKOFF_UNAUTHORIZED
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Определяет токен для следующей страницы пагинации"""
        if response.status_code == 204:
            logger.info(f"[{self.name}] Received 204 - no more data")
            return None
        
        try:
            data = response.json()
            if not data:
                logger.info(f"[{self.name}] Empty response body - stopping pagination")
                return None
            if not data.get('_embedded'):
                logger.info(
                    f"[{self.name}] Empty _embedded (keys: {list(data.keys())}) - "
                    f"stopping pagination"
                )
                return None
            
            records = data.get('_embedded', {}).get(self.name, [])
            records_count = len(records)
            logger.info(f"[{self.name}] Got {records_count} records on current page")
                
        except Exception as e:
            logger.warning(f"[{self.name}] Failed to parse JSON response: {e}")
            return None

        # Проверяем наличие _links.next
        next_link = data.get('_links', {}).get('next', {}).get('href')
        if not next_link:
            logger.info(f"[{self.name}] No _links.next found - last page reached")
            return None

        try:
            parsed = urlparse(next_link)
            qs = parse_qs(parsed.query)
            if 'page' in qs:
                next_page = int(qs['page'][0])
                logger.info(f"[{self.name}] Moving to page {next_page} via _links.next")
                return {'page': next_page}
            else:
                logger.warning(f"[{self.name}] _links.next found but no 'page' param in: {next_link}")
                return None
        except Exception as e:
            logger.warning(f"Pagination error: {e}. Url: {response.request.url}")
            return None

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        """Базовые параметры запроса"""
        params = {'limit': MAX_RECORDS_PER_PAGE}
        if next_page_token:
            params.update(next_page_token)
        else:
            params['page'] = 1
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """Парсит ответ API"""
        time.sleep(RATE_LIMIT_DELAY_SECONDS)
        
        if response.status_code == 204:
            return []
        if not response.ok:
            logger.error(f"API Error in {self.name}: {response.status_code} - {response.text}")
            response.raise_for_status()
        
        return response.json().get('_embedded', {}).get(self.name, [])


class AmoFullRefreshStream(AmoStream):
    """Базовый класс для справочников без инкрементальной синхронизации"""
    
    def __init__(self, token_manager, domain, **kwargs):
        super().__init__(token_manager, domain, start_date=0, **kwargs)
    
    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        return {'limit': MAX_RECORDS_PER_PAGE}
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Справочники обычно помещаются на одну страницу"""
        return None
