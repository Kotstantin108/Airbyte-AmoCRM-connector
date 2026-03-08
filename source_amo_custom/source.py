"""Основной модуль коннектора AmoCRM для Airbyte"""

import sys
import logging
import requests
import psycopg2
import traceback
from typing import Any, Iterable, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import ConnectorSpecification

from .spec import SPEC_DICT
from .token_manager import DatabaseTokenManager
from .constants import FULL_LOAD_THRESHOLD
from .base_stream import AmoStream, AmoFullRefreshStream
from .incremental_stream import AmoIncrementalStream
from .schemas import (
    get_leads_schema, get_contacts_schema, get_events_schema,
    get_pipelines_schema, get_custom_fields_schema, get_users_schema
)

# Настройка логирования
logger = logging.getLogger("airbyte")
logger.setLevel(logging.INFO)

# Глобальный перехватчик ошибок
def exception_hook(exctype, value, tb):
    logger.error("CRITICAL UNCAUGHT EXCEPTION:")
    logger.error(''.join(traceback.format_exception(exctype, value, tb)))
    sys.__excepthook__(exctype, value, tb)
sys.excepthook = exception_hook


# ============================================================================
# ИНКРЕМЕНТАЛЬНЫЕ ПОТОКИ
# ============================================================================

class Leads(AmoIncrementalStream):
    """Поток сделок (leads) — с историей изменений"""
    name = "leads"
    cursor_field = "updated_at"
    # Ключ только id (перезаписываем старые версии, чтобы Airbyte dbt
    # дедуплицировал записи и отдавал БД только последнюю актуальную версию).
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "leads"
        
    def request_params(self, stream_state=None, next_page_token=None, **kwargs):
        params = super().request_params(next_page_token, **kwargs)
        params['with'] = 'contacts'
        
        page = next_page_token.get('page', 1) if next_page_token else 1
        
        if self._is_full_load_mode():
            params.update(self._build_full_load_params(page))
        else:
            start = self._get_start_timestamp(stream_state)
            params.update(self._build_incremental_params(start, page))
        
        return params

    def get_json_schema(self) -> Mapping[str, Any]:
        return get_leads_schema()


class Contacts(AmoIncrementalStream):
    """Поток контактов"""
    name = "contacts"
    cursor_field = "updated_at"
    
    def path(self, **kwargs) -> str:
        return "contacts"
    
    def request_params(self, stream_state=None, next_page_token=None, **kwargs):
        params = super().request_params(next_page_token, **kwargs)
        page = next_page_token.get('page', 1) if next_page_token else 1
        
        if self._is_full_load_mode():
            params.update(self._build_full_load_params(page))
        else:
            start = self._get_start_timestamp(stream_state)
            params.update(self._build_incremental_params(start, page))
        
        return params

    def get_json_schema(self) -> Mapping[str, Any]:
        return get_contacts_schema()


class Events(AmoIncrementalStream):
    """Поток событий удаления"""
    name = "events"
    cursor_field = "created_at"
    
    def path(self, **kwargs) -> str:
        return "events"
    
    def request_params(self, stream_state=None, next_page_token=None, **kwargs):
        params = super().request_params(next_page_token, **kwargs)
        params['filter[type][0]'] = 'lead_deleted'
        params['filter[type][1]'] = 'contact_deleted'
        
        page = next_page_token.get('page', 1) if next_page_token else 1
        
        if self._is_full_load_mode():
            params.update(self._build_full_load_params(page))
        else:
            start = self._get_start_timestamp(stream_state)
            params.update(self._build_incremental_params(start, page))
        
        return params

    def get_json_schema(self) -> Mapping[str, Any]:
        return get_events_schema()


# ============================================================================
# FULL REFRESH ПОТОКИ (справочники)
# ============================================================================

class Pipelines(AmoFullRefreshStream):
    """Воронки и статусы сделок"""
    name = "pipelines"
    
    def path(self, **kwargs) -> str:
        return "leads/pipelines"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 204:
            return []
        if not response.ok:
            logger.error(f"API Error in {self.name}: {response.status_code} - {response.text}")
            response.raise_for_status()
        
        return response.json().get('_embedded', {}).get('pipelines', [])
    
    def get_json_schema(self) -> Mapping[str, Any]:
        return get_pipelines_schema()


class CustomFieldsLeads(AmoFullRefreshStream):
    """Описание кастомных полей сделок"""
    name = "custom_fields_leads"
    
    def path(self, **kwargs) -> str:
        return "leads/custom_fields"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 204:
            return []
        if not response.ok:
            logger.error(f"API Error in {self.name}: {response.status_code} - {response.text}")
            response.raise_for_status()
        
        return response.json().get('_embedded', {}).get('custom_fields', [])
    
    def get_json_schema(self) -> Mapping[str, Any]:
        return get_custom_fields_schema()


class CustomFieldsContacts(AmoFullRefreshStream):
    """Описание кастомных полей контактов"""
    name = "custom_fields_contacts"
    
    def path(self, **kwargs) -> str:
        return "contacts/custom_fields"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 204:
            return []
        if not response.ok:
            logger.error(f"API Error in {self.name}: {response.status_code} - {response.text}")
            response.raise_for_status()
        
        return response.json().get('_embedded', {}).get('custom_fields', [])
    
    def get_json_schema(self) -> Mapping[str, Any]:
        return get_custom_fields_schema()


class Users(AmoFullRefreshStream):
    """Пользователи AmoCRM"""
    name = "users"
    
    def path(self, **kwargs) -> str:
        return "users"
    
    def get_json_schema(self) -> Mapping[str, Any]:
        return get_users_schema()


# ============================================================================
# SOURCE
# ============================================================================

class SourceAmoCustom(AbstractSource):
    """Главный класс коннектора AmoCRM"""
    
    def spec(self, *args, **kwargs) -> ConnectorSpecification:
        return ConnectorSpecification(**SPEC_DICT)

    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        """Проверяет подключение к БД и AmoCRM API"""
        try:
            # Проверка подключения к PostgreSQL
            conn = psycopg2.connect(
                host=config["db_host"],
                port=config["db_port"],
                database=config["db_name"],
                user=config["db_user"],
                password=config["db_password"],
                connect_timeout=5
            )
            conn.close()
            
            # Проверка получения токена из AmoCRM
            mgr = DatabaseTokenManager({
                "host": config["db_host"],
                "port": config["db_port"],
                "database": config["db_name"],
                "user": config["db_user"],
                "password": config["db_password"]
            }, config['domain'])
            mgr.get_valid_token()
            
            return True, None
        except Exception as e:
            return False, f"Check failed: {e}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Создаёт и возвращает список потоков данных"""
        try:
            db_conf = {
                "host": config["db_host"],
                "port": config["db_port"],
                "database": config["db_name"],
                "user": config["db_user"],
                "password": config["db_password"]
            }
            domain = config['domain']
            
            raw_start_date = config.get('start_date')
            logger.info(
                f"CONFIG start_date raw value: {raw_start_date}, type: {type(raw_start_date)}"
            )
            
            start_date = int(raw_start_date) if raw_start_date is not None else FULL_LOAD_THRESHOLD
            logger.info(f"Using start_date: {start_date}")
            
            mgr = DatabaseTokenManager(db_conf, domain)
            
            return [
                # Full Refresh стримы (справочники) - загружаются первыми
                CustomFieldsLeads(mgr, domain),
                CustomFieldsContacts(mgr, domain),
                Pipelines(mgr, domain),
                Users(mgr, domain),
                # Инкрементальные стримы (основные данные) - загружаются после справочников
                Leads(mgr, domain, start_date),
                Contacts(mgr, domain, start_date),
                Events(mgr, domain, start_date),
            ]
        except Exception as e:
            logger.error(f"Failed to initialize streams: {traceback.format_exc()}")
            raise e
