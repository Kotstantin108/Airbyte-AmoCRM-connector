"""Базовый класс для инкрементальных потоков"""

import logging
import time
from typing import Any, Mapping, MutableMapping, Optional
from urllib.parse import parse_qs, urlparse

from .base_stream import AmoStream
from .constants import FULL_LOAD_THRESHOLD, OVERLAP_SECONDS, INCREMENTAL_WINDOW_SECONDS

logger = logging.getLogger("airbyte")


class AmoIncrementalStream(AmoStream):
    """Базовый класс для инкрементальных потоков с поддержкой cursor и full load режима"""
    
    _cursor_value = None
    
    @property
    def state(self) -> MutableMapping[str, Any]:
        """Возвращает текущее состояние cursor"""
        return {self.cursor_field: self._cursor_value} if self._cursor_value else {}
    
    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        """Устанавливает состояние, игнорируя его в режиме полной загрузки"""
        if self.start_date <= FULL_LOAD_THRESHOLD:
            logger.info(
                f"[{self.name}] start_date={self.start_date} (full reload), "
                f"ignoring saved state: {value}"
            )
            self._cursor_value = None
            return
        self._cursor_value = value.get(self.cursor_field)
    
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], 
                         latest_record: Mapping[str, Any]) -> MutableMapping[str, Any]:
        """Обновляет состояние cursor на основе последней записи"""
        latest_value = latest_record.get(self.cursor_field, 0)
        current_value = current_stream_state.get(self.cursor_field, 0)
        new_value = max(current_value, latest_value)
        self._cursor_value = new_value
        return {self.cursor_field: new_value}
    
    def _get_start_timestamp(self, stream_state: Optional[Mapping[str, Any]]) -> int:
        """Вычисляет начальный timestamp для фильтрации с overlap окном"""
        start = self.start_date
        if self._cursor_value:
            start = max(start, self._cursor_value)
        elif stream_state and self.cursor_field in stream_state:
            start = max(start, stream_state[self.cursor_field])
        # Overlap: откатываемся на OVERLAP_SECONDS назад, чтобы не потерять
        # записи из-за eventual consistency amoCRM API.
        # Дубли обрабатываются dedup-режимом Airbyte по primary_key.
        # Leads: составной ключ (id, updated_at) — сохраняет историю.
        # Остальные: ключ id — перезаписывается последняя версия.
        if start > self.start_date:
            start = max(self.start_date, start - OVERLAP_SECONDS)
        return start
    
    def _is_full_load_mode(self) -> bool:
        """Проверяет, включён ли режим полной загрузки"""
        return self.start_date <= FULL_LOAD_THRESHOLD

    def _get_window_bounds(
        self,
        start: int,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> tuple[int, int]:
        """Возвращает границы текущего инкрементального окна."""
        if next_page_token and '_ab_window_start' in next_page_token and '_ab_window_end' in next_page_token:
            return int(next_page_token['_ab_window_start']), int(next_page_token['_ab_window_end'])

        window_start = start
        window_end = min(int(time.time()), window_start + INCREMENTAL_WINDOW_SECONDS)
        return window_start, max(window_start, window_end)

    def _get_request_context(self, response) -> MutableMapping[str, Any]:
        """Извлекает page/window metadata из URL уже отправленного запроса."""
        parsed = urlparse(response.request.url)
        qs = parse_qs(parsed.query)
        page = int(qs.get('page', ['1'])[0])
        window_start = int(qs.get(f'filter[{self.cursor_field}][from]', [str(self.start_date)])[0])

        raw_window_end = qs.get(f'filter[{self.cursor_field}][to]')
        if raw_window_end:
            window_end = int(raw_window_end[0])
        else:
            window_end = window_start

        recovery = qs.get('_ab_recovery', ['0'])[0] == '1'
        return {
            'page': page,
            'window_start': window_start,
            'window_end': window_end,
            'recovery': recovery,
        }

    def _build_next_window_token(self, current_window_end: int) -> Optional[Mapping[str, Any]]:
        """Переходит к следующему временному окну с overlap."""
        now_ts = int(time.time())
        if current_window_end >= now_ts:
            return None

        next_window_start = max(self.start_date, current_window_end - OVERLAP_SECONDS)
        next_window_end = min(now_ts, next_window_start + INCREMENTAL_WINDOW_SECONDS)
        if next_window_end <= current_window_end:
            return None

        logger.info(
            f"[{self.name}] Advancing incremental window: "
            f"{current_window_end} -> {next_window_start}..{next_window_end}"
        )
        return {
            'page': 1,
            '_ab_window_start': next_window_start,
            '_ab_window_end': next_window_end,
        }

    def _log_page_diagnostics(
        self,
        records: list[Mapping[str, Any]],
        page: int,
        window_start: int,
        window_end: int,
        recovery: bool,
    ) -> None:
        """Пишет в лог краткую диагностику диапазона значений на странице."""
        if not records:
            logger.info(
                f"[{self.name}] Page {page} window {window_start}..{window_end} is empty"
            )
            return

        ids = [record.get(self.primary_key) for record in records if record.get(self.primary_key) is not None]
        cursor_values = [record.get(self.cursor_field) for record in records if record.get(self.cursor_field) is not None]

        first_id = ids[0] if ids else None
        last_id = ids[-1] if ids else None
        min_cursor = min(cursor_values) if cursor_values else None
        max_cursor = max(cursor_values) if cursor_values else None

        logger.info(
            f"[{self.name}] Page {page} diagnostics: window={window_start}..{window_end}, "
            f"records={len(records)}, ids={first_id}->{last_id}, "
            f"{self.cursor_field}={min_cursor}->{max_cursor}, recovery={recovery}"
        )

    def next_page_token(self, response) -> Optional[Mapping[str, Any]]:
        """Пагинация для инкрементальных потоков с временными окнами и recovery."""
        if self._is_full_load_mode():
            return super().next_page_token(response)

        context = self._get_request_context(response)
        page = context['page']
        window_start = context['window_start']
        window_end = context['window_end']
        recovery = context['recovery']

        if response.status_code == 204:
            if page > 1 and not recovery:
                logger.warning(
                    f"[{self.name}] Received 204 on page {page} for window "
                    f"{window_start}..{window_end}; restarting the same window once"
                )
                return {
                    'page': 1,
                    '_ab_window_start': window_start,
                    '_ab_window_end': window_end,
                    '_ab_recovery': 1,
                }

            logger.info(f"[{self.name}] Received 204 - current window exhausted")
            return self._build_next_window_token(window_end)

        try:
            data = response.json()
        except Exception as exc:
            logger.warning(f"[{self.name}] Failed to parse JSON response: {exc}")
            return None

        if not data:
            logger.info(f"[{self.name}] Empty response body - stopping current window")
            return self._build_next_window_token(window_end)

        if not data.get('_embedded'):
            logger.info(
                f"[{self.name}] Empty _embedded (keys: {list(data.keys())}) - stopping current window"
            )
            return self._build_next_window_token(window_end)

        records = data.get('_embedded', {}).get(self.name, [])
        logger.info(f"[{self.name}] Got {len(records)} records on current page")
        self._log_page_diagnostics(records, page, window_start, window_end, recovery)

        next_link = data.get('_links', {}).get('next', {}).get('href')
        if next_link:
            try:
                parsed = urlparse(next_link)
                qs = parse_qs(parsed.query)
                if 'page' in qs:
                    next_page = int(qs['page'][0])
                    logger.info(f"[{self.name}] Moving to page {next_page} via _links.next")
                    return {
                        'page': next_page,
                        '_ab_window_start': window_start,
                        '_ab_window_end': window_end,
                        '_ab_recovery': int(recovery),
                    }

                logger.warning(
                    f"[{self.name}] _links.next found but no 'page' param in: {next_link}"
                )
            except Exception as exc:
                logger.warning(f"Pagination error: {exc}. Url: {response.request.url}")
                return None

        logger.info(f"[{self.name}] No _links.next found - current window completed")
        return self._build_next_window_token(window_end)
    
    def _build_incremental_params(
        self,
        start: int,
        page: int,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """Строит параметры для инкрементального режима"""
        params = {}
        window_start, window_end = self._get_window_bounds(start, next_page_token)
        if page == 1:
            logger.info(
                f"[{self.name}] INCREMENTAL MODE - "
                f"filter[{self.cursor_field}][from]={window_start}, "
                f"filter[{self.cursor_field}][to]={window_end}"
            )
        params[f'filter[{self.cursor_field}][from]'] = window_start
        params[f'filter[{self.cursor_field}][to]'] = window_end
        params[f'order[{self.cursor_field}]'] = 'asc'
        return params
    
    def _build_full_load_params(self, page: int) -> MutableMapping[str, Any]:
        """Строит параметры для режима полной загрузки"""
        params = {}
        if page == 1:
            logger.info(f"[{self.name}] FULL LOAD MODE - no date filter (ignoring all state)")
        params['order[id]'] = 'asc'
        return params
