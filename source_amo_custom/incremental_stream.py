"""Базовый класс для инкрементальных потоков"""

import logging
from typing import Any, Mapping, MutableMapping, Optional

from .base_stream import AmoStream
from .constants import FULL_LOAD_THRESHOLD, OVERLAP_SECONDS

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
    
    def _build_incremental_params(self, start: int, page: int) -> MutableMapping[str, Any]:
        """Строит параметры для инкрементального режима"""
        params = {}
        if page == 1:
            logger.info(f"[{self.name}] INCREMENTAL MODE - filter[{self.cursor_field}][from]={start}")
        params[f'filter[{self.cursor_field}][from]'] = start
        params[f'order[{self.cursor_field}]'] = 'asc'
        return params
    
    def _build_full_load_params(self, page: int) -> MutableMapping[str, Any]:
        """Строит параметры для режима полной загрузки"""
        params = {}
        if page == 1:
            logger.info(f"[{self.name}] FULL LOAD MODE - no date filter (ignoring all state)")
        params['order[id]'] = 'asc'
        return params
