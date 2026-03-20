"""Константы для AmoCRM коннектора"""

# Порог для режима полной загрузки: 0 = не задана дата (полная загрузка)
# Любое конкретное значение start_date (включая 1420070400) → инкрементальный режим
FULL_LOAD_THRESHOLD = 0

# Лимиты AmoCRM API
MAX_RECORDS_PER_PAGE = 250
RATE_LIMIT_DELAY_SECONDS = 0.1

# Overlap window (секунды) — перезагружаем последние N секунд
# чтобы не потерять записи из-за eventual consistency API amoCRM
OVERLAP_SECONDS = 600

# Размер окна инкрементальной синхронизации (секунды).
# Разбиваем длинный диапазон на небольшие окна, чтобы снизить риск
# пропусков из-за нестабильной page-based пагинации в AmoCRM API.
INCREMENTAL_WINDOW_SECONDS = 3600

# Retry backoff times (секунды)
BACKOFF_RATE_LIMIT = 10.0
BACKOFF_SERVER_ERROR = 20.0
BACKOFF_UNAUTHORIZED = 1.0
