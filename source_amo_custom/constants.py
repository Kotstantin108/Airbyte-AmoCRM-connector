"""Константы для AmoCRM коннектора"""

# Порог для режима полной загрузки (1 января 2015)
FULL_LOAD_THRESHOLD = 1420070400

# Лимиты AmoCRM API
MAX_RECORDS_PER_PAGE = 250
RATE_LIMIT_DELAY_SECONDS = 0.1

# Overlap window (секунды) — перезагружаем последние N секунд
# чтобы не потерять записи из-за eventual consistency API amoCRM
OVERLAP_SECONDS = 60

# Retry backoff times (секунды)
BACKOFF_RATE_LIMIT = 10.0
BACKOFF_SERVER_ERROR = 20.0
BACKOFF_UNAUTHORIZED = 1.0
