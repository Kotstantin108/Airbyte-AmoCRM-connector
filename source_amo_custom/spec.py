"""Спецификация коннектора для Airbyte UI"""

SPEC_DICT = {
    "documentationUrl": "https://www.amocrm.ru/developers/",
    "connectionSpecification": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": [
            "domain", "client_id", "client_secret",
            "db_host", "db_port", "db_name", "db_user", "db_password"
        ],
        "properties": {
            "domain": {
                "type": "string",
                "title": "AmoCRM Subdomain",
                "description": "Субдомен вашего AmoCRM (например, 'mycompany' для mycompany.amocrm.ru)",
                "order": 0
            },
            "client_id": {
                "type": "string",
                "title": "Client ID",
                "description": "ID приложения из настроек интеграции AmoCRM",
                "order": 1
            },
            "client_secret": {
                "type": "string",
                "title": "Client Secret",
                "description": "Секретный ключ приложения",
                "airbyte_secret": True,
                "order": 2
            },
            "db_host": {
                "type": "string",
                "title": "Postgres Host",
                "description": "Хост PostgreSQL с таблицей токенов",
                "order": 3
            },
            "db_port": {
                "type": "integer",
                "title": "Postgres Port",
                "default": 5433,
                "order": 4
            },
            "db_name": {
                "type": "string",
                "title": "DB Name",
                "description": "Имя базы данных",
                "order": 5
            },
            "db_user": {
                "type": "string",
                "title": "DB User",
                "description": "Пользователь БД",
                "order": 6
            },
            "db_password": {
                "type": "string",
                "title": "DB Password",
                "description": "Пароль БД",
                "airbyte_secret": True,
                "order": 7
            },
            "start_date": {
                "type": "integer",
                "title": "Start Date (Unix)",
                "description": "Дата начала синхронизации в Unix timestamp (1420070400 = 1 января 2015)",
                "default": 1420070400,
                "order": 8
            }
        }
    }
}
