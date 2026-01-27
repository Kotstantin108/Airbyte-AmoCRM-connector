"""JSON схемы для потоков данных"""

from typing import Mapping, Any


def get_custom_field_schema() -> Mapping[str, Any]:
    """Общая схема для кастомных полей"""
    return {
        "type": ["array", "null"],
        "items": {
            "type": "object",
            "properties": {
                "field_id": {"type": "integer"},
                "field_name": {"type": ["string", "null"]},
                "field_code": {"type": ["string", "null"]},
                "field_type": {"type": ["string", "null"]},
                "values": {"type": ["array", "null"]}
            }
        }
    }


def get_base_schema() -> Mapping[str, Any]:
    """Базовая схема для всех объектов"""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object"
    }


def get_leads_schema() -> Mapping[str, Any]:
    """Схема для потока leads"""
    schema = get_base_schema()
    schema["properties"] = {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "price": {"type": ["integer", "null"]},
        "responsible_user_id": {"type": ["integer", "null"]},
        "group_id": {"type": ["integer", "null"]},
        "status_id": {"type": ["integer", "null"]},
        "pipeline_id": {"type": ["integer", "null"]},
        "loss_reason_id": {"type": ["integer", "null"]},
        "created_by": {"type": ["integer", "null"]},
        "updated_by": {"type": ["integer", "null"]},
        "created_at": {"type": "integer"},
        "updated_at": {"type": "integer"},
        "closed_at": {"type": ["integer", "null"]},
        "closest_task_at": {"type": ["integer", "null"]},
        "is_deleted": {"type": ["boolean", "null"]},
        "score": {"type": ["integer", "null"]},
        "account_id": {"type": ["integer", "null"]},
        "custom_fields_values": get_custom_field_schema(),
        "_embedded": {"type": ["object", "null"]}
    }
    return schema


def get_contacts_schema() -> Mapping[str, Any]:
    """Схема для потока contacts"""
    schema = get_base_schema()
    schema["properties"] = {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "first_name": {"type": ["string", "null"]},
        "last_name": {"type": ["string", "null"]},
        "responsible_user_id": {"type": ["integer", "null"]},
        "group_id": {"type": ["integer", "null"]},
        "created_by": {"type": ["integer", "null"]},
        "updated_by": {"type": ["integer", "null"]},
        "created_at": {"type": "integer"},
        "updated_at": {"type": "integer"},
        "closest_task_at": {"type": ["integer", "null"]},
        "is_deleted": {"type": ["boolean", "null"]},
        "account_id": {"type": ["integer", "null"]},
        "custom_fields_values": get_custom_field_schema(),
        "_embedded": {"type": ["object", "null"]}
    }
    return schema


def get_events_schema() -> Mapping[str, Any]:
    """Схема для потока events"""
    schema = get_base_schema()
    schema["properties"] = {
        "id": {"type": "string"},
        "type": {"type": ["string", "null"]},
        "entity_id": {"type": ["integer", "null"]},
        "entity_type": {"type": ["string", "null"]},
        "created_by": {"type": ["integer", "null"]},
        "created_at": {"type": "integer"},
        "value_after": {"type": ["array", "object", "string", "null"]},
        "value_before": {"type": ["array", "object", "string", "null"]},
        "account_id": {"type": ["integer", "null"]}
    }
    return schema


def get_pipelines_schema() -> Mapping[str, Any]:
    """Схема для потока pipelines"""
    schema = get_base_schema()
    schema["properties"] = {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "sort": {"type": ["integer", "null"]},
        "is_main": {"type": ["boolean", "null"]},
        "is_unsorted_on": {"type": ["boolean", "null"]},
        "is_archive": {"type": ["boolean", "null"]},
        "account_id": {"type": ["integer", "null"]},
        "_embedded": {
            "type": ["object", "null"],
            "properties": {
                "statuses": {
                    "type": ["array", "null"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": ["string", "null"]},
                            "sort": {"type": ["integer", "null"]},
                            "is_editable": {"type": ["boolean", "null"]},
                            "pipeline_id": {"type": ["integer", "null"]},
                            "color": {"type": ["string", "null"]},
                            "type": {"type": ["integer", "null"]},
                            "account_id": {"type": ["integer", "null"]}
                        }
                    }
                }
            }
        }
    }
    return schema


def get_custom_fields_schema() -> Mapping[str, Any]:
    """Схема для кастомных полей (leads/contacts)"""
    schema = get_base_schema()
    schema["properties"] = {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "code": {"type": ["string", "null"]},
        "sort": {"type": ["integer", "null"]},
        "type": {"type": ["string", "null"]},
        "entity_type": {"type": ["string", "null"]},
        "is_predefined": {"type": ["boolean", "null"]},
        "is_deletable": {"type": ["boolean", "null"]},
        "is_visible": {"type": ["boolean", "null"]},
        "is_required": {"type": ["boolean", "null"]},
        "group_id": {"type": ["string", "null"]},
        "required_statuses": {"type": ["array", "null"]},
        "enums": {
            "type": ["array", "null"],
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "value": {"type": ["string", "null"]},
                    "sort": {"type": ["integer", "null"]}
                }
            }
        },
        "account_id": {"type": ["integer", "null"]}
    }
    return schema


def get_users_schema() -> Mapping[str, Any]:
    """Схема для потока users"""
    schema = get_base_schema()
    schema["properties"] = {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "email": {"type": ["string", "null"]},
        "lang": {"type": ["string", "null"]},
        "rights": {"type": ["object", "null"]},
        "_embedded": {"type": ["object", "null"]}
    }
    return schema
