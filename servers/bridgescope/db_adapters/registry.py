from typing import Dict, Type, List
from db_adapters.base_adapter import BaseAdapter
from db_adapters.db_config import DBConfig
from db_adapters.db_constants import DB_ENUM


# A global registry that maps database types to corresponding adapter classes.
_ADAPTER_REGISTRY: Dict[DB_ENUM, Type[BaseAdapter]] = {}


def register(adapter_type: DB_ENUM):
    """
    Decorator function to register a new database adapter class.

    :param adapter_type: The type of the database (e.g., PostgreSQL, MySQL), as an enum value of DB_TYPE_ENUM
    :return: A decorator that registers the class in the _ADAPTER_REGISTRY.
    """

    def decorator(cls):
        _ADAPTER_REGISTRY[adapter_type] = cls
        return cls

    return decorator


def get_adapter_instance(db_config: DBConfig) -> BaseAdapter:
    """
    Returns an instance of the appropriate database adapter based on db_config.

    :param db_config: DBConfig instance for the database connection.
    :return: An instance of the registered adapter class.
    """
    try:
        adapter_type = DB_ENUM(db_config.db_type)
    except ValueError:
        raise ValueError(
            f"Invalid database type: {db_config.db_type}. Supported types are: {[e for e in DB_ENUM]}"
        )

    if adapter_type not in _ADAPTER_REGISTRY:
        raise ValueError(f"No adapter found for {adapter_type}")

    return _ADAPTER_REGISTRY[adapter_type](db_config)


def list_adapters() -> List[DB_ENUM]:
    """
    Lists all registered database types with available adapters.

    :return: A list of supported database types.
    """
    return list(_ADAPTER_REGISTRY.keys())
