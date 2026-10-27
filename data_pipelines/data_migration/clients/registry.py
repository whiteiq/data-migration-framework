from typing import Type
from data_pipelines.data_migration.clients.base import BaseDataClient

_REGISTRY: dict[str, Type["BaseDataClient"]] = {}


def register(engine: str):
    """Декоратор для регистрации клиента по engine."""
    def decorator(cls: Type["BaseDataClient"]) -> Type["BaseDataClient"]:
        _REGISTRY[engine] = cls
        return cls
    return decorator


def get_client_cls(engine: str) -> Type["BaseDataClient"]:
    try:
        return _REGISTRY[engine]
    except KeyError:
        raise ValueError(
            f"Engine '{engine}' не зарегистрирован. "
            f"Доступные: {list(_REGISTRY)}"
        )


def get_client(engine: str, config) -> "BaseDataClient":
    """Создаёт инстанс клиента по engine и pydantic-конфигу."""
    cls = get_client_cls(engine)
    return cls(config)