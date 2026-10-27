from abc import ABC, abstractmethod
from typing import Iterator
import pandas as pd
from pydantic import BaseModel


class BaseDataClient(ABC):
    """Фасад над любым источником/приёмником данных."""

    def __init__(self, config: "BaseModel"):
        self._config = config

    @abstractmethod
    def read(self) -> pd.DataFrame:
        """Полное чтение в DataFrame (для nextcloud / маленьких таблиц)."""

    @abstractmethod
    def write(self, df: pd.DataFrame) -> None:
        """Полная перезапись (overwrite)."""

    def execute_sql(self, sql: str) -> None:
        raise NotImplementedError(
            f"{self.__class__.__name__} не поддерживает execute_sql"
        )