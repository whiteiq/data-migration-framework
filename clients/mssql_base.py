from os import getenv
from abc import ABC
from dataclasses import dataclass


@dataclass
class MSSQL_BASE(ABC):
    """Класс для MSSQL-подключений
    
    Args:
        conn_id: airlfow connection id
        is_local_client: flag for local/airflow use
        database: server database for connect
        host: server host for connect
        port: server port for connect
        user: server user for server authentication
        password: server user for server authentication
        driver: use driver for work
    """

    conn_id: str | None = None
    is_local_client: bool | None = None
    database: str | None = None
    host: str | None = None
    port: str | None = None
    user: str | None = None
    password: str | None = None
    driver: str | None = None

    def __post_init__(self):
        self.conn_id = self.conn_id or "mssql_default"
        self.is_local_client = self._detect_local(self.is_local_client)
        self._config = self._get_config()

        self.database = self.database or self._config.get("database") or "Report"
        self.host = self.host or self._config["host"]
        self.port = self.port or self._config.get("port") or 1433
        self.user = self.user or self._config["user"]
        self.password = self.password or self._config["password"]
        self.driver = self.driver or self._config.get("driver")

        self._after_post_init()

    # Используем доп. метод инициализации
    # Если идти в начало, то эта логика по сути только ради корректной типизации наследников
    def _after_post_init(self) -> None:
        pass

    def _detect_local(self, is_local_client: bool) -> bool:
        """True - локальный запуск, False - Airflow, None - определяем автоматически."""
        if is_local_client is not None:
            return is_local_client
        is_airflow_home = getenv("AIRFLOW_HOME") or getenv("AIRFLOW__CORE__DAGS_FOLDER")
        return not is_airflow_home


    def _get_config(self) -> dict[str, str]:
        if self.is_local_client:
            return self._load_local_config()
        return self._load_airflow_config()

    def _load_airflow_config(self) -> dict[str, str]:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(self.conn_id)
        return {
            "host": conn.host,
            "port": conn.port,
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
            "driver": "ODBC Driver 18 for SQL Server",
        }

    def _load_local_config(self) -> dict[str, str]:
        from dotenv import load_dotenv
        load_dotenv()

        return {
            "host": getenv("GENDALF_HOST"),
            "port": getenv("GENDALF_PORT"),
            "user": getenv("GENDALF_USER"),
            "password": getenv("GENDALF_PASSWORD"),
            "driver": "ODBC Driver 17 for SQL Server",
        }