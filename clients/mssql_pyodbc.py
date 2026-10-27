from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
import pyodbc
import pandas as pd
from clients.mssql_base import MSSQL_BASE
from utils.logger_config import get_logger
logger = get_logger()

pyodbc.pooling = True 


@dataclass
class MSSQL_PYODBC(MSSQL_BASE):
    """Клиент для работы с MSSQL через pyodbc.

    Параметр `is_local_client` автоматически определяет среду выполнения
    (локальный ПК или Airflow). При необходимости значение можно задать вручную.

    Параметры подключения (`database`, `host`, `port`, `user`, `password`, `driver`)
    определяются автоматически на основе локального окружения или идентификатора
    подключения Airflow (`conn_id`), но также могут быть указаны явно.

    Методы можно вызывать самостоятельно - клиент сам откроет и закроет соединение.
    Или внутри блока `with mssql.get_cursor() as cursor:` — тогда они
    переиспользуют активный курсор, объединяя операции в одну транзакцию.

    Args:
        conn_id: Идентификатор подключения в Airflow (Admin -> Connections).
        is_local_client: Флаг локального режима (True) или Airflow (False).
        database: Имя базы данных.
        host: Адрес сервера.
        port: Порт сервера.
        user: Имя пользователя.
        password: Пароль пользователя.
        driver: ODBC-драйвер для подключения.

    Example:
        ### Автономные вызовы — клиент сам управляет жизнью соединения:
        >>> mssql = MSSQL_PYODBC()
        >>> rows = mssql.select_all(table="MyTable")
        >>> mssql.insert(table="MyTable", df=Pandas.DataFrame)

        ### Внутри with — методы переиспользуют открытый курсор и транзакцию:
        >>> with mssql.get_cursor() as cursor:
        ...     cursor.execute("SELECT 1")
        ...     mssql.insert(table="MyTable", df=Pandas.DataFrame)
        ...     rows = mssql.select_all(table="MyTable")
    """
    def _after_post_init(self) -> None:
        self.conn_str = self._build_conn_str()
        self._active_cursor = None

    def _build_conn_str(self) -> str:
        return (
            f"DRIVER={self.driver};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
            "MARS_Connection=Yes;"
        )
            
    @contextmanager
    def get_cursor(self):
        """Возвращает курсор внутри контекстного менеджера.

        При успешном завершении блока `with` выполняется commit.
        При ошибке — rollback. Вложенные вызовы запрещены.

        Yields:
            pyodbc.Cursor: Активный курсор соединения.

        Raises:
            RuntimeError: При попытке открыть вложенный курсор.
        """
        if self._active_cursor is not None:
            raise RuntimeError(
                "Вложенный get_cursor() не поддерживается" \
                "Используйте методы клиента / .execute внутри блока with"
            )

        conn = pyodbc.connect(self.conn_str, autocommit=False)
        cursor = conn.cursor()
        self._active_cursor = cursor
        
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
            
            self._active_cursor = None

    @contextmanager
    def _execution_scope(self):
        if self._active_cursor is not None:
            yield self._active_cursor
        else:
            with self.get_cursor() as cursor:
                yield cursor

    def select_all(self, table: str, schema: str = "dbo") -> tuple[list[str], list[tuple[Any, ...]]]:
        """Возвращает названия столбцов и все строки указанной таблицы.

        Args:
            table: Имя таблицы.
            schema: Имя схемы (по умолчанию 'dbo').

        Returns:
            Кортеж: (список имён столбцов, кортеж строк с данными).
        """
        query = f"SELECT * FROM {schema}.{table}"

        with self._execution_scope() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            logger.info(f"[OK] Received {len(rows)} rows from {schema}.{table}")
            return columns, rows
        
    def select_all_to_df(self, table: str, schema: str = "dbo") -> pd.DataFrame:
        """Возвращает всё содержимое таблицы в виде pandas DataFrame.

        Args:
            table: Имя таблицы.
            schema: Имя схемы (по умолчанию 'dbo').

        Returns:
            DataFrame с данными таблицы и именованными столбцами.
        """
        query = f"SELECT * FROM {schema}.{table}"

        with self._execution_scope() as cursor:
            df = pd.read_sql(query, cursor.connection)
            logger.info(f"[OK] DataFrame received from {schema}.{table}")
            return df

    def insert_df(
        self,
        table: str,
        df,
        schema: str = "dbo",
        create_if_missing: bool = False,
    ) -> None:
        """Вставляет данные из DataFrame в указанную таблицу.

        Пустой DataFrame игнорируется - вставка не выполняется.
        Используется fast_executemany для ускорения массовой загрузки.

        Args:
            table: Имя целевой таблицы.
            df: DataFrame с данными для вставки. Имена столбцов должны
                совпадать с таблицей (регистронезависимо).
            schema: Имя схемы (по умолчанию 'dbo').
        """
        if df.empty:
            return

        self._ensure_table_exists(table, df, schema, create_if_missing)

        columns = list(df.columns)
        column_names = ", ".join(f"[{column}]" for column in columns)
        placeholder = ", ".join("?" for _ in columns)

        query = f"""
            INSERT INTO {schema}.{table} ({column_names})
            VALUES ({placeholder})
        """

        data = list(df.itertuples(index=False, name=None))

        with self._execution_scope() as cursor:
            cursor.fast_executemany = True
            cursor.executemany(query, data)
            logger.info(f"[OK] DataFrame is inserted to {schema}.{table}")
            


    def overwrite_df(
        self,
        table: str,
        df,
        schema: str = "dbo",
        create_if_missing: bool = False,
    ) -> None:
        """Полностью заменяет содержимое таблицы данными из DataFrame.

        Выполняет `TRUNCATE TABLE`, затем массовую вставку.
        Если таблица отсутствует и `create_if_missing=True` - создаёт её.

        Args:
            table: Имя целевой таблицы.
            df: DataFrame с данными.
            schema: Имя схемы (по умолчанию 'dbo').
            create_if_missing: Создать таблицу, если она не существует.
        """
        if df.empty:
            return

        self._ensure_table_exists(table, df, schema, create_if_missing)

        columns = list(df.columns)
        column_names = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        data = list(df.itertuples(index=False, name=None))

        with self._execution_scope() as cursor:
            cursor.execute(f"TRUNCATE TABLE [{schema}].[{table}]")
            cursor.fast_executemany = True
            query = f"""
                INSERT INTO [{schema}].[{table}] ({column_names})
                VALUES ({placeholders})
            """
            cursor.executemany(query, data)
            logger.info(f"[OK] {schema}.{table} is owerwrited via dataframe")

    def _ensure_table_exists(
        self,
        table: str,
        df,
        schema: str,
        create_if_missing: bool,
    ) -> None:
        """Обеспечивает существование таблицы"""
        if self._table_exists(table, schema):
            return

        if create_if_missing:
            self._create_table_from_df(table, df, schema)
        else:
            raise RuntimeError(
                f"Таблица [{schema}].[{table}] не существует. "
                f"Передайте create_if_missing=True для автоматического создания."
            )

    def _table_exists(
        self,
        table: str,
        schema: str
    ) -> bool:
        """Проверяет существование таблицы"""

        query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        with self._execution_scope() as cursor:
            cursor.execute(query, (schema, table))
            return cursor.fetchone()[0] > 0

    def _create_table_from_df(
        self,
        table: str,
        df,
        schema: str
    ) -> None:
        """Генерирует и выполняет CREATE TABLE на основе dtypes DataFrame."""
        type_map = {
            "int64": "BIGINT",
            "int32": "INT",
            "Int64": "BIGINT",
            "Int32": "INT",
            "float64": "FLOAT",
            "float32": "REAL",
            "bool": "BIT",
            "boolean": "BIT",
            "datetime64[ns]": "DATETIME2",
            "datetime64[ns, UTC]": "DATETIME2",
            "timedelta64[ns]": "BIGINT",
            "category": "NVARCHAR(MAX)",
        }

        col_defs = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_map.get(str(dtype), "NVARCHAR(MAX)")
            col_defs.append(f"    [{col_name}] {sql_type}")

        ddl = (
            f"CREATE TABLE [{schema}].[{table}] (\n"
            + ",\n".join(col_defs)
            + "\n)"
        )

        with self._execution_scope() as cursor:
            cursor.execute(ddl)
            logger.info(f"[OK] {schema}.{table} is created via dataframe")

    def upsert(self):
        """Нереализованный метод"""
        pass