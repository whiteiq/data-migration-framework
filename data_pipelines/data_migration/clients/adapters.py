import pandas as pd
from io import BytesIO

from clients.mssql_pyodbc import MSSQL_PYODBC
from clients.nextcloud import NEXTCLOUD

from data_pipelines.data_migration.clients.base import BaseDataClient
from data_pipelines.data_migration.clients.registry import register


@register("mssql")
class MSSQLDataClient(BaseDataClient):
    """Фасад MSSQL_PYODBC под единый контракт."""

    def __init__(self, config):
        super().__init__(config)
        self._impl = MSSQL_PYODBC(
            conn_id=config.conn_id,
            database=config.database,
        )

    def read(self) -> pd.DataFrame:
        kwargs = {"table": self._config.table}
        if self._config.schema_name:
            kwargs["schema"] = self._config.schema_name
        return self._impl.select_all_to_df(**kwargs)

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        kwargs = {
            "table": self._config.table,
            "df": df,
            "create_if_missing": True,
        }
        if self._config.schema_name:
            kwargs["schema"] = self._config.schema_name
        self._impl.overwrite_df(**kwargs)

    def execute_sql(self, sql: str) -> None:
        with self._impl.get_cursor() as cursor:
            cursor.execute(sql)


@register("nextcloud")
class NextcloudDataClient(BaseDataClient):
    def __init__(self, config):
        super().__init__(config)
        self._impl = NEXTCLOUD(conn_id=config.conn_id)

    def read(self) -> pd.DataFrame:
        file_buffer = self._impl.download_file(self._config.file_path)
        sheet_name = self._config.sheet_name or 0

        df = pd.read_excel(
            file_buffer,
            sheet_name=sheet_name,
            engine="openpyxl",
            dtype=str,
        ).fillna("")

        df = df.replace(r"^\s*$", None, regex=True)

        # Удаляем полностью пустые столбцы (ни данных, ни заголовка)
        # Столбцы без данных, но с заголовком останутся
        cols_to_drop = [
            col for col in df.columns
            if str(col).lower().startswith("unnamed") and df[col].isna().all()
        ]
        df = df.drop(columns=cols_to_drop)

        # Удаляем пустые строки
        df = df.dropna(how="all")

        # Чистим названия колонок
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w\d]", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )
        return df

    def write(self, df: pd.DataFrame) -> None:
        buffer = BytesIO()
        df.to_excel(
            buffer,
            sheet_name=self._config.sheet_name or "Sheet1",
            engine="openpyxl",
            index=False,
        )
        buffer.seek(0)
        self._impl.upload_file(buffer=buffer, path=self._config.file_path)