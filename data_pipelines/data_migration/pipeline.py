# Выполняет один пайплайн по переносу данных из справочника в mssql
import importlib
import pandas as pd 
from pydantic import TypeAdapter
from data_pipelines.data_migration import model
from clients.mssql_pyodbc import MSSQL_PYODBC
from data_pipelines.data_migration.clients import adapters  # noqa: F401
from data_pipelines.data_migration.clients.registry import get_client
from utils.logger_config import get_logger
logger = get_logger()


def _load_config(name: str, adapter_type: type, cursor):
    cursor.execute("""
        SELECT config_json
        FROM Configs_Registry
        WHERE config_name = ?
    """, (name,))

    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Config '{name}' not found in Configs_Registry")

    return TypeAdapter(adapter_type).validate_json(row[0])


def _fetch_df(source):
    client = get_client(source.engine, source)
    return client.read()


def _preprocess_df(preprocess, df: pd.DataFrame) -> pd.DataFrame:
    module = importlib.import_module(preprocess.module)
    return module.transform(df)


def _write_data(target, df: pd.DataFrame) -> None:
    client = get_client(target.engine, target)
    client.write(df)


def run(yaml_file_name: str):
    mssql = MSSQL_PYODBC()
    with mssql.get_cursor() as cursor:
        pipelines = _load_config("data_migration", model.PipelinesMap, cursor)

    pipeline = pipelines.get(yaml_file_name)
    if pipeline is None:
        raise ValueError(f"Конфигураций к файлу '{yaml_file_name}' не найдено в Configs_Registry.data_migrations")

    source = pipeline.source
    preprocess = pipeline.preprocess
    target = pipeline.target

    df = _fetch_df(source)
    logger.info(f"[OK] DataFrame built")

    if preprocess:
        df = _preprocess_df(preprocess=preprocess, df=df)
        logger.info(f"[OK] DataFrame preprocessed")

    _write_data(target, df)
    logger.info(f"[OK] DataFrame written")

if __name__ == "__main__":
    yaml_file_name = input("\nВведите название конфигурационного .yaml файла для тестового выполнения: ").strip()    
    run(yaml_file_name)
