# Читает конфиг-файлы, валидирует по пайдантик модели, сохраняет в mssql
import yaml
from typing import Any
from pathlib import Path
from pydantic import TypeAdapter, ValidationError
from data_pipelines.data_migration import model
from clients.mssql_pyodbc import MSSQL_PYODBC
from utils.slugify_dag_id import slugify_dag_id
from utils.logger_config import get_logger
logger = get_logger()


def _save_config(cursor, name: str, data: Any, adapter: Any):
    json_bytes = TypeAdapter(adapter).dump_json(data, exclude_none=True)
    json_str = json_bytes.decode("utf-8")

    cursor.execute("""
        MERGE Configs_Registry AS target
        USING (VALUES (?, ?)) AS source (config_name, config_json)
        ON target.config_name = source.config_name
        WHEN MATCHED THEN UPDATE SET config_json = source.config_json, updated_at = GETDATE()
        WHEN NOT MATCHED THEN INSERT (config_name, config_json) VALUES (source.config_name, source.config_json);
    """, (name, json_str))

    logger.info(f"{name} сохранено в MsSql")


def _parse_yaml_doc(doc: dict[str, Any]) -> tuple[model.PipelineConfig, model.CacheDagConfig]:
    raw_source: dict[str, Any] = doc.get("source")
    raw_preprocess: str = doc.get("preprocess")
    raw_target: dict[str, Any] = doc.get("target")
    raw_postprocess: str = doc.get("postprocess")
    raw_dag: dict[str, Any] = doc.get("dag")

    # Сборка source
    if raw_source.get("sheet_name") is None:
        raw_source["sheet_name"] = 0

    # Сборка postprocess
    if raw_postprocess:
        postprocess = {
            **raw_postprocess,
            "engine": raw_target.get("engine"),
            "conn_id": raw_target.get("conn_id"),
            "database": raw_target.get("database")
        }
    else:
        postprocess = None

    # Сборка dag
    raw_dag_id = raw_dag.get("dag_id")
    table = raw_target.get("table") or ""

    dag_id = f"s2t_{slugify_dag_id(raw_dag_id)}" if raw_dag_id else f"s2t_{slugify_dag_id(table)}" 
    description = raw_dag.get("description") or raw_dag_id or table

    dag = {
        **raw_dag,
        "dag_id": dag_id,
        "description": description,
    }

    # Сборка моделей
    pipeline_cfg = model.PipelineConfig.model_validate({
        "source": raw_source, 
        "target": raw_target,
        "preprocess": raw_preprocess
    })

    cache_dag_cfg = model.CacheDagConfig.model_validate({
        "dag": dag,
        "postprocess": postprocess
    })

    return pipeline_cfg, cache_dag_cfg


def state_sync():
    base_dir = Path(__file__).resolve().parent.parent.parent / "configs" / "data_migrations"
    pipelines: model.PipelinesMap = {}
    cachedags: model.CacheDagsMap = {}
    errors: list[str] = []

    yaml_files = list(base_dir.glob("*.yaml"))
    for yaml_file in yaml_files:
        yaml_file_name = yaml_file.stem
        if yaml_file_name == "_example": continue
        doc = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        if not doc:
            logger.warning(f"Пустой файл: {yaml_file.name}")
            continue

        try:
            pipeline_cfg, cache_dag_cfg = _parse_yaml_doc(doc)
        except (ValidationError, ValueError) as e:
            error_msg = f"{yaml_file_name}: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            continue

        pipelines[yaml_file_name] = pipeline_cfg
        cachedags[yaml_file_name] = cache_dag_cfg

    if errors:
        failed_count = len(errors)
        total_count = len(yaml_files)
        raise RuntimeError(
            f"Валидация провалена для {failed_count}/{total_count} конфигов:\n"
            + "\n".join(errors)
        )
    
    state = model.SyncState(pipelines=pipelines, cachedags=cachedags)

    mssql = MSSQL_PYODBC()
    with mssql.get_cursor() as cursor:
        _save_config(cursor, "data_migration", state.pipelines, model.PipelinesMap)

    # Возвращаем кэш дагов для постройки дагов фабрикой
    return state.cachedags


if __name__ == "__main__":
    state = state_sync()