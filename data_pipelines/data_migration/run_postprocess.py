from pathlib import Path
from data_pipelines.data_migration.clients import adapters  # noqa: F401
from data_pipelines.data_migration.clients.registry import get_client
from data_pipelines.data_migration.model import PostprocessConfig

BASE_DIR = Path(__file__).parent.parent.parent.resolve()

def run_postprocess(postprocess):
    postprocess = PostprocessConfig.model_validate(postprocess)
    client = get_client(postprocess.engine, postprocess)
    sql_path = BASE_DIR / postprocess.sql_file_path
    sql = sql_path.read_text(encoding="utf-8")
    client.execute_sql(sql)