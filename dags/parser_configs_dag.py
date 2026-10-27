from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="1,16,31,46 * * * *",
    catchup=False,
    default_args={"owner": "shayakhmetov_dr"},
    tags=["factory", "transfer", "s2t"],
)
def s2t_parser_configs_dag():
    @task
    def state_sync_task():
        from data_pipelines.data_migration.parser_configs import state_sync
        from utils.logger_config import get_logger
        logger = get_logger()
        cache_dags = state_sync()

        dag_configs = {
            dag_id: dag_config.model_dump() for dag_id, dag_config in cache_dags.items()
        }
        variable_name = "data_migration_factory_cache_dags"
        Variable.set(
            key=variable_name,
            value=dag_configs,
            serialize_json=True
        )
        logger.info(f"[OK] Variable '{variable_name}' updated: {len(dag_configs)} DAGs")

    state_sync_task()

s2t_parser_configs_dag()
