from pydantic import BaseModel, Field, field_validator, model_validator
from typing import TypeAlias


LITERAL_ENGINE_TO_FIELD = {
    "mssql": "table",
    "nextcloud": "file_path"
}
LITERAL_ENGINE_TO_FIELD_FOR_POSTPROCESS = {
    "mssql": "sql_file_path"
}
DEFAULT_TAGS = ["factory", "transfer", "s2t"]


class BaseLocation(BaseModel):
    engine: str

    file_path: str | None = None
    sheet_name: str | int | None = None

    conn_id: str | None = None
    database: str | None = None
    schema_name: str | None = None
    table: str | None = None

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, value: str) -> str:
        cleaned_value = value.lower()
        allowed = tuple(LITERAL_ENGINE_TO_FIELD)
        if cleaned_value not in allowed:
            raise ValueError(
                f"Недопустимый engine '{value}'. "
                f"Разрешены только: {', '.join(allowed)}"
            )
        return cleaned_value

    @model_validator(mode="after")
    def validate_engine_dependencies(self) -> "BaseLocation":
        for engine, field in LITERAL_ENGINE_TO_FIELD.items():
            if self.engine == engine:
                if not getattr(self, field, None):
                    raise ValueError(
                        f"Для engine={engine} обязательно должно быть заполнено поле {field}"
                    )
        return self


# При необходимости добавим доп параметры в дочерний класс
# Например usecols \ доп.фильтр и пр.
class SourceConfig(BaseLocation):
    """Конфигурация источника данных."""
    pass


class TargetConfig(BaseLocation):
    """Конфигурация приёмника данных."""
    pass


class PreprocessConfig(BaseModel):
    module: str


class PostprocessConfig(BaseModel):
    engine: str
    conn_id: str | None = None
    database: str | None = None
    sql_file_path: str | None = None
    @field_validator("engine")
    @classmethod
    def validate_engine(cls, value: str) -> str:
        cleaned_value = value.lower()
        allowed = tuple(LITERAL_ENGINE_TO_FIELD_FOR_POSTPROCESS)
        if cleaned_value not in allowed:
            raise ValueError(f"Недопустимый engine '{value}'. Разрешены только: {', '.join(allowed)}")
            
        return cleaned_value

    @model_validator(mode="after")
    def validate_engine_dependencies(self) -> "PostprocessConfig":
        for engine, field in LITERAL_ENGINE_TO_FIELD_FOR_POSTPROCESS.items():
            if self.engine == engine:
                if not getattr(self, field, None):
                    raise ValueError(f"Для engine={engine} обязательно должно быть заполнено поле {field}")
                
        return self
    

class DagConfig(BaseModel):
    dag_id: str
    active: bool = True
    schedule: str = "0 0 * * *"
    owner: str = "airflow"
    tags: list[str] = Field(default_factory=list)
    description: str

    # Валидация для None, [], [None] и прочих значений
    @field_validator("tags", mode="before")
    @classmethod
    def clean_and_default_tags(cls, v):
        default_tags = DEFAULT_TAGS
        if not v: 
            return default_tags
        cleaned_tags = [item for item in v if item not in (None, "")]
        return cleaned_tags or default_tags

    # Валидация для полу-корректных значений
    @field_validator("active", mode="before")
    @classmethod
    def validate_active(cls, v):
        if v is None or v == "":
            return True
            
        if isinstance(v, str):
            if v.lower() in ("true", "yes", "1", "on"):
                return True
            if v.lower() in ("false", "no", "0", "off"):
                return False
            
        return v


class PipelineConfig(BaseModel):
    source: SourceConfig
    target: TargetConfig
    preprocess: PreprocessConfig | None = None

    @model_validator(mode="after")
    def validate_preprocess_for_memory_engines(self) -> "PipelineConfig":
        """
        Preprocess работает с DataFrame целиком в памяти.
        Если и источник, и приёмник — СУБД, полной материализации в pandas
        не происходит (или будет чанковая передача), поэтому preprocess
        запрещён — иначе пользователь получит скрытую ошибку логики.
        """
        if self.preprocess is None:
            return self

        memory_engines = {"nextcloud"}   # расширим при добавлении CSV, S3 и т.п.
        src_is_db = self.source.engine not in memory_engines
        tgt_is_db = self.target.engine not in memory_engines

        if src_is_db and tgt_is_db:
            raise ValueError(
                "preprocess может использоваться только когда "
                "источником или приёмником является файловое хранилище "
                "(например nextcloud), требующее полной выгрузки данных в память. "
                "При трансфере БД→БД используйте postprocess на стороне target."
            )
        return self
    
PipelinesMap: TypeAlias = dict[str, PipelineConfig]


class CacheDagConfig(BaseModel):
    dag: DagConfig
    postprocess: PostprocessConfig | None = None
CacheDagsMap: TypeAlias = dict[str, CacheDagConfig]


class SyncState(BaseModel):
    pipelines: PipelinesMap = Field(default_factory=dict)
    cachedags: CacheDagsMap = Field(default_factory=dict)
