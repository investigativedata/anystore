from multiprocessing import cpu_count

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from anystore.serialize import Mode


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="anystore_")

    uri: str | None = ".anystore"
    yaml_uri: str | None = None
    json_uri: str | None = None
    serialization_mode: Mode | None = "auto"
    raise_on_nonexist: bool = True
    default_ttl: int = 0

    debug: bool = Field(alias="debug", default=False)
    redis_debug: bool = Field(alias="redis_debug", default=False)
    log_json: bool = Field(alias="log_json", default=False)
    log_level: str = Field(alias="log_level", default="info")
    worker_threads: int = Field(alias="worker_threads", default=cpu_count())
    worker_heartbeat: int = Field(alias="worker_heartbeat", default=15)


class SqlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="anystore_sql_")

    table: str | None = "anystore"
    pool_size: int | None = 5
