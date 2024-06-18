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

    debug: bool = Field(alias="debug", default=False)
    redis_debug: bool = Field(alias="redis_debug", default=False)


class SqlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="anystore_sql_")

    table: str | None = "anystore"
    pool_size: int | None = 5
