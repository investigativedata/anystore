from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="anystore_")

    uri: str | None = ".anystore"
    yaml_uri: str | None = None
    json_uri: str | None = None
    use_pickle: bool = False
    raise_on_nonexist: bool = True
