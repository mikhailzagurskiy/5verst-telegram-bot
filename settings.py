from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr

from db.config import Config as DBConfig


class Settings(BaseSettings):
  bot_token: SecretStr
  db_config: DBConfig
  model_config = SettingsConfigDict(
    env_file='.env', env_file_encoding='utf-8', env_prefix='bot_5verst_', env_nested_delimiter='__')
