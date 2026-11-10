from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_HOST: str = "db"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASSWORD: str  # тільки через env, тут була в мене помилка
    DB_NAME: str = "appointments_db"
    # найбільша кількість відкритих з'єднань з базою на один екземпляр
    DB_POOL_SIZE: int = Field(10, ge=1)
    # ім'я екземпляра для заголовка X-Pod; якщо не задано, береться ім'я хоста
    POD_NAME: Optional[str] = None

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()