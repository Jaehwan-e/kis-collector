from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_key: str
    app_secret: str
    db_dsn: str = "postgresql://gyeol@localhost:5432/stock_data"
    symbols: str = "005930"
    ws_url: str = "ws://ops.koreainvestment.com:21000"
    rest_url: str = "https://openapi.koreainvestment.com:9443"
    log_level: str = "INFO"
    flush_interval: float = 1.0

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip() for s in self.symbols.split(",")]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
