"""Centralized config using pydantic-settings — reads from env vars."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9094"
    kafka_topic_clickstream: str = "ecommerce.clickstream.v1"

    events_per_second: int = 10
    event_generator_duration_seconds: int = 0  # 0 = forever

    # Realistic distribution weights (must sum to ~1.0)
    weight_page_view: float = 0.50
    weight_product_view: float = 0.25
    weight_add_to_cart: float = 0.10
    weight_remove_from_cart: float = 0.05
    weight_checkout_start: float = 0.07
    weight_purchase: float = 0.03

    num_users: int = 1000
    num_products: int = 200

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()