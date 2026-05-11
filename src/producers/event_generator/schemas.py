"""Pydantic schemas for clickstream events. Acts as the data contract."""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    CHECKOUT_START = "checkout_start"
    PURCHASE = "purchase"


class ClickstreamEvent(BaseModel):
    """Single user interaction event. This is the canonical contract.

    Any change here is a breaking change to downstream consumers.
    """

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    user_id: int | None = None  # null for anonymous browsing
    session_id: UUID
    product_id: int | None = None
    event_timestamp: datetime
    properties: dict[str, Any] = Field(default_factory=dict)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "event_id": "550e8400-e29b-41d4-a716-446655440000",
                    "event_type": "add_to_cart",
                    "user_id": 12345,
                    "session_id": "660e8400-e29b-41d4-a716-446655440001",
                    "product_id": 789,
                    "event_timestamp": "2026-05-08T12:34:56Z",
                    "properties": {"quantity": 2, "price_cents": 2999},
                }
            ]
        }
    }