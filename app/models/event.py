"""Pydantic models for SSE events."""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class StreamEvent(BaseModel):
    """Represents one event sent over the SSE stream."""

    id: int = Field(..., description="Monotonically increasing event identifier")
    event: str = Field(..., description="SSE event type")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp indicating when the event was generated",
    )
    payload: dict[str, object] = Field(
        default_factory=dict,
        description="Business payload encoded as JSON data",
    )
