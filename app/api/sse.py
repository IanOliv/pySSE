"""API endpoints for server-sent events."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.config import Settings, get_settings
from app.services.broadcaster import EventBroadcaster, format_sse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])


def get_broadcaster(request: Request) -> EventBroadcaster:
    """Resolve broadcaster service from app state."""

    return request.app.state.broadcaster


def get_heartbeat(settings: Settings = Depends(get_settings)) -> float:
    """Resolve heartbeat interval from application settings."""

    return settings.heartbeat_seconds


@router.get("/stream")
async def stream_events(
    request: Request,
    broadcaster: EventBroadcaster = Depends(get_broadcaster),
    heartbeat_seconds: float = Depends(get_heartbeat),
    last_event_id_header: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """Stream events in SSE format to connected clients."""

    parsed_last_id = _parse_last_event_id(last_event_id_header)

    async def event_generator() -> AsyncIterator[str]:
        subscription = broadcaster.subscribe(last_event_id=parsed_last_id)
        while True:
            if await request.is_disconnected():
                logger.info("Detected disconnected client")
                break

            try:
                event = await asyncio.wait_for(
                    anext(subscription), timeout=heartbeat_seconds
                )
                yield format_sse(event)
            except TimeoutError:
                yield ": keep-alive\n\n"
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error while streaming events")
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _parse_last_event_id(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid Last-Event-ID header") from exc
