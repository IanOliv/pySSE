"""Async event broadcaster for the SSE stream."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import AsyncIterator

from app.models.event import StreamEvent

logger = logging.getLogger(__name__)


class EventBroadcaster:
    """Generates periodic events and publishes them to subscribers."""

    def __init__(
        self,
        *,
        interval_seconds: float,
        event_type: str,
        queue_size: int,
        replay_buffer_size: int,
    ) -> None:
        self._interval = interval_seconds
        self._event_type = event_type
        self._queue_size = queue_size
        self._sequence = 0
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._subscribers: set[asyncio.Queue[StreamEvent]] = set()
        self._history: deque[StreamEvent] = deque(maxlen=replay_buffer_size)

    async def start(self) -> None:
        """Start background publishing task."""

        if self._task and not self._task.done():
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="event-broadcaster")
        logger.info("Started event broadcaster")

    async def stop(self) -> None:
        """Stop background publishing task."""

        self._stop_event.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("Stopped event broadcaster")

    async def _run(self) -> None:
        """Generate and distribute events periodically."""

        while not self._stop_event.is_set():
            event = self._build_event()
            self._history.append(event)
            self._publish(event)
            await asyncio.sleep(self._interval)

    def _build_event(self) -> StreamEvent:
        self._sequence += 1
        now = datetime.now(timezone.utc)
        return StreamEvent(
            id=self._sequence,
            event=self._event_type,
            timestamp=now,
            payload={
                "message": "Periodic event from pySSE",
                "sequence": self._sequence,
                "iso_time": now.isoformat(),
            },
        )

    def _publish(self, event: StreamEvent) -> None:
        stale_subscribers: list[asyncio.Queue[StreamEvent]] = []
        for queue in self._subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Subscriber queue full; dropping event for one client")
            except RuntimeError:
                stale_subscribers.append(queue)

        for queue in stale_subscribers:
            self._subscribers.discard(queue)

    async def subscribe(self, last_event_id: int | None = None) -> AsyncIterator[StreamEvent]:
        """Yield replayed and live events for one client subscription."""

        queue: asyncio.Queue[StreamEvent] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(queue)
        logger.info("Client subscribed. active_subscribers=%d", len(self._subscribers))

        try:
            if last_event_id is not None:
                for event in self._history:
                    if event.id > last_event_id:
                        yield event

            while True:
                event = await queue.get()
                yield event
        finally:
            self._subscribers.discard(queue)
            logger.info("Client disconnected. active_subscribers=%d", len(self._subscribers))


def format_sse(event: StreamEvent) -> str:
    """Convert a stream event into a valid SSE message frame."""

    payload = {
        "id": event.id,
        "event": event.event,
        "timestamp": event.timestamp.isoformat(),
        "payload": event.payload,
    }
    return (
        f"id: {event.id}\n"
        f"event: {event.event}\n"
        f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
    )
