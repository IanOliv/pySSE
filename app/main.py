"""Application entrypoint for the pySSE service."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.sse import router as sse_router
from app.core.config import get_settings
from app.services.broadcaster import EventBroadcaster


def configure_logging(level: str) -> None:
    """Configure global logging format and level."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle hooks for startup and shutdown."""

    settings = get_settings()
    configure_logging(settings.log_level)

    broadcaster = EventBroadcaster(
        interval_seconds=settings.event_interval_seconds,
        event_type=settings.event_type,
        queue_size=settings.queue_size,
        replay_buffer_size=settings.replay_buffer_size,
    )
    app.state.broadcaster = broadcaster
    await broadcaster.start()

    try:
        yield
    finally:
        await broadcaster.stop()


def create_app() -> FastAPI:
    """App factory for production deployments and tests."""

    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        """Simple readiness probe endpoint."""

        return {"status": "ok"}

    app.include_router(sse_router)

    return app


app = create_app()
