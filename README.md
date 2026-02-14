# pySSE

A production-ready **Server-Sent Events (SSE)** service built with **FastAPI** and Python 3.11+.

## What this service does

- Streams events continuously over HTTP from `GET /events/stream`
- Emits standards-compliant SSE frames (`id`, `event`, `data`, blank line separator)
- Supports multiple concurrent clients
- Uses async-first, non-blocking design with `asyncio`
- Handles disconnects gracefully
- Includes heartbeat keep-alive messages
- Provides a health endpoint at `GET /health`
- Supports optional reconnect behavior via `Last-Event-ID`
- Loads configuration from `.env` at startup using `python-dotenv`

## Project structure

```text
app/
  api/
    sse.py            # SSE endpoint and stream generator
  core/
    config.py         # environment-driven settings
  models/
    event.py          # Pydantic event schema
  services/
    broadcaster.py    # async event generation + in-memory pub/sub
  main.py             # FastAPI app entrypoint
```

## How SSE works (quick overview)

SSE keeps a single HTTP response open and pushes text frames over time.
Each event frame is plain text and looks like:

```text
id: 42
event: tick
data: {"id":42,"event":"tick","timestamp":"2026-01-01T00:00:00Z","payload":{"message":"..."}}

```

The browser's `EventSource` API parses these frames incrementally.
If a connection drops, clients can reconnect and optionally send `Last-Event-ID`.
This service keeps a small replay buffer and attempts to replay missed events on reconnect.

## Run locally

1. Create and activate a virtual environment.
2. Install dependencies.
3. Copy `.env.example` to `.env` and adjust values.
4. Start the server with uvicorn.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Configuration

All settings are read from environment variables in `.env` (prefix `PYSSE_`):

- `PYSSE_EVENT_INTERVAL_SECONDS` — period between generated events
- `PYSSE_HEARTBEAT_SECONDS` — keep-alive interval when no event is sent
- `PYSSE_EVENT_TYPE` — event name used in SSE frames
- `PYSSE_QUEUE_SIZE` — per-client queue size
- `PYSSE_REPLAY_BUFFER_SIZE` — in-memory replay cache length
- `PYSSE_CORS_ALLOW_ORIGINS` — `*`, comma-separated list, or JSON array

## Endpoints

- `GET /health` → `{ "status": "ok" }`
- `GET /events/stream` → `text/event-stream`

## Consume the stream

### Browser example (JavaScript)

```html
<script>
  const source = new EventSource('http://localhost:8000/events/stream');

  source.addEventListener('tick', (event) => {
    const parsed = JSON.parse(event.data);
    console.log('tick event:', parsed);
  });

  source.onerror = (err) => {
    console.error('SSE connection error', err);
  };
</script>
```

### curl example

```bash
curl -N http://localhost:8000/events/stream
```

Use `-N` to disable buffering so events print live.

## Notes for production

- Uvicorn/Gunicorn deployment can be added without code changes.
- Keep `X-Accel-Buffering: no` when running behind reverse proxies.
- Tune heartbeat and queue/replay sizes for traffic patterns.
