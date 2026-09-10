import asyncio
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse

from common import lifespan

app = FastAPI(lifespan=lifespan)


NOTICE_CHANNEL = "system.notices"
HTML_PATH = Path(__file__).parent / "pub_sub.html"


@app.get("/pub_sub")
async def index():
    return FileResponse(HTML_PATH)


@app.post("/publish-notice")
async def send_notice(message: str, request: Request):
    rd = request.app.state.redis
    subscriber_count = await rd.publish(NOTICE_CHANNEL, message)
    return {"status": "success", "received_subscribers": subscriber_count}


@app.get("/stream-notices")
async def stream_notices(request: Request):

    async def event_generator():
        async with request.app.state.redis.pubsub() as pubsub:
            await pubsub.subscribe(NOTICE_CHANNEL)

            try:
                while True:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message and message["type"] == "message":
                        data = message["data"]

                        yield f"data: {data}\n\n"

                    if await request.is_disconnected():
                        break

                    await asyncio.sleep(0.01)

            finally:
                await pubsub.unsubscribe(NOTICE_CHANNEL)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
