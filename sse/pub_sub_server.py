import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse

# uvicorn이 이미 핸들러를 붙여둔 로거를 그대로 빌려 쓴다.
# logging.getLogger(__name__)를 쓰면 root 레벨이 WARNING이라 info 로그가 안 보인다.
logger = logging.getLogger("uvicorn.error")

NOTICE_CHANNEL = "system:notices"
HTML_PATH = Path(__file__).parent / "pub_sub.html"


async def redis_listener(app: FastAPI):
    """서버당 단 하나만 실행되는 구독 전용 백그라운드 태스크.

    Redis 구독 커넥션은 이 태스크가 유지하는 1개뿐이고,
    수신한 메시지는 메모리에 들고 있는 클라이언트 큐들에 직접 나눠준다(Fan-out).
    """
    delay = 1

    while True:  # 재연결 루프: 리스너는 이 서버의 단일 장애점이므로 스스로 복구해야 한다
        try:
            # async with이 unsubscribe/aclose까지 책임진다
            async with app.state.redis.pubsub() as pubsub:
                await pubsub.subscribe(NOTICE_CHANNEL)
                logger.info("리스너 구독 시작: %s", NOTICE_CHANNEL)
                delay = 1  # 연결에 성공했으니 backoff 초기화

                while True:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )

                    if message and message["type"] == "message":
                        data = message["data"]
                        # 순회 도중 접속/종료로 set 크기가 변해도 안전하도록 사본을 쓴다
                        for client_queue in list(app.state.connected_clients):
                            try:
                                client_queue.put_nowait(data)
                            except asyncio.QueueFull:
                                logger.warning("큐 가득 참 - 메시지 폐기 (느린 클라이언트)")

        except asyncio.CancelledError:
            # 서버 종료 신호. 에러가 아니라 정상 흐름이므로 그대로 위로 넘긴다.
            # 반드시 아래 except Exception보다 위에 있어야 한다.
            logger.info("리스너 종료")
            raise
        except Exception:
            logger.exception("리스너 오류, %d초 후 재연결", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    app.state.connected_clients = set()
    # 지역변수로 두면 종료 시 cancel할 수 없고, GC가 실행 중인 태스크를 수거할 수도 있다
    app.state.listener_task = asyncio.create_task(redis_listener(app))

    yield

    app.state.listener_task.cancel()
    try:
        # 리스너가 finally까지 마치고 끝날 때까지 명시적으로 기다린다
        await app.state.listener_task
    except asyncio.CancelledError:
        pass
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/pub_sub")
async def index():
    return FileResponse(HTML_PATH)


@app.post("/publish-notice")
async def send_notice(message: str, request: Request):
    await request.app.state.redis.publish(NOTICE_CHANNEL, message)
    return {
        # 이 서버 인스턴스에 붙은 클라이언트 수. 전체 합계가 아니다.
        "status": "sent",
        "active_clients": len(request.app.state.connected_clients),
    }


@app.get("/stream-notices")
async def stream_notices(request: Request):

    # 브라우저 연결마다 하나씩 생기는 메시지함.
    # maxsize로 느린 클라이언트가 서버 메모리를 고갈시키는 것을 막는다.
    client_queue = asyncio.Queue(maxsize=100)
    request.app.state.connected_clients.add(client_queue)

    async def event_generator():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(client_queue.get(), timeout=5.0)
                    yield f"data: {data}\n\n"
                except TimeoutError:
                    # ':'로 시작하는 줄은 SSE 주석이라 브라우저가 무시한다.
                    # 프록시 타임아웃 방지 + 끊긴 연결 감지 용도.
                    yield ": ping\n\n"

                if await request.is_disconnected():
                    break

        finally:
            # remove()와 달리 없어도 KeyError를 내지 않는다
            request.app.state.connected_clients.discard(client_queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
