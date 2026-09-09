import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from common import lifespan as redis_lifespan
from common import load_script

BUCKET_SCRIPT = load_script("token_bucket")

# 양동이 용량. 놀고 있다가 한 번에 몰아 쓸 수 있는 최대치입니다.
CAPACITY = 5

# CAPACITY만큼 다시 채워지는 데 걸리는 시간. 평균 허용 속도를 정합니다.
REFILL_WINDOW = 60

# 초당 충전 토큰 수. 5 / 60 = 약 0.083개/초 = 12초에 1개.
REFILL_RATE = CAPACITY / REFILL_WINDOW

# 놀고 있는 버킷을 정리할 시간. REFILL_WINDOW가 지나면 어차피 가득 차고,
# 가득 찬 버킷은 키가 없는 것과 동작이 같으므로 그때 지워도 손해가 없습니다.
BUCKET_TTL = REFILL_WINDOW + 1


def fmt(value: float) -> str:
    return f"{value:.2f}"


def log(msg: str) -> None:
    """충전이 시간에 비례해 일어나는 것을 봐야 하므로 밀리초까지 찍습니다."""
    now = time.time()
    stamp = (
        f"{time.strftime('%H:%M:%S', time.localtime(now))}.{int(now % 1 * 1000):03d}"
    )
    print(f"[{stamp}] {msg}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 커넥션은 common.lifespan이 만든 것을 쓰고 Lua 스크립트 등록만 얹습니다.
    async with redis_lifespan(app):
        app.state.bucket_script = app.state.redis.register_script(BUCKET_SCRIPT)
        yield


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def token_bucket_middleware(request: Request, call_next):
    # 테스트를 위해 docs 제외
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    user_identifier = request.client.host if request.client else "127.0.0.1"

    # 고정 창과 달리 키에 시간이 들어가지 않습니다. 버킷은 리셋되는 것이 아니라
    # 계속 이어지면서 채워지고 비워지기 때문입니다.
    bucket_key = f"rate_limit:bucket:{user_identifier}"

    allowed, tokens, refilled, wait = await request.app.state.bucket_script(
        keys=[bucket_key],
        args=[CAPACITY, REFILL_RATE, BUCKET_TTL],
    )
    tokens, refilled, wait = float(tokens), float(refilled), float(wait)

    # 칸 너비를 고정해 두면 여러 줄이 세로로 정렬돼 변화를 따라가기 쉽습니다.
    refill_txt = f"+{fmt(refilled)}"
    remain_txt = f"{fmt(tokens)}/{CAPACITY}"

    if not allowed:
        log(
            f"버킷  충전 {refill_txt:<5}  잔여 {remain_txt:<8}"
            f"  ip={user_identifier}  ->  차단 ({wait:.1f}초 후 1개)"
        )
        return JSONResponse(
            status_code=429,
            # 표준 클라이언트와 SDK는 본문이 아니라 이 헤더를 보고 재시도합니다.
            headers={"Retry-After": str(max(1, round(wait)))},
            content={
                "error": "Too many requests",
                "detail": f"{REFILL_WINDOW}초에 {CAPACITY}회까지 요청가능",
                "retry_after": f"{wait:.1f}s",
            },
        )

    log(
        f"버킷  충전 {refill_txt:<5}  잔여 {remain_txt:<8}"
        f"  ip={user_identifier}  ->  통과"
    )
    response = await call_next(request)
    return response


@app.get("/data")
async def get_sensitive_data():
    return {"data": "Protected by token bucket"}
