import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from common import lifespan

app = FastAPI(lifespan=lifespan)

LIMIT = 5
WINDOW = 60

# 직전 요청이 어느 창에 속했는지 기억해 둡니다. 창이 넘어가는 순간을
# 로그에 표시하려는 관찰용이고, rate limit 동작 자체와는 무관합니다.
last_window = None


def log(msg: str) -> None:
    """창이 넘어가는 시점을 봐야 하므로 밀리초까지 찍습니다."""
    now = time.time()
    stamp = (
        f"{time.strftime('%H:%M:%S', time.localtime(now))}.{int(now % 1 * 1000):03d}"
    )
    print(f"[{stamp}] {msg}", flush=True)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    global last_window

    # 테스트를 위해 docs 제외
    if request.url.path in ["/docs", "/openapi.json"]:
        return await call_next(request)

    rd = request.app.state.redis

    user_identifier = request.client.host if request.client else "127.0.0.1"

    current_minute = int(time.time() // WINDOW)
    cache_key = f"rate_limit:{user_identifier}:{current_minute}"

    # 창이 바뀌면 키 이름 자체가 바뀝니다. 리셋 코드가 따로 없는데도
    # 카운터가 0부터 다시 시작하는 이유이자, 경계에서 2배가 통과하는 이유입니다.
    if last_window is not None and current_minute != last_window:
        log(
            f"{'─' * 12} 창 전환  #{last_window % 1000} -> #{current_minute % 1000}  카운터가 0에서 다시 시작 {'─' * 12}"
        )
    last_window = current_minute

    count = await rd.incr(cache_key)

    if count == 1:
        await rd.expire(cache_key, WINDOW)

    # 이 창이 끝나기까지 남은 시간. 차단된 클라이언트가 얼마나 기다려야 하는지입니다.
    remaining_window = WINDOW - (int(time.time()) % WINDOW)

    # 창 번호는 int(time.time() // 60)이라 8자리입니다. 로그에서는 뒤 3자리만 씁니다.
    window_tag = f"창#{current_minute % 1000}"

    if count > LIMIT:
        log(
            f"{window_tag}  카운터 {count}/{LIMIT}  창 남음 {remaining_window:2d}s"
            f"  ip={user_identifier}  ->  차단"
        )
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too many requests",
                "detail": f"1분에 {LIMIT}회까지 요청가능",
                "retry_after": f"{remaining_window}s",
            },
        )

    log(
        f"{window_tag}  카운터 {count}/{LIMIT}  창 남음 {remaining_window:2d}s"
        f"  ip={user_identifier}  ->  통과"
    )
    response = await call_next(request)
    return response


@app.get("/data")
async def get_sensitive_data():
    return {"data": "Protected by rate limit"}
