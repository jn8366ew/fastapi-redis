import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from common import lifespan as redis_lifespan
from common import load_script

RELEASE_SCRIPT = load_script("release_lock")
EXTEND_SCRIPT = load_script("extend_lock")

# 락의 수명. watchdog이 이 값의 1/3마다 깨어나 같은 값으로 되돌립니다.
LOCK_TIMEOUT_MS = 5000

# 임계구역에서 하는 "무거운 작업"의 길이. LOCK_TIMEOUT_MS보다 길게 두면
# watchdog 없이는 락이 먼저 풀립니다.
WORK_SECONDS = 6

# --- 아래 두 개는 락이 깨지는 순간을 눈으로 보기 위한 관찰용입니다 ---

# 지금 임계구역 안에 몇 명이 있는지 세는 카운터. 락이 제대로 걸려 있으면
# 항상 1이어야 하고, 2 이상이 찍히면 상호배제가 깨진 것입니다.
# asyncio는 단일 스레드라 await 없이 증감하는 한 이 카운터 자체는 안전합니다.
in_critical = 0


def log(msg: str) -> None:
    """겹치는 구간을 확인해야 하므로 밀리초까지 찍습니다."""
    now = time.time()
    stamp = (
        f"{time.strftime('%H:%M:%S', time.localtime(now))}.{int(now % 1 * 1000):03d}"
    )
    print(f"[{stamp}] {msg}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 커넥션은 common.lifespan이 만든 것을 그대로 쓰고 Lua 스크립트 등록만 얹습니다.
    # register_script는 코루틴이 아니라 동기 함수이고, SHA1 계산에 클라이언트의
    # 인코더가 필요해서 연결 객체가 생긴 뒤에 불러야 합니다.
    # 호출 시에는 EVALSHA를 쓰고, 서버가 스크립트를 모르면 자동으로 다시 적재합니다.
    async with redis_lifespan(app):
        app.state.release_script = app.state.redis.register_script(RELEASE_SCRIPT)
        app.state.extend_script = app.state.redis.register_script(EXTEND_SCRIPT)
        yield


app = FastAPI(lifespan=lifespan)


async def acquire_lock(
    rd,
    lock_name: str,
    acquire_timeout: float = 10.0,
    lock_timeout_ms: int = LOCK_TIMEOUT_MS,
):

    identifier = str(uuid.uuid4())
    end_time = time.time() + acquire_timeout

    while time.time() < end_time:
        if await rd.set(lock_name, identifier, nx=True, px=lock_timeout_ms):
            return identifier

        await asyncio.sleep(0.1)

    return False


async def release_lock(rd, lock_name: str, identifier: str):
    """GET과 DEL 사이의 원자성을 살펴보기 위해 사용하는 함수"""
    if await rd.get(lock_name) == identifier:
        await rd.delete(lock_name)
        return True
    return False


async def release_lock_atomic(release_script, lock_name: str, identifier: str):
    """비교와 삭제를 Lua 한 덩어리로 처리하는 해제 함수. 실무에서는 이쪽을 씁니다."""
    return await release_script(keys=[lock_name], args=[identifier]) == 1


async def lock_watchdog(
    extend_script,
    lock_name: str,
    identifier: str,
    lock_timeout_ms: int = LOCK_TIMEOUT_MS,
):
    """작업이 끝날 때까지 락의 TTL을 대신 늘려주는 백그라운드 태스크.

    TTL의 1/3마다 깨어나 원래 길이로 되돌립니다. 한두 번 실패해도 만료 전에
    만회할 여유를 두려는 비율이고, Redisson도 같은 방식(리스 30초 / 10초마다
    갱신)을 씁니다. 작업이 끝나면 호출한 쪽에서 cancel() 합니다.
    """
    interval = lock_timeout_ms / 3 / 1000

    while True:
        await asyncio.sleep(interval)

        extended = await extend_script(
            keys=[lock_name], args=[identifier, lock_timeout_ms]
        )
        if not extended:
            # 이미 만료됐거나 남이 가져간 상태. 더 연장할 것이 없으므로 물러납니다.
            log(f"     watchdog {identifier[:8]} 연장 실패. 락을 이미 잃었습니다")
            return

        log(f"     watchdog {identifier[:8]} TTL을 {lock_timeout_ms}ms로 되돌림")


@app.post("/stock/reduce/{item_id}")
async def reduce_stock(item_id: str, request: Request, user_id: str = "unknown"):
    global in_critical

    rd = request.app.state.redis
    lock_name = f"lock:item:{item_id}"

    lock_id = await acquire_lock(rd, lock_name)
    if not lock_id:
        log(f"포기 {user_id:<8} 대기 시간 초과")
        raise HTTPException(
            status_code=409,
            detail=f"접속자가 많아 처리가 지연중입니다. 다시 시도하세요 - 유저:{user_id}",
        )

    in_critical += 1
    log(
        f"획득 {user_id:<8} token={lock_id[:8]} item={item_id} 임계구역={in_critical}명"
    )
    if in_critical > 1:
        log(f"     !! 상호배제 깨짐. {in_critical}명이 동시에 재고를 차감하는 중")

    # 락을 잡은 순간부터 작업이 끝날 때까지 TTL을 대신 지켜줄 태스크를 띄웁니다.
    watchdog = asyncio.create_task(
        lock_watchdog(request.app.state.extend_script, lock_name, lock_id)
    )

    try:
        # 무거운 작업 돌리고 있다고 가정
        await asyncio.sleep(WORK_SECONDS)

        return {"message": f"Item {item_id} stock reduced successfully for {user_id}"}

    finally:
        watchdog.cancel()
        in_critical -= 1
        released = await release_lock_atomic(
            request.app.state.release_script, lock_name, lock_id
        )
        verdict = (
            "락 삭제함" if released else "삭제 안 함 (내 락이 아니거나 이미 만료됨)"
        )
        log(f"해제 {user_id:<8} token={lock_id[:8]} Lua={int(released)} {verdict}")
