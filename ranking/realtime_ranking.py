from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request

from common import lifespan as redis_lifespan
from common import load_script

AROUND_SCRIPT = load_script("rank_around_me")

# 일간 랭킹은 7일치만 남기고 Redis가 알아서 회수하게 둡니다.
RANKING_TTL = 7 * 24 * 60 * 60

# 내 순위 기준 위아래로 몇 명까지 보여줄지.
NEARBY_SPAN = 2

# 리더보드가 리셋되는 기준 타임존. 서버 로케일에 맡기면 UTC 컨테이너에
# 배포하는 순간 리셋 시각이 9시간 밀립니다. 기준은 코드에 박아둡니다.
# KST는 서머타임이 없어 항상 UTC+9이므로 zoneinfo(tzdata 의존) 대신 고정 오프셋으로 둡니다.
RANKING_TZ = timezone(timedelta(hours=9))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 커넥션은 common.lifespan이 만든 것을 쓰고 Lua 스크립트 등록만 얹습니다.
    # register_script는 코루틴이 아니라 동기 함수이고, SHA1을 미리 계산해 둔 뒤
    # 호출 시 EVALSHA를 먼저 시도합니다. Redis가 재시작해 스크립트 캐시가
    # 비어도 NOSCRIPT를 받으면 EVAL로 알아서 폴백합니다.
    async with redis_lifespan(app):
        app.state.around_script = app.state.redis.register_script(AROUND_SCRIPT)
        yield


app = FastAPI(lifespan=lifespan)


def daily_key() -> str:
    """오늘자 랭킹 키.

    날짜를 키에 넣으면 자정이 지나는 순간 새 리더보드가 시작됩니다.
    초기화 배치가 필요 없고, TTL을 걸어두면 정리까지 자동입니다.
    """
    return f"leaderboard:daily:{datetime.now(RANKING_TZ).date().isoformat()}"


@app.post("/rank/score")
async def update_score(user_id: str, score_delta: float, request: Request):
    rd = request.app.state.redis
    key = daily_key()

    # ZINCRBY는 단일 명령이라 원자적입니다. GET -> 더하기 -> SET 이 아니므로
    # 동시에 몇 명이 같은 유저의 점수를 올려도 갱신이 유실되지 않습니다.
    # TTL 연장은 순서만 맞으면 되므로 파이프라인으로 묶어 라운드트립을 한 번으로 줄입니다.
    async with rd.pipeline(transaction=False) as pipe:
        pipe.zincrby(key, score_delta, user_id)
        pipe.expire(key, RANKING_TTL)
        new_score, _ = await pipe.execute()

    return {"user_id": user_id, "current_score": new_score}


@app.get("/rank/top10")
async def get_top_rankers(request: Request):
    rd = request.app.state.redis

    # desc=True는 ZREVRANGE로 나갑니다. 인덱스는 높은 점수 기준 0-based.
    # O(log N + 10) 이므로 전체 인원 수와 무관하게 비용이 일정합니다.
    top_list = await rd.zrange(daily_key(), 0, 9, desc=True, withscores=True)

    result = [
        {"rank": i + 1, "user_id": m, "score": s} for i, (m, s) in enumerate(top_list)
    ]

    return {"top_rankers": result}


@app.get("/rank/around-me/{user_id}")
async def get_nearby_rank(user_id: str, request: Request):
    # 순위 계산과 구간 조회를 Lua 한 덩어리로 처리합니다. 둘을 따로 보내면
    # 그 사이에 남의 점수가 올라 순위가 밀리고, 내 주변 목록에 내가 없습니다.
    my_rank, start, rows = await request.app.state.around_script(
        keys=[daily_key()],
        args=[user_id, NEARBY_SPAN],
    )

    if my_rank == -1:
        raise HTTPException(status_code=404, detail="Ranking data not found")

    # 스크립트는 WITHSCORES 평탄 배열을 그대로 넘깁니다. (member, score) 튜플로
    # 묶어 주는 건 Redis가 아니라 redis-py의 zrange가 해 주던 일이라, Lua를
    # 거치면 여기서 두 칸씩 끊고 점수를 float으로 되돌려야 합니다.
    result = [
        {"rank": start + i + 1, "user_id": rows[i * 2], "score": float(rows[i * 2 + 1])}
        for i in range(len(rows) // 2)
    ]

    return {"user_id": user_id, "nearby_rankers": result}
