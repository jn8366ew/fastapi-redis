"""around-me의 race를 재현하고, Lua로 묶으면 사라지는지 확인합니다.

    uv run python -m ranking.race_test

ZREVRANK로 순위를 구하고 ZREVRANGE로 그 구간을 가져오는 사이에 다른 유저의
점수가 바뀌면 내 순위가 밀립니다. 그러면 "내 주변"이라고 내려준 목록에 정작
내가 없거나, 표시 순위가 실제와 어긋납니다.

혼자 눌러서는 절대 재현되지 않습니다. 두 명령 사이의 창은 1밀리초도 안 되기
때문에, 그 창 안에서 순위가 NEARBY_SPAN보다 크게 흔들릴 만큼 쓰기가 몰려야
합니다. 그래서 내 순위 주변에서 다수가 경쟁하는 상황을 만들어 놓고 잽니다.

세 단계를 같은 조건으로 돌려 비교합니다.

    1) 부하 없이  명령 2회   -> race가 부하 때문임을 확인
    2) 부하 중    명령 2회   -> 재현
    3) 부하 중    Lua 1회    -> 사라짐

실제 랭킹 키는 건드리지 않고 전용 키를 쓰고, 끝나면 지웁니다.
"""

import argparse
import asyncio
import random
import time

import redis.asyncio as redis

from common import load_script
from ranking.realtime_ranking import NEARBY_SPAN

AROUND_SCRIPT = load_script("rank_around_me")

KEY = "race:leaderboard"
VICTIM = "user:victim"

# 경쟁자들이 이 점수를 사이에 두고 오르내립니다.
VICTIM_SCORE = 50_000

# 경쟁이 벌어지는 점수 폭. 좁을수록 순위 교차가 잦습니다.
BAND = 10


async def build(rd, crowd: int, rivals: int) -> None:
    """구경꾼 다수 + 내 점수 근처에서 다투는 경쟁자 소수로 랭킹을 만듭니다."""
    await rd.delete(KEY)

    async with rd.pipeline(transaction=False) as pipe:
        pipe.zadd(KEY, {f"user:{i}": random.randint(1, 100_000) for i in range(crowd)})
        pipe.zadd(KEY, {VICTIM: VICTIM_SCORE})
        pipe.zadd(
            KEY,
            {
                f"rival:{i}": VICTIM_SCORE + random.uniform(-BAND, BAND)
                for i in range(rivals)
            },
        )
        await pipe.execute()


async def writer(rd, rivals: int, stop: asyncio.Event) -> int:
    """경쟁자들의 점수를 내 점수 위아래로 계속 뒤집습니다.

    ZADD로 값을 다시 써서 나를 추월했다 추월당했다를 반복시킵니다. 이래야 두
    명령 사이의 짧은 창 안에서 내 순위가 흔들립니다.
    """
    count = 0
    while not stop.is_set():
        await rd.zadd(
            KEY,
            {
                f"rival:{random.randrange(rivals)}": VICTIM_SCORE
                + random.uniform(-BAND, BAND)
            },
        )
        count += 1
    return count


async def read_two_commands(rd, script, span: int):
    """지금 코드가 Lua를 쓰기 전에 하던 방식. 명령 두 번."""
    my_rank = await rd.zrevrank(KEY, VICTIM)
    if my_rank is None:
        return None, 0, []
    start = max(0, my_rank - span)
    rows = await rd.zrange(KEY, start, my_rank + span, desc=True)
    return my_rank, start, rows


async def read_lua(rd, script, span: int):
    """지금 코드. 스크립트 한 번."""
    rank, start, rows = await script(keys=[KEY], args=[VICTIM, span])
    if rank == -1:
        return None, 0, []
    return rank, start, [rows[i * 2] for i in range(len(rows) // 2)]


async def run_phase(rd, script, reader, reads: int, span: int) -> dict:
    """조회를 반복하며 내가 목록에 있는지, 위치가 맞는지 셉니다."""
    missing = 0
    misplaced = 0

    for _ in range(reads):
        rank, start, members = await reader(rd, script, span)
        if rank is None:
            continue

        if VICTIM not in members:
            # 순위가 span보다 크게 밀려 창 밖으로 나갔습니다.
            missing += 1
        elif members.index(VICTIM) != rank - start:
            # 목록에는 있지만 내가 있다고 계산한 자리가 아닙니다.
            # 화면에 찍히는 등수가 틀어집니다.
            misplaced += 1

    return {"reads": reads, "missing": missing, "misplaced": misplaced}


async def with_load(rd, rivals: int, writers: int, coro):
    """쓰기 부하를 걸어 둔 채로 coro를 돌리고, 끝나면 부하를 멈춥니다."""
    stop = asyncio.Event()
    tasks = [asyncio.create_task(writer(rd, rivals, stop)) for _ in range(writers)]

    started = time.perf_counter()
    try:
        result = await coro
    finally:
        stop.set()
        counts = await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - started
    result["writes_per_sec"] = sum(counts) / elapsed
    return result


def report(label: str, r: dict) -> None:
    reads = r["reads"]
    bad = r["missing"] + r["misplaced"]
    rate = bad / reads * 100
    load = r.get("writes_per_sec")

    print(f"  {label}")
    print(f"    쓰기 부하     {f'초당 {load:,.0f}건' if load else '없음'}")
    print(f"    내가 없음     {r['missing']:,}회")
    print(f"    위치 어긋남   {r['misplaced']:,}회")
    print(f"    합계          {bad:,} / {reads:,}회  ({rate:.1f}%)")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crowd", type=int, default=5_000, help="전체 인원")
    parser.add_argument("--rivals", type=int, default=200, help="내 점수 근처 경쟁자")
    parser.add_argument("--writers", type=int, default=30, help="동시 쓰기 코루틴")
    parser.add_argument("--reads", type=int, default=1_000, help="단계별 조회 횟수")
    parser.add_argument(
        "--reverse",
        action="store_true",
        help="Lua를 먼저 재서 순서가 결과에 영향을 주는지 확인합니다",
    )
    args = parser.parse_args()

    span = NEARBY_SPAN
    rd = redis.from_url("redis://localhost:6379/0", decode_responses=True)

    try:
        await rd.ping()
    except redis.ConnectionError:
        print("Redis에 연결하지 못했습니다. localhost:6379 를 확인하세요.")
        return

    script = rd.register_script(AROUND_SCRIPT)

    print(
        f"전체 {args.crowd:,}명, 내 점수({VICTIM_SCORE:,}) ±{BAND} 안에서 "
        f"{args.rivals}명이 경쟁"
    )
    print(f"쓰기 {args.writers}개 동시 실행, 단계별 조회 {args.reads:,}회")
    print(
        f"내 주변 범위 NEARBY_SPAN={span} "
        f"(순위가 {span}칸보다 크게 밀리면 목록 밖으로 나갑니다)"
    )
    print()

    await build(rd, args.crowd, args.rivals)

    print("=" * 64)
    print(" 결과")
    print("=" * 64)

    # 대조군. 여기서 0이 나와야 뒤의 실패를 부하 탓으로 돌릴 수 있습니다.
    report(
        "1) 부하 없음 / 명령 2회",
        await run_phase(rd, script, read_two_commands, args.reads, span),
    )

    # 먼저 도는 쪽이 유리하거나 불리하지 않은지 --reverse로 뒤집어 확인합니다.
    loaded = [
        ("2회", "명령 2회   <- 고치기 전 방식", read_two_commands),
        ("Lua", "Lua 1회    <- 현재 코드", read_lua),
    ]
    if args.reverse:
        loaded.reverse()

    for i, (_, label, reader) in enumerate(loaded, start=2):
        report(
            f"{i}) 부하 중  / {label}",
            await with_load(
                rd,
                args.rivals,
                args.writers,
                run_phase(rd, script, reader, args.reads, span),
            ),
        )

    await rd.delete(KEY)
    await rd.aclose()


if __name__ == "__main__":
    asyncio.run(main())
