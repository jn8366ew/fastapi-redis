"""랭킹 키에 더미 데이터를 심고, 그 규모에서의 지연을 재 봅니다.

리더보드는 인원이 늘어야 보이는 것들이 있습니다. 10명일 때는 무엇을 해도
빠르기 때문에 ZSET을 쓰는 이유가 드러나지 않습니다.

    uv run python -m ranking.seed                  # 10만 명
    uv run python -m ranking.seed --count 1000000  # 100만 명
    uv run python -m ranking.seed --reset          # 기존 데이터를 지우고 새로

인원을 10배로 늘려도 조회 지연이 거의 그대로인 것이 핵심입니다. ZSET은 읽을
때 정렬하지 않고, 순위도 세는 게 아니라 스킵리스트의 span을 더해서 구하기
때문입니다.
"""

import argparse
import asyncio
import random
import statistics
import time

import redis.asyncio as redis

from common import load_script
from ranking.realtime_ranking import (
    NEARBY_SPAN,
    RANKING_TTL,
    daily_key,
)

AROUND_SCRIPT = load_script("rank_around_me")


async def seed(rd, key: str, count: int, batch: int, max_score: int) -> float:
    """파이프라인으로 나눠 심고 걸린 시간을 돌려줍니다.

    한 건씩 보내면 라운드트립이 count번입니다. 실제로 batch=1은 초당 2천 건,
    batch=500은 초당 47만 건으로 200배 가까이 차이가 납니다.

    묶음이 클수록 항상 빠른 건 아닙니다. 이 환경(Windows 루프백)에서는 batch가
    1000~2000일 때만 배치당 40ms 넘는 지연이 붙었습니다. TCP 지연 ACK 쪽으로
    보이는데, 어느 구간이 걸릴지는 환경마다 다르므로 느리면 --batch를 바꿔
    가며 재 보는 게 빠릅니다.
    """
    started = time.perf_counter()

    for offset in range(0, count, batch):
        size = min(batch, count - offset)
        # ZADD는 {member: score} 매핑을 한 번에 받습니다.
        members = {
            f"user:{offset + i}": random.randint(1, max_score) for i in range(size)
        }
        async with rd.pipeline(transaction=False) as pipe:
            pipe.zadd(key, members)
            pipe.expire(key, RANKING_TTL)
            await pipe.execute()

        done = offset + size
        print(f"\r  {done:,} / {count:,}", end="", flush=True)

    print()
    return time.perf_counter() - started


async def measure(fn, rounds: int) -> tuple[float, float]:
    """평균과 p99를 밀리초로 돌려줍니다. 평균만 보면 튀는 구간을 놓칩니다."""
    samples = []
    for _ in range(rounds):
        started = time.perf_counter()
        await fn()
        samples.append((time.perf_counter() - started) * 1000)

    samples.sort()
    return statistics.mean(samples), samples[int(len(samples) * 0.99)]


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100_000, help="심을 인원 수")
    parser.add_argument("--batch", type=int, default=500, help="파이프라인 묶음 크기")
    parser.add_argument("--max-score", type=int, default=1_000_000)
    parser.add_argument("--rounds", type=int, default=1_000, help="지연 측정 횟수")
    parser.add_argument("--key", default=None, help="기본값은 오늘자 랭킹 키")
    parser.add_argument("--reset", action="store_true", help="심기 전에 키를 지웁니다")
    args = parser.parse_args()

    key = args.key or daily_key()
    rd = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    around = rd.register_script(AROUND_SCRIPT)

    try:
        await rd.ping()
    except redis.ConnectionError:
        print("Redis에 연결하지 못했습니다. localhost:6379 가 떠 있는지 확인하세요.")
        return

    print(f"키: {key}")

    if args.reset:
        await rd.delete(key)
        print("기존 데이터 삭제")

    print(f"\n{args.count:,}명 심는 중 (묶음 {args.batch:,})")
    elapsed = await seed(rd, key, args.count, args.batch, args.max_score)
    print(f"  {elapsed:.2f}초, 초당 {args.count / elapsed:,.0f}건")

    total = await rd.zcard(key)
    used = await rd.memory_usage(key)
    print(f"\n총 {total:,}명, 메모리 {used / 1024 / 1024:.1f}MB")
    print(f"  멤버당 약 {used / total:.0f} bytes")

    # 측정 대상은 실제 API가 쓰는 경로 그대로입니다.
    victim = f"user:{args.count // 2}"
    cases = {
        "ZINCRBY (점수 갱신)": lambda: rd.zincrby(key, 1, victim),
        "ZREVRANGE (top10)": lambda: rd.zrange(key, 0, 9, desc=True, withscores=True),
        "ZREVRANK (내 순위)": lambda: rd.zrevrank(key, victim),
        "EVALSHA (around-me)": lambda: around(keys=[key], args=[victim, NEARBY_SPAN]),
    }

    print(f"\n지연 ({args.rounds:,}회, 라운드트립 포함)")
    print(f"  {'':<22} {'평균':>9} {'p99':>9}")
    for label, fn in cases.items():
        avg, p99 = await measure(fn, args.rounds)
        print(f"  {label:<22} {avg:>8.3f}ms {p99:>8.3f}ms")

    print("\n인원을 10배로 바꿔 다시 돌려보면 이 숫자가 거의 그대로입니다.")
    await rd.aclose()


if __name__ == "__main__":
    asyncio.run(main())
