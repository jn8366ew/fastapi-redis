"""랭킹 구현이 의도대로 동작하는지 한 번에 확인합니다.

    uv run python -m ranking.check

Redis에 직접 붙으므로 서버가 꺼져 있어도 됩니다. docker exec나 redis-cli를
따로 칠 필요 없이 확인할 것을 모아서 찍습니다.

  1) 키 상태     이름 / 인원 / TTL / 인코딩 / 메모리
  2) 경계 동작   1위와 꼴찌에서 구간이 제대로 잘리는지, 내가 목록에 있는지
  3) 지연        순위 위치에 따라 느려지지 않는지 (RDB였다면 꼴찌가 느립니다)
  4) 점유 시간   Lua가 Redis를 실제로 몇 µs 잡는지
"""

import argparse
import asyncio
import statistics
import time
import unicodedata

import redis.asyncio as redis

from common import load_script
from ranking.realtime_ranking import NEARBY_SPAN, daily_key

AROUND_SCRIPT = load_script("rank_around_me")

OK = "OK"
NG = "실패"


def pad(text: str, width: int, right: bool = False) -> str:
    """한글은 터미널에서 두 칸을 차지하므로 그만큼 빼고 채웁니다.

    str.ljust는 글자 수로 세기 때문에 한글이 섞이면 표가 어긋납니다.
    """
    span = sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)
    fill = " " * max(0, width - span)
    return fill + text if right else text + fill


def line(char: str = "-") -> None:
    print(char * 64)


def head(title: str) -> None:
    print()
    line("=")
    print(f" {title}")
    line("=")


def human_ttl(seconds: int) -> str:
    if seconds < 0:
        return "없음 (만료되지 않음)"
    d, rest = divmod(seconds, 86400)
    h, rest = divmod(rest, 3600)
    m = rest // 60
    return f"{d}일 {h}시간 {m}분"


async def show_key(rd, key: str) -> int:
    head("1. 키 상태")

    total = await rd.zcard(key)
    if not total:
        print(f"  {key} 가 비어 있습니다.")
        print("  먼저 심으세요:  uv run python -m ranking.seed")
        return 0

    ttl = await rd.ttl(key)
    encoding = await rd.object("encoding", key)
    used = await rd.memory_usage(key)

    print(f"  키          {key}")
    print(f"  인원        {total:,} 명")
    print(f"  TTL         {human_ttl(ttl):<24} {OK if ttl > 0 else NG}")
    print(
        f"  메모리      {used / 1024 / 1024:.1f} MB   (멤버당 {used / total:.0f} bytes)"
    )
    print(
        f"  인코딩      {encoding:<24} {OK if encoding == 'skiplist' else '작은 ZSET'}"
    )

    if encoding == "skiplist":
        print("              멤버가 128개를 넘어 listpack에서 승격됐습니다.")
        print("              해시테이블(멤버->점수) + 스킵리스트(정렬) 두 벌을 씁니다.")
    else:
        print("              128개 이하라 납작한 배열 하나로 처리됩니다.")
    return total


async def check_edges(rd, script, key: str, total: int) -> None:
    """1위와 꼴찌에서 구간이 잘리는지, 목록에 내가 있는지 확인합니다."""
    head("2. 경계 동작")

    span = NEARBY_SPAN
    full = span * 2 + 1

    first = (await rd.zrange(key, 0, 0, desc=True))[0]
    second = (await rd.zrange(key, 1, 1, desc=True))[0]
    middle = (await rd.zrange(key, total // 2, total // 2, desc=True))[0]
    last = (await rd.zrange(key, 0, 0))[0]

    cases = [
        ("1위", first, span + 1, "앞쪽이 0에서 잘림"),
        ("2위", second, span + 2, "앞쪽이 한 칸만 잘림"),
        ("중간", middle, full, "온전한 구간"),
        ("꼴찌", last, span + 1, "뒤쪽을 Redis가 자름"),
    ]

    print(
        f"  {pad('대상', 8)}{pad('순위', 12, True)}{pad('인원', 8, True)}"
        f"{pad('내포함', 9, True)}{pad('판정', 8, True)}  설명"
    )
    line()

    for label, uid, expect, note in cases:
        rank, start, rows = await script(keys=[key], args=[uid, span])
        members = [rows[i * 2] for i in range(len(rows) // 2)]

        found = uid in members
        # 목록 안에서의 내 위치가 순위 계산과 맞아떨어져야 합니다.
        placed = found and members.index(uid) == rank - start
        good = len(members) == expect and placed

        print(
            f"  {pad(label, 8)}{pad(f'{rank + 1:,}', 12, True)}"
            f"{pad(f'{len(members)}명', 8, True)}{pad(OK if found else NG, 9, True)}"
            f"{pad(OK if good else NG, 8, True)}  {note}"
        )

    rank, _, _ = await script(keys=[key], args=["없는유저", span])
    print(
        f"  {'없는유저':<6} {'-1':>10} {'':>6} {'':>7} {(OK if rank == -1 else NG):>6}  404로 이어짐"
    )


async def check_latency(rd, script, key: str, total: int, rounds: int) -> None:
    """순위 위치가 지연에 영향을 주는지 봅니다.

    RDB였다면 꼴찌 조회가 훨씬 느립니다. 나보다 위에 있는 행을 전부 세야 하기
    때문입니다. ZSET은 스킵리스트의 span을 더해서 구하므로 위치와 무관합니다.
    """
    head("3. 순위 위치별 지연")

    span = NEARBY_SPAN
    spots = {
        "1위": (await rd.zrange(key, 0, 0, desc=True))[0],
        "중간": (await rd.zrange(key, total // 2, total // 2, desc=True))[0],
        "꼴찌": (await rd.zrange(key, 0, 0))[0],
    }

    print(f"  {total:,}명 기준, 각 {rounds:,}회 (네트워크 왕복 포함)")
    print()
    print(f"  {pad('위치', 8)}{pad('평균', 11, True)}{pad('p99', 11, True)}")
    line()

    results = {}
    for label, uid in spots.items():
        samples = []
        for _ in range(rounds):
            started = time.perf_counter()
            await script(keys=[key], args=[uid, span])
            samples.append((time.perf_counter() - started) * 1000)
        samples.sort()
        avg = statistics.mean(samples)
        results[label] = avg
        p99 = samples[int(len(samples) * 0.99)]
        print(
            f"  {pad(label, 8)}{pad(f'{avg:.3f}ms', 11, True)}"
            f"{pad(f'{p99:.3f}ms', 11, True)}"
        )

    spread = max(results.values()) / min(results.values())
    line()
    print(f"  최대/최소 = {spread:.2f}배   {OK if spread < 1.5 else '차이가 큽니다'}")
    print("  위치와 무관하게 같아야 정상입니다. RDB라면 꼴찌가 수백 배 느립니다.")


async def check_blocking(rd, script, key: str, total: int, rounds: int) -> None:
    """스크립트가 Redis를 붙잡고 있던 시간을 잽니다.

    Redis는 싱글 스레드라 명령 하나가 도는 동안 다른 클라이언트는 대기합니다.
    Lua도 명령 하나로 취급되므로, 여기 찍히는 usec_per_call이 곧 '다른 요청이
    기다린 시간'입니다.
    """
    head("4. Redis 점유 시간")

    uid = (await rd.zrange(key, total // 2, total // 2, desc=True))[0]

    # 통계와 느린 명령 기록을 모두 비웁니다. 이걸 안 하면 데이터를 심을 때
    # 남은 기록이 섞여 이번 호출 때문인 것처럼 보입니다.
    await rd.config_resetstat()
    await rd.slowlog_reset()
    print(f"  통계를 초기화하고 around-me를 {rounds:,}회 호출합니다.")

    for _ in range(rounds):
        await script(keys=[key], args=[uid, NEARBY_SPAN])

    stats = await rd.info("commandstats")
    print()
    print(f"  {pad('명령', 14)}{pad('호출', 9, True)}{pad('평균', 13, True)}")
    line()
    for name, row in sorted(stats.items()):
        if row.get("calls", 0) < rounds // 2:
            continue
        calls = f"{row['calls']:,}"
        avg = f"{row['usec_per_call']:.2f}µs"
        print(
            f"  {pad(name.replace('cmdstat_', ''), 14)}"
            f"{pad(calls, 9, True)}{pad(avg, 13, True)}"
        )

    slow = await rd.slowlog_get(10)
    line()

    if not slow:
        print(f"  SLOWLOG     0건   {OK}")
        print("              10ms(기본 임계)를 넘긴 명령이 하나도 없습니다.")
        print("              스크립트가 도는 동안 다른 요청이 기다린 시간은")
        print("              위 evalsha 평균이 전부입니다.")
    else:
        print(f"  SLOWLOG     {len(slow)}건   {NG}")
        for row in slow:
            cmd = " ".join(str(a) for a in row["command"].split()[:3])
            print(f"              {row['duration']:>9,}µs  {cmd}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default=None, help="기본값은 오늘자 랭킹 키")
    parser.add_argument("--rounds", type=int, default=500)
    args = parser.parse_args()

    key = args.key or daily_key()
    rd = redis.from_url("redis://localhost:6379/0", decode_responses=True)

    try:
        await rd.ping()
    except redis.ConnectionError:
        print("Redis에 연결하지 못했습니다. localhost:6379 를 확인하세요.")
        return

    script = rd.register_script(AROUND_SCRIPT)

    total = await show_key(rd, key)
    if total:
        await check_edges(rd, script, key, total)
        await check_latency(rd, script, key, total, args.rounds)
        await check_blocking(rd, script, key, total, args.rounds)
        print()

    await rd.aclose()


if __name__ == "__main__":
    asyncio.run(main())
