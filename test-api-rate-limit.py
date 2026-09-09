import asyncio
import sys

import httpx
import redis.asyncio as redis

LIMIT = 5
TOTAL = 10

# 포트를 인자로 받습니다. 두 방식을 동시에 띄워놓고 비교할 때 씁니다.
#   uv run python test-api-rate-limit.py 8001
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000


async def reset_counters():
    """직전 실행이 남긴 카운터를 지웁니다.

    이게 없으면 두 번째 실행부터는 카운터가 6, 7, 8... 로 이어져서 전부 차단됩니다.
    테스트는 언제 돌려도 같은 결과가 나와야 하므로 시작 상태를 직접 만듭니다.

    [주의] KEYS는 전체를 훑는 명령이라 운영 환경에서는 쓰면 안 됩니다.
           키가 많으면 그 동안 Redis가 멈춥니다. 실무에서는 SCAN을 씁니다.
    """
    rd = redis.from_url("redis://localhost:6379/0", decode_responses=True)

    keys = await rd.keys("rate_limit:*")
    for key in keys:
        await rd.delete(key)

    await rd.aclose()
    return len(keys)


async def send_request(client, req_num):

    response = await client.get(f"http://localhost:{PORT}/data")
    if response.status_code == 200:
        print(f" 요청 {req_num:02d}: 성공, 통과")
        return True
    elif response.status_code == 429:
        print(
            f" 요청 {req_num:02d}: 차단. 많은 요청으로 - {response.json()['retry_after']} 대기"
        )
        return False
    else:
        print(f"{req_num:02d} 서버에러")
        return False


async def main():
    removed = await reset_counters()
    print(f"카운터 초기화 (키 {removed}개 삭제)")

    async with httpx.AsyncClient() as client:
        print(f"{TOTAL}개의 요청 시작 (포트 {PORT})")
        tasks = [send_request(client, i) for i in range(1, TOTAL + 1)]
        results = await asyncio.gather(*tasks)

    passed = sum(results)
    print(f"\n통과 {passed} / 차단 {TOTAL - passed}   (기대: 통과 {LIMIT})")
    print("판정:", "정상" if passed == LIMIT else f"이상! 통과가 {LIMIT}개가 아닙니다")


if __name__ == "__main__":
    asyncio.run(main())
