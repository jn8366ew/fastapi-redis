import asyncio
import time

import httpx


async def send_request(client, item_id, req_num):
    print(f"요청 {req_num} 진행")
    try:
        response = await client.post(
            f"http://localhost:8000/stock/reduce/{item_id}?user_id=User_{req_num}",
            timeout=15.0,
        )
        print(f"요청 {req_num} 결과: {response.json()}")
    except Exception as e:
        print(f"요청 {req_num} 에러: {e}")


async def main():
    start_time = time.time()

    async with httpx.AsyncClient() as client:
        tasks = [send_request(client, "100", i) for i in range(1, 11)]
        await asyncio.gather(*tasks)

    print(f"총 소요시간: {time.time() - start_time:.2f}초")


if __name__ == "__main__":
    asyncio.run(main())
