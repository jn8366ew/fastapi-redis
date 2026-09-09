import asyncio
import json

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from common import lifespan

# 가상의 DB 데이터 (실제로는 MySQL, PostgreSQL 등에서 가져옵니다)
fake_db = {
    "user:1": {"name": "Kim", "email": "kim@example.com", "tier": "Gold"},
    "user:2": {"name": "Lee", "email": "lee@example.com", "tier": "Silver"},
}


app = FastAPI(lifespan=lifespan)


@app.get("/users/{user_id}")
async def get_user_profile(user_id: str, request: Request):
    rd = request.app.state.redis

    cache_key = f"user:profile:{user_id}"

    # 1. 레디스에서 캐시 확인
    cached_user = await rd.get(cache_key)

    if cached_user:
        # 캐시 Hit
        print(f"🚀 Cache Hit! (user_id: {user_id})")

        # Redis에 저장된 JSON 문자열을 파이썬 딕셔너리로 변환하여 반환
        return json.loads(cached_user)

    # 2. 캐시 Miss 시 실제 DB 조회
    print(f"🐌 Cache Miss! Fetching from DB... (user_id: {user_id})")

    user_data = fake_db.get(f"user:{user_id}")

    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")

    # DB 조회가 느리다고 가정 (2초 대기 시뮬레이션)
    await asyncio.sleep(2)

    # 3. Redis에 데이터 저장 (TTL 300초 설정)
    # 파이썬 딕셔너리를 JSON 문자열로 변환(dumps)하여 저장합니다.
    # [주의] setex 대신 최신 권장 문법인 set(..., ex=...)을 사용합니다.
    await rd.set(cache_key, json.dumps(user_data), ex=300)

    return user_data


# 정보 업데이트를 위한 Request Body 스키마 정의 (DB 스키마와 일치)
class UserProfileUpdate(BaseModel):
    name: str
    email: str
    tier: str


@app.put("/users/{user_id}")
async def update_user_profile(
    user_id: str, profile: UserProfileUpdate, request: Request
):
    rd = request.app.state.redis
    cache_key = f"user:profile:{user_id}"

    # 1. 실제 DB 데이터 업데이트 (가상)
    if f"user:{user_id}" in fake_db:
        fake_db[f"user:{user_id}"]["name"] = profile.name
        fake_db[f"user:{user_id}"]["email"] = profile.email
        fake_db[f"user:{user_id}"]["tier"] = profile.tier

        # 2. [핵심] 기존 캐시 삭제 (Cache Invalidation)
        # 다음 요청 시 Cache Miss가 발생하여, DB의 최신 데이터를 다시 캐싱하게 만듭니다.
        await rd.delete(cache_key)

        return {
            "message": "updated successfully",
            "current_data": fake_db[f"user:{user_id}"],
        }

    raise HTTPException(status_code=404, detail="User not found")
