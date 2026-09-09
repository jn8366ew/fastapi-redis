from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as redis
from fastapi import FastAPI

# 실행 위치가 아니라 이 파일을 기준으로 잡습니다.
SCRIPT_DIR = Path(__file__).parent / "scripts"


def load_script(name: str) -> str:
    """scripts/<name>.lua 를 읽어 옵니다. register_script에 그대로 넘기면 됩니다."""
    return (SCRIPT_DIR / f"{name}.lua").read_text(encoding="utf-8")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 시작 시 Redis 커넥션 풀 생성
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    # Redis에 비밀번호가 설정된 경우
    # redis.from_url("redis://default:<비밀번호>@localhost:6379/0", decode_responses=True)

    yield

    # 서버 종료 시 안전하게 해제
    await app.state.redis.aclose()
