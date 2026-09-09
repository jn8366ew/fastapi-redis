import hashlib
import secrets
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from common import load_script

# 인증번호 유효 시간 (300초 = 5분)
AUTH_TIMEOUT = 300

# 인증번호 하나당 허용할 최대 시도 횟수. 초과하면 코드를 폐기합니다.
MAX_ATTEMPTS = 5

VERIFY_SCRIPT = load_script("verify_code")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Redis 연결 풀 생성
    app.state.redis = redis.from_url("redis://localhost:6379/0", decode_responses=True)

    # Redis에 비밀번호가 설정된 경우
    # app.state.redis = redis.from_url("redis://default:<비밀번호>@localhost:6379/0", decode_responses=True)

    # Lua 스크립트 등록. register_script는 코루틴이 아니라 동기 함수이고,
    # SHA1 계산에 클라이언트의 인코더가 필요해서 연결 객체가 생긴 뒤에 불러야 합니다.
    # 호출 시에는 EVALSHA를 쓰고, 서버가 스크립트를 모르면 자동으로 다시 적재합니다.
    app.state.verify_script = app.state.redis.register_script(VERIFY_SCRIPT)

    yield
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


def auth_keys(phone: str) -> tuple[str, str]:
    """(인증번호 키, 시도횟수 키). 전화번호는 해시해서 키에 넣습니다."""
    hashed = hashlib.sha256(phone.encode()).hexdigest()
    return f"auth:code:{hashed}", f"auth:attempt:{hashed}"


class SendCodeRequest(BaseModel):
    phone: str


class VerifyCodeRequest(BaseModel):
    phone: str
    input_code: str


@app.post("/auth/send")
async def send_verification_code(req_data: SendCodeRequest, request: Request):
    rd = request.app.state.redis

    # random 대신 secrets - 인증번호는 예측 불가능해야 합니다.
    code = f"{secrets.randbelow(1_000_000):06d}"

    code_key, attempt_key = auth_keys(req_data.phone)

    # 새 코드를 내주면서 이전 시도 횟수도 함께 지웁니다.
    # 안 지우면 직전에 다 틀린 사용자가 재발급을 받아도 첫 시도에서 바로 막힙니다.
    async with rd.pipeline(transaction=True) as pipe:
        await pipe.set(code_key, code, ex=AUTH_TIMEOUT).delete(attempt_key).execute()

    print(f"SMS 발송 To: {req_data.phone}, code: {code}")
    return {
        "message": "Verification code sent",
        "expires_in": AUTH_TIMEOUT,
    }


@app.post("/auth/verify")
async def verify_code(req_data: VerifyCodeRequest, request: Request):
    code_key, attempt_key = auth_keys(req_data.phone)

    status, remaining = await request.app.state.verify_script(
        keys=[code_key, attempt_key],
        args=[req_data.input_code, MAX_ATTEMPTS],
    )

    if status == 1:
        return {"message": "Authentication successful"}

    if status == 0:
        raise HTTPException(
            status_code=400, detail=f"Invalid code. {remaining} attempt(s) left"
        )

    if status == -1:
        raise HTTPException(status_code=400, detail="Code expired or not requested")

    raise HTTPException(
        status_code=429, detail="Too many failed attempts. Request a new code"
    )
