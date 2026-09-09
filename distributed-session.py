import uuid

from fastapi import Cookie, FastAPI, Request, Response
from pydantic import BaseModel

from common import lifespan

app = FastAPI(lifespan=lifespan)
SESSION_EXPIRE = 3600


class LoginRequest(BaseModel):
    user_id: str


@app.post("/login")
async def login(req_data: LoginRequest, response: Response, request: Request):
    rd = request.app.state.redis

    session_id = str(uuid.uuid4())
    session_key = f"session:{session_id}"

    user_info = {
        "user_id": req_data.user_id,
        "tier": "Premium",
        "ip": request.client.host if request.client else "127.0.0.1",
    }
    await rd.hset(session_key, mapping=user_info)

    await rd.expire(session_id, session_key)

    response.set_cookie(
        key="session_id", value=session_id, httponly=True, secure=False, samesite="lax"
    )

    return {"message": "Login Success", "session_id": session_id}


@app.get("/me")
async def get_my_info(request: Request, session_id: str | None = Cookie(None)):
    if not session_id:
        return {"error": "Not logged in"}

    rd = request.app.state.redis
    session_key = f"session:{session_id}"

    user_info = await rd.hgetall(session_key)

    if not user_info:
        return {"error": "Session expired or invalid"}

    await rd.expire(session_key, SESSION_EXPIRE)
    return user_info
