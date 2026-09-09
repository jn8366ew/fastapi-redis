from fastapi import FastAPI, Request

from common import lifespan

app = FastAPI(lifespan=lifespan)


@app.post("/products/{product_id}/view")
async def view_product(product_id: str, request: Request, user_id: str = "user_1"):
    rd = request.app.state.redis

    key = f"user:{user_id}:recent_views"

    await rd.lrem(key, 0, product_id)
    await rd.lpush(key, product_id)
    await rd.ltrim(key, 0, 4)

    return {"message": f"Product {product_id} added to recent views"}


@app.get("/users/{user_id}/recent-views")
async def get_recent_views(user_id: str, request: Request):
    rd = request.app.state.redis
    key = f"user:{user_id}:recent_views"

    views = await rd.lrange(key, 0, -1)
    return {"recent_views": views}
