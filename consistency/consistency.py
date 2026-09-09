from datetime import UTC, datetime

from fastapi import FastAPI, Request

from common import lifespan

app = FastAPI(lifespan=lifespan)

# 조회수 중복 방지 창(窓). 하루 단위로 Set을 새로 만들고 TTL로 자동 회수합니다.
VIEW_DEDUP_TTL = 60 * 60 * 24


def viewer_key_of(article_id: str) -> str:
    """오늘(UTC) 날짜가 붙은 조회자 Set 키. 날짜가 바뀌면 자연히 새 키를 쓰게 됩니다."""
    today = datetime.now(UTC).strftime("%Y%m%d")
    return f"article:{article_id}:viewers:{today}"


@app.post("/articles/{articles_id}/view")
async def increase_view_count(
    articles_id: str, request: Request, user_id: str = "user_1"
):
    rd = request.app.state.redis
    view_key = f"article:{articles_id}:views"
    viewer_key = viewer_key_of(articles_id)

    # SADD는 "새로 추가됐으면 1, 이미 있었으면 0"을 돌려줍니다.
    # 이 판정 자체가 Redis 안에서 원자적으로 끝나므로, 같은 유저가 새로고침을
    # 아무리 빠르게 연타해도 1을 받는 요청은 정확히 하나뿐입니다.
    is_new_viewer = await rd.sadd(viewer_key, user_id)

    if is_new_viewer:
        # 키가 방금 만들어졌을 때만 TTL을 겁니다.
        # (매번 걸면 조회가 이어지는 동안 만료가 계속 뒤로 밀립니다)
        await rd.expire(viewer_key, VIEW_DEDUP_TTL)

        current_views = await rd.incr(view_key)

        if current_views % 100 == 0:
            print(
                f"백업 - Article {articles_id} reached {current_views} views. Syncing to DB..."
            )
    else:
        # 중복 조회 - 카운터는 건드리지 않고 현재 값만 읽어서 돌려줍니다.
        current_views = int(await rd.get(view_key) or 0)

    return {
        "article_id": articles_id,
        "total_views": current_views,
        "counted": bool(is_new_viewer),
    }


@app.post("/articles/{article_id}/like")
async def like_article(article_id: str, request: Request, user_id: str = "user_1"):
    rd = request.app.state.redis
    likers_key = f"article:{article_id}:likers"

    # 별도 카운터 없이 Set 하나가 진실의 원천입니다.
    # 이미 눌렀던 유저가 다시 눌러도 SADD가 0을 돌려줄 뿐, 개수는 어긋나지 않습니다.
    added = await rd.sadd(likers_key, user_id)
    likes = await rd.scard(likers_key)

    return {
        "article_id": article_id,
        "liked": True,
        "changed": bool(added),
        "likes": likes,
    }


@app.delete("/articles/{article_id}/like")
async def unlike_article(article_id: str, request: Request, user_id: str = "user_1"):
    rd = request.app.state.redis
    likers_key = f"article:{article_id}:likers"

    removed = await rd.srem(likers_key, user_id)
    likes = await rd.scard(likers_key)

    return {
        "article_id": article_id,
        "liked": False,
        "changed": bool(removed),
        "likes": likes,
    }


@app.get("/articles/{article_id}/stats")
async def get_article_stats(article_id: str, request: Request, user_id: str = "user_1"):
    rd = request.app.state.redis
    view_key = f"article:{article_id}:views"
    likers_key = f"article:{article_id}:likers"
    viewer_key = viewer_key_of(article_id)

    views = await rd.get(view_key)
    likes = await rd.scard(likers_key)
    liked_by_me = await rd.sismember(likers_key, user_id)
    unique_viewers_today = await rd.scard(viewer_key)

    return {
        "article_id": article_id,
        "views": int(views) if views else 0,
        "likes": likes,
        "liked_by_me": bool(liked_by_me),
        "unique_viewers_today": unique_viewers_today,
    }
