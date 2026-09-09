# fastapi-redis

> Redis를 실무에서 쓸 때 부딪히는 개념들을 FastAPI 예제로 하나씩 확인해보는 저장소.

캐시 무효화, 중복 요청, 세션 저장소, 분산 락처럼 **"Redis를 쓰면 해결되는 줄 알았는데 안 되는"** 지점들을 주제별로 하나씩 만들어보고, 왜 안 되는지 실제로 재현한 다음 고치는 방식으로 정리합니다.

각 `.py` 파일은 **독립 실행 가능한 FastAPI 앱**이고 주제 하나씩만 다룹니다. 서로 import하지 않습니다.

---

## 실행

### 준비물

| | |
|---|---|
| Python | 3.11+ (`.python-version` 참고) |
| 패키지 | [uv](https://docs.astral.sh/uv/) |
| Redis | localhost:6379 |

```bash
# Redis (Docker)
docker run --name redis -p 6379:6379 -d redis:7

# 의존성 설치
uv sync
```

### 앱 실행

파일명에 하이픈이 있어 모듈 경로(`uvicorn distributed-lock:app`)로는 임포트되지 않습니다. **파일 경로로 실행**하세요.

```bash
uv run fastapi dev distributed-lock.py
```

- API 문서: http://localhost:8000/docs
- Redis에 비밀번호가 걸려 있으면 `common.py`의 연결 문자열을 수정합니다.

---

## 구성

| 파일 | 주제 | 핵심 Redis 기능 |
|---|---|---|
| `main.py` | Cache-Aside 패턴과 캐시 무효화 | `SET ex` / `GET` / `DEL` |
| `shop-list.py` | 최근 본 상품 (중복 제거 + 개수 상한) | List — `LREM` `LPUSH` `LTRIM` |
| `consistency.py` | 조회수 중복 방지, 좋아요 정합성 | Set — `SADD` `SCARD` `SISMEMBER` |
| `temporary-code.py` | SMS 인증번호 발급/검증 | Pipeline, **Lua** |
| `distributed-session.py` | 분산 환경 세션 저장소 | Hash — `HSET` `HGETALL`, 슬라이딩 TTL |
| `distributed-lock.py` | 분산 락 | `SET NX PX`, **Lua**, watchdog |
| `test-dis-lock.py` | 분산 락 동시성 테스트 클라이언트 | `httpx` + `asyncio.gather` |
| `common.py` | 공통 lifespan, Lua 로더 | — |
| `scripts/*.lua` | 원자성이 필요한 연산들 | — |
| `docs/CACHE-ASIDE-PLAN.md` | Cache-Aside 효과 측정 실험 기획서 | — |

---

## 주제별 정리

### `main.py` — Cache-Aside

가장 기본이 되는 캐싱 패턴. **읽을 때** 캐시를 먼저 보고 없으면 DB에서 가져와 채우고, **쓸 때** 캐시를 지웁니다(갱신이 아니라 삭제).

```
GET  /users/{user_id}    캐시 확인 → miss면 DB 조회(2초) 후 TTL 300초로 저장
PUT  /users/{user_id}    DB 갱신 후 캐시 DEL
```

갱신이 아니라 삭제하는 이유는, 갱신하려면 "DB 쓰기"와 "캐시 쓰기" 두 개의 순서를 맞춰야 하는데 그 사이에 다른 요청이 끼면 **오래된 값이 캐시에 영구히 박히기** 때문입니다. 지우면 다음 읽기가 알아서 최신값을 채웁니다.

### `shop-list.py` — 최근 본 상품

List 하나로 "중복 없이, 최신순으로, 최대 5개"를 구현합니다.

```python
await rd.lrem(key, 0, product_id)   # 기존 항목 제거 (있든 없든)
await rd.lpush(key, product_id)     # 맨 앞에 추가
await rd.ltrim(key, 0, 4)           # 5개로 자르기
```

`LTRIM`이 핵심입니다. 애플리케이션에서 개수를 세고 자르면 그 사이에 다른 요청이 끼어들지만, Redis 명령으로 넘기면 그럴 일이 없습니다.

### `consistency.py` — 조회수 / 좋아요

`INCR`만 쓰면 새로고침 연타에 조회수가 그대로 올라갑니다. **Set의 반환값**으로 막습니다.

```python
is_new_viewer = await rd.sadd(viewer_key, user_id)   # 새로 추가면 1, 이미 있으면 0
if is_new_viewer:
    current_views = await rd.incr(view_key)
```

`SADD`는 "추가됐는지"를 **원자적으로** 알려줍니다. 판정과 추가가 한 명령이라 아무리 빠르게 연타해도 `1`을 받는 요청은 정확히 하나뿐입니다.

좋아요는 아예 카운터를 두지 않고 **Set 하나를 진실의 원천**으로 씁니다(`SCARD`로 세기). 카운터와 목록을 따로 두면 반드시 어긋나는데, 애초에 하나만 두면 어긋날 대상이 없습니다.

중복 방지 Set은 날짜별 키(`...:viewers:20260909`)로 만들고 TTL로 자동 회수합니다. TTL은 **키가 새로 생겼을 때만** 겁니다 — 매번 걸면 조회가 이어지는 동안 만료가 계속 뒤로 밀립니다.

### `temporary-code.py` — SMS 인증번호

발급은 파이프라인, 검증은 Lua입니다.

```
POST /auth/send      6자리 코드 발급 (TTL 300초), 이전 시도 횟수 초기화
POST /auth/verify    코드 검증
```

검증이 Lua여야 하는 이유는 `scripts/verify_code.lua`가 하는 일을 보면 명확합니다 — **조회 → 비교 → (일치 시) 삭제 → (불일치 시) 시도 횟수 증가**가 전부 한 덩어리여야 합니다. 나눠 보내면 동시 요청으로 시도 상한을 우회할 수 있습니다.

코드 생성에 `random`이 아니라 `secrets`를 쓴 것도 의도적입니다. 인증번호는 예측 불가능해야 합니다.

### `distributed-session.py` — 세션 저장소

서버가 여러 대일 때 인메모리 세션은 못 씁니다. Hash로 Redis에 둡니다.

```
POST /login    세션 생성, HttpOnly 쿠키 발급
GET  /me       쿠키의 session_id로 조회 + TTL 갱신 (슬라이딩 만료)
```

조회할 때마다 `EXPIRE`를 다시 걸어 **활동 중인 사용자는 로그아웃되지 않게** 합니다.

### `distributed-lock.py` — 분산 락

이 저장소에서 가장 깊게 들어간 주제. 세 단계로 쌓아올립니다.

**① 획득 — `SET NX PX`**

```python
await rd.set(lock_name, identifier, nx=True, px=lock_timeout_ms)
```

"없으면 쓰고 동시에 TTL 걸기"가 단일 명령이라 그 자체로 원자적입니다. 값에 uuid를 넣는 건 "남의 락은 안 지운다"를 판별하기 위해서입니다. 획득에 실패하면 0.1초마다 재시도하는 **스핀락**입니다.

**② 해제 — Lua (`scripts/release_lock.lua`)**

```python
# 이렇게 하면 안 됩니다
if await rd.get(lock_name) == identifier:   # ①
    await rd.delete(lock_name)              # ②
```

①과 ② 사이에 락이 TTL로 만료되고 다른 클라이언트가 같은 이름으로 락을 새로 잡을 수 있습니다. 그러면 ②가 **남의 락을 지웁니다.** uuid를 넣은 의미가 통째로 사라지는 지점입니다.

"값이 일치할 때만 삭제"하는 단일 명령이 Redis에 없어서(`GETDEL`은 조건 없이 지웁니다) Lua로 직접 만듭니다. 관찰용 함수로 원자적이지 않은 `release_lock()`도 함께 남겨뒀습니다.

**③ watchdog — Lua (`scripts/extend_lock.lua`)**

Lua로 해제를 고쳐도 **작업이 TTL보다 오래 걸리면** 락이 손안에서 먼저 풀립니다. 백그라운드 태스크가 TTL의 1/3마다 깨어나 TTL을 되돌립니다.

```python
watchdog = asyncio.create_task(lock_watchdog(extend_script, lock_name, lock_id))
try:
    ...  # 작업
finally:
    watchdog.cancel()
```

연장도 원자적이어야 합니다. 확인과 `PEXPIRE`를 나눠 보내면 **남의 락 수명을 내가 늘려주는** 사고가 납니다.

프로세스가 죽으면 watchdog도 같이 죽어 TTL로 락이 회수됩니다. 이게 "무한 락"이 아니라 "갱신되는 리스"로 만들어주는 안전장치입니다.

---

## 분산 락 실험해보기

`distributed-lock.py` 상단의 상수 두 개로 시나리오를 바꿉니다.

```python
LOCK_TIMEOUT_MS = 5000   # 락 수명
WORK_SECONDS = 6         # 임계구역에서 하는 작업 길이
```

서버를 띄우고 다른 터미널에서 동시 요청 10개를 보냅니다.

```bash
uv run fastapi dev distributed-lock.py     # 터미널 1
uv run python test-dis-lock.py             # 터미널 2
```

서버 로그에 밀리초 타임스탬프와 함께 획득/연장/해제가 찍힙니다. `임계구역=N명` 카운터가 **2 이상이면 상호배제가 깨진 것**입니다.

| 실험 | 설정 | 결과 |
|---|---|---|
| 정상 | `WORK_SECONDS = 2` | 임계구역 계속 1명, 해제 시 `Lua=1` |
| 락 유실 | `WORK_SECONDS = 6` + watchdog 제거 | `!! 상호배제 깨짐` 발생, 해제 시 `Lua=0` |
| watchdog | `WORK_SECONDS = 6` | TTL 5초로 6초 작업 완주, 임계구역 1명 유지 |

두 번째 실험에서 `Lua=0`이 나오는 게 **Lua가 남의 락 삭제를 막아낸 순간**입니다. 락이 깨진 건 해제 때문이 아니라 TTL이 먼저 끝났기 때문이고, 그래서 watchdog이 필요하다는 흐름으로 이어집니다.

---

## Lua 스크립트

| 파일 | 하는 일 | 원자성이 필요한 이유 |
|---|---|---|
| `verify_code.lua` | 인증번호 조회→비교→삭제→시도 카운트 | 나누면 동시 요청으로 시도 상한 우회 |
| `release_lock.lua` | 락 조회→비교→삭제 | 나누면 남의 락을 지움 |
| `extend_lock.lua` | 락 조회→비교→TTL 갱신 | 나누면 남의 락 수명을 늘려줌 |

Redis는 스크립트를 **단일 명령처럼** 실행합니다. 도는 동안 다른 클라이언트 명령이 끼어들지 못하므로, 여러 단계를 한 덩어리로 묶을 수 있습니다.

`MULTI/EXEC`로는 안 됩니다. 큐에 쌓는 시점에 `GET` 결과를 알 수 없어 **분기를 짤 수 없기** 때문입니다. `WATCH`를 쓴 낙관적 락으로 우회할 수는 있지만 왕복 4~5회에 재시도 루프까지 직접 짜야 합니다.

로딩과 등록은 `common.py`의 `load_script()`를 씁니다.

```python
SCRIPT = load_script("release_lock")            # scripts/release_lock.lua 읽기
app.state.release_script = rd.register_script(SCRIPT)   # 동기 함수, 연결 생성 후 호출
result = await app.state.release_script(keys=[...], args=[...])
```

`register_script`는 코루틴이 아니라 동기 함수이고, SHA1 계산에 클라이언트 인코더가 필요해서 **연결 객체가 생긴 뒤에** 불러야 합니다. 호출 시엔 `EVALSHA`를 쓰고, 서버가 스크립트를 모르면 자동으로 다시 적재합니다.

---

## 알려진 한계

분산 락은 여기까지 와도 완성이 아닙니다. 다음 단계로 남아 있는 것들:

- **스핀락의 재시도 트래픽** — 대기자가 많을수록 0.1초마다 `SET NX`가 쏟아집니다. Java의 Redisson은 폴링 대신 **pub/sub**으로 대기를 처리합니다. Redis 리스트를 대기열로 쓰는 **`BLPOP` 핸드오프**도 대안입니다(`LPUSH` 하나가 정확히 한 명만 깨움).
- **순서 보장 없음** — 먼저 요청한 사람이 먼저 처리된다는 보장이 전혀 없습니다(barging). 실행할 때마다 성공하는 유저가 바뀝니다.
- **watchdog도 만능이 아님** — 프로세스가 살아있는데 멈추면(GC pause, 네트워크 단절) 연장을 못 해 락을 잃지만 **작업은 계속 굴러갑니다.** 완전한 보장은 스토리지 쪽에서 토큰을 검증하는 **fencing token**이 필요합니다.
- **무한 연장 위험** — 작업이 끝나지 않으면 watchdog이 영원히 연장해 데드락이 됩니다. 최대 연장 횟수/시간 상한이 필요합니다.
- **마스터 장애 시 락 유실** — 복제가 비동기라 페일오버 중 락이 사라질 수 있습니다. Redlock 또는 다른 합의 스토리지의 영역입니다.
