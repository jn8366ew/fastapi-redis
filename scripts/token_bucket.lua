-- 토큰 버킷: 충전 -> 판정 -> 소비 -> 저장을 한 덩어리로 처리합니다.
--
-- 고정 창의 INCR과 달리 토큰 버킷은 "읽고 -> 계산하고 -> 쓰는" 구조입니다.
-- 이걸 애플리케이션에서 나눠서 하면 동시 요청 둘이 같은 잔여량을 읽고
-- 둘 다 통과시켜 버립니다. 토큰 1개로 요청 2개가 지나가는 것이라,
-- 여기서 Lua는 선택이 아니라 필수입니다.
--
-- 시각도 Redis에서 가져옵니다. 앱 서버가 여러 대일 때 각자의 시계를 쓰면
-- 시계 차이만큼 제한이 어긋나므로, 시계도 하나로 통일합니다.
--
-- KEYS[1] = rate_limit:bucket:<식별자>
-- ARGV[1] = 양동이 용량 (순간 최대 허용량)
-- ARGV[2] = 초당 충전 토큰 수
-- ARGV[3] = 놀고 있는 버킷을 정리할 TTL (초)
--
-- 반환 { 통과여부, 남은토큰, 이번에_충전된양, 다음_토큰까지_대기초 }
--      소수점이 필요한 값은 문자열로 돌려줍니다. Lua가 Redis로 숫자를
--      넘길 때 소수점 아래가 잘려나가기 때문입니다.

local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

-- 처음 보는 상대는 가득 찬 양동이로 시작합니다.
-- 그래서 "키가 없는 것"과 "가득 찬 것"이 같은 의미가 되고,
-- TTL로 오래된 버킷을 지워도 아무 문제가 없습니다.
if tokens == nil then
    tokens = capacity
    last_refill = now
end

-- lazy refill: 타이머로 채우는 것이 아니라, 요청이 온 지금 시점에
-- "마지막으로 본 이후 이만큼 흘렀으니 이만큼 찼겠다"를 계산합니다.
-- 사용자가 백만 명이어도 타이머는 하나도 필요 없습니다.
local elapsed = math.max(0, now - last_refill)
local refilled = elapsed * refill_rate
tokens = math.min(capacity, tokens + refilled)

local allowed = 0
if tokens >= 1 then
    allowed = 1
    tokens = tokens - 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], ttl)

-- 차단된 경우 다음 토큰이 찰 때까지 남은 시간. Retry-After 로 내려줍니다.
local wait = 0
if allowed == 0 then
    wait = (1 - tokens) / refill_rate
end

return { allowed, tostring(tokens), tostring(refilled), tostring(wait) }
