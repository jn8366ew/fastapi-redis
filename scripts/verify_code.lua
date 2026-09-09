-- 인증번호 검증: 조회 -> 비교 -> (일치 시) 삭제를 한 덩어리로 처리합니다.
--
-- Redis는 Lua 스크립트를 단일 명령처럼 원자적으로 실행하므로 이 안에서는
-- 다른 클라이언트의 명령이 끼어들 수 없습니다. GETDEL과 달리 "일치할 때만"
-- 지우기 때문에 오타를 내도 재시도할 수 있고, 틀린 횟수 세기까지 같은 원자
-- 단위 안에 있어 동시 요청으로 시도 상한을 우회할 수 없습니다.
--
-- KEYS[1] = auth:code:<hash>      발급된 인증번호
-- KEYS[2] = auth:attempt:<hash>   틀린 횟수
-- ARGV[1] = 사용자가 입력한 코드
-- ARGV[2] = 최대 시도 횟수
--
-- 반환 { 상태, 남은시도 }
--    1  성공        (코드 삭제됨)
--    0  불일치      (코드 유지 -> 재시도 가능)
--   -1  만료/미발급
--   -2  시도 초과   (코드 폐기됨)

local saved = redis.call('GET', KEYS[1])

-- 키가 없으면 GET은 Lua의 false를 돌려줍니다.
if not saved then
    return {-1, 0}
end

if saved == ARGV[1] then
    redis.call('DEL', KEYS[1], KEYS[2])
    return {1, 0}
end

local attempts = redis.call('INCR', KEYS[2])

-- 시도 횟수 키가 인증번호와 같은 시점에 사라지도록 TTL을 맞춰 둡니다.
if attempts == 1 then
    local ttl = redis.call('TTL', KEYS[1])
    if ttl > 0 then
        redis.call('EXPIRE', KEYS[2], ttl)
    end
end

-- ARGV는 항상 문자열로 들어오므로 숫자 비교 전에 변환이 필요합니다.
if attempts >= tonumber(ARGV[2]) then
    redis.call('DEL', KEYS[1], KEYS[2])
    return {-2, 0}
end

return {0, tonumber(ARGV[2]) - attempts}
