-- 락 해제: 조회 -> 비교 -> (내 락일 때만) 삭제를 한 덩어리로 처리합니다.
--
-- GET과 DEL을 따로 보내면 그 사이에 락이 TTL로 만료되고 다른 클라이언트가
-- 같은 이름으로 락을 새로 잡을 수 있습니다. 그러면 DEL이 남의 락을 지워서
-- 두 클라이언트가 동시에 임계구역에 들어갑니다. 락 값에 uuid를 넣어 둔
-- 의미가 사라지는 지점이라, 비교와 삭제는 반드시 한 원자 단위여야 합니다.
--
-- "값이 일치할 때만 삭제"하는 단일 명령은 Redis에 없습니다. GETDEL은 조건
-- 없이 지워버리므로 쓸 수 없어서 Lua로 직접 만듭니다.
--
-- KEYS[1] = lock:item:<id>   락 키
-- ARGV[1] = 락을 획득할 때 발급한 uuid
--
-- 반환  1  내 락이 맞아서 해제함
--       0  이미 만료됐거나 남의 락이라 건드리지 않음

local token = redis.call('GET', KEYS[1])

-- 키가 없으면 GET은 Lua의 false를 돌려줍니다.
if not token or token ~= ARGV[1] then
    return 0
end

return redis.call('DEL', KEYS[1])
