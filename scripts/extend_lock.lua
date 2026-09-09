-- 락 연장(watchdog): 조회 -> 비교 -> (내 락일 때만) TTL 갱신을 한 덩어리로 처리합니다.
--
-- 해제와 같은 이유로 원자성이 필요합니다. GET으로 확인하고 PEXPIRE를 따로
-- 보내면, 그 사이에 락이 만료되고 다른 클라이언트가 새로 잡았을 때 남의 락
-- 수명을 내가 늘려주게 됩니다. 그 클라이언트는 자기가 정한 TTL보다 오래
-- 락을 붙들게 되어 상황이 더 나빠집니다.
--
-- KEYS[1] = lock:item:<id>   락 키
-- ARGV[1] = 락을 획득할 때 발급한 uuid
-- ARGV[2] = 새로 설정할 TTL (밀리초)
--
-- 반환  1  내 락이 맞아서 TTL을 갱신함
--       0  이미 만료됐거나 남의 락이라 건드리지 않음 (= 락을 잃은 상태)

local token = redis.call('GET', KEYS[1])

if not token or token ~= ARGV[1] then
    return 0
end

return redis.call('PEXPIRE', KEYS[1], ARGV[2])
