-- 내 주변 순위 조회: 순위 계산 -> 구간 조회를 한 덩어리로 처리합니다.
--
-- ZREVRANK로 순위를 받아 그 값으로 ZREVRANGE의 구간을 계산해야 하는데, 두
-- 명령을 따로 보내면 그 사이에 다른 유저의 ZINCRBY가 끼어들어 순위가 밀립니다.
-- 그러면 "내 주변"이라고 내려준 목록에 정작 내가 없거나, 표시 순위가 실제와
-- 어긋납니다.
--
-- MULTI/EXEC로는 풀 수 없습니다. 트랜잭션은 명령을 큐에 쌓아 뒀다가 EXEC에서
-- 한꺼번에 실행하므로 첫 명령의 결과를 둘째 명령의 인자로 쓸 수 없습니다.
-- 앞의 결과를 받아 뒤의 인자를 만들어야 하는 형태는 Lua뿐입니다.
--
-- 읽기만 하고 루프가 없어 O(log N + span*2+1) 안에 끝납니다. 스크립트가 도는
-- 동안 Redis 전체가 멈추므로, Lua에 넣어도 되는 것은 이렇게 짧은 것뿐입니다.
--
-- KEYS[1] = leaderboard:daily:<날짜>
-- ARGV[1] = user_id
-- ARGV[2] = 내 순위 기준 위아래로 몇 명까지 볼지
--
-- 반환 { 순위, 구간시작, 구간 }
--   순위      0-based. 랭킹에 없으면 -1
--   구간시작  ZREVRANGE에 실제로 쓴 시작 인덱스 (표시 순위 계산용)
--   구간      WITHSCORES 평탄 배열 {member, score, member, score, ...}

local rank = redis.call('ZREVRANK', KEYS[1], ARGV[1])

-- 멤버가 없으면 ZREVRANK는 Lua의 false를 돌려줍니다.
-- 테이블 안에 nil을 넣으면 그 지점에서 배열이 잘리므로, 모양은 세 칸으로
-- 고정해 두고 순위 -1을 없음의 표시로 씁니다.
if not rank then
    return {-1, 0, {}}
end

-- ARGV는 항상 문자열로 들어오므로 숫자 연산 전에 변환이 필요합니다.
local span = tonumber(ARGV[2])

-- 음수 인덱스는 Redis에서 "뒤에서부터"를 뜻합니다. 1~2위 유저를 조회할 때
-- 꼴찌 구간이 딸려오지 않도록 0에서 잘라줍니다. 뒤쪽은 범위를 넘겨도
-- Redis가 알아서 자르므로 손댈 필요가 없습니다.
local start = math.max(0, rank - span)

local rows = redis.call('ZREVRANGE', KEYS[1], start, rank + span, 'WITHSCORES')

-- rows를 Lua에서 가공하지 않고 그대로 넘깁니다. Lua 숫자를 반환하면 Redis
-- 정수로 변환되면서 소수점이 잘리는데, redis.call이 돌려준 값은 이미 문자열
-- 배열이라 손대지 않으면 점수의 정밀도가 그대로 보존됩니다.
return {rank, start, rows}
