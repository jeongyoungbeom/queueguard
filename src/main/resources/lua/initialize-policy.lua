-- KEYS: 같은 슬롯의 config 및 상태 키. ARGV: schemaVersion=2 정책 field/value 쌍.
local config = KEYS[1]
local kind = redis.call('TYPE', config).ok
if kind == 'none' then
    for i = 2, #KEYS do if redis.call('EXISTS', KEYS[i]) == 1 then return -2 end end
    redis.call('HSET', config, unpack(ARGV))
    redis.call('HSET', config, 'lastSequence', '0')
    return 1
end
if kind ~= 'hash' then return -2 end
local version = redis.call('HGET', config, 'schemaVersion')
if (version ~= '1' and version ~= '2') or redis.call('HLEN', config) ~= (version == '1' and 8 or 9) then return -2 end
for i = 1, #ARGV, 2 do
    if ARGV[i] ~= 'schemaVersion' and redis.call('HGET', config, ARGV[i]) ~= ARGV[i + 1] then return -1 end
end
local types = {'hash', 'string', 'zset', 'zset', 'zset', 'hash', 'hash', 'hash', 'hash', 'zset'}
for i = 2, #KEYS do
    local t = redis.call('TYPE', KEYS[i]).ok
    if t ~= 'none' and t ~= types[i] then return -2 end
end
local counter = redis.call('GET', KEYS[2])
if counter then
    if not string.match(counter, '^%d+$') or #counter > 16 or tonumber(counter) > 9007199254740991 then return -2 end
else
    for i = 3, #KEYS do if redis.call('EXISTS', KEYS[i]) == 1 then return -2 end end
end
if version == '2' and redis.call('HGET', config, 'lastSequence') ~= (counter or '0') then return -2 end
local waiting, active, terminal = redis.call('ZCARD', KEYS[3]), redis.call('ZCARD', KEYS[5]), redis.call('ZCARD', KEYS[10])
local admissions = redis.call('HLEN', KEYS[8])
if waiting ~= redis.call('ZCARD', KEYS[4]) or redis.call('HLEN', KEYS[7]) ~= waiting + active + terminal
    or redis.call('HLEN', KEYS[6]) < waiting + active or admissions < active or admissions > active + terminal
    or (admissions > 0 and redis.call('EXISTS', KEYS[9]) == 0) then return -2 end
if redis.call('EXISTS', KEYS[9]) == 1 then
    if redis.call('HLEN', KEYS[9]) ~= 2 then return -2 end
    local w, c = redis.call('HGET', KEYS[9], 'window'), redis.call('HGET', KEYS[9], 'count')
    if not w or not c or not string.match(w, '^%d+$') or #w > 16
        or not string.match(c, '^%d+$') or #c > 6
        or tonumber(w) > 9007199254740991
        or tonumber(c) > tonumber(redis.call('HGET', config, 'admissionRatePerSecond')) then return -2 end
end
if version == '1' then
    -- 이전 상태를 지우지 않고 검증한 순번을 보존 기준으로 승격한다.
    redis.call('HSET', config, 'schemaVersion', '2', 'lastSequence', counter or '0')
    return 2
end
return 0
