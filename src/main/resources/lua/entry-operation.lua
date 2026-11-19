-- KEYS: config, sequence, waiting, waiting-expirations, active, subjects, entries, admissions, rate, gc.
-- ARGV: operation, subjectId, entryId, capacity, waiting TTL, retention, active limit, rate, admission TTL, candidates JSON, admissionId, token, requestId, fingerprint.
local config, sequence, waiting, expirations, active = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local subjects, entries, admissions, rate, gc = KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[10]
local operation, subjectId, entryId = ARGV[1], ARGV[2], ARGV[3]
local MAX_SEQUENCE, BATCH = 9007199254740991, 100
local function requireState(ok) if not ok then error('Queue state unavailable') end end
local function decode(raw)
    requireState(raw)
    local ok, value = pcall(cjson.decode, raw)
    requireState(ok and type(value) == 'table')
    return value
end
local function timestamp(value)
    return type(value) == 'number' and value > 0 and value <= MAX_SEQUENCE and value == math.floor(value)
end
local expectedTypes = {'hash', 'string', 'zset', 'zset', 'zset', 'hash', 'hash', 'hash', 'hash', 'zset'}
for i = 1, #KEYS do
    local kind = redis.call('TYPE', KEYS[i]).ok
    requireState(kind == expectedTypes[i] or (i ~= 1 and kind == 'none'))
end
requireState(redis.call('HLEN', config) == 9 and redis.call('HGET', config, 'schemaVersion') == '2'
    and redis.call('HGET', config, 'enabled') == 'true')
local limits = {
    {'maxWaitingEntries', 4, 1, 10000000}, {'waitingTtlSeconds', 5, 1, 604800},
    {'terminalRetentionSeconds', 6, 86400, 2592000}, {'maxActiveAdmissions', 7, 1, 1000000},
    {'admissionRatePerSecond', 8, 1, 100000}, {'admissionTtlSeconds', 9, 1, 86400}
}
for _, limit in ipairs(limits) do
    local raw = redis.call('HGET', config, limit[1])
    local value = tonumber(raw)
    requireState(raw == ARGV[limit[2]] and value and value == math.floor(value) and value >= limit[3] and value <= limit[4])
end
local capacity, ttl, retention = tonumber(ARGV[4]), tonumber(ARGV[5]) * 1000, tonumber(ARGV[6]) * 1000
local activeLimit, rateLimit, admissionTtl = tonumber(ARGV[7]), tonumber(ARGV[8]), tonumber(ARGV[9]) * 1000
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local window = tonumber(clock[1])
local counter = redis.call('GET', sequence)
local floor = redis.call('HGET', config, 'lastSequence')
requireState(floor and string.match(floor, '^%d+$') and #floor <= 16 and tonumber(floor) <= MAX_SEQUENCE)
requireState((counter and counter == floor) or (not counter and floor == '0'))
if not counter then
    for i = 3, #KEYS do requireState(redis.call('EXISTS', KEYS[i]) == 0) end
else
    requireState(string.match(counter, '^%d+$') and #counter <= 16)
    counter = tonumber(counter)
    requireState(counter >= 0 and counter <= MAX_SEQUENCE)
end
requireState(redis.call('ZCARD', waiting) == redis.call('ZCARD', expirations))
local waitingSize, activeSize, terminalSize = redis.call('ZCARD', waiting), redis.call('ZCARD', active), redis.call('ZCARD', gc)
requireState(redis.call('HLEN', entries) == waitingSize + activeSize + terminalSize)
requireState(redis.call('HLEN', subjects) >= waitingSize + activeSize)
local admissionSize = redis.call('HLEN', admissions)
requireState(admissionSize >= activeSize and admissionSize <= activeSize + terminalSize)
requireState(admissionSize == 0 or redis.call('EXISTS', rate) == 1)
local rateWindow, rateCount
if redis.call('EXISTS', rate) == 1 then
    requireState(redis.call('HLEN', rate) == 2)
    local w, c = redis.call('HGET', rate, 'window'), redis.call('HGET', rate, 'count')
    requireState(w and c and string.match(w, '^%d+$') and #w <= 16 and string.match(c, '^%d+$') and #c <= 6)
    rateWindow, rateCount = tonumber(w), tonumber(c)
    requireState(rateWindow <= MAX_SEQUENCE and rateCount <= rateLimit)
end
local function envelope(value)
    value.code = value.code or 'OK'
    value.waitingCount = redis.call('ZCOUNT', expirations, '(' .. now, '+inf')
    value.activeCount = redis.call('ZCOUNT', active, '(' .. now, '+inf')
    value.windowCount = rateWindow == window and rateCount or 0
    value.now = now
    return cjson.encode(value)
end
local function result(code) return envelope({code = code}) end
local function isActive(entry) return entry.state == 'ADMITTED' or entry.state == 'CLAIMED' end
local function isLive(entry) return entry.state == 'WAITING' or isActive(entry) end
local admissionCache = {}
local function load(id)
    local entry = decode(redis.call('HGET', entries, id))
    requireState(entry.entryId == id and type(entry.subjectId) == 'string' and #entry.subjectId >= 1 and #entry.subjectId <= 128
        and type(entry.sequence) == 'string' and string.match(entry.sequence, '^%d+$') and #entry.sequence <= 16
        and timestamp(entry.waitingExpiresAt))
    local position = tonumber(entry.sequence)
    requireState(position >= 1 and position <= MAX_SEQUENCE and counter and position <= counter)
    local score, deadline = redis.call('ZSCORE', waiting, id), redis.call('ZSCORE', expirations, id)
    local deletion = redis.call('ZSCORE', gc, 'entry:' .. id)
    local live = isLive(entry)
    if live then
        requireState(not deletion and redis.call('HGET', subjects, entry.subjectId) == id)
    else
        requireState((entry.state == 'CANCELED' or entry.state == 'EXPIRED' or entry.state == 'RELEASED')
            and timestamp(entry.terminatedAt) and timestamp(entry.retainUntil)
            and entry.retainUntil == entry.terminatedAt + retention and deletion and tonumber(deletion) == entry.retainUntil)
    end
    if entry.state == 'WAITING' then
        requireState(not entry.admissionId and not entry.expiresAt and score and tonumber(score) == position
            and deadline and tonumber(deadline) == entry.waitingExpiresAt)
    else
        requireState(not score and not deadline)
    end
    if entry.admissionId then
        requireState(type(entry.admissionId) == 'string' and timestamp(entry.expiresAt))
        local admission = decode(redis.call('HGET', admissions, entry.admissionId))
        requireState(admission.admissionId == entry.admissionId and admission.entryId == id
            and admission.subjectId == entry.subjectId and admission.state == entry.state and admission.expiresAt == entry.expiresAt
            and type(admission.admissionToken) == 'string' and #admission.admissionToken == 43
            and string.match(admission.admissionToken, '^[%w_%-]+$'))
        local bound = admission.requestId ~= nil or admission.requestFingerprint ~= nil
        if bound then
            requireState(type(admission.requestId) == 'string' and #admission.requestId >= 1 and #admission.requestId <= 128
                and string.match(admission.requestId, '^[%w][%w%.%_:%@%-]*$')
                and type(admission.requestFingerprint) == 'string' and #admission.requestFingerprint == 64
                and string.match(admission.requestFingerprint, '^[0-9a-f]+$'))
        end
        requireState((entry.state == 'ADMITTED' or entry.state == 'CANCELED') and not bound
            or (entry.state == 'CLAIMED' or entry.state == 'RELEASED') and bound or entry.state == 'EXPIRED')
        local activeScore = redis.call('ZSCORE', active, entry.admissionId)
        if isActive(entry) then
            requireState(activeScore and tonumber(activeScore) == entry.expiresAt)
        else
            requireState(not activeScore and admission.terminatedAt == entry.terminatedAt and admission.retainUntil == entry.retainUntil)
        end
        admissionCache[id] = admission
    else
        requireState(not isActive(entry) and entry.state ~= 'RELEASED' and not entry.expiresAt)
    end
    return entry
end
local function terminate(entry, state, at)
    if isActive(entry) then
        local admission = admissionCache[entry.entryId]
        admission.state, admission.terminatedAt, admission.retainUntil = state, at, at + retention
        redis.call('HSET', admissions, admission.admissionId, cjson.encode(admission))
        redis.call('ZREM', active, admission.admissionId)
    end
    entry.state, entry.terminatedAt, entry.retainUntil = state, at, at + retention
    redis.call('HSET', entries, entry.entryId, cjson.encode(entry))
    redis.call('ZREM', waiting, entry.entryId)
    redis.call('ZREM', expirations, entry.entryId)
    redis.call('ZADD', gc, entry.retainUntil, 'entry:' .. entry.entryId)
end
local function liveDeadline(entry)
    return entry.state == 'WAITING' and entry.waitingExpiresAt or entry.expiresAt
end
local function view(entry, created)
    local ahead
    if entry.state == 'WAITING' then
        ahead = redis.call('ZRANK', waiting, entry.entryId)
        requireState(ahead ~= false)
    end
    local responseEntry = {}
    for k, v in pairs(entry) do responseEntry[k] = v end
    responseEntry.admissionToken = isActive(entry) and admissionCache[entry.entryId].admissionToken or nil
    responseEntry.requestId = entry.admissionId and admissionCache[entry.entryId].requestId or nil
    return envelope({created = created or false, entry = responseEntry, waitingAhead = ahead})
end
if operation == 'STATS' then return envelope({}) end
if operation == 'CLEANUP' then
    local due, deletions = {}, {}
    for _, id in ipairs(redis.call('ZRANGEBYSCORE', expirations, '-inf', now, 'LIMIT', 0, BATCH)) do
        local entry = load(id)
        requireState(entry.state == 'WAITING' and entry.waitingExpiresAt <= now)
        due[#due + 1] = entry
    end
    for _, id in ipairs(redis.call('ZRANGEBYSCORE', active, '-inf', now, 'LIMIT', 0, BATCH)) do
        local admission = decode(redis.call('HGET', admissions, id))
        requireState(type(admission.entryId) == 'string')
        local entry = load(admission.entryId)
        requireState(isActive(entry) and entry.admissionId == id and entry.expiresAt <= now)
        due[#due + 1] = entry
    end
    for _, member in ipairs(redis.call('ZRANGEBYSCORE', gc, '-inf', now, 'LIMIT', 0, BATCH)) do
        requireState(string.sub(member, 1, 6) == 'entry:')
        local entry = load(string.sub(member, 7))
        requireState(not isLive(entry) and entry.retainUntil <= now)
        deletions[#deletions + 1] = entry
    end
    -- Lua는 오류 시 롤백하지 않으므로 배치 전체를 검증한 뒤 변경한다.
    for _, entry in ipairs(due) do terminate(entry, 'EXPIRED', liveDeadline(entry)) end
    for _, entry in ipairs(deletions) do
        if redis.call('HGET', subjects, entry.subjectId) == entry.entryId then redis.call('HDEL', subjects, entry.subjectId) end
        if entry.admissionId then redis.call('HDEL', admissions, entry.admissionId) end
        redis.call('HDEL', entries, entry.entryId)
        redis.call('ZREM', gc, 'entry:' .. entry.entryId)
    end
    return envelope({processed = #due + #deletions})
end
if operation == 'ADMIT' then
    requireState(not rateWindow or rateWindow <= window)
    local candidates = decode(ARGV[10])
    requireState(#candidates >= 1 and #candidates <= BATCH)
    local budget = math.min(#candidates, activeLimit - redis.call('ZCOUNT', active, '(' .. now, '+inf'),
        rateLimit - (rateWindow == window and rateCount or 0))
    if budget <= 0 then return envelope({admitted = 0}) end
    local ids, tokens = {}, {}
    for _, candidate in ipairs(candidates) do
        requireState(type(candidate) == 'table' and type(candidate.admissionId) == 'string'
            and #candidate.admissionId == 36 and string.match(candidate.admissionId, '^[%x%-]+$')
            and type(candidate.admissionToken) == 'string' and #candidate.admissionToken == 43
            and string.match(candidate.admissionToken, '^[%w_%-]+$') and not ids[candidate.admissionId]
            and not tokens[candidate.admissionToken] and redis.call('HEXISTS', admissions, candidate.admissionId) == 0)
        ids[candidate.admissionId], tokens[candidate.admissionToken] = true, true
    end
    local selected, due = {}, {}
    for _, id in ipairs(redis.call('ZRANGE', waiting, 0, BATCH - 1)) do
        local entry = load(id)
        requireState(entry.state == 'WAITING')
        if entry.waitingExpiresAt <= now then due[#due + 1] = entry
        elseif #selected < budget then selected[#selected + 1] = entry end
    end
    for _, entry in ipairs(due) do terminate(entry, 'EXPIRED', entry.waitingExpiresAt) end
    for i, entry in ipairs(selected) do
        local candidate = candidates[i]
        entry.state, entry.admissionId, entry.expiresAt = 'ADMITTED', candidate.admissionId, now + admissionTtl
        local admission = {admissionId = candidate.admissionId, admissionToken = candidate.admissionToken,
            entryId = entry.entryId, subjectId = entry.subjectId, state = 'ADMITTED', expiresAt = entry.expiresAt}
        redis.call('HSET', admissions, admission.admissionId, cjson.encode(admission))
        redis.call('HSET', entries, entry.entryId, cjson.encode(entry))
        redis.call('ZREM', waiting, entry.entryId)
        redis.call('ZREM', expirations, entry.entryId)
        redis.call('ZADD', active, admission.expiresAt, admission.admissionId)
    end
    if #selected > 0 then
        rateCount = (rateWindow == window and rateCount or 0) + #selected
        rateWindow = window
        redis.call('HSET', rate, 'window', rateWindow, 'count', rateCount)
    end
    return envelope({admitted = #selected, processed = #due + #selected})
end
if operation == 'CLAIM' or operation == 'RELEASE' then
    local id, token, requestId, fingerprint = ARGV[11], ARGV[12], ARGV[13], ARGV[14]
    local raw = redis.call('HGET', admissions, id)
    if not raw then return result('NOT_FOUND') end
    local admission = decode(raw)
    requireState(type(admission.entryId) == 'string')
    local entry = load(admission.entryId)
    requireState(entry.admissionId == id)
    admission = admissionCache[entry.entryId]
    -- 토큰의 첫 불일치 위치와 무관하게 고정 길이 전체를 비교한다.
    local difference = 0
    if #token ~= 43 then return result('FORBIDDEN') end
    for i = 1, 43 do difference = bit.bor(difference, bit.bxor(string.byte(token, i), string.byte(admission.admissionToken, i))) end
    if entry.subjectId ~= subjectId or difference ~= 0 then return result('FORBIDDEN') end
    if isActive(entry) and entry.expiresAt <= now then terminate(entry, 'EXPIRED', entry.expiresAt) end
    if not isLive(entry) and entry.retainUntil <= now then return result('NOT_FOUND') end
    if operation == 'CLAIM' then
        if entry.state == 'EXPIRED' then return result('EXPIRED') end
        if entry.state ~= 'ADMITTED' and entry.state ~= 'CLAIMED' then return result('CONFLICT') end
        if entry.state == 'CLAIMED' then
            if admission.requestId ~= requestId or admission.requestFingerprint ~= fingerprint then return result('CONFLICT') end
        else
            requireState(type(requestId) == 'string' and #requestId >= 1 and #requestId <= 128
                and string.match(requestId, '^[%w][%w%.%_:%@%-]*$') and #fingerprint == 64 and string.match(fingerprint, '^[0-9a-f]+$'))
            entry.state, admission.state = 'CLAIMED', 'CLAIMED'
            admission.requestId, admission.requestFingerprint = requestId, fingerprint
            redis.call('HSET', entries, entry.entryId, cjson.encode(entry))
            redis.call('HSET', admissions, id, cjson.encode(admission))
        end
    else
        if admission.requestId ~= requestId then return result('CONFLICT') end
        if entry.state == 'CLAIMED' then terminate(entry, 'RELEASED', now)
        elseif entry.state ~= 'RELEASED' and entry.state ~= 'EXPIRED' then return result('CONFLICT') end
    end
    return view(entry)
end
requireState(operation == 'REGISTER' or operation == 'GET' or operation == 'CANCEL')
local currentId = operation == 'CANCEL' and entryId or redis.call('HGET', subjects, subjectId)
local current
if currentId then
    if operation == 'CANCEL' and redis.call('HEXISTS', entries, currentId) == 0 then return result('NOT_FOUND') end
    current = load(currentId)
    if current.subjectId ~= subjectId then return result('FORBIDDEN') end
end
if operation == 'REGISTER' then
    if current and isLive(current) and liveDeadline(current) > now then return view(current) end
    if redis.call('ZCOUNT', expirations, '(' .. now, '+inf') >= capacity then return result('CAPACITY_EXCEEDED') end
    requireState(not counter or counter < MAX_SEQUENCE)
    requireState(redis.call('HEXISTS', entries, entryId) == 0)
    redis.call('INCR', sequence)
    -- sequence는 cjson의 숫자 정밀도 제한을 피하도록 문자열로 저장한다.
    local nextSequence = redis.call('GET', sequence)
    redis.call('HSET', config, 'lastSequence', nextSequence)
    if current and isLive(current) then terminate(current, 'EXPIRED', liveDeadline(current)) end
    local entry = {entryId = entryId, subjectId = subjectId, state = 'WAITING', sequence = nextSequence, waitingExpiresAt = now + ttl}
    redis.call('HSET', entries, entryId, cjson.encode(entry))
    redis.call('HSET', subjects, subjectId, entryId)
    redis.call('ZADD', waiting, nextSequence, entryId)
    redis.call('ZADD', expirations, entry.waitingExpiresAt, entryId)
    return view(entry, true)
end
if not current then return result('NOT_FOUND') end
if isLive(current) and liveDeadline(current) <= now then terminate(current, 'EXPIRED', liveDeadline(current)) end
if not isLive(current) and current.retainUntil <= now then return result('NOT_FOUND') end
if operation == 'CANCEL' then
    if current.state == 'EXPIRED' then return result('EXPIRED') end
    if current.state == 'CLAIMED' or current.state == 'RELEASED' then return result('CONFLICT') end
    if isLive(current) then terminate(current, 'CANCELED', now) end
end
return view(current)
