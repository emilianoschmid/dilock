-- Dilock draft 1 reference. KEYS[1] = lease key.
-- ARGV[1] = full owner value; ARGV[2] = positive integer TTL in milliseconds.
local ttl = tonumber(ARGV[2])
if not ttl or ttl ~= math.floor(ttl) or ttl <= 0 or ttl > 86400000 then
    return redis.error_reply('DILOCK_INVALID_TTL')
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ttl)
end
return 0
