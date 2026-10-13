-- Dilock draft 1 reference. KEYS[1] = lease key; ARGV[1] = full owner value.
-- Atomic owner check prevents a stale releaser from deleting a successor.
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
