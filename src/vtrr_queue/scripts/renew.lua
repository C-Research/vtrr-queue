-- Re-lease an item that is still held under `token`: push its expiry out to (redis time + lease_seconds).
-- Used when a task retries. Returns 1 on success, 0 if the token no longer matches (lease was reclaimed / re-granted).
-- ARGV: 1=task_id  2=token  3=lease_seconds
if redis.call('HGET', LEASE, ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZADD', INFLIGHT, string.format('%.3f', redis_now() + tonumber(ARGV[3])), ARGV[1])
return 1
