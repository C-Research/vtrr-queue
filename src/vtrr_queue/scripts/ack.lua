-- Ack a leased task. Only succeeds if `token` matches the stored lease token, so a worker whose lease expired
-- (and was re-granted to someone else) cannot ack the new holder's lease. Returns 1 on success, 0 otherwise.
-- ARGV: 1=task_id  2=token
if redis.call('HGET', LEASE, ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZREM', INFLIGHT, ARGV[1])
redis.call('HDEL', INFLIGHT_TASK, ARGV[1])
redis.call('HDEL', LEASE, ARGV[1])
redis.call('HDEL', ATTEMPTS, ARGV[1])
redis.call('HDEL', META, ARGV[1])
return 1
