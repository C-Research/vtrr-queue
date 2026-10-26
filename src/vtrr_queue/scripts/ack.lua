-- Ack a leased task. Only succeeds if `token` matches the stored lease token, so a worker whose lease expired
-- (and was re-granted to someone else) cannot ack the new holder's lease. Returns 1 on success, 0 otherwise.
-- ARGV: 1=task_id  2=token
if redis.call('HGET', LEASE, ARGV[1]) ~= ARGV[2] then
  -- Caveat: An expired lease can still be acked. This is if the worker finishes after the lease has expired
  -- but before the lease is reclaimed which is mostly a non-issue. To prevent this, set the worker's hard time limit to less
  -- than the lease duration.
  return 0
end
redis.call('ZREM', INFLIGHT, ARGV[1])
redis.call('HDEL', INFLIGHT_TASK, ARGV[1])
redis.call('HDEL', LEASE, ARGV[1])
redis.call('HDEL', TIMEOUTS, ARGV[1])
redis.call('HDEL', META, ARGV[1])
return 1
