-- Dequeue a task with the smallest virtual-time from the virutal time round-robin queue (vtrr:queue) and lease it.
-- Expired leases are reclaimed first. The task payload moves from vtrr:task to vtrr:inflight_task, the task is
-- added to vtrr:inflight scored by (redis time + lease_seconds) and its lease token stored. The item's timeouts count
-- (how many times its lease expired and it was reclaimed; 0 on the first dequeue) is returned unchanged. vtrr:current_virtual_time is updated to the virutal-time of the dequeued task.
-- The caller must ack (ack.lua) with the returned token when done.

-- ARGV: 1=lease_seconds  2=lease_token
-- Returns {task_id, task, token, timeouts}, or {} if the queue is empty.
local now = redis_now()
vtrr_reclaim(now)

local popped = redis.call('ZPOPMIN', QUEUE)
if #popped == 0 then
  return {}
end

local task_id, task_virtual_time = popped[1], popped[2]
local task = redis.call('HGET', TASK, task_id)
redis.call('HDEL', TASK, task_id)

if redis.call('ZCARD', QUEUE) == 0 then
  -- reset all values when queue drains (in-flight state is untouched)
  redis.call('SET', CURRENT_VT, 0)
  redis.call('DEL', TASK)
  redis.call('DEL', PARTITION_VT)
else
  redis.call('SET', CURRENT_VT, task_virtual_time)
end

local timeouts = tonumber(redis.call('HGET', TIMEOUTS, task_id) or '0')
redis.call('ZADD', INFLIGHT, string.format('%.3f', now + tonumber(ARGV[1])), task_id)
redis.call('HSET', INFLIGHT_TASK, task_id, task)
redis.call('HSET', LEASE, task_id, ARGV[2])

return {task_id, task, ARGV[2], timeouts}
