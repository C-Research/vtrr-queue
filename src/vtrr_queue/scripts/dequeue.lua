-- Dequeue a task with the smallest virtual-time from the virutal time round-robin queue (vtrr:queue).
-- Task info is fetched then deleted from the vtrr:task look-up table. And vtrr:current_virtual_time is
-- updated to the virutal-time of the dequeued task.

-- KEYS: 1=vtrr:queue  2=vtrr:current_virtual_time  3=vtrr:task  4=vtrr:user_virtual_time
local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then
  return {}
end

local task_id, task_virtual_time = popped[1], popped[2]
local task = redis.call('HGET', KEYS[3], task_id)
redis.call('HDEL', KEYS[3], task_id)

if redis.call('ZCARD', KEYS[1]) == 0 then
  -- reset all values when queue is drained
  redis.call('SET', KEYS[2], 0)
  redis.call('DEL', KEYS[3])
  redis.call('DEL', KEYS[4])
else
  redis.call('SET', KEYS[2], task_virtual_time)
end

return {task_id, task}