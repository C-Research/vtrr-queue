-- Bulk enqueue to the virutal time round-robin (vtrr). Each task is saved in vtrr:task look-up table, and the task_id
-- is enqueued into the vtrr:queue with a virtual-time that's based off of how many tasks the user has enqueued so far (managed by vtrr:user_virtual_time)
-- The vtrr:current_virtual_time is the lowest virtual-time of any task in the queue.

-- KEYS: 1=vtrr:current_virtual_time  2=vtrr:user_virtual_time  3=vtrr:queue  4=vtrr:task
-- ARGV: flat triples of (user_id, task_id, task_json)
local current_virtual_time = tonumber(redis.call('GET', KEYS[1]) or '0')
local user_vt_cache = {}
local i = 1
while i <= #ARGV do
  local user, task_id, task = ARGV[i], ARGV[i+1], ARGV[i+2]
  local user_virtual_time = user_vt_cache[user]
  if user_virtual_time == nil then
    user_virtual_time = tonumber(redis.call('HGET', KEYS[2], user) or '0')
    if current_virtual_time > user_virtual_time then user_virtual_time = current_virtual_time end
  end
  user_virtual_time = user_virtual_time + 1
  redis.call('ZADD', KEYS[3], user_virtual_time, task_id)
  redis.call('HSET', KEYS[4], task_id, task)
  user_vt_cache[user] = user_virtual_time
  i = i + 3
end
for user, uvt in pairs(user_vt_cache) do
  redis.call('HSET', KEYS[2], user, uvt)
end
return redis.status_reply('OK')