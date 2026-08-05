-- Bulk enqueue to the virutal time round-robin (vtrr). Each task is saved in vtrr:task look-up table, and the task_id
-- is enqueued into the vtrr:queue with a virtual-time that's based off of how many tasks the partition has enqueued so far (managed by vtrr:partition_virtual_time)
-- The vtrr:current_virtual_time is the lowest virtual-time of any task in the queue.

-- KEYS: 1=vtrr:current_virtual_time  2=vtrr:partition_virtual_time  3=vtrr:queue  4=vtrr:task
-- ARGV: flat triples of (partition_id, task_id, task_json)
local current_virtual_time = tonumber(redis.call('GET', KEYS[1]) or '0')
local partition_vt_cache = {}
local i = 1
while i <= #ARGV do
  local partition_key, task_id, task_weight, task = ARGV[i], ARGV[i+1], tonumber(ARGV[i+2]), ARGV[i+3]
  local partition_virtual_time = partition_vt_cache[partition_key]
  if partition_virtual_time == nil then
    partition_virtual_time = tonumber(redis.call('HGET', KEYS[2], partition_key) or '0')
    if current_virtual_time + 1 > partition_virtual_time then partition_virtual_time = current_virtual_time + 1 end
  end
  redis.call('ZADD', KEYS[3], partition_virtual_time, task_id)
  partition_virtual_time = partition_virtual_time + task_weight
  redis.call('HSET', KEYS[4], task_id, task)
  partition_vt_cache[partition_key] = partition_virtual_time
  i = i + 4
end
for partition_key, uvt in pairs(partition_vt_cache) do
  redis.call('HSET', KEYS[2], partition_key, uvt)
end
return redis.status_reply('OK')