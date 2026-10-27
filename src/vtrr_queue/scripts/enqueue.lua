-- Bulk enqueue to the virutal time round-robin (vtrr). Each task is saved in vtrr:task look-up table, and the task_id
-- is enqueued into the vtrr:queue with a virtual-time that's based off of how many tasks the partition has enqueued so far (managed by vtrr:partition_virtual_time)
-- The vtrr:current_virtual_time is the lowest virtual-time of any task in the queue.
-- ARGV: flat quads of (partition_id, task_id, task_weight, task_json)
local items = {}
for i = 1, #ARGV, 4 do
  items[#items + 1] = {ARGV[i], ARGV[i+1], tonumber(ARGV[i+2]), ARGV[i+3]}
end
vtrr_enqueue(items)
return redis.status_reply('OK')
