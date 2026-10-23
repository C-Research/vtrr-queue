-- KEYS: 1=vtrr:queue  2=vtrr:current_virtual_time  3=vtrr:partition_virtual_time  4=vtrr:task
--       5=vtrr:meta  6=vtrr:inflight  7=vtrr:inflight_task  8=vtrr:lease  9=vtrr:timeouts
local QUEUE, CURRENT_VT, PARTITION_VT, TASK = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local META, INFLIGHT, INFLIGHT_TASK, LEASE, TIMEOUTS = KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[9]

-- Redis server time in seconds.
local function redis_now()
  local t = redis.call('TIME')
  return tonumber(t[1]) + tonumber(t[2]) / 1000000
end

-- Score and enqueue items. items: list of {partition_key, task_id, weight, payload}.
-- A partition's virtual time is max(its own counter, current_virtual_time + 1); each item advances it by its weight.
local function vtrr_enqueue(items)
  local current_virtual_time = tonumber(redis.call('GET', CURRENT_VT) or '0')
  local partition_vt_cache = {}
  for _, item in ipairs(items) do
    local partition_key, task_id, task_weight, task = item[1], item[2], item[3], item[4]
    local partition_virtual_time = partition_vt_cache[partition_key]
    if partition_virtual_time == nil then
      partition_virtual_time = tonumber(redis.call('HGET', PARTITION_VT, partition_key) or '0')
      if current_virtual_time + 1 > partition_virtual_time then partition_virtual_time = current_virtual_time + 1 end
    end
    redis.call('ZADD', QUEUE, partition_virtual_time, task_id)
    partition_virtual_time = partition_virtual_time + task_weight
    redis.call('HSET', TASK, task_id, task)
    -- partition + weight are needed to rescore the item if its lease expires; kept until ack
    redis.call('HSET', META, task_id, task_weight .. ':' .. partition_key)
    partition_vt_cache[partition_key] = partition_virtual_time
  end
  for partition_key, vt in pairs(partition_vt_cache) do
    redis.call('HSET', PARTITION_VT, partition_key, vt)
  end
end

-- Move all expired in-flight items back into the queue (rescored via vtrr_enqueue).
-- Each reclaim is one timeout for the item: its timeouts counter is incremented here and never reset.
-- Returns the ids that were re-enqueued.
local function vtrr_reclaim(now)
  local expired = redis.call('ZRANGEBYSCORE', INFLIGHT, '-inf', string.format('%.3f', now))
  local items, ids = {}, {}
  for _, task_id in ipairs(expired) do
    local payload = redis.call('HGET', INFLIGHT_TASK, task_id)
    redis.call('ZREM', INFLIGHT, task_id)
    redis.call('HDEL', INFLIGHT_TASK, task_id)
    redis.call('HDEL', LEASE, task_id)
    redis.call('HINCRBY', TIMEOUTS, task_id, 1)
    local weight, partition_key = string.match(redis.call('HGET', META, task_id), '^(%d+):(.*)$')
    items[#items + 1] = {partition_key, task_id, tonumber(weight), payload}
    ids[#ids + 1] = task_id
  end
  if #items > 0 then vtrr_enqueue(items) end
  return ids
end
