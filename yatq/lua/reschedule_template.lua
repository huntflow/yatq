--[[
Template file of Lua function, used to return task to queue
--]]
local pending_key = "$pending_key"           -- Redis key with pending tasks ZSET
local processing_key = "$processing_key"     -- Redis key with processing tasks ZSET
local task_mapping_key = "$task_mapping_key" -- Redis key with task key:id mapping
local channel = "$event_channel"             -- Queue's event pubsub channel

local task_key = ARGV[1]
local task_id = ARGV[2]
local task_data = ARGV[3]
local after = tonumber(ARGV[4])


local function incr_metric_key (key)
	local incr_success, value = redis.pcall("INCR", key)

	if not incr_success then
		redis.pcall("SET", key, 1)
	end
end


--[[
In some cases (extreme desync, manually removed keys, etc.) task key and id may not match
ones stored in queue's key:id mapping. If this happens, we can't really do anything - key is
already used by another task.
--]]
local queue_task_id = redis.call("HGET", task_mapping_key, task_key)
if queue_task_id ~= task_id then
	-- Broken task - queue id and provided id does not match
	local message = string.format("INVALID_STATE %s", task_key)
	redis.call("PUBLISH", channel, message)

	incr_metric_key("$metrics_broken_key")

	return cjson.encode({success = false, reason = "task_missing"})
end

redis.call("ZREM", processing_key, task_key)
redis.call("ZADD", pending_key, after, task_key)

local task_data_key = string.format("$task_key_prefix:%s", task_id)
redis.call("SET", task_data_key, task_data)

local message = string.format("RESCHEDULE %s %s", task_id, task_key)
redis.call("PUBLISH", channel, message)

incr_metric_key("$metrics_requeued_key")

return cjson.encode({success = true})
