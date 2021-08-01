--[[
Template file of Lua function, used to mark tasks as completed.
--]]
local pending_key = "$pending_key"           -- Redis key with pending tasks ZSET
local processing_key = "$processing_key"     -- Redis key with processing tasks ZSET
local task_mapping_key = "$task_mapping_key" -- Redis key with task key:id mapping
local channel = "$event_channel"             -- Queue's event pubsub channel

local task_key = ARGV[1]                     -- Task's key
local task_id = ARGV[2]                      -- Task's ID, unique for each invocation
local final_task_data = ARGV[3]              -- Task's final state
local ttl = tonumber(ARGV[4])                -- Completed Task's time to keep


local function incr_metric_key (key)
	local incr_success, value = redis.pcall("INCR", key)

	if not incr_success then
		redis.pcall("SET", key, 1)
	end
end


local task_data_key = string.format("$task_key_prefix:%s", task_id)

local queue_task_id = redis.call("HGET", task_mapping_key, task_key)
if queue_task_id ~= task_id then
	-- Broken task - queue id and provided id does not match
	redis.call("SETEX", task_data_key, $default_task_expiration, final_task_data)

	local message = string.format("RESURRECTED %s %s", task_id, task_key)
	redis.call("PUBLISH", channel, message)

	incr_metric_key("$metrics_resurrected_key")

	return cjson.encode({success = true})
end

redis.pcall("ZREM", pending_key, task_key)
redis.pcall("ZREM", processing_key, task_key)

local task_id = redis.call("HGET", task_mapping_key, task_key)
redis.pcall("HDEL", task_mapping_key, task_key)
if task_id == nil then
-- FIXME: how we can get here? queue_task_id was already checked earlier
	local error_message = string.format("BROKEN NO_ID %s", task_key)
	redis.pcall("PUBLISH", channel, error_message)

	return cjson.encode({success = false})
end

if ttl > 0 then
	redis.call("SETEX", task_data_key, ttl, final_task_data)
else
	redis.call("DEL", task_data_key)
end

local message = string.format("COMPLETED %s %s", task_id, task_key)
redis.call("PUBLISH", channel, message)

incr_metric_key("$metrics_completed_key")

return cjson.encode({success = true})
