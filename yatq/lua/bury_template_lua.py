BURY_TEMPLATE = """
--[[
Template file of Lua function, used to drop stale tasks from processing set
--]]
local pending_key = "$pending_key"           -- Redis key with pending tasks ZSET
local processing_key = "$processing_key"     -- Redis key with processing tasks ZSET
local task_mapping_key = "$task_mapping_key" -- Redis key with task key:id mapping
local channel = "$event_channel"             -- Queue's event pubsub channel
local default_ttl = $default_task_expiration -- Default result TTL

local before_time = tonumber(ARGV[1])

--[[
NOTE: System time has to be passed from caller, since script invocation should be deterministic.
Since current time isn't, script receives it from outside as argument.
--]]


--[[ Updates task data and removes task from queue

Function sets task state as BURIES, removes key from processing set and logs BURIED event
in pubsub.
--]]
local function mark_task_as_buried (task_id)
	local task_data_key = string.format("$task_key_prefix:%s", task_id)
	
	if default_ttl > 0 then
		local task_data = redis.call("GET", task_data_key)
		local decoded_task_data = cjson.decode(task_data)
	
		decoded_task_data["state"] = "BURIED"
		task_data = cjson.encode(decoded_task_data)

		redis.call("SETEX", task_data_key, default_ttl, task_data)
	else
		redis.call("DEL", task_data_key)
	end
end

--[[ Checks whenever specified key is used in queue's pending or processing set
--]]
local function key_is_used (task_key)
	local existing_pending_task_rank = redis.call("ZRANK", pending_key, task_key)
	if type(existing_pending_task_rank) == "number" then
		return true
	end

	local existing_processing_task_rank = redis.call("ZRANK", processing_key, task_key)
	if type(existing_processing_task_rank) == "number" then
		return true
	end

	return false
end

--[[ "Buries" tasks

Buried tasks are removed from processing set, their key:id association is removed from queue and their
state is set to BURIED.
--]]
local function bury_task (task_key)
	redis.call("ZREM", processing_key, task_key)

	local task_id = redis.call("HGET", task_mapping_key, task_key)
	if task_id == nil then
		local message = string.format("INVALID_STATE %s", task_key)
		redis.call("PUBLISH", channel, message)
		return
	end

	redis.call("HDEL", task_mapping_key, task_key)
	local success, result = pcall(mark_task_as_buried, task_id)
	if success == false then
		local message = string.format("BURY_ERROR %s %s %s", task_id, task_key, result)
		redis.call("PUBLISH", channel, message)
	else
		local message = string.format("BURIED %s %s", task_id, task_key)
		redis.call("PUBLISH", channel, message)
	end
end

local function incr_metric_key_by (key, value)
	local incr_success, value = redis.pcall("INCRBY", key, value)

	if not incr_success then
		redis.pcall("SET", key, value)
	end
end

--[[ Checking for expired tasks in processing queue

In this queue implementation, task's deadline is represented by it's key's score. If score is lower than
current date, task is considered "dead" and is removed from processing set, as well as marked as "BURIED".
Number of buried tasks is returned as "count" counter.
--]]
local buried_count = 0
local stale_tasks = redis.call("ZRANGEBYSCORE", processing_key, 0, before_time)
for index, stale_task in pairs(stale_tasks) do
	pcall(bury_task, stale_task)
	buried_count = buried_count + 1
end
incr_metric_key_by("$metrics_buried_key", buried_count)

--[[ Checking for invalid keys in key:id mapping

In some cases (manual key removal), queue may be left with keys in key:id mapping that are not
scheduled for execution. Since we don't know original task state (processing or pending), those keys
are removed. Number of removed keys is returned as "cleaned_up" counter.
--]]
local cleaned_up = 0
local mapping_keys = redis.call("HKEYS", task_mapping_key)
for i, key in pairs(mapping_keys) do
	if not key_is_used(key) then
		redis.call("HDEL", task_mapping_key, key)
		cleaned_up = cleaned_up + 1
	end
end
incr_metric_key_by("$metrics_broken_key", cleaned_up)

return cjson.encode({success = true, count = buried_count, cleaned_up = cleaned_up})
"""
