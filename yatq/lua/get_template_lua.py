GET_TEMPLATE = """
--[[
Template file of Lua function, used to get tasks from queue
--]]
local pending_key = "$pending_key"           -- Redis key with pending tasks ZSET
local processing_key = "$processing_key"     -- Redis key with processing tasks ZSET
local task_mapping_key = "$task_mapping_key" -- Redis key with task key:id mapping
local channel = "$event_channel"             -- Queue's event pubsub channel

local time = tonumber(ARGV[1])

--[[
NOTE: System time has to be passed from caller, since script invocation should be deterministic.
Since current time isn't, script receives it from outside as argument.
--]]


local function incr_metric_key (key)
	local incr_success, value = redis.pcall("INCR", key)

	if not incr_success then
		redis.pcall("SET", key, 1)
	end
end

local function incr_metric_key_by (key, amount)
	local incr_success, value = redis.pcall("INCRBY", key, amount)

	if not incr_success then
		redis.pcall("SET", key, amount)
	end
end


--[[ Drops invalid task keys from queue

If script encounters invalid task (missing key:id mapping entry, invalid state, etc),
it drops all *task key* related values from queue. Task data, mapped by task's ID, is left as is.
--]]
local function nuke_broken_task (broken_task_key)
	redis.pcall("ZREM", processing_key, broken_task_key)
	redis.pcall("ZREM", pending_key, broken_task_key)
	redis.pcall("HDEL", task_mapping_key, broken_task_key)

	local error_message = string.format("BROKEN NO_ID %s", broken_task_key)
	redis.pcall("PUBLISH", channel, error_message)

	incr_metric_key("$metrics_broken_key")
end


local function get_task_deadline (data)
	return time + data["timeout"]
end


local available_tasks = redis.call("ZRANGEBYSCORE", pending_key, 0, time, "LIMIT", 0, 1)
local task_key = available_tasks[1]

if task_key == nil then
	return cjson.encode({
		success = false
	})
end

local task_score = tonumber(available_tasks[2])

redis.call("ZREM", pending_key, task_key)
local task_id = redis.call("HGET", task_mapping_key, task_key)
if not task_id then
	-- Broken task - no ID associated with key
	nuke_broken_task(task_key)
	return cjson.encode({
		success = false,
		error = "BROKEN_TASK_NO_ID"
	})
end

local task_data_key = string.format("$task_key_prefix:%s", task_id)
local task_data = redis.call("GET", task_data_key)

local parse_success, task_data_parsed = pcall(cjson.decode, task_data)
if not parse_success then
	-- Broken task - invalid JSON in data
	nuke_broken_task(task_key)
	redis.pcall("DEL", task_data_key)

	return cjson.encode({
		success = false,
		error = "BROKEN_TASK_INVALID_JSON"
	})
end

task_data_parsed["state"] = "PROCESSING"
task_data = cjson.encode(task_data_parsed)
redis.call("SET", task_data_key, task_data)

local deadline_success, task_deadline = pcall(get_task_deadline, task_data_parsed)
if not deadline_success then
	task_deadline = time + $default_timeout
end

local add_result = redis.call("ZADD", processing_key, "NX", task_deadline, task_key)
if add_result ~= 1 then
	-- Broken task - key already exists in processing set
	nuke_broken_task(task_key)
	redis.pcall("DEL", task_data_key)

	return cjson.encode({
		success = false,
		error = "BROKEN_TASK_ALREADY_PROCESSING"
	})
end

local message = string.format("TAKEN %s %s", task_id, task_key)
redis.call("PUBLISH", channel, message)

incr_metric_key("$metrics_taken_key")
incr_metric_key_by("$metrics_time_wait", math.floor((time - task_score) * 1000))

return cjson.encode({
	success = true,
	key = task_key,
	id = task_id,
	deadline = task_deadline,
	data = task_data_parsed,
})
"""
