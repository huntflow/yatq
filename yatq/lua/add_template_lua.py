ADD_TEMPLATE = """
--[[
Template file of Lua function, used to add new tasks to queue
--]]
local pending_key = "$pending_key"           -- Redis key with pending tasks ZSET
local processing_key = "$processing_key"     -- Redis key with processing tasks ZSET
local task_mapping_key = "$task_mapping_key" -- Redis key with task key:id mapping
local channel = "$event_channel"             -- Queue's event pubsub channel

local task_key = ARGV[1]                     -- Task's key
local task_id = ARGV[2]                      -- Task's ID, unique for each invocation
local task_data = ARGV[3]                    -- Task's ddata
local time = tonumber(ARGV[4])               -- Current system time

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


local task_data_key = string.format("$task_key_prefix:%s", task_id)

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

--[[ Validates task data

Queue guarantees that task data contains valid json. In case it is not so, script deletes
broken task data.
--]]
local function validate_task_by_id (task_id)
	local existing_task_data_key = string.format("$task_key_prefix:%s", task_id)
	local existing_task_data = redis.call("GET", existing_task_data_key)
	local decode_success, data = pcall(cjson.decode, existing_task_data)

	if not decode_success then
		redis.pcall("DEL", existing_task_data_key)
	end

	return decode_success
end


-- Looking for existing task with specified key is processing set
local existing_processing_task_rank = redis.call("ZRANK", processing_key, task_key)
if type(existing_processing_task_rank) == "number" then
	-- Task with specified key already exists and is currently being processed
	local existing_task_id = redis.call("HGET", task_mapping_key, task_key)

	if existing_task_id and validate_task_by_id(existing_task_id) then
		return cjson.encode({
			success = false,
			reason = "already_exists",
			state = "PROCESSING",
			id = existing_task_id,
		})
	end

	nuke_broken_task(task_key)
end

-- Looking for existing task with specified key is pending set
local existing_pending_task_rank = redis.call("ZRANK", pending_key, task_key)
if type(existing_pending_task_rank) == "number" then
	-- Task with specified key already exists and is currently pending
	local existing_task_id = redis.call("HGET", task_mapping_key, task_key)

	if existing_task_id and validate_task_by_id(existing_task_id) then
		return cjson.encode({
			success = false,
			reason = "already_exists",
			state = "PENDING",
			id = existing_task_id,
		})
	end

	nuke_broken_task(task_key)
end

redis.call("ZADD", pending_key, "NX", time, task_key)
redis.call("HSET", task_mapping_key, task_key, task_id)
redis.call("SET", task_data_key, task_data)

local message = string.format("ADDED %s %s", task_id, task_key)
redis.call("PUBLISH", channel, message)

incr_metric_key("$metrics_added_key")

return cjson.encode({success = true})
"""
