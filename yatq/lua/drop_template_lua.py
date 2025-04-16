DROP_TEMPLATE = """
--[[
Template file of Lua function, used to drop pending tasks from queue
--]]
local pending_key = "$pending_key"           -- Redis key with pending tasks ZSET
local processing_key = "$processing_key"     -- Redis key with processing tasks ZSET
local task_mapping_key = "$task_mapping_key" -- Redis key with task key:id mapping
local channel = "$event_channel"             -- Queue's event pubsub channel

local task_key = ARGV[1]                     -- Task's key


-- Looking for existing task with specified key is processing set
local existing_processing_task_rank = redis.call("ZRANK", processing_key, task_key)
if type(existing_processing_task_rank) == "number" then
	return cjson.encode({
		success = false,
		reason = "task_being_processed",
	})
end


-- Looking for existing task with specified key is pending set
local existing_pending_task_rank = redis.call("ZRANK", pending_key, task_key)
if type(existing_pending_task_rank) == "number" then
	-- Task with specified key already exists and is currently pending
	local existing_task_id = redis.call("HGET", task_mapping_key, task_key)

	if existing_task_id then
		local existing_task_data_key = string.format("$task_key_prefix:%s", existing_task_id)

		redis.pcall("HDEL", task_mapping_key, task_key)
		redis.pcall("DEL", existing_task_data_key)
	end
end

redis.pcall("ZREM", pending_key, task_key)

return cjson.encode({success = true})
"""