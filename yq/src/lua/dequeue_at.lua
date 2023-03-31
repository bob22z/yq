-- KEYS
local q_mids_ready_key = KEYS[1];
local q_mid_circle_key = KEYS[2];
local schedule_key = KEYS[3];

-- ARGV
local run_at = tonumber(ARGV[1]);

--------------------------------------------------------------------------------
if redis.call('exists', q_mid_circle_key) ~= 1 then
    redis.call('lpush',  q_mid_circle_key, 'end-of-circle');
end

local mids = redis.call("ZRANGEBYSCORE", schedule_key, 0, run_at);
local count = 0;
for i, mid in ipairs(mids) do
    redis.call('lpush', q_mids_ready_key, mid);
    count = count + 1
end
if count > 0 then
    redis.call('ZREMRANGEBYSCORE', schedule_key, 0, run_at);
    return {'dequeued', count};
else
    return {'no-job'};
end
