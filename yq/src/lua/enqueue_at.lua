-- KEYS
local q_mid_seq_key = KEYS[1];
local q_messages_key = KEYS[2];
local schedule_key = KEYS[3];

-- ARGV
local mcnt_arg = ARGV[1];
local run_at = tonumber(ARGV[2]);

--------------------------------------------------------------------------------

local mid = tonumber(redis.call('incr', q_mid_seq_key));

redis.call('hset', q_messages_key, mid, mcnt_arg);

redis.call('zadd', schedule_key, run_at, mid);

return { 'added', mid };
