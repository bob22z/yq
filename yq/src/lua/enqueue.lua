-- KEYS
local q_mid_seq_key = KEYS[1];
local q_messages_key = KEYS[2];
local q_lock_times_key = KEYS[3];
local q_mids_ready_key = KEYS[4];
local q_mid_circle_key = KEYS[5];
local q_isleep_a_key = KEYS[6];
local q_isleep_b_key = KEYS[7];

-- ARGV
local mcnt_arg = ARGV[1];
local lock_ms_arg = ARGV[2];

--------------------------------------------------------------------------------
-- Return {action, error}

local interrupt_sleep = function ()
    if redis.call('rpoplpush', q_isleep_a_key, q_isleep_b_key) then
        return 'to_sleep_b'
    elseif redis.call('rpoplpush', q_isleep_b_key, q_isleep_a_key) then
        return 'to_sleep_a'
    else   redis.call('lpush',     q_isleep_a_key, '_');
        return 'to_sleep_a_init'
    end
end

if redis.call('exists', q_mid_circle_key) ~= 1 then
    redis.call('lpush',  q_mid_circle_key, 'end-of-circle');
end

local mid = tonumber(redis.call('incr', q_mid_seq_key));
redis.call('lpush', q_mids_ready_key, mid); -- -> Priority queue

redis.call('hset',   q_messages_key, mid, mcnt_arg);

local lock_ms = tonumber(lock_ms_arg);
if   (lock_ms ~= -1) then
    redis.call('hset', q_lock_times_key, mid, lock_ms);
else
    redis.call('hdel', q_lock_times_key, mid);
end

local to_sleep = interrupt_sleep();
return {'added', to_sleep, mid};
