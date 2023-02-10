-- KEYS
local q_messages_key = KEYS[1];
local q_lock_times_key = KEYS[2];
local q_locks_key = KEYS[3];
local q_done_key = KEYS[4];
local q_mids_ready_key = KEYS[5];
local q_mid_circle_key = KEYS[6];
local q_ndry_runs_key = KEYS[7];
local q_isleep_b_key = KEYS[8];

-- ARGV
local now_arg = ARGV[1];
local default_lock_ms_arg = ARGV[2];

-- Prioritize mids from ready list
local mid = redis.call('rpoplpush', q_mids_ready_key, q_mid_circle_key) or
        redis.call('rpoplpush', q_mid_circle_key, q_mid_circle_key);

if ((not mid) or (mid == 'end-of-circle')) then -- Uninit'd or eoq
    local new_ndry_runs = tonumber(redis.call('incr', q_ndry_runs_key));

    local isleep_on;
    if (redis.call('llen', q_isleep_b_key) == 0) then isleep_on = 'b'; else isleep_on = 'a'; end

    return {'sleep', 'end-of-circle', isleep_on, new_ndry_runs};
end

-- From msg_status.lua ---------------------------------------------------------
local now = tonumber(now_arg);

local status;

if (redis.call('hexists', q_messages_key, mid) == 1) then
    if (redis.call('sismember', q_done_key, mid) == 1) then
        status = 'done';
    else
        local exp_lock = tonumber(redis.call('hget', q_locks_key, mid)) or 0;
        if (now < exp_lock) then
            status = 'locked';
        else
            status = 'queued';
        end
    end
else
    status = 'nx';
end
--------------------------------------------------------------------------------

if (status == 'locked') then
    return {'skip', 'locked', mid};
end

redis.call('set', q_ndry_runs_key, 0); -- Doing useful work

if (status == 'done') then
    -- {done, -bo, -rq} -> full GC now
    redis.call('hdel',  q_messages_key,      mid);
    redis.call('hdel',  q_lock_times_key,    mid);
    redis.call('hdel',  q_locks_key,         mid);
    redis.call('srem',  q_done_key,          mid);
    redis.call('ltrim', q_mid_circle_key, 1, -1);
    return {'skip', 'did-gc', mid};
elseif (status == 'nx') then
    redis.call('hdel',  q_lock_times_key,    mid);
    redis.call('hdel',  q_locks_key,         mid);
    redis.call('srem',  q_done_key,          mid);
    redis.call('ltrim', q_mid_circle_key, 1, -1);
    return {'skip', 'msg-missing', mid};
elseif (status == 'queued') then
    -- {queued, -bo, _rq} -> handle now
    local lock_ms = tonumber(redis.call('hget', q_lock_times_key, mid)) or tonumber(default_lock_ms_arg);

    redis.call('hset',    q_locks_key,     mid, now + lock_ms); -- Acquire
    local mcontent  = redis.call('hget',    q_messages_key,  mid);

    return {'handle', mid, mcontent, lock_ms};
else
    return {'unexpected', status, mid};
end