"""Lua scripts. Redis runs each script atomically: no other command interleaves, so
check-then-set races (two buyers, one seat) cannot happen. Time comes from Redis' own
clock (TIME), never from app servers, so clock skew between servers cannot extend a hold."""

NOW_MS = "local t = redis.call('TIME'); local now = t[1] * 1000 + math.floor(t[2] / 1000)\n"

# KEYS: seat, sold set, user's holds zset | ARGV: user, ttl_ms, max_holds, seat_id
HOLD = NOW_MS + """
if redis.call('SISMEMBER', KEYS[2], ARGV[4]) == 1 then return 'SOLD' end
local owner = redis.call('GET', KEYS[1])
if owner then
  if owner == ARGV[1] then return 'ALREADY_YOURS' end
  return 'TAKEN'
end
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
if redis.call('ZCARD', KEYS[3]) >= tonumber(ARGV[3]) then return 'LIMIT' end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), ARGV[4])
redis.call('PEXPIRE', KEYS[3], ARGV[2])
return 'HELD'
"""

# KEYS: seat, user's holds zset | ARGV: user, seat_id
RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[2])
  return 1
end
return 0
"""

# Verify ownership and extend the hold so it cannot expire while payment is in flight.
# KEYS: seat | ARGV: user, grace_ms   -> remaining ms, or -1 if not the owner
CLAIM = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < tonumber(ARGV[2]) then redis.call('PEXPIRE', KEYS[1], ARGV[2]); ttl = tonumber(ARGV[2]) end
return ttl
"""

# KEYS: seat, sold set, user's holds zset | ARGV: seat_id
MARK_SOLD = """
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
"""

# Atomic join: a user keeps their original place if they join twice.
# KEYS: queue zset, sequence counter | ARGV: user  -> 0-based rank
JOIN = """
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], redis.call('INCR', KEYS[2]), ARGV[1])
end
return redis.call('ZRANK', KEYS[1], ARGV[1])
"""

# Token bucket. KEYS: bucket | ARGV: capacity, refill_per_sec, cost -> {allowed, tokens_left}
TOKEN_BUCKET = """
local t = redis.call('TIME'); local now = t[1] + t[2] / 1e6
local cap, rate, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or cap
local ts = tonumber(b[2]) or now
tokens = math.min(cap, tokens + (now - ts) * rate)
local ok = 0
if tokens >= cost then tokens = tokens - cost; ok = 1 end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], math.ceil(cap / rate) + 1)
return {ok, tostring(tokens)}
"""
