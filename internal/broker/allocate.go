package broker

import (
    "context"
    "errors"
    "time"

    "github.com/google/uuid"
    "github.com/redis/go-redis/v9"
)

var ErrNotFound = errors.New("session not found")

type Session struct {
    ID            string `json:"id"`
    UserID        string `json:"user_id"`
    NodeID        string `json:"node_id,omitempty"`
    Region        string `json:"region"`
    Status        string `json:"status"`
    QueuePosition int64  `json:"queue_position,omitempty"`
}

type Broker struct {
    rdb *redis.Client
}

func New(rdb *redis.Client) *Broker {
    return &Broker{rdb: rdb}
}

// allocateScript picks the least-loaded ALIVE node in the region with free
// capacity, increments its used count and creates the session, all atomically.
// Returns the node id, or false (nil reply) when the region is full.
var allocateScript = redis.NewScript(`
local region = ARGV[1]
local session_id = ARGV[2]
local user_id = ARGV[3]
local now = ARGV[4]

local nodes = redis.call('SMEMBERS', 'nodes')
local best = nil
local best_used = nil
for _, id in ipairs(nodes) do
  local nk = 'node:' .. id
  if redis.call('EXISTS', nk .. ':alive') == 1
     and redis.call('HGET', nk, 'region') == region then
    local used = tonumber(redis.call('HGET', nk, 'used'))
    local cap = tonumber(redis.call('HGET', nk, 'capacity'))
    if used < cap and (best == nil or used < best_used) then
      best = id
      best_used = used
    end
  end
end

if not best then
  return false
end

redis.call('HINCRBY', 'node:' .. best, 'used', 1)
redis.call('HSET', 'session:' .. session_id,
  'user_id', user_id, 'node_id', best, 'region', region,
  'status', 'RUNNING', 'created_at', now)
redis.call('SADD', 'node:' .. best .. ':sessions', session_id)
return best
`)

// releaseScript frees the slot of a RUNNING session, or removes a QUEUED one
// from its queue, then deletes the session. Atomic.
var releaseScript = redis.NewScript(`
local sk = 'session:' .. ARGV[1]
if redis.call('EXISTS', sk) == 0 then
  return 0
end
local status = redis.call('HGET', sk, 'status')
if status == 'RUNNING' then
  local node = redis.call('HGET', sk, 'node_id')
  local used = tonumber(redis.call('HGET', 'node:' .. node, 'used'))
  if used > 0 then
    redis.call('HINCRBY', 'node:' .. node, 'used', -1)
  end
  redis.call('SREM', 'node:' .. node .. ':sessions', ARGV[1])
else
  redis.call('LREM', 'queue:' .. redis.call('HGET', sk, 'region'), 0, ARGV[1])
end
redis.call('DEL', sk)
return 1
`)

func (b *Broker) Allocate(ctx context.Context, userID, region string) (*Session, error) {
    id := uuid.NewString()
    res, err := allocateScript.Run(ctx, b.rdb, nil, region, id, userID, time.Now().Unix()).Result()

    if err == redis.Nil {
        // Region is full: park the session in the FIFO queue.
        pipe := b.rdb.TxPipeline()
        pipe.HSet(ctx, "session:"+id, "user_id", userID, "region", region, "status", "QUEUED")
        pipe.RPush(ctx, "queue:"+region, id)
        if _, err := pipe.Exec(ctx); err != nil {
            return nil, err
        }
        s := &Session{ID: id, UserID: userID, Region: region, Status: "QUEUED"}
        if pos, err := b.rdb.LPos(ctx, "queue:"+region, id, redis.LPosArgs{}).Result(); err == nil {
            s.QueuePosition = pos + 1
        }
        return s, nil
    }
    if err != nil {
        return nil, err
    }
    return &Session{ID: id, UserID: userID, NodeID: res.(string), Region: region, Status: "RUNNING"}, nil
}

func (b *Broker) Get(ctx context.Context, id string) (*Session, error) {
    h, err := b.rdb.HGetAll(ctx, "session:"+id).Result()
    if err != nil {
        return nil, err
    }
    if len(h) == 0 {
        return nil, ErrNotFound
    }
    s := &Session{ID: id, UserID: h["user_id"], NodeID: h["node_id"], Region: h["region"], Status: h["status"]}
    if s.Status == "QUEUED" {
        if pos, err := b.rdb.LPos(ctx, "queue:"+s.Region, id, redis.LPosArgs{}).Result(); err == nil {
            s.QueuePosition = pos + 1
        }
    }
    return s, nil
}

func (b *Broker) Release(ctx context.Context, id string) error {
    n, err := releaseScript.Run(ctx, b.rdb, nil, id).Int64()
    if err != nil {
        return err
    }
    if n == 0 {
        return ErrNotFound
    }
    return nil
}