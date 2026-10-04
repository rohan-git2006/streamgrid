package broker

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// promoteScript moves ONE queued session of a region onto a node with free
// capacity. Returns {session_id, node_id}, or false if queue empty / no capacity.
var promoteScript = redis.NewScript(`
local region = ARGV[1]
local sid = redis.call('LINDEX', 'queue:' .. region, 0)
if not sid then
  return false
end

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

redis.call('LPOP', 'queue:' .. region)
redis.call('HINCRBY', 'node:' .. best, 'used', 1)
redis.call('HSET', 'session:' .. sid, 'node_id', best, 'status', 'RUNNING')
redis.call('SADD', 'node:' .. best .. ':sessions', sid)
return {sid, best}
`)

// reapScript handles one dead node: pushes its sessions to the FRONT of their
// region queue, resets used to 0 and clears its session set. Returns the
// number of sessions re-queued.
var reapScript = redis.NewScript(`
local node = ARGV[1]
local sessions = redis.call('SMEMBERS', 'node:' .. node .. ':sessions')
local n = 0
for _, sid in ipairs(sessions) do
  local sk = 'session:' .. sid
  if redis.call('EXISTS', sk) == 1 then
    local region = redis.call('HGET', sk, 'region')
    redis.call('HSET', sk, 'status', 'QUEUED')
    redis.call('HDEL', sk, 'node_id')
    redis.call('LPUSH', 'queue:' .. region, sid)
    n = n + 1
  end
end
redis.call('DEL', 'node:' .. node .. ':sessions')
redis.call('HSET', 'node:' .. node, 'used', 0)
return n
`)

type EventLogger interface {
	RecordNodeEvent(nodeID, event, detail string) error
}

// Run starts the drainer and reaper until ctx is cancelled.
func (b *Broker) Run(ctx context.Context, ev EventLogger) {
	go b.drainLoop(ctx, ev)
	go b.reapLoop(ctx, ev)
}

func (b *Broker) drainLoop(ctx context.Context, ev EventLogger) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			regions, err := b.rdb.SMembers(ctx, "regions").Result()
			if err != nil {
				continue
			}
			for _, region := range regions {
				for { // promote until the queue is empty or capacity runs out
					res, err := promoteScript.Run(ctx, b.rdb, nil, region).Result()
					if err != nil { // redis.Nil: nothing to promote
						break
					}
					pair := res.([]interface{})
					log.Printf("promoted session %v to %v", pair[0], pair[1])
					_ = ev.RecordNodeEvent(pair[1].(string), "SESSION_PROMOTED", pair[0].(string))
				}
			}
		}
	}
}

func (b *Broker) reapLoop(ctx context.Context, ev EventLogger) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ids, err := b.rdb.SMembers(ctx, "nodes").Result()
			if err != nil {
				continue
			}
			for _, id := range ids {
				alive, _ := b.rdb.Exists(ctx, "node:"+id+":alive").Result()
				if alive == 1 {
					continue
				}
				// Dead node: skip if it holds nothing (already reaped).
				cnt, _ := b.rdb.SCard(ctx, "node:"+id+":sessions").Result()
				if cnt == 0 {
					continue
				}
				n, err := reapScript.Run(ctx, b.rdb, nil, id).Int64()
				if err == nil && n > 0 {
					log.Printf("node %s dead: re-queued %d sessions", id, n)
					_ = ev.RecordNodeEvent(id, "NODE_DEAD", "requeued sessions="+itoa(n))
				}
			}
		}
	}
}

func itoa(n int64) string {
	return string(append([]byte(nil), []byte(fmtInt(n))...))
}

func fmtInt(n int64) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
