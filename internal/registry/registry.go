package registry

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const HeartbeatTTL = 6 * time.Second

type Node struct {
	ID       string `json:"id"`
	Region   string `json:"region"`
	Capacity int    `json:"capacity"`
	Used     int    `json:"used"`
	Alive    bool   `json:"alive"`
}

type Registry struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Registry {
	return &Registry{rdb: rdb}
}

func nodeKey(id string) string  { return "node:" + id }
func aliveKey(id string) string { return "node:" + id + ":alive" }

func (r *Registry) Register(ctx context.Context, n Node) error {
	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, nodeKey(n.ID), "region", n.Region, "capacity", n.Capacity)
	pipe.HSetNX(ctx, nodeKey(n.ID), "used", 0)
	pipe.SAdd(ctx, "nodes", n.ID)
	pipe.SAdd(ctx, "regions", n.Region)
	pipe.Set(ctx, aliveKey(n.ID), 1, HeartbeatTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *Registry) Heartbeat(ctx context.Context, id string) error {
	return r.rdb.Set(ctx, aliveKey(id), 1, HeartbeatTTL).Err()
}

func (r *Registry) List(ctx context.Context) ([]Node, error) {
	ids, err := r.rdb.SMembers(ctx, "nodes").Result()
	if err != nil {
		return nil, err
	}
	nodes := make([]Node, 0, len(ids))
	for _, id := range ids {
		h, err := r.rdb.HGetAll(ctx, nodeKey(id)).Result()
		if err != nil || len(h) == 0 {
			continue
		}
		capacity, _ := strconv.Atoi(h["capacity"])
		used, _ := strconv.Atoi(h["used"])
		alive, _ := r.rdb.Exists(ctx, aliveKey(id)).Result()
		nodes = append(nodes, Node{
			ID: id, Region: h["region"], Capacity: capacity, Used: used, Alive: alive == 1,
		})
	}
	return nodes, nil
}
