package broker

import (
	"context"
	"time"

	"github.com/rohan-git2006/streamgrid/internal/metrics"
)

// collectLoop samples fleet and queue state from Redis for Prometheus.
func (b *Broker) collectLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ids, err := b.rdb.SMembers(ctx, "nodes").Result(); err == nil {
				alive, used := 0, 0
				for _, id := range ids {
					if n, _ := b.rdb.Exists(ctx, "node:"+id+":alive").Result(); n == 1 {
						alive++
						u, _ := b.rdb.HGet(ctx, "node:"+id, "used").Int()
						used += u
					}
				}
				metrics.NodesAlive.Set(float64(alive))
				metrics.SessionsRunning.Set(float64(used))
			}
			if regions, err := b.rdb.SMembers(ctx, "regions").Result(); err == nil {
				for _, r := range regions {
					l, _ := b.rdb.LLen(ctx, "queue:"+r).Result()
					metrics.QueueLength.WithLabelValues(r).Set(float64(l))
				}
			}
		}
	}
}
