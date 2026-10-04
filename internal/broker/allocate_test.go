package broker_test

import (
    "context"
    "errors"
    "fmt"
    "sync"
    "testing"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/rohan-git2006/streamgrid/internal/broker"
    "github.com/rohan-git2006/streamgrid/internal/registry"
)

func setup(tb testing.TB) (*redis.Client, *broker.Broker, *registry.Registry) {
    tb.Helper()
    ctx := context.Background()
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15, PoolSize: 200})
    if err := rdb.Ping(ctx).Err(); err != nil {
        tb.Skipf("redis not available: %v", err)
    }
    rdb.FlushDB(ctx)
    tb.Cleanup(func() {
        rdb.FlushDB(ctx)
        rdb.Close()
    })
    return rdb, broker.New(rdb), registry.New(rdb)
}

func registerNodes(tb testing.TB, reg *registry.Registry, count, capacity int, region string) {
    tb.Helper()
    for i := 0; i < count; i++ {
        n := registry.Node{ID: fmt.Sprintf("node-%03d", i), Region: region, Capacity: capacity}
        if err := reg.Register(context.Background(), n); err != nil {
            tb.Fatal(err)
        }
    }
}

// 1000 users compete for 40 slots at the same instant.
// Exactly 40 may run, 960 must queue, and no node may exceed its capacity.
func TestNoOverAllocationUnderConcurrency(t *testing.T) {
    rdb, b, reg := setup(t)
    ctx := context.Background()
    registerNodes(t, reg, 10, 4, "asia-south")

    const users = 1000
    var wg sync.WaitGroup
    var mu sync.Mutex
    running, queued, failed := 0, 0, 0

    for i := 0; i < users; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            s, err := b.Allocate(ctx, fmt.Sprintf("user-%d", i), "asia-south")
            mu.Lock()
            defer mu.Unlock()
            switch {
            case err != nil:
                failed++
            case s.Status == "RUNNING":
                running++
            default:
                queued++
            }
        }(i)
    }
    wg.Wait()

    if failed != 0 {
        t.Fatalf("%d requests failed", failed)
    }
    if running != 40 {
        t.Fatalf("expected exactly 40 running sessions, got %d", running)
    }
    if queued != 960 {
        t.Fatalf("expected 960 queued sessions, got %d", queued)
    }

    nodes, err := reg.List(ctx)
    if err != nil {
        t.Fatal(err)
    }
    for _, n := range nodes {
        if n.Used > n.Capacity {
            t.Fatalf("%s over-allocated: used %d > capacity %d", n.ID, n.Used, n.Capacity)
        }
        if n.Used != n.Capacity {
            t.Fatalf("%s should be full: used %d, capacity %d", n.ID, n.Used, n.Capacity)
        }
    }
    if l, _ := rdb.LLen(ctx, "queue:asia-south").Result(); l != 960 {
        t.Fatalf("queue length should be 960, got %d", l)
    }
}

// Releasing a session frees its slot and deletes it. A second release is a 404.
func TestReleaseFreesSlot(t *testing.T) {
    _, b, reg := setup(t)
    ctx := context.Background()
    registerNodes(t, reg, 1, 1, "eu-west")

    s1, err := b.Allocate(ctx, "u1", "eu-west")
    if err != nil || s1.Status != "RUNNING" {
        t.Fatalf("u1 should run: %v %+v", err, s1)
    }
    s2, err := b.Allocate(ctx, "u2", "eu-west")
    if err != nil || s2.Status != "QUEUED" {
        t.Fatalf("u2 should queue: %v %+v", err, s2)
    }

    if err := b.Release(ctx, s1.ID); err != nil {
        t.Fatal(err)
    }
    nodes, _ := reg.List(ctx)
    if nodes[0].Used != 0 {
        t.Fatalf("slot not freed, used=%d", nodes[0].Used)
    }
    if _, err := b.Get(ctx, s1.ID); !errors.Is(err, broker.ErrNotFound) {
        t.Fatalf("released session should be gone, got %v", err)
    }
    if err := b.Release(ctx, s1.ID); !errors.Is(err, broker.ErrNotFound) {
        t.Fatalf("second release should be ErrNotFound, got %v", err)
    }
}

// A node whose heartbeat lease has expired must never be handed out.
func TestExpiredLeaseIsNotAllocated(t *testing.T) {
    rdb, b, reg := setup(t)
    ctx := context.Background()
    registerNodes(t, reg, 1, 4, "us-east")

    s, err := b.Allocate(ctx, "alive-user", "us-east")
    if err != nil || s.Status != "RUNNING" {
        t.Fatalf("live node should accept: %v %+v", err, s)
    }

    // Simulate missed heartbeats: shrink the liveness TTL and let it expire.
    rdb.PExpire(ctx, "node:node-000:alive", 100*time.Millisecond)
    time.Sleep(300 * time.Millisecond)

    s, err = b.Allocate(ctx, "late-user", "us-east")
    if err != nil {
        t.Fatal(err)
    }
    if s.Status != "QUEUED" {
        t.Fatalf("dead node must not receive sessions, got %s on %s", s.Status, s.NodeID)
    }
}

// Throughput of the atomic Lua allocation path under parallel load.
func BenchmarkAllocate(b *testing.B) {
    _, br, reg := setup(b)
    ctx := context.Background()
    registerNodes(b, reg, 20, 1000000000, "bench")

    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            if _, err := br.Allocate(ctx, "bench-user", "bench"); err != nil {
                b.Error(err)
                return
            }
        }
    })
}