package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/rohan-git2006/streamgrid/internal/registry"
    "github.com/rohan-git2006/streamgrid/internal/store"
)

func env(k, def string) string {
    if v := os.Getenv(k); v != "" {
        return v
    }
    return def
}

func main() {
    count := flag.Int("nodes", 10, "number of simulated GPU nodes")
    capacity := flag.Int("capacity", 4, "concurrent sessions per node")
    regions := flag.String("regions", "asia-south,eu-west,us-east", "comma-separated regions")
    flag.Parse()

    rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
    reg := registry.New(rdb)

    st, err := store.Connect(env("CASSANDRA_HOST", "localhost"))
    if err != nil {
        log.Fatal(err)
    }
    defer st.Close()

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    regionList := strings.Split(*regions, ",")
    for i := 0; i < *count; i++ {
        n := registry.Node{
            ID:       fmt.Sprintf("node-%03d", i),
            Region:   regionList[i%len(regionList)],
            Capacity: *capacity,
        }
        if err := reg.Register(ctx, n); err != nil {
            log.Fatalf("register %s: %v", n.ID, err)
        }
        if err := st.RecordNodeEvent(n.ID, "REGISTERED", fmt.Sprintf("region=%s capacity=%d", n.Region, n.Capacity)); err != nil {
            log.Printf("event %s: %v", n.ID, err)
        }
        go heartbeatLoop(ctx, reg, n.ID)
    }
    log.Printf("agent running: %d nodes, capacity %d each", *count, *capacity)

    <-ctx.Done()
    log.Println("agent stopping (nodes will expire after TTL)")
}

func heartbeatLoop(ctx context.Context, reg *registry.Registry, id string) {
    t := time.NewTicker(2 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            if err := reg.Heartbeat(ctx, id); err != nil {
                log.Printf("heartbeat %s: %v", id, err)
            }
        }
    }
}