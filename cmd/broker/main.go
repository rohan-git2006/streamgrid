package main

import (
    "log"
    "os"

    "github.com/gin-gonic/gin"
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
    rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
    reg := registry.New(rdb)

    st, err := store.Connect(env("CASSANDRA_HOST", "localhost"))
    if err != nil {
        log.Fatal(err)
    }
    defer st.Close()

    r := gin.Default()

    r.GET("/health", func(c *gin.Context) {
        status := gin.H{"redis": "ok", "cassandra": "ok"}
        code := 200
        if err := rdb.Ping(c).Err(); err != nil {
            status["redis"] = "down"
            code = 503
        }
        if err := st.Ping(); err != nil {
            status["cassandra"] = "down"
            code = 503
        }
        c.JSON(code, status)
    })

    r.GET("/nodes", func(c *gin.Context) {
        nodes, err := reg.List(c)
        if err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        alive := 0
        for _, n := range nodes {
            if n.Alive {
                alive++
            }
        }
        c.JSON(200, gin.H{"total": len(nodes), "alive": alive, "nodes": nodes})
    })

    r.GET("/nodes/:id/events", func(c *gin.Context) {
        events, err := st.NodeEvents(c.Param("id"), 50)
        if err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        c.JSON(200, events)
    })

    r.Run(":8080")
}