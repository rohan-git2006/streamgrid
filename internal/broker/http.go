package broker

import (
    "errors"

    "github.com/gin-gonic/gin"
)

type createReq struct {
    UserID string `json:"user_id" binding:"required"`
    Region string `json:"region" binding:"required"`
}

func (b *Broker) Routes(r *gin.Engine) {
    r.POST("/sessions", func(c *gin.Context) {
        var req createReq
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(400, gin.H{"error": err.Error()})
            return
        }
        s, err := b.Allocate(c, req.UserID, req.Region)
        if err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        if s.Status == "QUEUED" {
            c.JSON(202, s)
            return
        }
        c.JSON(201, s)
    })

    r.GET("/sessions/:id", func(c *gin.Context) {
        s, err := b.Get(c, c.Param("id"))
        if errors.Is(err, ErrNotFound) {
            c.JSON(404, gin.H{"error": "session not found"})
            return
        }
        if err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        c.JSON(200, s)
    })

    r.DELETE("/sessions/:id", func(c *gin.Context) {
        err := b.Release(c, c.Param("id"))
        if errors.Is(err, ErrNotFound) {
            c.JSON(404, gin.H{"error": "session not found"})
            return
        }
        if err != nil {
            c.JSON(500, gin.H{"error": err.Error()})
            return
        }
        c.JSON(200, gin.H{"released": true})
    })
}