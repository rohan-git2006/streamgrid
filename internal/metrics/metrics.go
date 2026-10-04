package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: []float64{.001, .002, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"method", "route", "code"})

	Allocations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamgrid_allocations_total",
		Help: "Session requests by outcome (running or queued)",
	}, []string{"outcome"})

	Promotions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamgrid_promotions_total",
		Help: "Queued sessions promoted onto a node",
	})

	Requeued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamgrid_requeued_sessions_total",
		Help: "Sessions re-queued because their node died",
	})

	NodesAlive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamgrid_nodes_alive",
		Help: "Nodes currently heartbeating",
	})

	SessionsRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamgrid_sessions_running",
		Help: "Slots in use across alive nodes",
	})

	QueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamgrid_queue_length",
		Help: "Users waiting per region",
	}, []string{"region"})
)

func Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		HTTPDuration.WithLabelValues(c.Request.Method, route, strconv.Itoa(c.Writer.Status())).
			Observe(time.Since(start).Seconds())
	}
}

func Handler() gin.HandlerFunc {
	h := promhttp.Handler()
	return func(c *gin.Context) { h.ServeHTTP(c.Writer, c.Request) }
}
