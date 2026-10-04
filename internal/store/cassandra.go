package store

import (
    "fmt"
    "time"

    "github.com/gocql/gocql"
)

type Event struct {
    TS     time.Time `json:"ts"`
    Event  string    `json:"event"`
    Detail string    `json:"detail"`
}

type Store struct {
    s *gocql.Session
}

// Connect retries for ~90s because Cassandra starts slowly.
func Connect(host string) (*Store, error) {
    cluster := gocql.NewCluster(host)
    cluster.Keyspace = "streamgrid"
    cluster.Consistency = gocql.One
    cluster.Timeout = 5 * time.Second

    var lastErr error
    for i := 0; i < 30; i++ {
        sess, err := cluster.CreateSession()
        if err == nil {
            return &Store{s: sess}, nil
        }
        lastErr = err
        time.Sleep(3 * time.Second)
    }
    return nil, fmt.Errorf("cassandra not reachable: %w", lastErr)
}

func (st *Store) Close() { st.s.Close() }

func (st *Store) Ping() error {
    return st.s.Query(`SELECT release_version FROM system.local`).Exec()
}

func (st *Store) RecordNodeEvent(nodeID, event, detail string) error {
    now := time.Now().UTC()
    return st.s.Query(
        `INSERT INTO node_events (node_id, day, ts, event, detail) VALUES (?, ?, ?, ?, ?)`,
        nodeID, now, now, event, detail,
    ).Exec()
}

// NodeEvents returns today's events for a node, newest first.
func (st *Store) NodeEvents(nodeID string, limit int) ([]Event, error) {
    iter := st.s.Query(
        `SELECT ts, event, detail FROM node_events WHERE node_id = ? AND day = ? LIMIT ?`,
        nodeID, time.Now().UTC(), limit,
    ).Iter()

    events := []Event{}
    var e Event
    for iter.Scan(&e.TS, &e.Event, &e.Detail) {
        events = append(events, e)
    }
    return events, iter.Close()
}