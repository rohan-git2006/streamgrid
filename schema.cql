CREATE KEYSPACE IF NOT EXISTS streamgrid
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE streamgrid;

CREATE TABLE IF NOT EXISTS node_events (
  node_id text,
  day date,
  ts timestamp,
  event text,
  detail text,
  PRIMARY KEY ((node_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS sessions_by_user (
  user_id text,
  started_at timestamp,
  session_id uuid,
  node_id text,
  region text,
  status text,
  PRIMARY KEY ((user_id), started_at, session_id)
) WITH CLUSTERING ORDER BY (started_at DESC, session_id ASC);