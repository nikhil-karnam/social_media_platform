CREATE KEYSPACE IF NOT EXISTS tweets WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS tweets.posts (
  username text,
  posted_at bigint,
  tweet text,
  PRIMARY KEY (username, posted_at)
) WITH CLUSTERING ORDER BY (posted_at DESC);

CREATE TABLE IF NOT EXISTS tweets.outbox (
  username text,
  posted_at bigint,
  mail text,
  PRIMARY KEY (username, posted_at)
) WITH gc_grace_seconds = 3600;

CREATE TABLE IF NOT EXISTS tweets.celeb_outbox (
  username text,
  posted_at bigint,
  mail text,
  PRIMARY KEY (username, posted_at)
) WITH gc_grace_seconds = 3600;