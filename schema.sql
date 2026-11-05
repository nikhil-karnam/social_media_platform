CREATE DATABASE IF NOT EXISTS twitter;

CREATE TABLE IF NOT EXISTS twitter.users (
  username       varchar(255) NOT NULL,
  email          varchar(255) NOT NULL,
  follower_count int NOT NULL DEFAULT 0,
  PRIMARY KEY (username),
  UNIQUE KEY (email)
);

CREATE TABLE IF NOT EXISTS twitter.follows (
  user   varchar(255) NOT NULL,
  target varchar(255) NOT NULL,
  PRIMARY KEY (user, target),
  KEY (target),
  FOREIGN KEY (user)   REFERENCES twitter.users (username),
  FOREIGN KEY (target) REFERENCES twitter.users (username)
);
