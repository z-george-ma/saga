CREATE SCHEMA IF NOT EXISTS saga;

DROP TABLE IF EXISTS saga.instance;
CREATE TABLE saga.instance (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100)  NOT NULL,
  instance_id VARCHAR(100) NOT NULL,
  step VARCHAR(100) NOT NULL,
  input BYTEA,
  state BYTEA,
  owner VARCHAR(100),
  start_after TIMESTAMP WITHOUT TIME ZONE,
  status INT NOT NULL -- 0: pending, 1: in_progress, 2: complete, 3: error
)
--

CREATE UNIQUE INDEX IX_instance_name_instance_id ON saga.instance(name, instance_id); -- ok
CREATE INDEX IX_instance_owner_status ON saga.instance(owner, status);

DROP TABLE IF EXISTS saga.consumer;
CREATE TABLE saga.consumer (
  id SERIAL PRIMARY key,
  owner VARCHAR(100),
  last_seen TIMESTAMP WITHOUT TIME ZONE
)

CREATE INDEX IX_saga_consumer_owner ON saga.consumer(owner);
CREATE INDEX IX_saga_consumer_last_seen ON saga.consumer(last_seen);

CREATE OR REPLACE PROCEDURE saga.heart_beat(owner_id VARCHAR(100), timeout int)
AS $$
  DECLARE 
    consumer_id BIGINT;
    now TIMESTAMP WITHOUT TIME ZONE = NOW();
  BEGIN
    UPDATE saga.consumer SET last_seen = now WHERE owner = owner_id RETURNING id INTO consumer_id;
    IF consumer_id IS NULL THEN
      INSERT INTO saga.consumer (owner, last_seen) VALUES(owner_id, now);
    END IF;
    CALL saga.clean_up_dead_owner(timeout);
  END
$$
LANGUAGE plpgsql

CREATE OR REPLACE PROCEDURE saga.clean_up_dead_owner(timeout int)
AS $$
  DECLARE 
    now TIMESTAMP WITHOUT TIME ZONE = NOW();
  BEGIN
    DELETE FROM saga.consumer WHERE last_seen < now - timeout * interval '1 seconds';
    UPDATE saga.instance AS s
    SET status = 0, start_after = now
    FROM saga.instance AS i
    LEFT OUTER JOIN saga.consumer AS c
    ON i.owner = c.owner
    WHERE c.id IS NULL AND s.id = i.id AND s.status = 1;
  END
$$
LANGUAGE plpgsql