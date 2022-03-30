from collections import namedtuple
import pickle

from database import Database, Dao, SqlOutput

SagaDTO = namedtuple(
    "SagaDTO",
    "id, name, instance_id, step, input, state, runner_id, delay, status, error",
)


class SagaDAL:
    class ConcurrencyException(Exception):
        pass

    def __init__(self, db: Database):
        self._db = db

    async def new_saga(self, saga: SagaDTO):
        dao = Dao.from_named_tuple(
            saga._replace(
                input=pickle.dumps(saga.input), state=pickle.dumps(saga.state)
            )
        ).exclude("id, delay")
        id = await self._db.fetchval(
            f"INSERT INTO saga.instance ({dao.fields()},start_after) VALUES({dao.placeholders()},now()::timestamp+interval '1 seconds'* ${dao.next}) RETURNING id",
            *dao.values(),
            saga.delay,
        )
        return saga._replace(id=id)

    async def get_instance(self, name, instance_id):
        so = SqlOutput(dict).map("state", lambda s: pickle.loads(s))
        return await self._db.fetchrow(
            "SELECT id, state FROM saga.instance WHERE name = $1 AND instance_id = $2 AND status = 0",
            so,
            name,
            instance_id,
        )
        # TODO: empty

    async def get_state(self, name, instance_id):
        ret = await self._db.fetchval(
            "SELECT state FROM saga.instance WHERE name = $1 AND instance_id = $2",
            name,
            instance_id,
        )
        return pickle.loads(ret)

    async def update_processing(self, saga: SagaDTO, fields: str):
        dao = Dao.from_named_tuple(
            saga._replace(
                input=pickle.dumps(saga.input), state=pickle.dumps(saga.state)
            )
        )
        dao.include(fields).exclude("delay")
        id = await self._db.fetchval(
            f"UPDATE saga.instance SET {dao.update()},start_after=now()::timestamp+interval '1 seconds'* ${dao.next} WHERE id = ${dao.next} AND status = 1 AND runner_id = ${dao.next} RETURNING id",
            *dao.values(),
            saga.delay,
            saga.id,
            saga.runner_id,
        )
        if id == None:
            raise SagaDAL.ConcurrencyException()

    async def update_pending(self, saga: SagaDTO, fields: str):
        dao = Dao.from_named_tuple(
            saga._replace(
                input=pickle.dumps(saga.input), state=pickle.dumps(saga.state)
            )
        )
        dao.include(fields).exclude("delay")
        id = await self._db.fetchval(
            f"UPDATE saga.instance SET {dao.update()},start_after=now()::timestamp+interval '1 seconds'* ${dao.next} WHERE id = ${dao.next} AND status = 0 RETURNING id",
            *dao.values(),
            saga.delay,
            saga.id,
        )
        if id == None:
            raise SagaDAL.ConcurrencyException()

    async def get_pending_saga(self, name, runner_id, batch_size):
        so = (
            SqlOutput(SagaDTO)
            .map("input", lambda s: pickle.loads(s))
            .map("state", lambda s: pickle.loads(s))
            .map("start_after", (lambda s: 0), "delay")
        )

        ret = await self._db.fetch(
            """
WITH cte AS (
  SELECT id FROM saga.instance
  WHERE name = $1 AND status = 0 AND start_after < now()::timestamp
  ORDER BY start_after
  LIMIT $2
)
UPDATE saga.instance i
SET    status = 1, runner_id = $3
FROM   cte
WHERE  cte.id = i.id AND i.status = 0
RETURNING i.*
""",
            so,
            name,
            batch_size,
            runner_id,
        )
        return ret

    async def keep_alive(self, runner_id, timeout):
        await self._db.execute("call saga.heart_beat($1, $2)", runner_id, timeout)

    async def init_db(self):
        await self._db.execute(
            """
BEGIN;
CREATE SCHEMA IF NOT EXISTS saga;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS saga.instance (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100)  NOT NULL,
  instance_id VARCHAR(100) NOT NULL,
  step VARCHAR(100) NOT NULL,
  input BYTEA,
  state BYTEA,
  runner_id VARCHAR(100),
  start_after TIMESTAMP WITHOUT TIME ZONE,
  status INT NOT NULL, -- 0: pending, 1: in_progress, 2: complete, 3: error
  error TEXT
);
CREATE TABLE IF NOT EXISTS saga.runner (
  id SERIAL PRIMARY key,
  runner_id VARCHAR(100),
  last_seen TIMESTAMP WITHOUT TIME ZONE
);
COMMIT;

BEGIN;
CREATE UNIQUE INDEX IF NOT EXISTS IX_instance_name_instance_id ON saga.instance(name, instance_id); -- ok
CREATE INDEX IF NOT EXISTS IX_instance_runner_id_status ON saga.instance(runner_id, status);
CREATE INDEX IF NOT EXISTS IX_saga_runner_runner_id ON saga.runner(runner_id);
CREATE INDEX IF NOT EXISTS IX_saga_runner_last_seen ON saga.runner(last_seen);
CREATE OR REPLACE PROCEDURE saga.heart_beat(runner VARCHAR(100), timeout int)
AS $$
  DECLARE 
    r_id BIGINT;
    now TIMESTAMP WITHOUT TIME ZONE = NOW();
  BEGIN
    UPDATE saga.runner SET last_seen = now WHERE runner_id = runner RETURNING id INTO r_id;
    IF r_id IS NULL THEN
      INSERT INTO saga.runner (runner_id, last_seen) VALUES(r_id, now);
    END IF;
    CALL saga.clean_up_dead_runner(timeout);
  END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE saga.clean_up_dead_runner(timeout int)
AS $$
  DECLARE 
    now TIMESTAMP WITHOUT TIME ZONE = NOW();
  BEGIN
    DELETE FROM saga.runner WHERE last_seen < now - timeout * interval '1 seconds';
    UPDATE saga.instance AS s
    SET status = 0, start_after = now
    FROM saga.instance AS i
    LEFT OUTER JOIN saga.runner AS c
    ON i.runner_id = c.runner_id
    WHERE c.id IS NULL AND s.id = i.id AND s.status = 1;
  END
$$
LANGUAGE plpgsql;
COMMIT;
        """
        )
