from dataclasses import dataclass
import pickle
from typing import Any, Dict, Optional
from .active_record import activerecord

from .database import (
    Database,
    Mapping,
    from_data_class,
    updates,
    fields,
    placeholders,
)


@activerecord
@dataclass
class SagaDTO:
    id: int
    name: str
    instance: str
    step: str
    input: Any
    state: Any
    runner: str
    delay: float
    status: int
    error: Exception


class SagaDAL:
    class ConcurrencyException(Exception):
        pass

    def __init__(self, db: Database):
        self._db = db

    async def new_saga(self, saga: SagaDTO):
        """Return a new saga

        Examples:
        >>> dal = getfixture('dal')
        >>> saga = run_async(
        ...   dal.new_saga(
        ...     SagaDTO(
        ...       id=1,
        ...       name="test",
        ...       instance="random1",
        ...       step="step",
        ...       input="abc",
        ...       state="abc",
        ...       runner="runner",
        ...       delay=None,
        ...       status=1,
        ...       error=None,
        ...     )
        ...   )
        ... )
        >>> saga.name
        'test'
        """

        dao = from_data_class(
            saga,
            input=lambda x: pickle.dumps(x),
            state=lambda x: pickle.dumps(x),
            instance_id=("instance", lambda x: x),
            runner_id=("runner", lambda x: x),
        )
        delay = dao.pop("delay", 0)
        dao.pop("id", 0)

        id = await self._db.fetchval(
            f"INSERT INTO saga.instance ({fields(dao)},start_after) VALUES({placeholders(dao)},now()::timestamp+interval '1 seconds'* :delay) RETURNING id",
            **dao,
            delay=delay or 0,
        )
        saga.id = id
        saga.clear()
        return saga

    async def get_instance(self, name, instance) -> Optional[Dict]:
        """Return id and state for a pending saga with given name and instance.

        Examples
        >>> dal = getfixture('dal')
        >>> run_async(dal.get_instance('test', 'random'))
        {'id': 1, 'state': 'abc'}
        """
        return await self._db.fetchrow(
            "SELECT id, state FROM saga.instance WHERE name = :name AND instance_id = :instance AND status = 0",
            Mapping(dict, state=lambda s: pickle.loads(s)),
            name=name,
            instance=instance,
        )

    async def get_state(self, name, instance):
        """Return state for a saga with given name and instance.

        Examples
        >>> dal = getfixture('dal')
        >>> run_async(dal.get_state('test', 'random'))
        'abc'
        """
        ret = await self._db.fetchval(
            "SELECT state FROM saga.instance WHERE name = :name AND instance_id = :instance",
            name=name,
            instance=instance,
        )
        return pickle.loads(ret)

    async def update_processing(self, saga: SagaDTO):
        """Update the saga if it is in processing status and runner matches the value in *saga*.
        Otherwise it will throw ConcurrencyException.

        Examples
        >>> dal = getfixture('dal')
        >>> saga = SagaDTO(id=3,name="test",instance="random3",step="step",input="abc",state="abc",runner='runner',delay=None,status=1,error=None,)
        >>> saga = run_async(dal.new_saga(saga))
        >>> saga.input="def"
        >>> saga.state="def"
        >>> run_async(dal.update_processing(saga))
        >>> run_async(dal.get_state('test', 'random3'))
        'def'
        >>> saga.state = '111'
        >>> saga.runner='runner123'
        >>> with raises(SagaDAL.ConcurrencyException): run_async(dal.update_processing(saga))
        """
        dao = from_data_class(
            saga,
            saga.changes(),
            input=lambda x: pickle.dumps(x),
            state=lambda x: pickle.dumps(x),
            instance_id=("instance", lambda x: x),
        )
        saga.clear()

        # runner cannot be updated
        dao.pop("runner", None)
        # delay will be covered manually
        dao.pop("delay", 0)

        if len(dao) == 0:
            return None

        id = await self._db.fetchval(
            f"UPDATE saga.instance SET {updates(dao)},start_after=now()::timestamp+interval '1 seconds'* :delay WHERE id = :id AND status = 1 AND runner_id = :runner_id RETURNING id",
            **dao,
            delay=saga.delay or 0,
            id=saga.id,
            runner_id=saga.runner,
        )
        if id == None:
            raise SagaDAL.ConcurrencyException()

    async def update_pending(self, saga: SagaDTO):
        """Return state for a saga with given name and instance.

        Examples
        >>> dal = getfixture('dal')
        >>> saga = SagaDTO(id=4,name="test",instance="random4",step="step",input="abc",state="abc",runner='runner',delay=None,status=0,error=None,)
        >>> saga = run_async(dal.new_saga(saga))
        >>> saga.runner="runner4"
        >>> saga.state="def"
        >>> run_async(dal.update_pending(saga))
        >>> run_async(dal.get_state('test', 'random4'))
        'def'

        If the saga isn't in pending status, it will throw a ConcurrencyException

        >>> saga.status = 1
        >>> saga.runner = 'my_runner'
        >>> run_async(dal.update_pending(saga))
        >>> saga.state = 'aaa'
        >>> with raises(SagaDAL.ConcurrencyException): run_async(dal.update_pending(saga))
        """
        if len(saga.changes()) == 0:
            return None

        dao = from_data_class(
            saga,
            saga.changes(),
            input=lambda x: pickle.dumps(x),
            state=lambda x: pickle.dumps(x),
            instance_id=("instance", lambda x: x),
            runner_id=("runner", lambda x: x),
        )
        saga.clear()

        # id and delay will be covered manually
        dao.pop("id", 0)
        dao.pop("delay", 0)

        id = await self._db.fetchval(
            f"UPDATE saga.instance SET {updates(dao)},start_after=now()::timestamp+interval '1 seconds'* :delay WHERE id = :id AND status = 0 RETURNING id",
            **dao,
            delay=saga.delay or 0,
            id=saga.id,
        )
        if id == None:
            raise SagaDAL.ConcurrencyException()

    async def get_pending_saga(self, name, runner, batch_size):
        """Return pending sagas that are scheduled to be started, and set them to be in_progress

        *name*: saga name
        *runner*: runner_id to be set to for the pending sagas
        *batch_size*: number of pending sagas to retrieve

        Examples:
        >>> dal = getfixture('dal')
        >>> run_async(dal.get_pending_saga("random_name", "runner1", 100))
        []
        """
        return await self._db.fetch(
            """
WITH cte AS (
  SELECT id FROM saga.instance
  WHERE name = :name AND status = 0 AND start_after < now()::timestamp
  ORDER BY start_after
  LIMIT :limit
)
UPDATE saga.instance i
SET    status = 1, runner_id = :runner
FROM   cte
WHERE  cte.id = i.id AND i.status = 0
RETURNING i.*
""",
            Mapping(
                SagaDTO,
                input=lambda x: pickle.loads(x),
                state=lambda x: pickle.loads(x),
                instance=("instance_id", lambda x: x),
                runner=("runner_id", lambda x: x),
                delay=("start_after", lambda x: 0),
            ),
            name=name,
            limit=batch_size,
            runner=runner,
        )

    async def keep_alive(self, runner, timeout):
        """Keep current runner alive and clean up timed out runners based on *timeout* value

        Examples:
        >>> dal = getfixture('dal')
        >>> run_async(dal.keep_alive("my_runner", 5))
        """
        await self._db.execute(
            "call saga.heart_beat(:runner, :timeout)",
            runner=runner,
            timeout=timeout,
        )

    async def init_db(self):
        """Initialize the database"""
        await self._db.execute(
            """
BEGIN;
CREATE SCHEMA IF NOT EXISTS saga;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS saga.instance (
  id BIGSERIAL PRIMARY KEY,
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
  id BIGSERIAL PRIMARY key,
  runner_id VARCHAR(100),
  last_seen TIMESTAMP WITHOUT TIME ZONE
);
COMMIT;

BEGIN;
CREATE UNIQUE INDEX IF NOT EXISTS IX_instance_name_instance_id ON saga.instance(name, instance_id); -- ok
CREATE INDEX IF NOT EXISTS IX_instance_runner_id_status ON saga.instance(runner_id, status);
CREATE INDEX IF NOT EXISTS IX_saga_runner_runner_id ON saga.runner(runner_id);
CREATE INDEX IF NOT EXISTS IX_saga_runner_last_seen ON saga.runner(last_seen);

DROP PROCEDURE IF EXISTS saga.clean_up_dead_runner;
CREATE OR REPLACE PROCEDURE saga.heart_beat(current_runner VARCHAR(100), timeout int)
AS $$
  DECLARE 
    r_id BIGINT;
    now TIMESTAMP WITHOUT TIME ZONE = NOW();
  BEGIN
    UPDATE saga.runner SET last_seen = now WHERE runner_id = current_runner RETURNING id INTO r_id;
    IF r_id IS NULL THEN
      INSERT INTO saga.runner (runner_id, last_seen) VALUES(current_runner, now);
    END IF;
    CALL saga.clean_up_dead_runner(timeout);
  END
$$
LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS saga.clean_up_dead_runner;
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
