from collections import namedtuple
import pickle

from database import Database, Dao, SqlOutput

SagaDTO = namedtuple(
    "SagaDTO", "id, name, instance_id, step, input, state, owner, delay, status"
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
            f"UPDATE saga.instance SET {dao.update()},start_after=now()::timestamp+interval '1 seconds'* ${dao.next} WHERE id = ${dao.next} AND status = 1 AND owner = ${dao.next} RETURNING id",
            *dao.values(),
            saga.delay,
            saga.id,
            saga.owner,
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

    async def get_pending_saga(self, name, owner, batch_size):
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
SET    status = 1, owner = $3
FROM   cte
WHERE  cte.id = i.id AND i.status = 0
RETURNING i.*
""",
            so,
            name,
            batch_size,
            owner,
        )
        return ret

    async def keep_alive(self, owner, timeout):
        await self._db.execute("call saga.heart_beat($1, $2)", owner, timeout)
