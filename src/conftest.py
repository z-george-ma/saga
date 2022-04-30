import asyncio
from typing import Coroutine
import uuid
from pytest import fixture, raises
from .dal import SagaDAL, SagaDTO
from .database import Database


@fixture(scope="session")
def db():
    async def run():
        db = Database("postgresql://postgres:1234@localhost:5432/postgres")
        await db.execute(
            "DROP TABLE IF EXISTS test;\
CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, column_1 VARCHAR(100));\
INSERT INTO test (id, column_1) VALUES(1, 'abc');\
INSERT INTO test (id, column_1) VALUES(2, 'def');\
INSERT INTO test (id, column_1) VALUES(3, 'def');"
        )
        return db

    return run_async(run())


@fixture(scope="session")
def dal(db):
    dal = SagaDAL(db)
    run_async(
        db.execute(
            "DROP TABLE IF EXISTS saga.instance;\
DROP TABLE IF EXISTS saga.runner;\
"
        )
    )
    run_async(dal.init_db())
    run_async(
        dal.new_saga(
            SagaDTO(
                id=1,
                name="test",
                instance="random",
                step="step",
                input="abc",
                state="abc",
                runner="runner",
                delay=None,
                status=0,
                error=None,
            )
        )
    )
    run_async(
        dal.new_saga(
            SagaDTO(
                id=2,
                name="event_loop",
                instance="random",
                step="step123",
                input="abc",
                state="abc",
                runner="runner",
                delay=-20,
                status=0,
                error=None,
            )
        )
    )
    return dal


def run_async(co: Coroutine):
    return asyncio.get_event_loop().run_until_complete(co)


@fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["asyncio"] = asyncio
    doctest_namespace["raises"] = raises
    doctest_namespace["run_async"] = run_async
    doctest_namespace["uuid"] = uuid
