from asyncio import (
    CancelledError,
    gather,
    get_event_loop,
    sleep,
)
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, TypeVar, Union
import uuid, sys
from dal import SagaDAL, SagaDTO
from stream_utils import AsyncIterator, merge, start_loop, poll

from database import Database


TInput = TypeVar("TInput")
TState = TypeVar("TState")


@dataclass
class SagaReturn:
    step: str
    input: TInput
    state: Union[TState, None] = None
    delay: float = None


class SagaError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Saga:
    def _set_state(dal: SagaDAL, saga: SagaDTO):
        """Return a function to set internal state for a saga"""

        async def func(state):
            new_saga = saga._replace(state=state)
            await dal.update_processing(new_saga, "state")

        return func

    def _saga_step_decorator_generator(self):
        def saga_step_decorator(
            func: Callable[
                [TInput, TState, Callable[[TState], Coroutine[Any, Any, None]]],
                Union[SagaReturn, Any],
            ]
        ):
            """Decorator for a saga function

            See example for the format of a saga function

            Saga function can either return another saga function for continuation, or any other value as final state to terminate the saga

            Example:
            >>> @saga.step
            >>> my_func(input, state, set_state):
            >>>    return another_saga_func(input, state, delay=10) # delay execution for 10 seconds

            """

            async def saga_wrapper(saga: SagaDTO):
                try:
                    # call saga function
                    ret = await func(
                        saga.input, saga.state, Saga._set_state(self._dal, saga)
                    )

                    if isinstance(ret, SagaReturn):
                        saga = self._create_saga_dto(
                            saga.name,
                            saga.runner_id,
                            saga.instance_id,
                            ret.step,
                            ret.input,
                            ret.state,
                            saga.id,
                            ret.delay,
                        )
                        fields = "step, input, runner_id, delay, status"
                    else:
                        # if return value isn't a SagaReturn object, it is treated as end of saga, with return value as final state if not none
                        saga = saga._replace(state=ret, status=2)
                        fields = "status"

                    if saga.state != None:
                        fields += ", state"

                    await self._dal.update_processing(saga, fields)

                    if saga.status == 1:
                        self._in_mem_sagas.send(saga)
                except SagaDAL.ConcurrencyException:
                    # update failed - the saga is claimed by another process
                    pass
                except SagaError as e:
                    saga = saga._replace(error=repr(e), status=3)
                    await self._dal.update_processing(saga, "status, error")
                except Exception as e:
                    try:
                        print(
                            "Error:",
                            sys._getframe(1).f_code.co_name,
                            sys._getframe(1).f_lineno,
                            e,
                        )
                        # put back to the database for future pickup
                        saga = saga._replace(status=0)
                        await self._dal.update_processing(saga, "status")
                    except:
                        # TODO: review
                        pass

            name = func.__name__
            self._function_dict[name] = saga_wrapper

            # Consider the following example:
            #
            # @saga.step
            # def step1(input, state, set_state):
            #    return step2(input)
            #
            # the function will convert the next step into a SagaReturn
            #
            def meta_definition(
                input: TInput, state: Union[TState, None] = None, delay: float = None
            ):
                return SagaReturn(name, input, state, delay)

            # set __name__ back to original function name
            meta_definition.__name__ = name
            return meta_definition

        return saga_step_decorator

    def __init__(
        self,
        name: str,
        **kw_args,
    ):
        self.name = name
        self.runner_id = uuid.uuid4().hex

        # SagaDAL can be injected in for internal testing. Outside this module, caller will pass in dsn
        self._dal = kw_args.get("dal") or SagaDAL(Database(kw_args["dsn"]))

        self._function_dict = {}
        self._in_mem_sagas = AsyncIterator()

        self.step = self._saga_step_decorator_generator()

    async def start(self, instance_id: str, step, input, state, delay: float = None):
        """Start a new saga

        Note:
        Worker pool is not created until `start_event_loop` is called.

        If *delay* is set, the item will be enqueued into memory for immediate processing.

        The item may get stuck for some time if the worker pool hasn't been created.

        So for web server that doesn't need worker pool, make sure *delay* is set to be None.
        """
        saga = self._create_saga_dto(
            self.name, self.runner_id, instance_id, step, input, state, delay=delay
        )
        saga = await self._dal.new_saga(saga)

        # if status of saga is in_progress (immediate start), send to task queue for consumption
        if saga.status == 1:
            self._in_mem_sagas.send(saga)

    async def get_state(self, instance_id: str):
        """Return the internal state of a saga"""

        return await self._dal.get_state(self.name, instance_id)

    async def call(self, instance_id: str, step, input, delay: float = None):
        """Overwrite a saga with new step, input and delay"""

        ret = await self._dal.get_instance(self.name, instance_id)
        saga = self._create_saga_dto(
            self.name,
            self.runner_id,
            instance_id,
            step,
            input,
            ret["state"],
            ret["id"],
            delay,
        )
        await self._dal.update_pending(saga, "step, input, runner_id, delay, status")

        # if status of saga is in_progress (immediate start), send to task queue for consumption
        if saga.status == 1:
            self._in_mem_sagas.send(saga)

    async def start_event_loop(
        self,
        batch_size: int,
        low_watermark: int,
        backoff: float,
        available_workers: int,
        health_check_timeout: int,
    ):
        """Start the event loop to do the following

        1. heart beat to register and keep the instance alive
        2. poll DB for pending sagas. It will *backoff* in seconds if in mem queue size > *low_watermark*, or get less items from db, per *batch_size*
        3. create worker pool to execute saga functions (pool size defined by *available_workers*)
        """
        cancelled = False
        cancel = lambda: cancelled

        async def get_pending_saga():
            try:
                return await self._dal.get_pending_saga(
                    self.name, self.runner_id, batch_size
                )
            except Exception as e:
                return []

        async def execute_saga(saga: SagaDTO):
            if not saga.step in self._function_dict:
                saga = saga._replace(
                    error=repr(SagaError("Unrecognized saga step")),
                    status=3,
                )
                await self._dal.update_processing(saga, "error, status")
            else:
                await self._function_dict[saga.step](saga)

        # send regular heart beat to DB
        async def health_check(cancel):
            while not cancel():
                try:
                    await self._dal.keep_alive(self.runner_id, health_check_timeout)
                except:
                    pass
                await sleep(1)

        # merge two queues (async generator) - tasks from polling and in-memory ones into a single stream
        task_queue = merge(
            poll(get_pending_saga, low_watermark, backoff, cancel),
            self._in_mem_sagas,
        )

        try:
            await gather(
                health_check(cancel),
                start_loop(execute_saga, task_queue, available_workers, cancel),
            )
        except CancelledError:
            # asyncio.get_event_loop.stop() will cancel all tasks in event loop, which generates CancelledError
            # seeing this error means event_loop has stopped, so set the flag to break all loops
            cancelled = True
            # TODO: more clean up, e.g. terminate task queue, return items back to DB

    @staticmethod
    def _create_saga_dto(
        name,
        runner_id: str,
        instance_id: str,
        step,
        input,
        state,
        id=0,
        delay: float = None,
        error: str = None,
    ):
        """Construct SagaDTO for consumption by SagaDAL"""

        # if delay is not set, set status to be in_progress for immediate consumption
        if delay == None:
            status = 1
            delay = 0
        else:
            status = 0

        # convert function into its name
        if callable(step):
            step = step.__name__

        return SagaDTO(
            id, name, instance_id, step, input, state, runner_id, delay, status, error
        )


dal = SagaDAL(Database("postgresql://postgres:1234@localhost:5432/postgres"))
saga = Saga("deposit", dal=dal)


@saga.step
async def step1(input, state, set_state):
    print(input, state)
    await sleep(3)
    print("call step2")
    return step2("new input", "new step", delay=3)


@saga.step
async def step2(input, state, set_state):
    print("step2", input, state)
    await set_state("updated state")
    return step3("end")


@saga.step
async def step3(input, state, set_state):
    print("step2", input, state)
    return


class Test:
    async def main(self):
        await saga._dal.init_db()
        await saga.start(uuid.uuid4().hex, "step11", "my test", "initial state", 0)
        await saga.start_event_loop(100, 30, 0.5, 5, 10)


if __name__ == "__main__":
    get_event_loop().run_until_complete(Test().main())
    # get_event_loop().run_until_complete(gather(*all_tasks()))

# orc.setup(step1, step2)

# while orc.get_available_instance()
