from asyncio import (
    CancelledError,
    Event,
    gather,
    sleep,
)
from dataclasses import dataclass
from time import time
from traceback import format_exc
from typing import Any, AsyncGenerator, Callable, Coroutine, Optional, TypeVar, Union
import uuid
from .dal import SagaDAL, SagaDTO
from .logger import LogEntry, Logger
from .stream_utils import AsyncIterator, merge, start_loop, poll
from .database import Database


TInput = TypeVar("TInput")
TState = TypeVar("TState")


@dataclass
class SagaReturn:
    step: str
    input: TInput
    state: Optional[TState] = None
    delay: float = None


class SagaError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


async def default_log_handler(log: AsyncGenerator[LogEntry, None]):
    async for l in log:
        pass


class Saga:
    def _set_state(dal: SagaDAL, saga: SagaDTO):
        """Return a function to set internal state for a saga"""

        async def func(state):
            saga.state = state
            await dal.update_processing(saga)

        return func

    def _saga_step_decorator_generator(self):
        def saga_step_decorator(
            func: Callable[
                [TInput, TState, Callable[[TState], Coroutine[Any, Any, None]], Logger],
                Union[SagaReturn, Any],
            ]
        ):
            """Decorator for a saga function

            Saga function can either return another saga function for continuation, or any other value as final state to terminate the saga

            Example:
            >>> @saga.step
            ... my_func(input, state, set_state, logger):
            ...   return another_saga_func(input, state, delay=10) # delay execution for 10 seconds

            """

            async def saga_wrapper(saga: SagaDTO):
                try:
                    logger = self._logger.with_context(
                        saga.id, saga.instance, saga.step
                    )
                    logger.log_info("Executing", source="saga_wrapper")
                    ret = await func(
                        saga.input, saga.state, Saga._set_state(self._dal, saga), logger
                    )

                    if isinstance(ret, SagaReturn):
                        step, delay, status = self._get_step_delay_status(
                            ret.step, ret.delay
                        )
                        saga.step = step
                        saga.delay = delay
                        saga.status = status
                        saga.input = ret.input

                        if ret.state != None:
                            saga.state = ret.state
                    else:
                        # if return value isn't a SagaReturn object, it is treated as end of saga
                        saga.status = 2

                        if ret != None:
                            saga.state = ret

                    logger.log_info(
                        "Updating Saga",
                        new_step=saga.step,
                        delay=saga.delay,
                        status=saga.status,
                    )
                except SagaError as e:
                    saga.status = 3
                    saga.error = repr(e)
                    logger.log_info("Returning SagaError", e)
                except Exception as e:
                    # put back to the database for future pickup
                    saga.status = 0
                    logger.log_error(e, format_exc())

                # queue up update saga (to allow retrying)
                self._update_saga_queue.send(
                    {"saga": saga, "time": time(), "retry_count": 0}
                )

            name = func.__name__
            self._function_dict[name] = saga_wrapper

            # Consider the following example:
            #
            # @saga.step
            # def step1(input, state, set_state, logger):
            #    return step2(input)
            #
            # the function will convert the next step into a SagaReturn
            #
            def meta_definition(
                input: TInput, state: Optional[TState] = None, delay: float = None
            ):
                return SagaReturn(name, input, state, delay)

            # set __name__ back to original function name
            meta_definition.__name__ = name
            return meta_definition

        return saga_step_decorator

    def __init__(self, name: str, dal=None, dsn=None):
        self.name = name
        self.runner = uuid.uuid4().hex

        # SagaDAL can be injected in for internal testing. Outside this module, caller will pass in dsn
        self._dal = dal or SagaDAL(Database(dsn))

        self._function_dict = {}
        self._in_mem_sagas = AsyncIterator()
        self._update_saga_queue = AsyncIterator()
        self._event_loop_started = False

        self._logger = Logger(name, self.runner)

        self.step = self._saga_step_decorator_generator()

    async def start(self, instance: str, step, input, state, delay: float = None):
        """Start a new saga

        *instance*: a unique identifier for the saga instance
        *step*: step of the saga. Can be a function or string
        *input*: input to the saga function
        *state*: internal state of saga function
        *delay*: for delay start of a saga function. If not set, the saga will be enqueued for immediate processing (when event loop starts)

        Examples:
        >>> dal = getfixture('dal')
        >>> saga = Saga("deposit", dal)
        >>> @saga.step
        ... def step1(input, state, set_state, logger):
        ...   print(f'step1 started with {input} and {state}')

        >>> run_async(saga.start(uuid.uuid4().hex, step1, 'input', 'state'))
        """
        step, delay, status = self._get_step_delay_status(step, delay)
        saga = SagaDTO(
            0, self.name, instance, step, input, state, self.runner, delay, status, None
        )

        logger = self._logger.with_context(None, instance, step)
        logger.log_info("Starting saga", delay=delay, status=status)
        saga = await self._dal.new_saga(saga)
        self._logger.with_context(saga.id, instance, step).log_info("Started")

        # if status of saga is in_progress (immediate start), send to task queue for consumption
        if saga.status == 1:
            self._in_mem_sagas.send(saga)

    async def get_state(self, instance: str):
        """Return the internal state of a saga

        Examples:
        >>> dal = getfixture('dal')
        >>> saga = Saga("test", dal)
        >>> run_async(saga.get_state('random'))
        'abc'
        """
        self._logger.with_context(None, instance, None).log_trace(
            "Getting state", source="get_state"
        )
        return await self._dal.get_state(self.name, instance)

    async def call(
        self,
        instance: str,
        step: Union[Callable, str],
        input: TInput,
        delay: Optional[float] = None,
    ):
        """Overwrite a saga with new step, input and delay"""

        ret = await self._dal.get_instance(self.name, instance)
        step, delay, status = self._get_step_delay_status(step, delay)

        saga = SagaDTO(
            ret["id"],
            self.name,
            instance,
            None,
            None,
            ret["state"],
            None,
            None,
            None,
            None,
        )

        # SagaDTO is an active record. Setting the values so it can be captured in the changeset.
        saga.step = step
        saga.input = input
        saga.runner = self.runner
        saga.delay = delay
        saga.status = status

        logger = self._logger.with_context(saga.id, instance, saga.step)

        logger.log_trace(
            "Updating", delay=saga.delay, status=saga.status, source="call"
        )

        await self._dal.update_pending(saga)

        # if status of saga is in_progress (immediate start), send to task queue for consumption
        if saga.status == 1:
            self._in_mem_sagas.send(saga)

    def init_db(self):
        return self._dal.init_db()

    async def start_event_loop(
        self,
        batch_size: int,
        low_watermark: int,
        backoff: float,
        available_workers: int,
        available_update_threads: int,
        health_check_timeout: int,
        log_handler: Callable[
            [AsyncGenerator[LogEntry, None]], Coroutine[None, None, None]
        ] = default_log_handler,
    ):
        """Start the event loop to do the following

        1. heart beat to register and keep the instance alive
        2. poll DB for pending sagas. It will *backoff* in seconds if in mem queue size > *low_watermark*, or get less items from db, per *batch_size*
        3. create worker pool to execute saga functions (pool size defined by *available_workers*)
        4. maintain a queue to update saga (and retry). worker threads is defined as *available_update_threads*

        Examples:
        >>> dal = getfixture('dal')
        >>> saga = Saga("event_loop", dal)
        >>> loop = asyncio.get_event_loop().create_task(saga.start_event_loop(10, 10, 0.2, 1, 1, 5))
        >>> @saga.step
        ... async def step123(input, state, set_state, logger):
        ...   loop.cancel()
        >>> run_async(loop)
        """
        cancel = Event()
        self._event_loop_started = True

        async def get_pending_saga():
            try:
                self._logger.log_trace(
                    "Getting pending sagas", source="get_pending_saga"
                )
                return await self._dal.get_pending_saga(
                    self.name, self.runner, batch_size
                )
            except Exception as e:
                self._logger.log_error(e, format_exc(), source="get_pending_saga")
                return []

        async def execute_saga(saga: SagaDTO):
            if not saga.step in self._function_dict:
                e = SagaError("Unrecognized saga step")
                saga.error = repr(e)
                saga.status = 3
                self._logger.log_error(e, source="execute_saga")
                self._update_saga_queue.send((saga, time(), 0))
            else:
                await self._function_dict[saga.step](saga)

        async def update_saga(input):
            saga: SagaDTO = input["saga"]
            retry_count: int = input["retry_count"]

            logger = self._logger.with_context(
                saga.id, saga.instance, saga.step, "update_saga"
            )

            # calibrate delay
            if saga.delay != None and retry_count > 0:
                now = time()
                delay = saga.delay - (now - input["time"])
                saga.delay = 0 if delay < 0 else delay

            try:
                await self._dal.update_processing(saga)
                if saga.status == 1:
                    self._in_mem_sagas.send(saga)
            except SagaDAL.ConcurrencyException:
                logger.log_warn("Update concurrency failure")
                return
            except Exception as e:
                # could be temporary glitch, or db connectivity issues.
                retry_count += 1
                logger.log_error(
                    e, format_exc(), source="update_saga", retry_count=retry_count
                )
                # put back into the queue for retry
                self._update_saga_queue.send(
                    {"saga": saga, "time": now, "retry_count": retry_count}
                )

        # send regular heartbeat to DB
        async def health_check(cancel: Event):
            while not cancel.is_set():
                try:
                    await self._dal.keep_alive(self.runner, health_check_timeout)
                except Exception as e:
                    self._logger.log_error(e, format_exc(), source="health_check")
                    pass
                await sleep(1)

        def end_loop(_):
            self._logger.log_info("Event loop ended")
            self._logger.logstream.end()

        # merge two queues (async generator) - tasks from polling and in-memory ones into a single stream
        task_queue = merge(
            poll(get_pending_saga, low_watermark, batch_size, backoff, cancel),
            self._in_mem_sagas,
        )

        try:
            execute_saga_loop = start_loop(execute_saga, task_queue, available_workers)
            execute_saga_loop.add_done_callback(lambda _: self._update_saga_queue.end())

            update_saga_loop = start_loop(
                update_saga, self._update_saga_queue, available_update_threads
            )

            update_saga_loop.add_done_callback(end_loop)

            await gather(
                health_check(cancel),
                execute_saga_loop,
                update_saga_loop,
                log_handler(self._logger.logstream),
            )
        except CancelledError:
            # asyncio.get_event_loop.stop() will cancel all tasks in event loop, which generates CancelledError
            # seeing this error means event_loop has stopped, so set the flag to break all loops
            cancel.set()
            self._event_loop_started = False
            self._logger.log_info("Event loop cancelled", source="start_event_loop")

    def _get_step_delay_status(self, step, delay: float = None):
        # if delay is not set, set status to be in_progress for immediate consumption
        if self._event_loop_started and delay == None:
            status = 1
        else:
            status = 0

        # convert function into its name
        if callable(step):
            step = step.__name__

        return (step, delay, status)
