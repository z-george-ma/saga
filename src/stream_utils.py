from collections import deque
from concurrent.futures import FIRST_COMPLETED
from threading import Lock
from asyncio import (
    Future,
    Task,
    Event,
    create_task,
    gather,
    get_event_loop,
    sleep,
    wait,
)
from time import time
from typing import (
    AsyncGenerator,
    Callable,
    Coroutine,
    Generator,
    Generic,
    List,
    TypeVar,
)

T = TypeVar("T")


class AsyncIterator(AsyncGenerator[T, None], Generic[T]):
    """AsyncIterator implements async generator interface for streaming data.

    It also includes a *send* method for caller to push data into the stream.

    Examples:

    >>> ai = AsyncIterator()
    >>> ai.send(123)
    >>> run_async(ai.anext())
    123
    >>> ai.end()
    >>> with raises(StopAsyncIteration): run_async(ai.anext())

    """

    def __init__(self):
        self.__ll = deque()
        # Rule of thumb: never use threading.Lock in async functions
        self.__lock = Lock()
        self.__ll.append(Future())

        async def source(ll: deque):
            while True:
                try:
                    r = await ll[0]
                    ll.popleft()
                    yield r
                except StopAsyncIteration:
                    break

        self.__source = source(self.__ll)

    def send(self, item: T):
        with self.__lock:
            self.__ll.append(Future())
            self.__ll[-2].set_result(item)

    def end(self):
        with self.__lock:
            self.__ll[-1].set_exception(StopAsyncIteration())

    def __aiter__(self):
        return self.__source.__aiter__()

    def __anext__(self):
        with self.__lock:
            return self.__source.__anext__()

    def asend(self, value):
        raise NotImplementedError()

    def athrow(self, **args):
        raise NotImplementedError()

    def anext(self):
        return self.__anext__()


def to_sync(gen: AsyncGenerator[T, None], loop=None):
    """Convert an async generator to a list of futures

    Examples:

    >>> async def gen():
    ...   for i in range(1, 4):
    ...     yield i
    >>> sync = to_sync(gen())
    >>> ret = (run_async(i) for i in sync)
    >>> [next(ret), next(ret), next(ret)]
    [1, 2, 3]
    >>> with raises(StopAsyncIteration): next(ret)
    """

    async def _cont(gen, last_value):
        if last_value != None:
            await last_value
        return await gen.__anext__()

    loop = loop or get_event_loop()
    lock = Lock()
    ret = None
    while True:
        with lock:
            ret = loop.create_task(_cont(gen, ret))
            yield ret


async def _loop_one(
    func: Callable[[T], Coroutine[None, None, None]],
    gen: Generator[Task[T], None, None],
):
    while True:
        item = await next(gen)
        try:
            await func(item)
        except:
            continue


def start_loop(
    func: Callable[[T], Coroutine[None, None, None]],
    gen: AsyncGenerator[T, None],
    size: int,
):
    """Start a worker pool to execute function on items from stream

    *func*: function to process items. Exception will be swallowed.
    *gen*: async stream to read data from
    *size*: pool size
    *cancel*: cancellation event

    Examples:
    >>> async def gen():
    ...   for i in range(1, 1001):
    ...     yield i
    >>> class Data:
    ...   sum_all = 0
    >>> data = Data()
    >>> def worker_generator(data):
    ...   def worker(i):
    ...     data.sum_all += i
    ...   return worker
    >>> _ = run_async(start_loop(worker_generator(data), gen(), 10))
    >>> data.sum_all
    500500
    """
    it = to_sync(gen)
    arr = [_loop_one(func, it) for i in range(0, size)]
    return gather(*arr, return_exceptions=True)


async def poll(
    func: Callable[[], Coroutine[None, None, List[T]]],
    low_watermark: int,
    expected_len: int,
    backoff: float,
    cancel: Event,
):
    """
    Call func periodically to get list of items and return as async stream.

    *func*: async function that returns a list of items
    *low_watermark*: don't poll if remaining items in the queue >= low_watermark
    *expected_len*: back off if *func* returns less item than *expected_len*
    *backoff*: back off time in seconds
    *cancel*: cancellation event

    Examples:
    >>> l = iter([30, 9, 1])
    >>> import time
    >>> async def func():
    ...   now = time.time()
    ...   return [now for i in range(0, next(l))]
    >>> evt = asyncio.Event()
    >>> stream = poll(func, 10, 20, 0.2, evt)
    >>> batch1 = [run_async(stream.__anext__()) for i in range(0, 20)][0]
    >>> batch2 = [run_async(stream.__anext__()) for i in range(0, 10)][-1]
    >>> batch1 == batch2
    True
    >>> batch3 = [run_async(stream.__anext__()) for i in range(0, 10)]
    >>> batch3[0] == batch3[8]
    True
    >>> batch3[9] - batch3[0] > 0.2
    True
    >>> batch3[0] - batch2 < 0.2
    True
    >>> async def cancel_event():
    ...   await asyncio.sleep(0.2)
    ...   evt.set()
    >>> _ = asyncio.get_event_loop().create_task(cancel_event())
    >>> with raises(StopAsyncIteration): run_async(stream.__anext__())
    """

    async def delayed_func(state):
        last_run, last_count = state

        delay = backoff + last_run - time() if last_count < expected_len else -1
        if delay > 0:
            await sleep(delay)

        state[0] = time()
        try:
            ret = await func()
        except:
            ret = []
        state[1] = len(ret)
        return ret

    values: List[T] = []
    task, state = None, [0, expected_len]

    while not cancel.is_set():
        l = len(values)
        if l < low_watermark and task == None:
            task = delayed_func(state)

        if l == 0:
            values, task = await task, None
            continue

        ret, *values = values
        yield ret


async def _coroutine_tuple(task, *args):
    return (await task, *args)


async def merge(*generators, when_exception="IGNORE"):
    """Merge multiple streams into a single stream

    Examples:
    >>> async def gen1():
    ...  for i in range(0, 10):
    ...    await asyncio.sleep(0.01)
    ...    yield i
    >>> async def gen2():
    ...  for i in range(0, 10):
    ...    await asyncio.sleep(0.02)
    ...    yield i
    >>> new_stream = merge(gen1(), gen2())
    >>> sum([run_async(new_stream.__anext__()) for i in range(0, 20)])
    90
    """
    nexts = [create_task(_coroutine_tuple(gen.__anext__(), gen)) for gen in generators]
    while len(nexts):
        done, pending = await wait(nexts, return_when=FIRST_COMPLETED)
        for d in done:
            e = d.exception()
            if (e != None and when_exception == "IGNORE") or isinstance(
                e, StopAsyncIteration
            ):
                continue
            value, gen = d.result()
            pending.add(create_task(_coroutine_tuple(gen.__anext__(), gen)))
            yield value
        nexts = pending
