from collections import deque
from concurrent.futures import FIRST_COMPLETED
from random import randrange
from threading import Lock
from asyncio import Future, Task, create_task, gather, get_event_loop, sleep, wait
from time import time
from typing import AsyncGenerator, Callable, Coroutine, Generator, List, TypeVar

T = TypeVar("T")


class AsyncIterator:
    """AsyncIterator implements async generator interface for streaming data.

    It also includes a *send* method for caller to push data into the stream.
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

    def send(self, item):
        with self.__lock:
            self.__ll.append(Future())
            self.__ll[-2].set_result(item)

    def close(self):
        with self.__lock:
            self.__ll[-1].set_exception(StopAsyncIteration())

    def __aiter__(self):
        return self.__source.__aiter__()

    def __anext__(self):
        with self.__lock:
            return self.__source.__anext__()

    def anext(self):
        return self.__anext__()


def to_sync(gen: AsyncGenerator[T, None], cancel: Callable[[], bool]):
    """Convert an async generator to a list of futures"""

    async def _cont(gen, last_value):
        if last_value != None:
            await last_value
        return await gen.__anext__()

    lock = Lock()
    ret = None
    while not cancel():
        with lock:
            ret = create_task(_cont(gen, ret))
            yield ret


async def _loop_one(
    func: Callable[[T], Coroutine[None, None, None]],
    gen: Generator[Task[T], None, None],
    cancel: Callable[[], bool],
):
    while not cancel():
        item = await next(gen)
        try:
            await func(item)
        except:
            continue


def start_loop(
    func: Callable[[T], Coroutine[None, None, None]],
    gen: AsyncGenerator[T, None],
    size: int,
    cancel: Callable[[], bool],
):
    """Start a worker pool to listen to *gen* stream and pass to *func* to process. Pool size is determined by *size*"""
    it = to_sync(gen, cancel)
    arr = [_loop_one(func, it, cancel) for i in range(0, size)]
    return gather(*arr, return_exceptions=True)


async def poll(
    func: Callable[[], Coroutine[None, None, List[T]]],
    low_watermark: int,
    backoff: float,
    cancel: Callable[[], bool],
):
    """
    Poll func to get list of items and return as async iterator.

    Polling rules
    1. don't poll if all consumers are busy
    2. if returned items < low_watermark, it will wait for backoff before next poll
    3. if returned items >= low_watermark, it means items are backing up upstream, so don't wait.

    """

    async def delayed_func(state):
        last_run, last_count = state

        delay = backoff + last_run - time() if last_count < low_watermark else -1
        if delay > 0:
            await sleep(delay)

        state[0] = time()
        ret = await func()
        state[1] = len(ret)
        return ret

    task, state = None, [0, low_watermark]
    values: List[T] = []

    while not cancel():
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
    """Merge multiple streams into a single stream"""
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


class Test:
    @staticmethod
    async def generate(x):
        for i in range(0, x):
            if i % 5 == 0:
                await sleep(0.1)
            yield i

    @staticmethod
    async def process(i):
        print(f"processing {i}")
        await sleep(0.01 + (i % 5) * 0.1)
        if i == 3:
            raise Exception()

    async def main(self):
        # await start_loop(Test.process, Test.generate(100), 5)
        # async for i in poll(Test.get_data, 5, backoff=1):
        #  print(f'processed {i}')
        async for i in merge(
            Test.generate(10),
            Test.generate(100),
            Test.generate(1000),
            Test.generate(10000),
            Test.generate(100000),
        ):
            print(i)

    @staticmethod
    async def get_data():
        await sleep(0.1)
        i = randrange(0, 10)
        print(f"returning {i} items")
        return range(0, i)


if __name__ == "__main__":
    get_event_loop().run_until_complete(Test().main())
