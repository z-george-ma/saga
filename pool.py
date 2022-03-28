from collections import deque
from concurrent.futures import FIRST_COMPLETED
from random import randrange
from threading import Lock
from asyncio import Future, Task, create_task, gather, get_event_loop, sleep, wait
from time import time
from typing import AsyncGenerator, Callable, Coroutine, Generator, List, TypeVar
from Cancel import Cancel

T = TypeVar('T')

class AsyncIterator():
  def __init__(self):
    self.__ll = deque()
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
    self.__lock.acquire()
    self.__ll.append(Future())
    self.__ll[-2].set_result(item)
    self.__lock.release()

  def close(self):
    self.__lock.acquire()
    self.__ll[-1].set_exception(StopAsyncIteration())
    self.__lock.release()

  def __aiter__(self):
      return self.__source.__aiter__()

  def __anext__(self):
    return self.__source.__anext__()

  
def to_sync(gen: AsyncGenerator[T, None], cancel: Cancel):
  async def _cont(gen, last_value):
    if last_value != None:
      await last_value
    return await gen.__anext__()
    
  lock = Lock()
  ret = None
  while not cancel.requested:
    lock.acquire()
    ret = create_task(_cont(gen, ret))
    lock.release()
    yield ret

async def _loop_one(func: Callable[[T], Coroutine[None, None, None]], gen: Generator[Task[T], None, None]):
  while True: 
    item = await next(gen)
    try:
      await func(item)
    except:
      continue

def start_loop(func: Callable[[T], Coroutine[None, None, None]], gen: AsyncGenerator[T, None], size: int, cancel: Cancel):
  it = to_sync(gen, cancel)
  arr = [_loop_one(func, it) for i in range(0, size)]
  return gather(*arr, return_exceptions=True)

async def poll(func: Callable[[], Coroutine[None, None,List[T]]], low_watermark: int, backoff: float, cancel: Cancel):
  '''
  Poll func to get list of items and return as async iterator.

  Polling rules
  1. don't poll if all consumers are busy
  2. if returned items < low_watermark, it will wait for backoff before next poll
  3. if returned items >= low_watermark, it means items are backing up upstream, so don't wait.

  '''
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

  while not cancel.requested:
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

async def merge(*generators, when_exception='IGNORE'):
  nexts = [create_task(_coroutine_tuple(i.__anext__(), i)) for i in generators]
  while len(nexts):
    done, pending = await wait(nexts, return_when=FIRST_COMPLETED)
    for d in done:
      e = d.exception()
      if (e != None and when_exception == 'IGNORE') or isinstance(e, StopAsyncIteration): continue
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
    #await start_loop(Test.process, Test.generate(100), 5)
    #async for i in poll(Test.get_data, 5, backoff=1):
    #  print(f'processed {i}')
    async for i in merge(Test.generate(10), Test.generate(100), Test.generate(1000), Test.generate(10000), Test.generate(100000)):
      print(i)
  
  @staticmethod
  async def get_data():
    await sleep(0.1)
    i = randrange(0, 10)
    print(f'returning {i} items')
    return range(0, i)

if __name__ == '__main__':
  get_event_loop().run_until_complete(Test().main())

