from asyncio import all_tasks, create_task, gather, get_event_loop, sleep
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Callable
import uuid, msgpack, sys
from Cancel import Cancel
from pool import AsyncIterator, merge, start_loop, poll

from database import Database, Dao

SagaDTO = namedtuple(
  'SagaDTO', 'id, name, instance_id, step, input, state, owner, delay, status'
)

@dataclass
class SagaNext:
  step: Callable
  input: Any
  state: Any = None
  delay: float = None

@dataclass
class SagaThrow:
  exception: Exception

@dataclass
class SagaEnd:
  state: Any = None

class SagaDAL:
  class ConcurrencyException(Exception):
    pass

  def __init__(self, db: Database):
    self._db = db

  async def new_saga(self, saga: SagaDTO):
    dao = Dao.from_named_tuple(saga._replace(input = msgpack.packb(saga.input), state = msgpack.packb(saga.state))).exclude('id, delay')
    id = await self._db.fetchval(f'INSERT INTO saga.instance ({dao.fields()},start_after) VALUES({dao.placeholders()},now()::timestamp+interval \'1 seconds\'* ${dao.next}) RETURNING id', *dao.values(), saga.delay)
    return saga._replace(id = id)

  async def get_instance(self, name, instance_id):
    ret =  await self._db.fetchrow('SELECT id, state FROM saga.instance WHERE name = $1 AND instance_id = $2 AND status = 0', dict, name, instance_id)
    ret['state'] = msgpack.unpackb(ret['state'])
    return ret
    # TODO: empty

  async def get_state(self, name, instance_id):
    ret = await self._db.fetchval('SELECT state FROM saga.instance WHERE name = $1 AND instance_id = $2', name, instance_id)
    return msgpack.unpackb(ret)

  async def update_processing(self, saga: SagaDTO, fields: str):
    dao = Dao.from_named_tuple(saga._replace(input = msgpack.packb(saga.input), state = msgpack.packb(saga.state)))
    dao.include(fields).exclude('delay')
    id = await self._db.fetchval(f'UPDATE saga.instance SET {dao.update()},start_after=now()::timestamp+interval \'1 seconds\'* ${dao.next} WHERE id = ${dao.next} AND status = 1 AND owner = ${dao.next} RETURNING id', *dao.values(), saga.delay, saga.id, saga.owner)
    if id == None: 
      raise SagaDAL.ConcurrencyException()

  async def update_pending(self, saga: SagaDTO, fields: str):
    dao = Dao.from_named_tuple(saga._replace(input = msgpack.packb(saga.input), state = msgpack.packb(saga.state)))
    dao.include(fields).exclude('delay')
    id = await self._db.fetchval(f'UPDATE saga.instance SET {dao.update()},start_after=now()::timestamp+interval \'1 seconds\'* ${dao.next} WHERE id = ${dao.next} AND status = 0 RETURNING id', *dao.values(), saga.delay, saga.id)
    if id == None: 
      raise SagaDAL.ConcurrencyException()

  async def get_pending_saga(self, name, owner, batch_size):
    ret = await self._db.fetch('''
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
''', dict, name, batch_size, owner)
    for i in ret:
      i['input'] = msgpack.unpackb(i['input'])
      i['state'] = msgpack.unpackb(i['state'])
      i['delay'] = 0
      del i['start_after']
    print('polling', ret)
    return [SagaDTO(**saga) for saga in ret]

  async def keep_alive(self, owner, timeout):
    await self._db.execute('call saga.heart_beat($1, $2)', owner, timeout)

class Saga:
  _function_map = {}
  _async_iter = AsyncIterator()
  _cancel = Cancel.from_signal()

  def __init__(self, name: str, batch_size, low_watermark, backoff, worker:int, health_check_timeout: int, dal: SagaDAL):
    self.name = name
    self.owner = uuid.uuid4().hex
    self.worker = worker
    self.health_check_timeout = health_check_timeout
    self._dal = dal

    def factory(func):
      async def new_func(saga):
        try:
          print('entering', name, saga)
          ret = await func(SagaContext(self, saga), saga.input, saga.state)
          print('returning', ret)
          if isinstance(ret, SagaNext):
            saga = self._create_saga_dto(saga.name, saga.owner, saga.instance_id, ret.step, ret.input, ret.state, saga.id, ret.delay)
            fields = 'step, input, owner, delay, status'
          elif isinstance(ret, SagaEnd):
            saga = saga._replace(state = ret.state, status = 2)
            fields = 'status'
          elif isinstance(ret, SagaThrow):
            saga = saga._replace(state = ret.exception, status = 3)
            fields = 'status'
          
          if ret.state != None: fields += ', state'
          print('updating', saga, fields)
          await self._dal.update_processing(saga, fields)
            
          if saga.status == 1:
            self._async_iter.send(saga)
        except SagaDAL.ConcurrencyException:
          print('Concurrency exception')
        except Exception as e:
          try:
            print('Error:', sys._getframe(1).f_code.co_name, sys._getframe(1).f_lineno, e)
            saga = saga._replace(status = 0)
            await self._dal.update_processing(saga, 'status')
          except:
            pass

      name = func.__name__
      self._function_map[name] = new_func
      return func

    self.step = factory

    async def get_pending_saga():
      try:
        return await dal.get_pending_saga(self.name, self.owner, batch_size)
      except:
        return []

    self.event_gen = merge(
      poll(get_pending_saga, low_watermark, backoff, self._cancel),
      self._async_iter
    )

    self._cancel.on_cancel(self._async_iter.close, self._dal._db.close)

  async def start(self, instance_id: str, step, input, state, delay:float = None):
    saga = self._create_saga_dto(self.name, self.owner, instance_id, step, input, state, delay = delay)
    saga = await self._dal.new_saga(saga)
    if saga.status == 1:
      self._async_iter.send(saga)

  async def get_state(self, instance_id: str):
    return await self._dal.get_state(self.name, instance_id)

  async def call(self, instance_id: str, step, input, delay:float = None):
    ret = await self._dal.get_instance(self.name, instance_id)
    saga = self._create_saga_dto(self.name, self.owner, instance_id, step, input, ret['state'], ret['id'], delay)
    await self._dal.update_pending(saga, 'step, input, owner, delay, status')
    if saga.status == 1:
      self._async_iter.send(saga)

  @staticmethod
  def _create_saga_dto(name, owner, instance_id: str, step, input, state, id = 0, delay:float = None):
    if callable(step):
      step = step.__name__
    pass

    if delay == None:
      status = 1
      delay = 0
    else:
      status = 0

    return SagaDTO(id, name, instance_id, step, input, state, owner, delay, status)

  async def start_event_loop(self):
    async def process(saga: SagaDTO):
      await self._function_map[saga.step](saga)

    async def health_check():
      while not self._cancel.requested:
        try:
          await self._dal.keep_alive(self.owner, self.health_check_timeout)
        except:
          pass
        await sleep(1)
        
    await gather(health_check(), start_loop(process, self.event_gen, self.worker, self._cancel))
    
class SagaContext:
  def __init__(self, saga: Saga, instance: SagaDTO):
    self._dal = saga._dal
    self._saga = saga
    self._instance = instance

  async def set_state(self, state):
    self._saga = self._saga._replace(state = state)
    await self._dal.update_processing(self._instance, 'state')

dal = SagaDAL(Database('postgresql://postgres:1234@192.168.64.3:32768/postgres'))
saga = Saga('deposit', 100, 30, 0.5, 5, 10, dal)

@saga.step
async def step1(ctx: SagaContext, input, state):
  print(input, state)
  await sleep(3)
  print('call step2')
  return SagaNext(step2, "new input", "new step")

@saga.step
async def step2(ctx: SagaContext, input, state):
  print('step2', input, state)
  return SagaEnd("end")

class Test:
  async def main(self):
    #print(await dal.get_state(2))
    #print(await dal.new_saga(d))
    #print(await dal.get_pending_saga('deposit', saga.owner, 5))
    await saga.start(uuid.uuid4().hex, step1, "my test", "initial state")
    await saga.start_event_loop()

if __name__ == '__main__':
  get_event_loop().run_until_complete(Test().main())
  get_event_loop().run_until_complete(gather(*all_tasks()))

#orc.setup(step1, step2)

#while orc.get_available_instance()