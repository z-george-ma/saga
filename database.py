from typing import Callable, Dict, Mapping, NamedTuple, OrderedDict
from asyncpg import create_pool

class SqlOutput(Mapping):
  def __init__(self, cls_type):
    self._cls_type = cls_type
    self._transform = []

  def __iter__(self):
    return iter(self.dict)

  def __getitem__(self, item):
    return self.dict[item]

  def __len__(self):
    return len(self.dict)

  def value(self):
    for field, func, new_field in self._transform:
      t = func(self.dict[field])
      if new_field == None:
        self.dict[field] = t
      else:
        self.dict[new_field] = t
        del self.dict[field]

    return self._cls_type(**self.dict)


  def map(self, field: str, func: Callable, new_field: str = None):
    self._transform.append((field, func, new_field))
    return self

  def load(self, **kw_args):
    self.dict = kw_args
    return self

class Dao():
  @classmethod
  def from_named_tuple(cls, nt: NamedTuple):
    return cls(**nt._asdict())

  def __init__(self, **kw_args):
    self._record = kw_args
    self._len = len(kw_args)
    
  def include(self, *args):
    r, new_r = self._record, OrderedDict()
    for arg in args:
      if isinstance(arg, str):
        arg = list(map(str.strip, arg.split(',')))
      
      for i in arg:
        if i in r: new_r[i] = r[i]

    self._record = new_r
    self._len = len(new_r)
    return self

  def exclude(self, *args):
    r = self._record
    for arg in args:
      if isinstance(arg, str):
        arg = list(map(str.strip, arg.split(',')))

      for i in arg:
        if i in r: del r[i]

    self._len = len(r)
    return self

  def set(self, field, val):
    r = self._record
    r[field] = val
    self._len = len(r)
    return self

  def fields(self, prefix = ''):
    return ','.join([f'{prefix}{i}' for i in self._record.keys()])

  def placeholders(self):
    return ','.join([f'${i+1}' for i in range(0, len(self._record))])

  def values(self):
    return self._record.values()

  def insert(self):
    return f'({self.fields()}) VALUES({self.placeholders()})'

  def update(self):
    l = list(self._record.keys())
    return ','.join([f'{l[i]}=${i+1}' for i in range(0, len(self._record))])
  
  @property
  def next(self):
    self._len += 1
    return self._len

class Database:
  def __init__(self, dsn: str, command_timeout=60):
    self._pool = create_pool(dsn, command_timeout = command_timeout)

  def __getattr__(self, attr):
    """
    I am being lazy. 
    
    Being a proxy class like this exposes asyncpg.Pool methods to outside world, and breaks encapsulation.
    
    It should explicitly define operations - fetch, execute, fetch_row, etc
    """
    func = getattr(self._pool, attr)
    if (self._pool._initialized):
      return func
    return self.__async_wrapper(func)

  async def fetch(self, sql, so: SqlOutput, *args, **kw_args):
    await self._pool
    records = await self._pool.fetch(sql, *args, **kw_args)
    return [so.load(**record).value() for record in records]

  async def fetchrow(self, sql, so: SqlOutput, *args, **kw_args):
    await self._pool
    record = await self._pool.fetchrow(sql, *args, **kw_args)
    if record != None:
      return so.load(**record).value()
    else:
      return None

  def __async_wrapper(self, func):
    async def decorator(*args, **kw_args):
      await self._pool
      return await func(*args, **kw_args)
    return decorator

  def close(self):
    self._pool.close()

"""
Example:

import asyncio, uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def main():
  db = Database('postgresql://postgres:1234@192.168.64.3:32768/postgres')
  r = await db.fetch('select 1 as a, 3 as b')
  print(r[0].a)

asyncio.get_event_loop().run_until_complete(main())
"""


