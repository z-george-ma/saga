from asyncio import Task, get_event_loop
from dataclasses import asdict
from typing import Any, Dict, List, OrderedDict, overload
import typing
from asyncpg import Connection, Pool, create_pool


class Mapping(typing.Mapping):
    """Maps SQL record to data class

    *cls_type*: the data class type to map record to
    *kw_args*: named argument to define any transformation required before setting the value for data class

    Examples:
    >>> from dataclasses import dataclass
    >>> @dataclass
    ... class MyType:
    ...   a: int
    ...   b: int
    ...   c: int
    >>> # define simple mapping without transformation
    >>> mapping = Mapping(MyType)
    >>> # define mapping with transformation
    >>> mapping = Mapping(MyType, c=lambda x: x + 1)
    >>> # transform database column columnX to c
    >>> mapping = Mapping(MyType, c=('columnX', lambda x: x))
    """

    def __init__(self, cls_type, **kw_args):
        self._cls_type = cls_type
        self._transform = []
        for k in kw_args:
            v = kw_args[k]
            if callable(v):
                new, old, func, remove_flag = k, k, v, False
            else:
                if len(v) == 2:
                    v = (*v, True)

                new, old, func, remove_flag = (k, *v)

            self._transform.append((new, old, func, remove_flag))

    def __iter__(self):
        return iter(self.dict)

    def __getitem__(self, item):
        return self.dict[item]

    def __len__(self):
        return len(self.dict)

    def value(self):
        return self._cls_type(**self.dict)

    def load(self, **kw_args):
        self.dict = dict(kw_args)
        for new, old, func, remove_flag in self._transform:
            self.dict[new] = func(self.dict[old])

            if remove_flag:
                del self.dict[old]

        return self


def fields(m: typing.Mapping) -> str:
    """Return keys in the mapping as a comma separated string

    Example:
    >>> fields(dict(id=1, col1="abc"))
    'id,col1'
    """
    return ",".join([k for k in m])


def placeholders(m: typing.Mapping) -> str:
    """Return keys in the mapping as a comma separated placeholders

    Example:
    >>> placeholders(dict(id=1, col1="abc"))
    ':id,:col1'
    """
    return ",".join([":" + k for k in m])


def updates(m: typing.Mapping, prefix: str = None) -> str:
    """Return keys in *m* in a format acceptable for UPDATE

    Example:
    >>> updates(dict(id=1, col1="abc"), prefix='my_table')
    'my_table.id=:id,my_table.col1=:col1'
    """
    prefix = prefix + "." if prefix != None else ""
    return ",".join([f"{prefix}{k}=:{k}" for k in m])


def from_data_class(value, includes: List[str] = None, **kw_args) -> typing.Mapping:
    """Return a Mapping from *value*, applying functions to the fields

    *includes*: if specified, trim down the returned mapping to only the attributes in *includes*
    Example:
    >>> from dataclasses import dataclass
    >>> @dataclass
    ... class MyType:
    ...   a: int
    ...   b: int
    ...   c: int
    >>> input = MyType(a=1,b=2,c=3)
    >>> from_data_class(input, ['a', 'b'],
    ...   a = lambda x: x+10,
    ...   new_b = ('b', lambda x: x),
    ... )
    OrderedDict([('a', 11), ('new_b', 2)])
    """
    value = include(asdict(value), includes) if includes != None else asdict(value)
    return apply(value, **kw_args)


def apply(value: typing.Mapping, **kw_args) -> typing.Mapping:
    """Return a Mapping, applying functions to the fields

    Keyword argument can be a function or a tuple with the format of (old_field, mapping_function, remove_old_field)
    *remove_old_field* can be omitted (default True)

    Examples:
    >>> apply(dict(id=1, col1="abc", col2='def'),\
            id=lambda x: x+1,\
            col1=lambda x: x+'1',\
            col3=('col2', lambda x: x+'2')\
        )
    {'id': 2, 'col1': 'abc1', 'col3': 'def2'}
    """
    for k in kw_args:
        v = kw_args[k]
        if callable(v):
            new, old, func, remove_flag = k, k, v, False
        else:
            if len(v) == 2:
                v = (*v, True)

            new, old, func, remove_flag = k, *v
        if old in value:
            value[new] = func(value[old])
            if remove_flag:
                del value[old]
    return value


@overload
def include(m: typing.Mapping, arg_list: List[str]) -> OrderedDict:
    ...


def include(m: typing.Mapping, *args) -> OrderedDict:
    """Return a new mapping contains only keys specified in *args*.

    *args* can be strings, comma separated strings or list of strings.

    Examples
    >>> include(dict(id=1,col1="abc",col2="def"), ["col1"], "id")
    OrderedDict([('col1', 'abc'), ('id', 1)])
    """
    new_map = OrderedDict()
    for arg in args:
        if isinstance(arg, str):
            arg = list(map(str.strip, arg.split(",")))

        for k in arg:
            if k in m:
                new_map[k] = m[k]

    return new_map


class _Connection:
    @staticmethod
    def _format_sql(sql: str, **kw_args):
        params_list = []
        for k in kw_args:
            i = len(params_list) + 1
            new_sql = sql.replace(f":{k}", f"${i}")
            if new_sql != sql:
                sql = new_sql
                params_list.append(kw_args[k])
        return (sql, params_list)

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    async def fetchval(self, sql, col=0, command_timeout: float = None, **kw_args):
        sql, args = self._format_sql(sql, **kw_args)
        return await self._conn.fetchval(
            sql, *args, column=col, timeout=command_timeout
        )

    async def fetch(self, sql, sm: Mapping, command_timeout: float = None, **kw_args):
        sql, args = self._format_sql(sql, **kw_args)
        records = await self._conn.fetch(sql, *args, timeout=command_timeout)
        return [sm.load(**record).value() for record in records]

    async def fetchrow(
        self, sql, sm: Mapping, command_timeout: float = None, **kw_args
    ):
        sql, args = self._format_sql(sql, **kw_args)
        record = await self._conn.fetchrow(sql, *args, timeout=command_timeout)
        if record != None:
            return sm.load(**record).value()
        else:
            return None

    async def execute(self, sql: str, command_timeout: float = None, **kw_args):
        sql, args = self._format_sql(sql, **kw_args)
        return await self._conn.execute(sql, *args, timeout=command_timeout)

    async def execute_many(
        self, sql: str, values: List[Dict[str, Any]], command_timeout: float = None
    ):
        new_sql, args = self._format_sql(sql, **values[0])
        args = [self._format_sql(sql, **v)[1] for v in values]
        return await self._conn.executemany(new_sql, args, timeout=command_timeout)

    def transaction(self, *, isolation=None, readonly=False, deferrable=False):
        return self._conn.transaction(
            isolation=isolation, readonly=readonly, deferrable=deferrable
        )


class Session:
    def __init__(self, pool: Task[Pool], timeout=None) -> None:
        self._pool = pool
        self._timeout = timeout

    async def __aenter__(self):
        pool = await self._pool
        self._conn = _Connection(await pool.acquire(timeout=self._timeout))
        return self._conn

    async def __aexit__(self, *exc):
        pool = await self._pool
        await pool.release(self._conn._conn)
        self._conn = None


class Transaction:
    def __init__(
        self, pool: Task[Pool], timeout, isolation, readonly, deferrable
    ) -> None:
        self._pool = pool
        self._timeout = timeout
        self._isolation = isolation
        self._readonly = readonly
        self._deferrable = deferrable

    async def __aenter__(self):
        pool = await self._pool
        conn = self._conn = _Connection(await pool.acquire(timeout=self._timeout))
        self._tx = conn.transaction(
            isolation=self._isolation,
            readonly=self._readonly,
            deferrable=self._deferrable,
        )
        return self._tx

    async def __aexit__(self, *exc):
        pool = await self._pool
        await self._tx.__aexit__()
        self._tx = None
        await pool.release(self._conn._conn)
        self._conn = None


class Database:
    def __init__(
        self,
        dsn: str,
        command_timeout=60,
        min_size=10,
        max_size=10,
        max_queries=50000,
        max_inactive_connection_lifetime=300.0,
        loop=None,
    ):
        loop = loop or get_event_loop()

        async def start_pool():
            return await create_pool(
                dsn,
                command_timeout=command_timeout,
                min_size=min_size,
                max_size=max_size,
                max_queries=max_queries,
                max_inactive_connection_lifetime=max_inactive_connection_lifetime,
                loop=loop,
            )

        self._pool = loop.create_task(start_pool())

    async def fetchval(self, sql, col=0, command_timeout: float = None, **kw_args):
        """Return a value based on *sql* query.

        *sql*: sql text, with colon prefixed parameters
        *column*: column of the result set
        *command_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> run_async(db.fetchval("SELECT id, column_1 FROM test", col=1))
        'abc'
        """
        async with Session(self._pool) as session:
            return await session.fetchval(sql, col, command_timeout, **kw_args)

    async def fetch(self, sql, sm: Mapping, command_timeout: float = None, **kw_args):
        """Return multiple rows based on *sql* query.

        *sql*: sql text, with colon prefixed parameters
        *sm*: maps the row into a class
        *command_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> sm = Mapping(dict)
        >>> run_async(db.fetch("SELECT * FROM test where id <= 2 ORDER BY id", sm))
        [{'id': 1, 'column_1': 'abc'}, {'id': 2, 'column_1': 'def'}]
        """
        async with Session(self._pool) as session:
            return await session.fetch(sql, sm, command_timeout, **kw_args)

    async def fetchrow(
        self, sql, sm: Mapping, command_timeout: float = None, **kw_args
    ):
        """Return a row based on *sql* query.

        *sql*: sql text, with colon prefixed parameters
        *sm*: maps the row into a class
        *command_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> sm = Mapping(dict)
        >>> run_async(db.fetchrow("SELECT * FROM test WHERE id=:id", sm, id=2))
        {'id': 2, 'column_1': 'def'}
        """
        async with Session(self._pool) as session:
            return await session.fetchrow(sql, sm, command_timeout, **kw_args)

    async def execute(self, sql: str, command_timeout: float = None, **kw_args):
        """Execute an SQL *command* for each sequence of arguments in *values* list.

        *sql*: sql text, with colon prefixed parameters
        *command_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> run_async(db.execute("INSERT INTO test (id, column_1) VALUES(:id, :col1)", id=3, col1='abcdef'))
        'INSERT 0 1'
        """
        async with Session(self._pool) as session:
            return await session.execute(sql, command_timeout, **kw_args)

    async def execute_many(
        self, sql: str, values: List[Dict[str, Any]], command_timeout: float = None
    ):
        """Execute an SQL *command* for each sequence of arguments in *values* list.

        *values* hold a list of arguments as dict in identical structure. The list has to contain at least one item
        *command_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> values = [dict(id=4, col1='abc'), dict(id=5, col1='def')]
        >>> run_async(db.execute_many("INSERT INTO test (id, column_1) VALUES(:id, :col1)", values))
        """
        async with Session(self._pool) as session:
            return await session.execute_many(sql, values, command_timeout)

    def session(self, timeout=None):
        """Return a database session. It is possible to create transaction from session.

        *timeout*: timeout for getting connection from pool

        Examples:
        >>> db = getfixture('db')
        >>> async def try_transaction():
        ...   async with db.session() as ss:
        ...     async with ss.transaction() as tx:
        ...       await ss.execute("UPDATE test SET column_1=:col", col='useless')
        ...       raise Exception()
        >>> with raises(Exception): run_async(try_transaction())
        """
        return Session(self._pool, timeout)

    def transaction(
        self, *, timeout=None, isolation=None, readonly=False, deferrable=False
    ):
        """Return a database transaction.

        :param timeout: timeout for getting connection from pool

        :param isolation: Transaction isolation mode, can be one of:
                          `'serializable'`, `'repeatable_read'`,
                          `'read_committed'`. If not specified, the behavior
                          is up to the server and session, which is usually
                          ``read_committed``.

        :param readonly: Specifies whether or not this transaction is
                         read-only.

        :param deferrable: Specifies whether or not this transaction is
                           deferrable.
        Examples:
        >>> db = getfixture('db')
        >>> async def try_transaction():
        ...   async with db.transaction() as tx:
        ...     await ss.execute("UPDATE test SET column_1=:col", col='useless')
        ...     raise Exception()
        >>> with raises(Exception): run_async(try_transaction())
        """
        return Transaction(self._pool, timeout, isolation, readonly, deferrable)

    async def close(self):
        pool = await self._pool
        return await pool.close()
