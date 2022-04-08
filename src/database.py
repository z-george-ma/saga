from asyncio import create_task, get_event_loop
from dataclasses import asdict
from typing import Any, Dict, List, Mapping, OrderedDict, overload
from asyncpg import create_pool


class SqlMapping(Mapping):
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


def fields(m: Mapping) -> str:
    """Return keys in the mapping as a comma separated string

    Example:
    >>> fields(dict(id=1, col1="abc"))
    'id,col1'
    """
    return ",".join([k for k in m])


def placeholders(m: Mapping) -> str:
    """Return keys in the mapping as a comma separated placeholders

    Example:
    >>> placeholders(dict(id=1, col1="abc"))
    ':id,:col1'
    """
    return ",".join([":" + k for k in m])


def updates(m: Mapping, prefix: str = None) -> str:
    """Return keys in *m* in a format acceptable for UPDATE

    Example:
    >>> updates(dict(id=1, col1="abc"), prefix='my_table')
    'my_table.id=:id,my_table.col1=:col1'
    """
    prefix = prefix + "." if prefix != None else ""
    return ",".join([f"{prefix}{k}=:{k}" for k in m])


def from_data_class(value, includes: List[str] = None, **kw_args) -> Mapping:
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


def apply(value: Mapping, **kw_args) -> Mapping:
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
def include(m: Mapping, arg_list: List[str]) -> OrderedDict:
    ...


def include(m: Mapping, *args) -> OrderedDict:
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


class Database:
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

    async def fetchval(self, sql, col=0, sql_timeout: float = None, **kw_args):
        """Return a value based on *sql* query.

        *sql*: sql text, with colon prefixed parameters
        *column*: column of the result set
        *sql_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> run_async(db.fetchval("SELECT id, column_1 FROM test", col=1))
        'abc'
        """
        sql, args = Database._format_sql(sql, **kw_args)
        pool = await self._pool
        async with pool.acquire() as conn:
            return await conn.fetchval(sql, *args, column=col, timeout=sql_timeout)

    async def fetch(self, sql, sm: SqlMapping, sql_timeout: float = None, **kw_args):
        """Return multiple rows based on *sql* query.

        *sql*: sql text, with colon prefixed parameters
        *sm*: maps the row into a class
        *sql_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> sm = SqlMapping(dict)
        >>> run_async(db.fetch("SELECT * FROM test where id <= 2 ORDER BY id", sm))
        [{'id': 1, 'column_1': 'abc'}, {'id': 2, 'column_1': 'def'}]
        """
        sql, args = Database._format_sql(sql, **kw_args)
        pool = await self._pool
        async with pool.acquire() as conn:
            records = await conn.fetch(sql, *args, timeout=sql_timeout)
            return [sm.load(**record).value() for record in records]

    async def fetchrow(self, sql, sm: SqlMapping, sql_timeout: float = None, **kw_args):
        """Return a row based on *sql* query.

        *sql*: sql text, with colon prefixed parameters
        *sm*: maps the row into a class
        *sql_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> sm = SqlMapping(dict)
        >>> run_async(db.fetchrow("SELECT * FROM test WHERE id=:id", sm, id=2))
        {'id': 2, 'column_1': 'def'}
        """

        sql, args = Database._format_sql(sql, **kw_args)
        pool = await self._pool
        async with pool.acquire() as conn:
            record = await conn.fetchrow(sql, *args, timeout=sql_timeout)
            if record != None:
                return sm.load(**record).value()
            else:
                return None

    async def execute(self, sql: str, sql_timeout: float = None, **kw_args):
        """Execute an SQL *command* for each sequence of arguments in *values* list.

        *sql*: sql text, with colon prefixed parameters
        *sql_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> run_async(db.execute("INSERT INTO test (id, column_1) VALUES(:id, :col1)", id=3, col1='abcdef'))
        'INSERT 0 1'
        """
        sql, args = Database._format_sql(sql, **kw_args)
        pool = await self._pool
        async with pool.acquire() as conn:
            return await conn.execute(sql, *args, timeout=sql_timeout)

    async def execute_many(
        self, sql: str, values: List[Dict[str, Any]], sql_timeout: float = None
    ):
        """Execute an SQL *command* for each sequence of arguments in *values* list.

        *values* hold a list of arguments as dict in identical structure. The list has to contain at least one item
        *sql_timeout*: timeout for sql execution

        Examples:
        >>> db = getfixture('db')
        >>> values = [dict(id=4, col1='abc'), dict(id=5, col1='def')]
        >>> run_async(db.execute_many("INSERT INTO test (id, column_1) VALUES(:id, :col1)", values))
        """

        new_sql, args = Database._format_sql(sql, **values[0])
        args = [Database._format_sql(sql, **v)[1] for v in values]
        pool = await self._pool
        async with pool.acquire() as conn:
            return await conn.executemany(new_sql, args, timeout=sql_timeout)

    def close(self):
        return self._pool.close()
