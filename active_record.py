from typing import TypeVar


T = TypeVar("T")


def activerecord(cls: T) -> T:
    """ActiveRecord decorator to keep track of changes happened to a data class

    Examples:
    >>> from dataclasses import dataclass
    >>> @activerecord
    ... @dataclass
    ... class MyRecord:
    ...   col1: int
    ...   col2: str
    >>> record = MyRecord(col1=1, col2="a")
    >>> record.col1 = 2
    >>> record.col1
    2
    >>> record.changes()
    ['col1']
    >>> record.clear()
    >>> record.changes()
    []
    """

    class ActiveRecord(cls):
        def __init__(self, *args, **kargs):
            super().__init__(*args, **kargs)
            self._changeset = set()

        def __setattr__(self, name, value):
            if hasattr(self, "_changeset"):
                self._changeset.add(name)

            return super().__setattr__(name, value)

        def changes(self):
            """Return the properties that have been changed"""
            return [*self._changeset]

        def clear(self):
            """Clear the change log"""
            self._changeset.clear()

    return ActiveRecord
