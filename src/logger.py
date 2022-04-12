from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import AsyncGenerator
from .stream_utils import AsyncIterator


class LogLevel(Enum):
    TRACE = "TRACE"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass
class LogEntry:
    # logging data
    time: datetime
    log_level: LogLevel
    message: str
    meta: dict
    error: Exception
    # from logging context
    source: str
    saga_id: int
    saga_name: str
    saga_instance: str
    saga_step: str
    saga_runner: str


class Logger:
    logstream: AsyncIterator[LogEntry]

    def __init__(self, name, runner, logstream=None):
        self.logstream = logstream or AsyncIterator[LogEntry]()
        self._prototype = dict(
            source=None,
            saga_id=None,
            saga_name=name,
            saga_instance=None,
            saga_step=None,
            saga_runner=runner,
        )

    def with_context(
        self,
        saga_id: int,
        saga_instance: str,
        saga_step: str,
        source: str = None,
    ):
        """Return a new logger with context

        Examples:
        >>> logger = Logger('my_logger', 'abc')
        >>> new_logger = logger.with_context(1, 'instance', 'step', 'source')
        >>> new_logger.log_trace('message', data=1)
        >>> log_entry = run_async(logger.logstream.__anext__())
        >>> log_entry.saga_id == 1
        True

        """
        ret = Logger(
            self._prototype["saga_name"], self._prototype["saga_runner"], self.logstream
        )
        ret._prototype["source"] = source or saga_step
        ret._prototype["saga_id"] = saga_id
        ret._prototype["saga_instance"] = saga_instance
        ret._prototype["saga_step"] = saga_step
        return ret

    def _log(self, log_level, message, error, source=None, **kw_args):
        self.logstream.send(
            LogEntry(
                time=datetime.utcnow(),
                log_level=log_level,
                message=message,
                error=error,
                source=source or self._prototype["source"],
                saga_name=self._prototype["saga_name"],
                saga_runner=self._prototype["saga_runner"],
                saga_id=self._prototype["saga_id"],
                saga_instance=self._prototype["saga_instance"],
                saga_step=self._prototype["saga_step"],
                meta=kw_args,
            )
        )

    def log_trace(self, message, error=None, source=None, **kw_args):
        """Log trace level message

        Examples:
        >>> logger = Logger('my_logger', 'abc')
        >>> logger.log_trace('message', error=Exception(), source = 'source', data=1)
        >>> log_entry = run_async(logger.logstream.__anext__())
        >>> log_entry.saga_name == 'my_logger' and log_entry.saga_runner == 'abc'
        True
        >>> log_entry.log_level == LogLevel.TRACE and log_entry.message == 'message' and log_entry.meta["data"] == 1 and repr(log_entry.error) == 'Exception()' and log_entry.source == 'source'
        True
        """
        self._log(LogLevel.TRACE, message, error, source, **kw_args)

    def log_info(self, message, error=None, source=None, **kw_args):
        """Log info level message

        Examples:
        >>> logger = Logger('my_logger', 'abc')
        >>> logger.log_info('message', error=Exception(), data=1)
        >>> log_entry = run_async(logger.logstream.__anext__())
        >>> log_entry.log_level == LogLevel.INFO and log_entry.meta["data"] == 1 and repr(log_entry.error) == 'Exception()'
        True
        """
        self._log(LogLevel.INFO, message, error, source, **kw_args)

    def log_warn(self, message, error=None, source=None, **kw_args):
        """Log warning level message

        Examples:
        >>> logger = Logger('my_logger', 'abc')
        >>> logger.log_warn('message', error=Exception(), data=1)
        >>> log_entry = run_async(logger.logstream.__anext__())
        >>> log_entry.log_level == LogLevel.WARN and log_entry.meta["data"] == 1 and repr(log_entry.error) == 'Exception()'
        True
        """
        self._log(LogLevel.WARN, message, error, source, **kw_args)

    def log_error(self, error, message=None, source=None, **kw_args):
        """Log error level message

        Examples:
        >>> logger = Logger('my_logger', 'abc')
        >>> logger.log_error(error=Exception('message'), data=1)
        >>> log_entry = run_async(logger.logstream.__anext__())
        >>> log_entry.log_level == LogLevel.ERROR and log_entry.meta["data"] == 1 and repr(log_entry.error) == "Exception('message')"
        True
        """
        self._log(LogLevel.ERROR, message or repr(error), error, source, **kw_args)
