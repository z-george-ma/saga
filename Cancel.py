from asyncio import create_task, iscoroutine
import signal

class Cancel:
  requested = False
  _callbacks = set()

  def __init__(self):
    pass

  @classmethod
  def from_signal(cls):
    ret = cls()
    signal.signal(signal.SIGINT, ret._cancel)
    signal.signal(signal.SIGTERM, ret._cancel)
    return ret
  
  def on_cancel(self, *func):
    [self._callbacks.add(f) for f in func]

  def _cancel(self, *args):
    self.requested = True
    for c in self._callbacks:
      r = c()
      if iscoroutine(r):
        create_task(r)


  