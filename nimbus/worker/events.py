import functools

from nimbus.worker.context import logger


class EventContext:
    def __init__(self):
        self._events = {}

    def listen(self, name):
        def decorator(func):
            @functools.wraps(func)
            def decorated(*args, **kwargs):
                # TODO: check on arguments depening on the event name
                return func(*args, **kwargs)

            self._events.setdefault(name, []).append(func)

            return decorated

        return decorator

    def publish(self, name, *args, **kwargs):
        # TODO: check on arguments depening on the event name
        logger.debug('Publish event: {} - {} - {}'.format(name, args, kwargs))
        for func in self._events.get(name, []):
            func(*args, **kwargs)


ctx_events = EventContext()
