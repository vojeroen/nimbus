import functools
import uuid

from nimbus.log import get_logger
from nimbus.subscriber.publication import Publication
from nimbus.worker.errors import WrongEndpoint

logger = get_logger(__name__)


class SubscribeContext:
    def __init__(self):
        self._services_by_topic = {}
        self._services = {}
        self._topics = {}

    @property
    def topics(self):
        return sorted(self._services_by_topic.keys())

    def get_service_by_topic(self, topic):
        return self._services[self._services_by_topic[topic]]

    def listen(self, topic):
        def decorator(func):

            @functools.wraps(func)
            def decorated(publication, *args, **kwargs):

                if not isinstance(publication, Publication):
                    raise SyntaxError('First argument of function {} must be Publication'.format(func.__name__))

                if publication.topic != topic:
                    raise WrongEndpoint('Function {} called with unexpected topic {}'.format(func.__name__,
                                                                                             publication.topic))

                return func(publication, *args, **kwargs)

            service_id = uuid.uuid4().hex

            self._services_by_topic[topic] = service_id
            self._services[service_id] = decorated
            self._topics[service_id] = topic

            return decorated

        return decorator


ctx_subscriber = SubscribeContext()
