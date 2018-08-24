import functools
import uuid

from nimbus.log import get_logger
from nimbus.worker.errors import EndpointDoesNotExist, MissingParameter, WrongEndpoint, WrongMethod
from nimbus.worker.request import Request

logger = get_logger(__name__)


class RequestContext:
    def __init__(self):
        self._endpoints_by_label = {}
        self._services_by_endpoint = {}
        self._services = {}
        self._endpoints = {}
        self._labels = {}

    @property
    def endpoints(self):
        return sorted(set(e[1] for e in self._services_by_endpoint.keys()))

    def get_service_by_endpoint(self, endpoint, method='GET'):
        return self._services[self._services_by_endpoint[(method, endpoint)]]

    def endpoint_for(self, label):
        try:
            return self._endpoints_by_label[label]
        except KeyError:
            raise EndpointDoesNotExist

    def route(self, endpoint, methods, parameters=None, label=None):
        def decorator(func):
            nonlocal label

            @functools.wraps(func)
            def decorated(request, *args, **kwargs):
                nonlocal parameters
                if parameters is None:
                    parameters = []

                if not isinstance(request, Request):
                    raise SyntaxError('First argument of function {} must be Request'.format(func.__name__))

                if request.endpoint != endpoint:
                    raise WrongEndpoint('Function {} called with unexpected endpoint {}'.format(func.__name__,
                                                                                                request.endpoint))

                if request.method not in methods:
                    raise WrongMethod('Function {} called with unexpected method {}'.format(func.__name__,
                                                                                            request.method))

                for parameter in parameters:
                    if parameter not in request.parameters:
                        raise MissingParameter('Function {} requires parameter {}'.format(func.__name__,
                                                                                          parameter))

                return func(request, *args, **kwargs)

            for method in methods:

                if label is None:
                    label = func.__name__
                service_id = uuid.uuid4().hex

                if label in self._endpoints_by_label:
                    assert self._endpoints_by_label[label] == endpoint
                else:
                    self._endpoints_by_label[label] = endpoint
                self._services_by_endpoint[(method, endpoint)] = service_id
                self._services[service_id] = decorated
                self._endpoints[service_id] = (method, endpoint)
                self._labels[service_id] = (method, label)

            return decorated

        return decorator


ctx_request = RequestContext()
