import msgpack

from nimbus.helpers import decode
from nimbus.log import get_logger

logger = get_logger(__name__)


class Request:
    def __init__(self, request):
        request = msgpack.unpackb(request)
        self._id = decode(request[b'id'])  # str
        self._method = decode(request[b'method'])  # str
        self._endpoint = decode(request[b'endpoint'])  # str
        if b'parameters' in request:
            self._parameters = decode(request[b'parameters'])  # dict of str
        else:
            self._parameters = {}
        if b'data' in request:
            self._data = request[b'data']  # dict of bytes
        else:
            self._data = {}

    @property
    def id(self):
        """
        :return: str 
        """
        return self._id

    @property
    def method(self):
        """
        :return: str 
        """
        return self._method

    @property
    def endpoint(self):
        """
        :return: str 
        """
        return self._endpoint

    @property
    def parameters(self):
        """
        :return: dict of str 
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = value

    @property
    def data(self):
        """
        :return: dict of bytes 
        """
        return self._data

    @property
    def original(self):
        return msgpack.packb({
            'id': self._id,
            'method': self._method,
            'endpoint': self._endpoint,
            'parameters': self._parameters,
            'data': self._data,
        })
