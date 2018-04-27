import msgpack

from nimbus.helpers import decode
from nimbus.log import get_logger
from nimbus.worker.errors import DataNotComplete, DataTypeNotCorrect

logger = get_logger(__name__)


def assert_key_present(key, data):
    if key not in data.keys():
        raise DataNotComplete('The payload must contain the key "{key_name}"'.format(key_name=key))


def assert_correct_type(data, data_type):
    if not isinstance(data, data_type):
        raise DataTypeNotCorrect('The payload contains incorrect data types')


def assert_correct_key(data, key, data_type):
    assert_key_present(key, data)
    assert_correct_type(data[key], data_type)


class Message:
    def __init__(self, message):
        message = msgpack.unpackb(message)
        self._id = decode(message[b'id'])
        self._method = decode(message[b'method'])
        self._endpoint = decode(message[b'endpoint'])
        if b'parameters' in message:
            self._parameters = decode(message[b'parameters'])
        else:
            self._parameters = None
        if b'data' in message:
            self._data = message[b'data']
        else:
            self._data = None

    @property
    def id(self):
        return self._id

    @property
    def method(self):
        return self._method

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def parameters(self):
        return self._parameters

    @property
    def data(self):
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
