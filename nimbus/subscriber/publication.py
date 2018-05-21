import msgpack

from nimbus.helpers import decode
from nimbus.log import get_logger

logger = get_logger(__name__)


class Publication:
    def __init__(self, publication):
        publication = msgpack.unpackb(publication)
        self._id = decode(publication[b'id'])  # str
        self._topic = decode(publication[b'topic'])  # str
        self._data = publication[b'data']  # dict of bytes

    @property
    def id(self):
        """
        :return: str 
        """
        return self._id

    @property
    def topic(self):
        """
        :return: str 
        """
        return self._topic

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
            'topic': self._topic,
            'data': self._data,
        })
