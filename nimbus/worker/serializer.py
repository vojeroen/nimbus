import datetime
from collections.abc import Iterable

from nimbus.worker.errors import DataNotCorrect


class SerializerField:
    def __init__(self, model, message=None):
        self.model = model
        if message is None:
            self.message = model
        else:
            self.message = message


class Serializer:
    MODEL = None
    FIELDS = []

    def __init__(self, obj, list_allowed=True):
        self._object = obj
        self._serialized_data = None
        if isinstance(obj, self.__class__.MODEL):
            self._is_iterable = False
        elif list_allowed and isinstance(obj, Iterable):
            self._is_iterable = True
        else:
            raise DataNotCorrect

    @property
    def object(self):
        return self._object

    @property
    def data(self):
        if self._serialized_data is None:
            self._serialized_data = self._serialize()
        return self._serialized_data

    def _serialize(self):
        if self._is_iterable:
            data = []
            for obj in self._object:
                data.append(self.__class__(obj).data)
        else:
            data = self.serialize()
        return data

    def serialize(self):
        data = {}
        for field in self.__class__.FIELDS:
            data[field.message] = getattr(self.object, field.model)
            if isinstance(data[field.message], datetime.date):
                data[field.message] = data[field.message].isoformat()
        return data
