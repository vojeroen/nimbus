from abc import ABCMeta, abstractclassmethod

from nimbus import errors
from nimbus.models import Session


def assert_key_present(key, data):
    if key not in data.keys():
        raise errors.PayloadNotComplete('The payload must contain the key "{key_name}"'.format(key_name=key))


def assert_correct_type(data, data_type):
    if not isinstance(data, data_type):
        raise errors.PayloadNotCorrect('The payload contains incorrect data types')


def assert_correct_key(data, key, data_type):
    assert_key_present(key, data)
    assert_correct_type(data[key], data_type)


class Serializer:
    __metaclass__ = ABCMeta

    class Meta:
        model = None

    def __init__(self, data=None, parent=None, serialize_children=True, instance=None):
        self._parent = parent
        self._raw_data = None
        self._validated_data = None
        self._serialized_data = None
        self._serialize_children = serialize_children
        self._session = Session()

        if isinstance(data, dict):
            self._raw_data = data
            self._instance = instance
            self._must_update_instance = True

        elif isinstance(data, self.Meta.model):
            self._instance = data
            self._must_update_instance = False

    @property
    def raw_data(self):
        return self._raw_data

    @property
    def validated_data(self):
        if self._validated_data is None:
            self.validate()
        return self._validated_data

    @property
    def instance(self):
        if self._must_update_instance:
            self.create_instance()
            self._must_update_instance = False
        return self._instance

    @property
    def serialized_data(self):
        if self._serialized_data is None:
            self.serialize()
        return self._serialized_data

    @property
    def model(self):
        return self.Meta.model

    @property
    def parent(self):
        return self._parent

    @property
    def serialize_children(self):
        return self._serialize_children

    def validate_key(self, key, data_type):
        if key.encode() in self.raw_data.keys():
            assert_correct_type(self.raw_data[key.encode()], data_type)
            if data_type == bytes:
                self._validated_data[key] = self.raw_data[key.encode()].decode()
            else:
                self._validated_data[key] = self.raw_data[key.encode()]

    @abstractclassmethod
    def validate(self):
        """Transform self.raw_data to self._validated_data"""
        pass

    @abstractclassmethod
    def create_instance(self):
        """Transform self.validated_data to self._instance"""
        pass

    @abstractclassmethod
    def serialize(self):
        """Transform self.instance to self._serialized_data"""
        pass
