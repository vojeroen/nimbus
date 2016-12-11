from abc import ABCMeta, abstractclassmethod

from nimbus import errors


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

    def __init__(self, data=None):
        self._raw_data = None
        self._validated_data = None
        self._serialized_data = None
        self._instance = None

        if isinstance(data, dict):
            self._raw_data = data

        elif isinstance(data, self.Meta.model):
            self._instance = data

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
        if self._instance is None:
            self.create_instance()
        return self._instance

    @property
    def serialized_data(self):
        if self._serialized_data is None:
            self.serialize()
        return self._serialized_data

    @property
    def model(self):
        return self.Meta.model

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
