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
        parent = None

    def __init__(self, data=None, parent=None, instance=None, strict_parent_checking=True):
        self._parent = parent
        self._raw_data = None
        self._validated_data = None
        self._serialized_data = None
        self._strict_parent_checking = strict_parent_checking
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

        try:
            assert self.Meta.parent is not None
        except AttributeError:
            assert self._parent is None
            return self._instance

            # Check if the parent object is complete
            # WARNING: never commit during the create_instance method
        if self._parent:
            assert isinstance(self._parent, self.Meta.parent)
            if self._strict_parent_checking:
                assert self._parent.is_complete
        elif self.Meta.parent and self._strict_parent_checking:
            assert self._session.query(self.Meta.parent).join(self.Meta.model) \
                .filter(self.Meta.model.uuid == self._instance.uuid).one().is_complete
        return self._instance

    @property
    def serialized_data(self):
        if self._serialized_data is None:
            self.serialize()
        return self._serialized_data

    @property
    def parent(self):
        return self._parent

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
