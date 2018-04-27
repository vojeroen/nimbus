from nimbus.worker.errors import DataNotCorrect
from nimbus.worker.message import assert_correct_type


class Manipulator:
    MODEL = None
    FIELDS = []

    def __init__(self, obj):
        self._object = obj
        if not isinstance(obj, self.__class__.MODEL):
            raise DataNotCorrect

    @property
    def object(self):
        return self._object

    def create(self, data):
        self.update(data)

    def update(self, data):
        raise NotImplementedError

    def add_attribute_if_present(self, data, attribute_name_message, data_type, attribute_name_model=None):
        if attribute_name_model is None:
            attribute_name_model = attribute_name_message
        if attribute_name_message in data:
            assert_correct_type(data[attribute_name_message], data_type)
        setattr(self.object, attribute_name_model, data[attribute_name_message])
