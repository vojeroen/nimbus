from nimbus import errors


def assert_key_present(key, data):
    if key not in data.keys():
        raise errors.PayloadNotComplete, 'The payload must contain the key "{key_name}"'.format(key_name=key)


def assert_correct_type(data, data_type):
    if not isinstance(data, data_type):
        raise errors.PayloadNotCorrect, 'The payload contains incorrect data types'


class Serializer:
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

    # @validated_data.setter
    # def validated_data(self, value):
    #     assert isinstance(value, dict), 'Validated data must be a dictionary'
    #     self._validated_data = value

    @property
    def serialized_data(self):
        if self._serialized_data is None:
            self.serialize()
        return self._serialized_data

    @property
    def instance(self):
        if self._instance is None:
            self._create_instance()
        return self._instance

    @property
    def model(self):
        return self.Meta.model

    def _create_instance(self):
        self._instance = self.Meta.model(**self.validated_data)

    def validate(self):
        """Transform self.raw_data to self._validated_data"""
        pass

    def serialize(self):
        """Transform self.instance to self._serialized_data"""
        pass
