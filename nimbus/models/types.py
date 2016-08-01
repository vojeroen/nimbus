import uuid

from sqlalchemy import types
from sqlalchemy import String, Integer


class UUID(types.TypeDecorator):
    impl = types.String

    def __init__(self):
        self.impl.length = 32
        types.TypeDecorator.__init__(self, length=self.impl.length)

    def process_bind_param(self, value, dialect=None):
        if value and isinstance(value, str):
            value = uuid.UUID(value)
        if value and isinstance(value, uuid.UUID):
            return value.hex
        elif value and not isinstance(value, uuid.UUID):
            raise ValueError, 'value %s is not a valid uuid.UUID' % value
        else:
            return None

    def process_result_value(self, value, dialect=None):
        if value:
            return uuid.UUID(value)
        else:
            return None

    def is_mutable(self):
        return False
