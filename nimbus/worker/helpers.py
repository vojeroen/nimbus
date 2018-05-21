from nimbus.worker.errors import DataNotComplete, DataTypeNotCorrect


def assert_key_present(key, data):
    if key not in data.keys():
        raise DataNotComplete('The payload must contain the key "{key_name}"'.format(key_name=key))


def assert_correct_type(data, data_type):
    if not isinstance(data, data_type):
        raise DataTypeNotCorrect('The payload contains incorrect data types')


def assert_correct_key(data, key, data_type):
    assert_key_present(key, data)
    assert_correct_type(data[key], data_type)
