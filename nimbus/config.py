import configparser

CONFIG_FILE_PATH = 'configuration.ini'

cparser = configparser.ConfigParser()
cparser.read(CONFIG_FILE_PATH)


def get(*args, **kwargs):
    return cparser.get(*args, **kwargs)


def has_option(*args, **kwargs):
    return cparser.has_option(*args, **kwargs)
