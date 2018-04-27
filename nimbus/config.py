import configparser

cparser = configparser.ConfigParser()
cparser.read('configuration')


def get(*args, **kwargs):
    return cparser.get(*args, **kwargs)


def has_option(*args, **kwargs):
    return cparser.has_option(*args, **kwargs)
