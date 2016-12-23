import configparser
import logging
import sys
from logging.handlers import SMTPHandler

cparser = configparser.ConfigParser()
cparser.read('log.config')

log_level_mapper = {
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}

LOG_FORMAT = '%(asctime)s - %(levelname)-8s - %(name)s - %(message)s'

LOG_STDOUT_LEVEL = log_level_mapper[cparser.get('nimbus', 'log_stdout')]

LOG_FILE_LEVEL = logging.DEBUG
LOG_FILE_LOCATION = 'gateway.log'

LOG_MAIL_FORMAT = '%(asctime)s\n%(levelname)-8s\n%(name)s\n%(message)s'
LOG_MAIL_LEVEL = logging.ERROR

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
stream_handler.setLevel(LOG_STDOUT_LEVEL)
root_logger.addHandler(stream_handler)

file_handler = logging.FileHandler(LOG_FILE_LOCATION)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
file_handler.setLevel(LOG_FILE_LEVEL)
root_logger.addHandler(file_handler)

mail_handler = SMTPHandler(mailhost='localhost',
                           fromaddr='gateway@mist-services.eu',
                           toaddrs='vo.jeroen@gmail.com',
                           subject='An error occured in nimbus')
mail_handler.setFormatter(logging.Formatter(LOG_MAIL_FORMAT))
mail_handler.setLevel(LOG_MAIL_LEVEL)
root_logger.addHandler(mail_handler)


def get_logger(name):
    return root_logger.getChild(name)
