import logging
import sys
from logging.handlers import SMTPHandler

from nimbus import config

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)


def get_logger(name):
    return root_logger.getChild(name)


log_format = '%(asctime)s - %(levelname)-8s - %(name)s - %(message)s'
mail_log_format = '%(asctime)s\n%(levelname)-8s\n%(name)s\n%(message)s'

log_level_mapper = {
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}

# Logging to stdout
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter(log_format))
stream_handler.setLevel(log_level_mapper[config.get('logging', 'stdout_level')])
root_logger.addHandler(stream_handler)

# Logging to file
file_handler = logging.FileHandler(config.get('logging', 'file_path'))
file_handler.setFormatter(logging.Formatter(log_format))
file_handler.setLevel(log_level_mapper[config.get('logging', 'file_level')])
root_logger.addHandler(file_handler)

# Logging to mail
if config.has_option('logging', 'mail_level'):
    mail_handler = SMTPHandler(mailhost='localhost',
                               fromaddr=config.get('logging', 'mail_from'),
                               toaddrs=config.get('logging', 'mail_to'),
                               subject='Error in {}'.format(config.get('general', 'name')))
    mail_handler.setFormatter(logging.Formatter(mail_log_format))
    mail_handler.setLevel(log_level_mapper[config.get('logging', 'mail_level')])
    root_logger.addHandler(mail_handler)
