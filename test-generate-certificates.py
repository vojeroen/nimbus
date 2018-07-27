import os
import shutil

import zmq.auth
from Crypto.PublicKey import DSA

BROKER_CONNECTION_KEYS_DIR = 'keys/connection/broker'
BROKER_CONNECTION_PUBLIC_KEYS_DIR = 'keys/connection/broker/public'
BROKER_MESSAGE_KEYS_DIR = 'keys/message/broker'
BROKER_MESSAGE_PUBLIC_KEYS_DIR = 'keys/message/broker/public'

WORKER_CONNECTION_KEYS_DIR = 'keys/connection/worker'
WORKER_MESSAGE_KEYS_DIR = 'keys/message/worker'
WORKER_MESSAGE_PUBLIC_KEYS_DIR = 'keys/message/worker/public'


def generate_certificates(base_dir):
    """ Generate client and server CURVE certificate files"""

    # Create directories for certificates, remove old content if necessary
    for d in [BROKER_CONNECTION_KEYS_DIR, BROKER_CONNECTION_PUBLIC_KEYS_DIR, WORKER_CONNECTION_KEYS_DIR,
              BROKER_MESSAGE_KEYS_DIR, BROKER_MESSAGE_PUBLIC_KEYS_DIR,
              WORKER_MESSAGE_KEYS_DIR, WORKER_MESSAGE_PUBLIC_KEYS_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.mkdir(d)

    # connection: create new keys
    broker_public_file, broker_secret_file = zmq.auth.create_certificates(BROKER_CONNECTION_KEYS_DIR, 'broker')
    worker_public_file, worker_secret_file = zmq.auth.create_certificates(WORKER_CONNECTION_KEYS_DIR, 'worker')

    # connection: move public keys to appropriate directory
    for key_file in os.listdir(WORKER_CONNECTION_KEYS_DIR):
        if key_file.endswith(".key"):
            shutil.copy(os.path.join(WORKER_CONNECTION_KEYS_DIR, key_file),
                        os.path.join(BROKER_CONNECTION_PUBLIC_KEYS_DIR, '.'))

    # message: create new keys
    broker_key = DSA.generate(2048)
    with open(os.path.join(BROKER_MESSAGE_KEYS_DIR, 'broker.secret.pem'), 'wb') as ofile:
        ofile.write(broker_key.export_key('PEM'))
    with open(os.path.join(BROKER_MESSAGE_KEYS_DIR, 'broker.pem'), 'wb') as ofile:
        ofile.write(broker_key.publickey().export_key('PEM'))

    worker_key = DSA.generate(2048)
    with open(os.path.join(WORKER_MESSAGE_KEYS_DIR, 'secret.pem'), 'wb') as ofile:
        ofile.write(worker_key.export_key('PEM'))
    with open(os.path.join(WORKER_MESSAGE_KEYS_DIR, 'worker-1.pem'), 'wb') as ofile:
        ofile.write(worker_key.publickey().export_key('PEM'))

    # message: move public keys to appropriate directory
    for key_file in os.listdir(WORKER_MESSAGE_KEYS_DIR):
        if key_file.endswith('.pem') and not key_file.endswith('secret.pem'):
            shutil.copy(os.path.join(WORKER_MESSAGE_KEYS_DIR, key_file),
                        os.path.join(BROKER_MESSAGE_PUBLIC_KEYS_DIR, '.'))
    shutil.copy(os.path.join(BROKER_MESSAGE_KEYS_DIR, 'broker.pem'),
                os.path.join(WORKER_MESSAGE_PUBLIC_KEYS_DIR, '.'))


if __name__ == '__main__':
    if zmq.zmq_version_info() < (4, 0):
        raise RuntimeError(
            "Security is not supported in libzmq version < 4.0. libzmq version {0}".format(zmq.zmq_version()))

    generate_certificates(os.path.dirname(__file__))
