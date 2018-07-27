import zmq
from zmq.auth.thread import ThreadAuthenticator

from nimbus.crypto import SecurityManager


class WorkerSecurityManager(SecurityManager):
    """
    Provide security for connections and messages.
    """

    @property
    def secure_connection(self):
        return super().secure_connection and self._connection_public_keys is not None

    def secure_socket(self, socket):
        super().secure_socket(socket)
        worker_public, worker_secret = zmq.auth.load_certificate(self._connection_secret_key)
        socket.curve_secretkey = worker_secret
        socket.curve_publickey = worker_public

        broker_public, _ = zmq.auth.load_certificate(self._connection_broker_public_key)
        socket.curve_serverkey = broker_public
