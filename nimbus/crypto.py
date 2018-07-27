import os

import msgpack
from Crypto.Hash import SHA256
from Crypto.PublicKey import DSA
from Crypto.Signature import DSS
from zmq.auth import CURVE_ALLOW_ANY
from zmq.auth.thread import ThreadAuthenticator

from nimbus.helpers import extract_source_from_message, extract_content_from_message


class SecurityManager:
    """
    Provide security for connections and messages.
    """

    def __init__(self,
                 connection_secret_key=None,
                 connection_public_keys=None,
                 connection_broker_public_key=None,
                 message_secret_key=None,
                 message_public_keys=None):
        self._auths = []

        self._connection_secret_key = connection_secret_key
        if connection_public_keys is None:
            self._connection_public_keys = CURVE_ALLOW_ANY
        else:
            self._connection_public_keys = connection_public_keys
        self._connection_broker_public_key = connection_broker_public_key

        self._message_secret_key = message_secret_key
        self._message_public_keys = message_public_keys

        self._message_secret_key_cache = None
        self._message_public_keys_cache = {}

    @property
    def secure_connection(self):
        return self._connection_secret_key is not None and self._connection_public_keys is not None

    @property
    def secure_messages(self):
        return self._message_secret_key is not None and self._message_public_keys is not None

    def configure_connection_security(self, context):
        if not self.secure_connection:
            raise RuntimeError('This SecurityManager does not secure connections, ' +
                               'and you are attempting to configure connection security')
        auth = ThreadAuthenticator(context)
        auth.start()
        auth.configure_curve(domain='*', location=self._connection_public_keys)
        self._auths.append(auth)

    def secure_socket(self, socket):
        if not self.secure_connection:
            raise RuntimeError('This SecurityManager does not secure connections, ' +
                               'and you are attempting to secure a socket')

    def _get_message_secret_key(self):
        if self._message_secret_key_cache is None:
            with open(os.path.join(self._message_secret_key), 'rb') as key_file:
                private_key = DSA.import_key(key_file.read())
            self._message_secret_key_cache = private_key
        else:
            private_key = self._message_secret_key_cache
        return private_key

    def _get_message_public_key(self, identity):
        if isinstance(identity, bytes):
            identity = identity.decode()

        identity = identity.lower()

        try:
            public_key = self._message_public_keys_cache[identity]
        except KeyError:
            with open(os.path.join(self._message_public_keys, identity + '.pem'), 'rb') as key_file:
                public_key = DSA.import_key(key_file.read())
            self._message_public_keys_cache[identity] = public_key
        return public_key

    def _sign_message(self, message):
        if not self.secure_messages:
            return msgpack.packb({'message': message})

        private_key = self._get_message_secret_key()

        message_hash = SHA256.new(message)
        signature = DSS.new(private_key, mode='fips-186-3').sign(message_hash)

        return msgpack.packb({
            'message': message,
            'signature': signature,
        })

    def _verify_message(self, identity, message):
        message = msgpack.unpackb(message)

        if not self.secure_messages:
            return message[b'message']

        public_key = self._get_message_public_key(identity)
        signature = message[b'signature']
        message = message[b'message']

        message_hash = SHA256.new(message)
        DSS.new(public_key, mode='fips-186-3').verify(message_hash, signature)

        return message

    def read_socket(self, socket, verify=None):
        message = socket.recv_multipart()
        source = extract_source_from_message(message)
        content = extract_content_from_message(message)

        if verify is not None:
            content = self._verify_message(verify, content)
        else:
            content = self._verify_message(source[0], content)
            assert len(source) == 1

        return source, content

    def send_message(self, socket, message, source=None):
        message = [
            b'',
            self._sign_message(msgpack.packb(message))
        ]

        if isinstance(source, bytes):
            source = [source]

        if source is not None:
            message = source + message

        socket.send_multipart(message)
