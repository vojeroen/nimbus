import copy
import time
import uuid
from collections import deque, abc, namedtuple

import msgpack
import zmq
import zmq.auth
from redis import StrictRedis

from nimbus import config
from nimbus.crypto import SecurityManager
from nimbus.helpers.message import decode, extract_source_from_message, extract_content_from_message
from nimbus.log import get_logger
from nimbus.statemanager import ConnectionStateManager

logger = get_logger(__name__)

SECONDS_BEFORE_CONTACT_CHECK = int(config.get('control', 'seconds_before_contact_check'))
SECONDS_BEFORE_UNREGISTER = int(config.get('control', 'seconds_before_unregister'))


class EmptyQueue(LookupError):
    pass


class WorkerIsAlreadyRegistered(AttributeError):
    pass


class ClientRequest:
    """
    Representation of a client request.
    """

    def __init__(self, source, content):
        self._id = uuid.uuid4().hex  # str
        self._source = copy.deepcopy(source)  # list of bytes
        self._content = msgpack.unpackb(content)  # dictionary of bytes
        self._method = decode(self._content[b'method'])  # str
        self._endpoint = decode(self._content[b'endpoint'])  # str
        logger.debug('Creating or loading ClientRequest {}@{}: {}/{}'.format(self._id,
                                                                             self._endpoint,
                                                                             self._source,
                                                                             self._content))

    def __eq__(self, other):
        if isinstance(other, ClientRequest):
            return self.id == other.id \
                   and self.source == other.source \
                   and self.method == other.method \
                   and self.endpoint == other.endpoint \
                   and self.content == other.content
        else:
            return NotImplemented

    @classmethod
    def fromcache(cls, cached_data):
        """
        :param cached_data: msgpack of source and contant, as outputed by cached_data 
        :return: ClientRequest
        """
        cached_data = msgpack.unpackb(cached_data)
        new_instance = cls(source=cached_data[b'source'],
                           content=msgpack.packb(cached_data[b'content']))
        new_instance._id = cached_data[b'content'][b'id'].decode()
        return new_instance

    @property
    def id(self):
        """
        :return: str
        """
        return self._id

    @property
    def source(self):
        """
        :return: list of bytes
        """
        return self._source

    @property
    def method(self):
        """
        :return: str
        """
        return self._method

    @property
    def endpoint(self):
        """
        :return: str
        """
        return self._endpoint

    @property
    def content(self):
        """
        :return: dict with bytes as key and value
        """
        content = self._content.copy()
        content.update({b'id': self._id.encode()})
        return content

    @property
    def packed_data(self):
        """
        :return: msgpack of content
        """
        return msgpack.packb(self.content)

    @property
    def cached_data(self):
        """
        :return: msgpack of source and content
        """
        return msgpack.packb({
            'source': self._source,
            'content': self.content
        })


class ControlRequest:
    """
    Representation of a client request.
    """

    PING = 'ping'
    PONG = 'pong'
    KICK = 'kick'

    _CONTENT = {
        PING: 'ping',
        PONG: 'pong',
        KICK: 'kick',
    }

    def __init__(self, type_):
        self._type = type_

    @property
    def content(self):
        return {'control': ControlRequest._CONTENT[self._type]}


class MockRedis:
    def __init__(self):
        self._dict = {}

    def set(self, key, value):
        self._dict[key] = value

    def get(self, key, default_value=None):
        return self._dict.get(key, default_value)

    def delete(self, key):
        del self._dict[key]


class RequestQueue(abc.MutableMapping):
    """
    Queue of ClientRequests for a specific endpoint.
    Similar to an ordered dict, but with redis storage.
    """

    STATUS_WAITING = 'waiting'
    STATUS_PROCESSING = 'processing'
    ClientRequestPeek = namedtuple('ClientRequestPeek', 'id timestamp')

    def __init__(self, redis_host, redis_port, redis_db):
        self._id = uuid.uuid4().hex
        self._deque = deque()  # to keep the order of the keys
        self._timestamps = dict()  # to quickly determine if a key is still in the deque, and keep the timestamp
        if redis_host is not None and redis_host is not None and redis_db is not None:
            self._redis = StrictRedis(host=redis_host, port=redis_port, db=redis_db)
        else:
            self._redis = MockRedis()

    def generate_key_content(self, id_):
        return 'broker:' + self._id + ':request:content:' + id_

    def generate_key_status(self, id_):
        return 'broker:' + self._id + ':request:status:' + id_

    def generate_key_timestamp(self, id_):
        return 'broker:' + self._id + ':request:timestamp:' + id_

    @property
    def id(self):
        return self._id

    def __contains__(self, id_):
        try:
            obj = self[id_]
        except KeyError:
            return False
        return obj is not None

    def __len__(self):
        assert len(self._deque) == len(self._timestamps)
        return len(self._timestamps)

    def __setitem__(self, id_, value: ClientRequest):
        """
        Append a ClientRequest to the queue and store it in redis.
        :param id_: 
        :param value: 
        :return: 
        """
        logger.info('Adding ClientRequest to Queue: {} / {}'.format(value.endpoint, id_))
        self._deque.append(id_)
        self._timestamps[id_] = time.time()
        self._redis.set(self.generate_key_content(id_), value.cached_data)
        self._redis.set(self.generate_key_status(id_), self.STATUS_WAITING)
        self._redis.set(self.generate_key_timestamp(id_), self._timestamps[id_])

    def __getitem__(self, id_):
        """
        Get a specific ClientRequest by id.
        :param id_: 
        :return: 
        """
        key_content = self.generate_key_content(id_)
        cached_data = self._redis.get(key_content)
        if cached_data is None:
            raise KeyError
        return ClientRequest.fromcache(cached_data)

    def __delitem__(self, id_):
        """
        Remove a ClientRequest from the queue (if it still exists) and from redis.
        :param id_: 
        :return: 
        """
        for redis_key in [self.generate_key_content(id_),
                          self.generate_key_status(id_),
                          self.generate_key_timestamp(id_)]:
            self._redis.delete(redis_key)
        if id_ in self._timestamps:
            del self._timestamps[id_]
            self._deque.remove(id_)

    def __iter__(self):
        for item in self._deque:
            yield self[item]

    def append(self, client_request):
        """
        Append a ClientRequest to the queue.
        :param client_request: 
        :return: 
        """
        self[client_request.id] = client_request

    def popitem(self):
        """
        Take the next ClientRequest from the queue. The ClientRequest is removed from the queue, and its status in 
        redis is changed to STATUS_PROCESSING.
        :return: 
        """
        try:
            id_ = self._deque.popleft()
            del self._timestamps[id_]
        except (IndexError, KeyError):
            raise EmptyQueue
        self._redis.set(self.generate_key_status(id_), self.STATUS_PROCESSING)
        return self[id_]

    def peek(self):
        """
        Get the id and timestamp of the first ClientRequest in the queue, without accessing redis.
        :return: 
        """
        try:
            id_ = self._deque[0]
        except IndexError:
            raise EmptyQueue
        return RequestQueue.ClientRequestPeek(id=id_, timestamp=self._timestamps[id_])


class QueueManager:
    """
    Manage a pool of RequestQueues, where each RequestQueue is assigned to a specific endpoint.
    """

    def __init__(self, redis_host, redis_port, redis_db):
        self._redis = (redis_host, redis_port, redis_db)
        self._queue_by_endpoint = dict()

    def __len__(self):
        return sum([len(queue) for endpoint, queue in self._queue_by_endpoint.items()])

    def load_redis(self):
        """
        Load the queue from redis. Useful to recover after a crash.
        :return: None
        """
        raise NotImplementedError

    def append(self, client_request: ClientRequest) -> None:
        """
        Append a ClientRequest to the correct RequestQueue, based on its endpoint.
        :param client_request: 
        :return: 
        """
        self.get_queue(client_request.endpoint).append(client_request)

    def retrieve(self, client_request_id: str) -> ClientRequest:
        """
        Retrieve the ClientRequest identified by client_request_id.
        :param client_request_id: 
        :return: 
        """
        for endpoint, queue in self._queue_by_endpoint.items():
            if client_request_id in queue:
                return queue[client_request_id]
        raise KeyError

    def remove(self, client_request_id: str) -> None:
        """
        Remove the ClientRequest, identified by client_request_id, from the RequestQueue.
        :param client_request_id: 
        :return: 
        """
        for endpoint, queue in self._queue_by_endpoint.items():
            if client_request_id in queue:
                logger.debug('Removing ClientRequest {} from RequestQueue for {}'.format(client_request_id, endpoint))
                del queue[client_request_id]
                return
        raise KeyError

    def get_queue(self, endpoint: str) -> RequestQueue:
        """
        Get the RequestQueue for the endpoint. 
        :param endpoint: 
        :return: 
        """
        return self._queue_by_endpoint.setdefault(endpoint, RequestQueue(*self._redis))

    def select_queue(self, endpoints: list) -> RequestQueue:
        """
        Select the RequestQueue among the endpoints that has the oldest ClientRequest.
        :param endpoints: 
        :return: 
        """
        available_client_requests = []
        for endpoint in endpoints:
            queue = self.get_queue(endpoint)
            if len(queue) > 0:
                available_client_requests.append(queue)

        if len(available_client_requests) == 0:
            raise EmptyQueue

        return sorted(available_client_requests, key=lambda q: q.peek().timestamp)[0]


class RequestManager:
    """
    Manage all requests and abstract the queues itself.
    """

    def __init__(self, redis_host, redis_port, redis_db):
        self._manager = QueueManager(redis_host, redis_port, redis_db)
        self._endpoints_by_worker = dict()
        self._waiting_workers = set()

    def __len__(self):
        return len(self._manager)

    @property
    def registered_workers(self):
        return list(self._endpoints_by_worker.keys())

    def register(self, worker_id, endpoints):
        """
        Registers a Worker to handle ClientRequests for endpoints.
        :param worker_id: 
        :param endpoints: 
        :return: 
        """
        logger.info('Registering worker {} to RequestManager for endpoints {}'.format(worker_id, endpoints))
        # if worker_id in self._endpoints_by_worker:
        #     raise WorkerIsAlreadyRegistered
        self._endpoints_by_worker[worker_id] = set(endpoints)
        self.worker_available(worker_id)

    def unregister(self, worker_id):
        """
        Unregisters a Worker from all its endpoits.
        :param worker_id: 
        :return: 
        """
        try:
            del self._endpoints_by_worker[worker_id]
        except KeyError:
            pass
        try:
            self._waiting_workers.remove(worker_id)
        except KeyError:
            pass

    def worker_available(self, worker_id):
        """
        Mark a Worker as available for new ClientRequests.
        :param worker_id: 
        :return: 
        """
        logger.info('Worker {} is waiting for next request'.format(worker_id))
        self._waiting_workers.add(worker_id)

    def append(self, client_request: ClientRequest):
        """
        Add a ClientRequest.
        :param client_request: 
        :return: 
        """
        self._manager.get_queue(client_request.endpoint).append(client_request)

    def __getitem__(self, client_request_id: str) -> ClientRequest:
        """
        Get a ClientRequest.
        :param worker_id: 
        :return: 
        """
        return self._manager.retrieve(client_request_id)

    def __delitem__(self, client_request_id: str):
        """
        Remove a ClientRequest when it is completed.
        :param client_request_id: 
        :return: 
        """
        self._manager.remove(client_request_id)

    def __call__(self, *args, **kwargs):
        """
        Get the next ClientRequest for all available Workers.
        :param args: 
        :param kwargs: 
        :return: 
        """
        to_process = {}
        for worker_id in self._waiting_workers.copy():
            try:
                to_process[worker_id] = self._manager.select_queue(self._endpoints_by_worker[worker_id]).popitem()
                self._waiting_workers.remove(worker_id)
            except EmptyQueue:
                pass
        if len(to_process) > 0:
            logger.debug('To process: {}'.format(to_process))
        return to_process.items()


class BrokerSecurityManager(SecurityManager):

    def secure_socket(self, socket):
        super().secure_socket(socket)
        public_key, secret_key = zmq.auth.load_certificate(self._connection_secret_key)
        socket.curve_secretkey = secret_key
        socket.curve_publickey = public_key
        socket.curve_server = True


class Broker:
    def __init__(self,
                 worker_response_bind,
                 worker_control_bind,
                 client_bind,
                 redis_host=None,
                 redis_port=None,
                 redis_db=None,
                 security_manager=None):
        logger.info('Creating worker response socket on {}'.format(worker_response_bind))
        logger.info('Creating worker control socket on {}'.format(worker_control_bind))
        logger.info('Creating client socket on {}'.format(client_bind))

        if security_manager is None:
            self.security_manager = SecurityManager()
        else:
            self.security_manager = security_manager

        self._worker_context = zmq.Context.instance()
        self._context = zmq.Context.instance()

        if self.security_manager.secure_connection:
            self.security_manager.configure_connection_security(self._worker_context)

        self._worker_response_socket = self._worker_context.socket(zmq.ROUTER)
        self._worker_control_socket = self._worker_context.socket(zmq.ROUTER)
        self._client_socket = self._context.socket(zmq.ROUTER)

        if self.security_manager.secure_connection:
            for socket in [self._worker_response_socket, self._worker_control_socket]:
                self.security_manager.secure_socket(socket)

        self._worker_response_socket.bind(worker_response_bind)
        self._worker_control_socket.bind(worker_control_bind)
        self._client_socket.bind(client_bind)

        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_db = redis_db

    def send_ping(self, worker_id):
        self.security_manager.send_message(
            self._worker_control_socket,
            ControlRequest(ControlRequest.PING).content,
            worker_id
        )

    def send_pong(self, worker_id):
        self.security_manager.send_message(
            self._worker_control_socket,
            ControlRequest(ControlRequest.PONG).content,
            worker_id
        )

    def send_kick(self, worker_id):
        self.security_manager.send_message(
            self._worker_control_socket,
            ControlRequest(ControlRequest.KICK).content,
            worker_id
        )

    def run(self):
        poller = zmq.Poller()
        poller.register(self._client_socket, zmq.POLLIN)
        poller.register(self._worker_control_socket, zmq.POLLIN)
        poller.register(self._worker_response_socket, zmq.POLLIN)

        request_manager = RequestManager(redis_host=self._redis_host,
                                         redis_port=self._redis_port,
                                         redis_db=self._redis_db)

        state_manager = ConnectionStateManager(seconds_before_contact_check=SECONDS_BEFORE_CONTACT_CHECK,
                                               seconds_before_disconnect=SECONDS_BEFORE_UNREGISTER)

        poller_timeout = max([int(min([SECONDS_BEFORE_CONTACT_CHECK,
                                       SECONDS_BEFORE_UNREGISTER]) / 10.0 * 1000),
                              500])
        loop = True
        while loop:
            sockets = dict(poller.poll(poller_timeout))

            # get a new client request
            if self._client_socket in sockets and sockets[self._client_socket] == zmq.POLLIN:
                message = self._client_socket.recv_multipart()
                client_request = ClientRequest(source=extract_source_from_message(message),
                                               content=extract_content_from_message(message))
                request_manager.append(client_request)

            # register endpoints of a worker or mark the worker as waiting
            if self._worker_control_socket in sockets and sockets[self._worker_control_socket] == zmq.POLLIN:
                source, content = self.security_manager.read_socket(self._worker_control_socket)
                assert len(source) == 1
                worker_id = source[0]
                content = decode(msgpack.unpackb(content))

                state_manager.contact_from(worker_id)

                if 'endpoints' in content:
                    # first connection to register endpoints
                    request_manager.register(worker_id, content['endpoints'])

                if 'ping' in content and content['ping']:
                    # ping to check if broker is still available
                    logger.debug('Received ping from {}'.format(worker_id))
                    if worker_id in request_manager.registered_workers:
                        # only respond to registered workers, otherwise disconnect
                        self.send_pong(worker_id)
                    else:
                        # we don't know this worker, so remove it
                        self.send_kick(worker_id)
                        state_manager.disconnect(worker_id)

                if 'pong' in content and content['pong']:
                    logger.debug('Received pong from {}'.format(worker_id))

                if 'disconnect' in content and content['disconnect']:
                    # disconnect worker
                    request_manager.unregister(worker_id)
                    state_manager.disconnect(worker_id)

                if 'r' in content:
                    # acknowledge reception of task
                    pass

                if 'w' in content and content['w']:
                    # signal that task is done
                    request_manager.worker_available(worker_id)

            # receive responses and send them back to the client
            if self._worker_response_socket in sockets and sockets[self._worker_response_socket] == zmq.POLLIN:
                source, content = self.security_manager.read_socket(self._worker_response_socket)
                response = msgpack.unpackb(content)

                self.security_manager.send_message(
                    self._worker_response_socket,
                    b'OK',
                    source
                )

                request = request_manager[response[b'id'].decode()]
                del request_manager[response[b'id'].decode()]
                del response[b'id']

                client_response = request.source + [b''] + [msgpack.packb(response)]
                self._client_socket.send_multipart(client_response)

            # send requests to workers
            for worker_id, request in request_manager():
                logger.info('Sending {} to {}'.format(request.id, worker_id))
                self.security_manager.send_message(
                    self._worker_control_socket,
                    request.content,
                    worker_id
                )

            # send ping messages to workers
            for worker_id in state_manager.get_connections_to_ping():
                logger.debug('Pinging {}'.format(worker_id))
                self.send_ping(worker_id)

            # send kick messages to non-responsive workers
            for worker_id in state_manager.get_connections_to_disconnect():
                logger.info('Kicking {}'.format(worker_id))
                self.send_kick(worker_id)
                request_manager.unregister(worker_id)
