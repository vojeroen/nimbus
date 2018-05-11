import copy
import time
import uuid
from collections import deque, abc, namedtuple

import msgpack
import zmq
from redis import StrictRedis

from nimbus.helpers import decode, extract_source_from_message, extract_content_from_message
from nimbus.log import get_logger

logger = get_logger(__name__)


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
        logger.debug('Creating ClientRequest {}@{}: {}/{}'.format(self._id,
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
        self._redis = StrictRedis(host=redis_host, port=redis_port, db=redis_db)

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
        logger.debug('Adding ClientRequest to Queue: {} / {}'.format(value.endpoint, id_))
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

    def register(self, worker_id, endpoints):
        """
        Registers a Worker to handle ClientRequests for endpoints.
        :param worker_id: 
        :param endpoints: 
        :return: 
        """
        logger.debug('Registering worker {} to RequestManager for endpoints {}'.format(worker_id, endpoints))
        if worker_id in self._endpoints_by_worker:
            raise WorkerIsAlreadyRegistered
        self._endpoints_by_worker[worker_id] = set(endpoints)
        self.worker_available(worker_id)

    def worker_available(self, worker_id):
        """
        Mark a Worker as available for new ClientRequests.
        :param worker_id: 
        :return: 
        """
        logger.debug('Worker {} is waiting for next request'.format(worker_id))
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
        logger.debug('To process: {}'.format(to_process))
        return to_process.items()


class Broker:
    def __init__(self,
                 worker_response_bind='tcp://127.0.0.1:5002',
                 worker_control_bind='tcp://127.0.0.1:5001',
                 client_bind='tcp://127.0.0.1:5003',
                 redis_host='localhost',
                 redis_port=6379,
                 redis_db=0):
        self._context = zmq.Context.instance()

        logger.info('Creating worker response socket on {}'.format(worker_response_bind))
        self._worker_response_socket = self._context.socket(zmq.ROUTER)
        self._worker_response_socket.bind(worker_response_bind)

        logger.info('Creating worker control socket on {}'.format(worker_control_bind))
        self._worker_control_socket = self._context.socket(zmq.ROUTER)
        self._worker_control_socket.bind(worker_control_bind)

        logger.info('Creating client socket on {}'.format(client_bind))
        self._client_socket = self._context.socket(zmq.ROUTER)
        self._client_socket.bind(client_bind)

        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_db = redis_db

    def run(self):
        poller = zmq.Poller()
        poller.register(self._client_socket, zmq.POLLIN)
        poller.register(self._worker_control_socket, zmq.POLLIN)
        poller.register(self._worker_response_socket, zmq.POLLIN)

        request_manager = RequestManager(redis_host=self._redis_host,
                                         redis_port=self._redis_port,
                                         redis_db=self._redis_db)

        loop = True
        while loop:
            logger.debug('Listening...')
            sockets = dict(poller.poll())

            # get a new client request
            if self._client_socket in sockets and sockets[self._client_socket] == zmq.POLLIN:
                message = self._client_socket.recv_multipart()
                client_request = ClientRequest(source=extract_source_from_message(message),
                                               content=extract_content_from_message(message))
                request_manager.append(client_request)

            # register endpoints of a worker or mark the worker as waiting
            if self._worker_control_socket in sockets and sockets[self._worker_control_socket] == zmq.POLLIN:
                message = self._worker_control_socket.recv_multipart()
                source = extract_source_from_message(message)
                content = extract_content_from_message(message)

                assert len(source) == 1
                worker_id = source[0]
                content = decode(msgpack.unpackb(content))

                if 'endpoints' in content:
                    request_manager.register(worker_id, content['endpoints'])

                if 'w' in content and content['w']:
                    request_manager.worker_available(worker_id)

                if 'r' in content:
                    pass
                # TODO use this to detect non-receiving workers and remove them from the queue

            # receive responses and send them back to the client
            if self._worker_response_socket in sockets and sockets[self._worker_response_socket] == zmq.POLLIN:
                message = self._worker_response_socket.recv_multipart()
                source = extract_source_from_message(message)
                response = extract_content_from_message(message)
                response = msgpack.unpackb(response)

                worker_response = source + [b''] + [b'OK']
                self._worker_response_socket.send_multipart(worker_response)

                request = request_manager[response[b'id'].decode()]
                del request_manager[response[b'id'].decode()]
                del response[b'id']

                client_response = request.source + [b''] + [msgpack.packb(response)]
                self._client_socket.send_multipart(client_response)

            # send requests to workers
            for worker_id, request in request_manager.items():
                logger.debug('Sending to {}: {}'.format(worker_id, request.content))
                self._worker_control_socket.send_multipart([worker_id, b'', msgpack.packb(request.content)])
