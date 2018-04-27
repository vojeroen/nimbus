import uuid
from collections import deque

import msgpack
import zmq
from redis import StrictRedis

from nimbus.helpers import decode, extract_source_from_message, extract_content_from_message
from nimbus.log import get_logger

logger = get_logger(__name__)


class ClientRequest:
    def __init__(self, source, content):
        self._id = uuid.uuid4().hex
        self._source = source
        self._content = msgpack.unpackb(content)
        self._endpoint = decode(self._content[b'endpoint'])
        logger.debug('Creating ClientRequest {}@{}: {}/{}'.format(self._id,
                                                                  self._endpoint,
                                                                  self._source,
                                                                  self._content))

    @classmethod
    def generate_key(cls, _id):
        return 'broker:request:' + _id

    @property
    def key(self):
        return ClientRequest.generate_key(self._id)

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def content(self):
        content = self._content.copy()
        content.update({'id': self._id})
        return content

    @property
    def packed(self):
        return msgpack.packb(self.content)

    @property
    def cache(self):
        return msgpack.packb({
            'id': self._id,
            'source': self._source,
            'content': self.content
        })

    def store(self, redis):
        logger.debug('Storing ClientRequest {} in the cache'.format(self._id))
        redis.set(self.key, self.cache)


class ClientRequestQueue:
    def __init__(self, redis_host, redis_port, redis_db):
        self._queue = {}
        self._redis = StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    def add(self, client_request):
        endpoint = client_request.endpoint
        key = client_request.key

        logger.debug('Adding ClientRequest to Queue: {} / {}'.format(endpoint, key))

        client_request.store(self._redis)
        try:
            self._queue[endpoint].append(key)
        except KeyError:
            self._queue[endpoint] = deque([key])

    def get(self, key):
        request = self._redis.get(key)
        request = msgpack.unpackb(request)
        return request

    def next(self, endpoint):
        try:
            key = self._queue[endpoint].popleft()
            return self.get(key)[b'content']
        except IndexError:
            return None
        except KeyError:
            return None


class WorkerQueue:
    def __init__(self, request_queue):
        self._request_queue = request_queue
        self._endpoints = {}
        self._waiting_workers = []

    def register(self, worker_id, endpoints):
        logger.debug('Registering worker {} to WorkerQueue for endpoints {}'.format(worker_id, endpoints))
        assert worker_id not in self._endpoints
        self._endpoints[worker_id] = endpoints
        self._waiting_workers.append(worker_id)

    def waits_for_next_item(self, worker_id):
        logger.debug('Worker {} is waiting for next request'.format(worker_id))
        if worker_id not in self._waiting_workers:
            self._waiting_workers.append(worker_id)

    def process(self):
        to_process = {}
        request = None
        for worker_id in self._waiting_workers[:]:
            for endpoint in self._endpoints[worker_id]:
                request = self._request_queue.next(endpoint)
                if request is not None:
                    break
            if request is not None:
                to_process[worker_id] = request
                self._waiting_workers.remove(worker_id)
        logger.debug('To process: {}'.format(to_process))
        return to_process


class Broker:
    def __init__(self,
                 worker_response_bind='tcp://127.0.0.1:5002',
                 worker_control_bind='tcp://127.0.0.1:5001',
                 client_bind='tcp://127.0.0.1:5000',
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

        request_queue = ClientRequestQueue(redis_host=self._redis_host,
                                           redis_port=self._redis_port,
                                           redis_db=self._redis_db)
        worker_queue = WorkerQueue(request_queue)

        loop = True
        while loop:
            logger.debug('Listening...')
            sockets = dict(poller.poll())

            # get a new client request
            if self._client_socket in sockets and sockets[self._client_socket] == zmq.POLLIN:
                message = self._client_socket.recv_multipart()
                client_request = ClientRequest(source=extract_source_from_message(message),
                                               content=extract_content_from_message(message))
                request_queue.add(client_request)

            # register endpoints of a worker or mark the worker as waiting
            if self._worker_control_socket in sockets and sockets[self._worker_control_socket] == zmq.POLLIN:
                message = self._worker_control_socket.recv_multipart()
                source = extract_source_from_message(message)
                content = extract_content_from_message(message)

                assert len(source) == 1
                worker_id = source[0]
                content = decode(msgpack.unpackb(content))

                if 'endpoints' in content:
                    worker_queue.register(worker_id, content['endpoints'])

                if 'w' in content and content['w']:
                    worker_queue.waits_for_next_item(worker_id)

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

                request = request_queue.get(ClientRequest.generate_key(response[b'id'].decode()))
                del response[b'id']

                client_response = request[b'source'] + [b''] + [msgpack.packb(response)]
                self._client_socket.send_multipart(client_response)

            # check if any requests can be processed
            to_process = worker_queue.process()

            # send requests to workers
            for worker_id, request in to_process.items():
                logger.debug('Sending to {}: {}'.format(worker_id, request))
                self._worker_control_socket.send_multipart([worker_id, b'', msgpack.packb(request)])
