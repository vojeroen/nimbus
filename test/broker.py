import copy
import unittest
from collections import namedtuple

import msgpack
from redis import StrictRedis

from nimbus.broker import ClientRequest, RequestQueue, EmptyQueue, QueueManager, RequestManager

REDIS_HOST = '192.168.0.237'
REDIS_PORT = 6379
REDIS_DB = 0

METHOD = 'GET'
ENDPOINT1 = 'account'
SOURCE = [b'source']
REQUEST_CONTENT1 = {b'method': METHOD.encode(),
                    b'endpoint': ENDPOINT1.encode(),
                    b'data': b'\x99'}

ENDPOINT2 = 'invoice'
REQUEST_CONTENT2 = {b'method': METHOD.encode(),
                    b'endpoint': ENDPOINT2.encode(),
                    b'data': b'\x98'}

REQUEST_CONTENT3 = {b'method': METHOD.encode(),
                    b'endpoint': ENDPOINT2.encode(),
                    b'data': b'\x97'}

Worker = namedtuple('Worker', 'id endpoints')
WORKER1 = Worker('w1', [ENDPOINT1])
WORKER2 = Worker('w2', [ENDPOINT1, ENDPOINT2])


class TestClientRequest(unittest.TestCase):
    def setUp(self):
        self.client_request = ClientRequest(SOURCE,
                                            msgpack.packb(REQUEST_CONTENT1))
        self.request_content = copy.deepcopy(REQUEST_CONTENT1)

    def test_method(self):
        self.assertEqual(METHOD,
                         self.client_request.method)

    def test_endpoint(self):
        self.assertEqual(ENDPOINT1,
                         self.client_request.endpoint)

    def test_content(self):
        self.request_content.update({b'id': self.client_request.id.encode()})
        self.assertEqual(self.request_content,
                         self.client_request.content)

    def test_packed_data(self):
        self.request_content.update({b'id': self.client_request.id.encode()})
        self.assertEqual(self.client_request.packed_data,
                         msgpack.packb(self.request_content))

    def test_cached_data(self):
        self.request_content.update({b'id': self.client_request.id.encode()})
        self.assertEqual(msgpack.packb({'source': SOURCE,
                                        'content': self.request_content}),
                         self.client_request.cached_data)

    def test_equal(self):
        client_request_eq = ClientRequest(SOURCE,
                                          msgpack.packb(REQUEST_CONTENT1))
        client_request_eq._id = self.client_request.id

        client_request_neq = ClientRequest(SOURCE,
                                           msgpack.packb(REQUEST_CONTENT1))

        self.assertTrue(self.client_request == client_request_eq)
        self.assertTrue(self.client_request != client_request_neq)

    def test_fromcache(self):
        self.request_content.update({b'id': self.client_request.id.encode()})
        self.assertEqual(ClientRequest.fromcache(msgpack.packb({'source': SOURCE,
                                                                'content': self.request_content})),
                         self.client_request)


class TestRequestQueue(unittest.TestCase):

    def setUp(self):
        self.redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.redis.flushdb()

        self.client_request = ClientRequest(SOURCE,
                                            msgpack.packb(REQUEST_CONTENT1))
        self.request_queue = RequestQueue(redis_host=REDIS_HOST,
                                          redis_port=REDIS_PORT,
                                          redis_db=REDIS_DB)

        self.request_queue.append(self.client_request)

    def test_generate_keys(self):
        self.assertEqual('broker:' + self.request_queue.id + ':request:content:' + self.client_request.id,
                         self.request_queue.generate_key_content(self.client_request.id))
        self.assertEqual('broker:' + self.request_queue.id + ':request:status:' + self.client_request.id,
                         self.request_queue.generate_key_status(self.client_request.id))
        self.assertEqual('broker:' + self.request_queue.id + ':request:timestamp:' + self.client_request.id,
                         self.request_queue.generate_key_timestamp(self.client_request.id))

    def test_add_client_request(self):
        self.assertEqual(len(self.redis.keys('*')), 3)
        self.assertEqual(self.redis.get(self.request_queue.generate_key_content(self.client_request.id)),
                         self.client_request.cached_data)
        self.assertEqual(self.redis.get(self.request_queue.generate_key_status(self.client_request.id)).decode(),
                         RequestQueue.STATUS_WAITING)
        # TODO how to test timestamp storage? 

    def test_contains(self):
        self.assertTrue(self.client_request.id in self.request_queue)
        self.assertFalse('random_id' in self.request_queue)

    def test_setitem_getitem(self):
        ID = 'id_unrelated_to_client_request'
        self.request_queue[ID] = self.client_request
        self.assertTrue(ID in self.request_queue)
        self.assertEqual(self.client_request,
                         self.request_queue[ID])
        self.assertEqual(2, len(self.request_queue))
        self.assertEqual(6, len(self.redis.keys('*')))

    def test_delitem(self):
        ID = 'id_unrelated_to_client_request'
        self.request_queue[ID] = self.client_request
        del self.request_queue[ID]
        del self.request_queue[self.client_request.id]
        self.assertEqual(0, len(self.request_queue))
        self.assertEqual(0, len(self.redis.keys('*')))

    def test_iter(self):
        client_request2 = ClientRequest(source=SOURCE,
                                        content=msgpack.packb(REQUEST_CONTENT1))
        self.request_queue.append(client_request2)
        self.assertEqual([self.client_request, client_request2],
                         [r for r in self.request_queue])

    def test_popitem(self):
        cr = self.request_queue.popitem()
        self.assertEqual(cr,
                         self.client_request)
        self.assertEqual(self.redis.get(self.request_queue.generate_key_status(self.client_request.id)).decode(),
                         RequestQueue.STATUS_PROCESSING)
        self.assertEqual(self.client_request,
                         self.request_queue[cr.id])

        with self.assertRaises(EmptyQueue):
            self.request_queue.popitem()

    def test_peek(self):
        self.assertEqual(self.client_request.id,
                         self.request_queue.peek().id)


class TestQueueManager(unittest.TestCase):
    def setUp(self):
        self.redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.redis.flushdb()

        self.client_request1 = ClientRequest(SOURCE,
                                             msgpack.packb(REQUEST_CONTENT1))
        self.client_request2 = ClientRequest(SOURCE,
                                             msgpack.packb(REQUEST_CONTENT2))
        self.client_request3 = ClientRequest(SOURCE,
                                             msgpack.packb(REQUEST_CONTENT3))
        self.queue_manager = QueueManager(redis_host=REDIS_HOST,
                                          redis_port=REDIS_PORT,
                                          redis_db=REDIS_DB)

    def append_requests(self):
        self.queue_manager.append(self.client_request1)
        self.queue_manager.append(self.client_request2)
        self.queue_manager.append(self.client_request3)

    def test_load_redis(self):
        with self.assertRaises(NotImplementedError):
            self.queue_manager.load_redis()

    def test_append_and_retrieve(self):
        self.append_requests()
        self.assertEqual(self.client_request1,
                         self.queue_manager.retrieve(self.client_request1.id))
        self.assertEqual(self.client_request2,
                         self.queue_manager.retrieve(self.client_request2.id))
        self.assertEqual(self.client_request3,
                         self.queue_manager.retrieve(self.client_request3.id))

        with self.assertRaises(KeyError):
            self.queue_manager.retrieve('unknown_id')

    def test_remove(self):
        self.append_requests()
        self.assertIs(None,
                      self.queue_manager.remove(self.client_request3.id))
        self.assertIs(None,
                      self.queue_manager.remove(self.client_request2.id))
        self.assertIs(None,
                      self.queue_manager.remove(self.client_request1.id))

        self.assertEqual(0,
                         len(self.queue_manager))

        with self.assertRaises(KeyError):
            self.queue_manager.remove('unknown_id')

    def test_get_queue(self):
        self.append_requests()
        for cr in [self.client_request1, self.client_request2, self.client_request3]:
            self.assertIsInstance(self.queue_manager.get_queue(cr.endpoint),
                                  RequestQueue)
            self.assertTrue(cr.id in self.queue_manager.get_queue(cr.endpoint))

    def test_select_queue_order1(self):
        self.queue_manager.append(self.client_request1)
        self.queue_manager.append(self.client_request2)
        self.queue_manager.append(self.client_request3)
        self.assertEqual(self.client_request1,
                         self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2]).popitem())
        self.assertEqual(self.client_request2,
                         self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2]).popitem())
        self.assertEqual(self.client_request3,
                         self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2]).popitem())
        with self.assertRaises(EmptyQueue):
            self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2])

    def test_select_queue_order2(self):
        self.queue_manager.append(self.client_request3)
        self.queue_manager.append(self.client_request2)
        self.queue_manager.append(self.client_request1)
        self.assertEqual(self.client_request3,
                         self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2]).popitem())
        self.assertEqual(self.client_request2,
                         self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2]).popitem())
        self.assertEqual(self.client_request1,
                         self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2]).popitem())
        with self.assertRaises(EmptyQueue):
            self.queue_manager.select_queue([ENDPOINT1, ENDPOINT2])

    def test_len(self):
        self.append_requests()
        self.assertEqual(3,
                         len(self.queue_manager))


class TestRequestManager(unittest.TestCase):
    def setUp(self):
        self.redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.redis.flushdb()

        self.client_request1 = ClientRequest(SOURCE,
                                             msgpack.packb(REQUEST_CONTENT1))
        self.client_request2 = ClientRequest(SOURCE,
                                             msgpack.packb(REQUEST_CONTENT2))
        self.client_request3 = ClientRequest(SOURCE,
                                             msgpack.packb(REQUEST_CONTENT3))
        self.request_manager = RequestManager(redis_host=REDIS_HOST,
                                              redis_port=REDIS_PORT,
                                              redis_db=REDIS_DB)

    def append_requests(self):
        self.request_manager.append(self.client_request1)
        self.request_manager.append(self.client_request2)
        self.request_manager.append(self.client_request3)

    def register_workers(self):
        self.request_manager.register(WORKER1.id, WORKER1.endpoints)
        self.request_manager.register(WORKER2.id, WORKER2.endpoints)

    def test_len(self):
        self.append_requests()
        self.assertEqual(3,
                         len(self.request_manager))

    def test_getitem(self):
        self.append_requests()
        for cr in [self.client_request1, self.client_request2, self.client_request3]:
            self.assertEqual(cr,
                             self.request_manager[cr.id])

    def test_delitem(self):
        self.append_requests()
        for cr in [self.client_request1, self.client_request2, self.client_request3]:
            del self.request_manager[cr.id]
        self.assertEqual(0,
                         len(self.request_manager))

    def test_call(self):
        def remove_requests():
            for cr in [self.client_request1, self.client_request2, self.client_request3]:
                try:
                    del self.request_manager[cr.id]
                except KeyError:
                    pass

        self.append_requests()
        self.assertEqual(0,
                         len(self.request_manager()))
        remove_requests()

        self.append_requests()
        self.register_workers()
