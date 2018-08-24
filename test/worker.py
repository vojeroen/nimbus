import random
import unittest

import msgpack

from nimbus.worker.context import RequestContext
from nimbus.worker.errors import EndpointDoesNotExist
from nimbus.worker.request import Request

ENDPOINT = 'endpoint'
METHODS = ['GET', 'POST']

REQUEST_CONTENT1 = {'id': 'request_id',
                    'method': 'GET',
                    'endpoint': 'endpoint',
                    'data': b'\x99'}


class TestContext(unittest.TestCase):
    def setUp(self):
        self.context = RequestContext()
        self.request = Request(msgpack.packb(REQUEST_CONTENT1))

    def decorate(self):
        def func_to_decorate(request, a_, b_):
            return a_ + b_

        decorated_func = self.context.route(ENDPOINT, METHODS)(func_to_decorate)

        return func_to_decorate, decorated_func

    def random(self):
        return random.randint(0, 100), random.randint(0, 100)

    def test_decorator(self):
        func_to_decorate, decorated_func = self.decorate()
        a, b = self.random()

        self.assertEqual(func_to_decorate(self.request, a, b),
                         decorated_func(self.request, a, b))

    def test_endpoints(self):
        self.decorate()

        self.assertEqual([ENDPOINT],
                         self.context.endpoints)

    def test_endpoint_for(self):
        self.decorate()

        self.assertEqual(ENDPOINT,
                         self.context.endpoint_for('func_to_decorate'))

        with self.assertRaises(EndpointDoesNotExist):
            self.context.endpoint_for('decorated_func')

    def test_endpoint_for_custom_label(self):
        self.context.route(ENDPOINT, METHODS, label='func_label')(lambda: 0)

        self.assertEqual(ENDPOINT,
                         self.context.endpoint_for('func_label'))

    def test_get_service_by_endpoint(self):
        func_to_decorate, decorated_func = self.decorate()
        a, b = self.random()

        for method in METHODS:
            self.assertEqual(func_to_decorate(self.request, a, b),
                             self.context.get_service_by_endpoint(ENDPOINT, method)(self.request, a, b))
