import datetime
import inspect

import pytz
from sqlalchemy.orm.session import Session as SessionClass

from nimbus import errors
from nimbus.serializers import Serializer

message_routes = []


def assert_key_present(key, message):
    if key not in message.keys():
        raise errors.MessageNotComplete('The message must contain the key "{key_name}"'.format(key_name=key))


def assert_correct_type(data, data_type):
    if not isinstance(data, data_type):
        raise errors.MessageNotCorrect('The message contains incorrect data types')


def assert_correct_key(data, key, data_type):
    assert_key_present(key, data)
    assert_correct_type(data[key], data_type)


def add_route(route_id, new_route):
    for route in message_routes:
        if route.id == route_id:
            raise errors.RouteAlreadyExists('Route with id {route_id} exists already'.format(route_id=route_id))
    message_routes.append(new_route)


def find_route(message_route):
    for route in message_routes:
        if route.id == message_route:
            return route
    raise errors.RouteNotFound('Requested route {route_id} not found'.format(route_id=message_route))


class Route:
    def __init__(self, id, action, name=''):
        assert isinstance(id, str), 'Route ID must be a string'
        assert inspect.isfunction(action) or inspect.ismethod(action), 'Route action must be a function or a method'
        assert isinstance(name, str), 'Route name must be a string'

        self._id = id
        self._action = action
        if name:
            self._name = name
        else:
            self._name = self._id.replace('/', '-')

        add_route(self._id, self)

    @property
    def id(self):
        return self._id

    @property
    def action(self):
        return self._action


class Message:
    def __init__(self, message, session):
        assert isinstance(message, dict)
        assert isinstance(session, SessionClass)

        assert_correct_key(message, b'route', bytes)

        self._route = find_route(message[b'route'].decode())
        self._received_timestamp = pytz.utc.localize(datetime.datetime.utcnow())
        self._session = session
        self._payload = {}
        self._parameters = {}
        self._processing_started = False

        if b'payload' in message.keys():
            assert_correct_key(message, b'payload', dict)
            self._payload = message[b'payload']

        if b'parameters' in message.keys():
            assert_correct_key(message, b'parameters', dict)
            for key, value in message[b'parameters'].items():
                assert isinstance(key, bytes)
                assert isinstance(value, bytes)
                self._parameters[key.decode()] = value.decode()

    @property
    def payload(self):
        return self._payload

    @property
    def session(self):
        return self._session

    @property
    def parameters(self):
        return self._parameters

    def process(self):
        if not self._processing_started:
            self._processing_started = True
            response = self._route.action(self)
            return response
        else:
            raise errors.MessageAlreadyProcessed('Message has already been processed')

    def _generate_response_from_one_item(self, serializer):
        assert isinstance(serializer, Serializer), 'The function argument must be a Serializer instance'
        serialized_data = serializer.serialized_data
        return serialized_data

    def generate_response(self, serializer):
        if isinstance(serializer, Serializer):
            response = self._generate_response_from_one_item(serializer)
        elif isinstance(serializer, list):
            response = []
            for s in serializer:
                response.append(self._generate_response_from_one_item(s))
        else:
            response = None

        return response
