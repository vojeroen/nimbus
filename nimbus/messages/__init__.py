import datetime
import inspect

import pytz
from sqlalchemy.orm.session import Session as SessionClass

from nimbus import errors
from nimbus.serializers import Serializer

message_routes = []


def assert_key_present(key, message):
    if key not in message.keys():
        raise errors.MessageNotComplete, 'The message must contain the key "{key_name}"'.format(key_name=key)


def assert_correct_type(data, data_type):
    if not isinstance(data, data_type):
        raise errors.MessageNotCorrect, 'The message contains incorrect data types'


def add_route(route_id, new_route):
    for route in message_routes:
        if route.id == route_id:
            raise errors.RouteAlreadyExists, 'Route with id {route_id} exists already'.format(route_id=route_id)
    message_routes.append(new_route)


def find_route(message_route):
    for route in message_routes:
        if route.id == message_route:
            return route
    raise errors.RouteNotFound, 'Requested route {route_id} not found'.format(route_id=message_route)


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

        assert_key_present('route', message)
        assert_key_present('payload', message)
        assert_correct_type(message['route'], str)
        assert_correct_type(message['payload'], dict)

        self._route = find_route(message['route'])
        self._payload = message['payload']
        self._received_timestamp = pytz.utc.localize(datetime.datetime.utcnow())
        self._session = session
        self._processing_started = False

        if 'select' in message.keys():
            assert_correct_type(message['select'], list)
            self._select = message['select']
        else:
            self._select = None

    @property
    def payload(self):
        return self._payload

    @property
    def session(self):
        return self._session

    @property
    def select(self):
        return self._select

    def process(self):
        if not self._processing_started:
            self._processing_started = True
            response = self._route.action(self)
            return response
        else:
            raise errors.MessageAlreadyProcessed, 'Message has already been processed'

    def _generate_response_from_one_item(self, serializer):
        assert isinstance(serializer, Serializer), 'The function argument must be a Serializer instance'

        serialized_data = serializer.serialized_data

        if self.select:
            for key in self._select:
                assert key in serialized_data.keys(), 'The select statement contains keys that are not in the model'
            return {key: serialized_data[key] for key in self.select}
        else:
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
