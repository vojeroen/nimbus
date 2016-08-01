import datetime
import inspect

import pytz
from sqlalchemy.orm.session import Session as SessionClass

from nimbus import errors

message_routes = []


def assert_key_present(key, message):
    if key not in message.keys():
        raise errors.MessageNotComplete, 'The message must contain the key "{key_name}"'.format(key_name=key)


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

        self._route = find_route(message['route'])
        self._payload = message['payload']
        self._received_timestamp = pytz.utc.localize(datetime.datetime.utcnow())
        self._session = session
        self._processing_started = False

    @property
    def payload(self):
        return self._payload

    @property
    def session(self):
        return self._session

    def process(self):
        if not self._processing_started:
            self._processing_started = True
            response = self._route.action(self)
            return response
        else:
            raise errors.MessageAlreadyProcessed, 'Message has already been processed'
