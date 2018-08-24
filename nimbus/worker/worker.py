import sys
import traceback

import msgpack
import zmq
from requests import codes

from nimbus import config
from nimbus.helpers import extract_content_from_message, decode
from nimbus.log import get_logger
from nimbus.statemanager import ConnectionStateManager
from nimbus.worker.context import ctx_request
from nimbus.worker.errors import RequestError
from nimbus.worker.request import Request

logger = get_logger(__name__)

SECONDS_BEFORE_CONTACT_CHECK = int(config.get('control', 'seconds_before_contact_check'))
SECONDS_BEFORE_DISCONNECT = int(config.get('control', 'seconds_before_disconnect'))


class BrokerStateManager:
    def __init__(self, seconds_before_contact_check, seconds_before_disconnect):
        self._manager = ConnectionStateManager(seconds_before_contact_check, seconds_before_disconnect)
        self._broker = b'broker'

    def contact_from_broker(self):
        self._manager.contact_from(self._broker)

    def ping_broker(self):
        return self._broker in self._manager.get_connections_to_ping()

    def disconnect_broker(self):
        return self._broker in self._manager.get_connections_to_disconnect()


class Worker:
    def __init__(self,
                 connect_control,
                 connect_response):
        self._url_connect_control = connect_control
        self._url_connect_response = connect_response

    def close(self):
        self._socket_control.close()
        self._socket_response.close()

    def _connect(self):
        self._context = zmq.Context.instance()

        logger.info('Connecting to broker control socket on {}'.format(self._url_connect_control))
        self._socket_control = self._context.socket(zmq.DEALER)
        self._socket_control.connect(self._url_connect_control)

        logger.info('Connecting to broker response socket on {}'.format(self._url_connect_response))
        self._socket_response = self._context.socket(zmq.REQ)
        self._socket_response.connect(self._url_connect_response)

    def run(self):
        self._connect()

        poller = zmq.Poller()
        poller.register(self._socket_control, zmq.POLLIN)

        logger.info('Starting with endpoints {}'.format(ctx_request.endpoints))

        state_manager = BrokerStateManager(seconds_before_contact_check=SECONDS_BEFORE_CONTACT_CHECK,
                                           seconds_before_disconnect=SECONDS_BEFORE_DISCONNECT)

        loop = True
        init_connection = True

        poller_timeout = max([int(min([SECONDS_BEFORE_CONTACT_CHECK,
                                       SECONDS_BEFORE_DISCONNECT]) / 10.0 * 1000),
                              500])

        while loop:
            if init_connection:
                self._socket_control.send_multipart([b'', msgpack.packb({'endpoints': ctx_request.endpoints,
                                                                         'w': True})])
                init_connection = False

            sockets = dict(poller.poll(poller_timeout))

            # if sockets:
            #     logger.debug('Received: {}'.format(sockets))

            if self._socket_control in sockets and sockets[self._socket_control] == zmq.POLLIN:
                # get message content
                content = extract_content_from_message(self._socket_control.recv_multipart())
                content = msgpack.unpackb(content)

                state_manager.contact_from_broker()

                # process control messages
                if b'control' in content:
                    control = decode(content[b'control'])
                    if control == 'ping':
                        # logger.debug('Received ping from broker')
                        self._socket_control.send_multipart([b'', msgpack.packb({'pong': True})])
                    elif control == 'pong':
                        # logger.debug('Received pong from broker')
                        pass
                    elif control == 'kick':
                        init_connection = True


                # process client request messages
                else:
                    message = Request(content)
                    self._socket_control.send_multipart([b'', msgpack.packb({'r': message.id})])
                    try:
                        logger.info('Received: {} {}'.format(message.method, message.endpoint))
                        service = ctx_request.get_service_by_endpoint(message.endpoint, message.method)
                        response = service(message)
                        if isinstance(response, tuple):
                            response, status = response
                        else:
                            status = codes.OK
                    except RequestError as e:
                        response = str(e)
                        status = e.status_code
                    except:
                        logger.error(sys.exc_info()[0])
                        logger.error(sys.exc_info()[1])
                        logger.error(traceback.extract_tb(sys.exc_info()[2]))
                        response = {}
                        status = codes.SERVER_ERROR
                    self._socket_response.send(msgpack.packb({'id': message.id,
                                                              'status': status,
                                                              'response': response}))
                    self._socket_control.send_multipart([b'', msgpack.packb({'w': True})])
                    self._socket_response.recv()

            if state_manager.ping_broker():
                # logger.debug('Pinging broker')
                self._socket_control.send_multipart([b'', msgpack.packb({'ping': True})])

            if state_manager.disconnect_broker():
                logger.info('Disconnecting from broker')
                self._socket_control.send_multipart([b'', msgpack.packb({'disconnect': True})])
                raise RuntimeError
