import sys
import traceback

import zmq

from nimbus.log import get_logger
from nimbus.subscriber.context import ctx_subscriber
from nimbus.subscriber.publication import Publication

logger = get_logger(__name__)


class Subscriber:
    def __init__(self, publisher):
        self._url_publisher = publisher

    def close(self):
        self._socket_publisher.close()

    def _connect(self):
        self._context = zmq.Context.instance()

        logger.debug('Connecting to publisher socket on {}'.format(self._url_publisher))
        self._socket_publisher = self._context.socket(zmq.SUB)
        self._socket_publisher.connect(self._url_publisher)

    def subscribe(self, topic):
        if '_socket_publisher' not in dir(self):
            self._connect()
        self._socket_publisher.setsockopt_string(zmq.SUBSCRIBE, topic)

    def run(self):
        self._connect()

        poller = zmq.Poller()
        poller.register(self._socket_publisher, zmq.POLLIN)

        for topic in ctx_subscriber.topics:
            self.subscribe(topic)

        logger.debug('Starting with topics {}'.format(ctx_subscriber.topics))

        loop = True
        while loop:
            logger.debug('Listening...')
            sockets = dict(poller.poll())
            logger.debug('Received: {}'.format(sockets))

            if self._socket_publisher in sockets and sockets[self._socket_publisher] == zmq.POLLIN:
                message = self._socket_publisher.recv()
                message = message[message.find(b' ') + 1:]  # filter out the topic and the space
                message = Publication(message)
                try:
                    logger.debug('Received: {}'.format(message.topic))
                    service = ctx_subscriber.get_service_by_topic(message.topic)
                    service(message)
                except:
                    logger.error(sys.exc_info()[0])
                    logger.error(sys.exc_info()[1])
                    logger.error(traceback.extract_tb(sys.exc_info()[2]))
