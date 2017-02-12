import msgpack
import zmq

from nimbus import config

logger = config.get_logger(__name__)


def run():
    context = zmq.Context.instance()

    worker_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'worker_pub_hostname'),
                                      config.cparser.get('zmq', 'worker_pub_port'))
    api_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'api_pub_hostname'),
                                   config.cparser.get('zmq', 'api_pub_port'))

    worker = context.socket(zmq.ROUTER)
    worker.bind(worker_url)

    api = context.socket(zmq.PUB)
    api.bind(api_url)

    poller = zmq.Poller()
    poller.register(worker, zmq.POLLIN)

    logger.debug('Publisher worker on {}'.format(worker_url))
    logger.debug('Publisher API on {}'.format(api_url))

    while True:
        sockets = dict(poller.poll())
        logger.info('Received data')

        if worker in sockets.keys() and sockets[worker] == zmq.POLLIN:
            worker_id, empty, packed_data = worker.recv_multipart()
            worker.send_multipart([worker_id, empty, msgpack.packb({'message': 'ok'})])
            data = msgpack.unpackb(packed_data)
            logger.debug('Publishing data: {}'.format(data))

            pub_topic = data[b'topic']
            pub_data = data[b'data']

            api.send(pub_topic + b' ' + msgpack.packb(pub_data))
