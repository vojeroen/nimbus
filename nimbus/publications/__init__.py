import msgpack
import zmq

from nimbus import config

worker_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'worker_pub_hostname'),
                                  config.cparser.get('zmq', 'worker_pub_port'))


# TODO WARNING: if project publisher is not listening, messages will get lost

def publish(topic, data):
    context = zmq.Context.instance()
    socket = context.socket(zmq.PUB)
    socket.connect(worker_url)
    socket.send(b'pub ' + msgpack.packb({'topic': topic, 'data': data}))
    socket.close()
