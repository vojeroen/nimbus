from nimbus import config
from nimbus.broker import Broker

zmq_worker_response_url = 'tcp://{}:{}'.format(config.get('requests', 'worker_response_hostname'),
                                               config.get('requests', 'worker_response_port'))
zmq_worker_control_url = 'tcp://{}:{}'.format(config.get('requests', 'worker_control_hostname'),
                                              config.get('requests', 'worker_control_port'))
zmq_client_url = 'tcp://{}:{}'.format(config.get('requests', 'client_hostname'),
                                      config.get('requests', 'client_port'))

redis_host = config.get('redis', 'host')
redis_port = config.get('redis', 'port')
redis_db = config.get('redis', 'db')

broker = Broker(worker_response_bind=zmq_worker_response_url,
                worker_control_bind=zmq_worker_control_url,
                client_bind=zmq_client_url,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db)
broker.run()
