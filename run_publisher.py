from nimbus import config
from nimbus.publisher.publisher import Publisher

zmq_worker_url = 'tcp://{}:{}'.format(config.get('publisher', 'worker_hostname'),
                                      config.get('publisher', 'worker_port'))
zmq_client_url = 'tcp://{}:{}'.format(config.get('publisher', 'client_hostname'),
                                      config.get('publisher', 'client_port'))

redis_host = config.get('redis', 'host')
redis_port = config.get('redis', 'port')
redis_db = config.get('redis', 'db')

publisher = Publisher(worker_bind=zmq_worker_url,
                      client_bind=zmq_client_url,
                      redis_host=redis_host,
                      redis_port=redis_port,
                      redis_db=redis_db)
publisher.run()
