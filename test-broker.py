from nimbus.broker import Broker

REDIS_HOST = '192.168.0.237'
REDIS_PORT = 6379
REDIS_DB = 0

broker = Broker(
    worker_response_bind='tcp://127.0.0.1:5002',
    worker_control_bind='tcp://127.0.0.1:5001',
    client_bind='tcp://127.0.0.1:5003',
    redis_host=REDIS_HOST,
    redis_port=REDIS_PORT,
    redis_db=REDIS_DB)
broker.run()
