from nimbus.broker import Broker

REDIS_HOST = '192.168.0.237'
REDIS_PORT = 6379
REDIS_DB = 0

broker = Broker(
    redis_host=REDIS_HOST,
    redis_port=REDIS_PORT,
    redis_db=REDIS_DB)
broker.run()
