from nimbus.publisher import Publisher

REDIS_HOST = '192.168.0.237'
REDIS_PORT = 6379
REDIS_DB = 0

publisher = Publisher(
    redis_host=REDIS_HOST,
    redis_port=REDIS_PORT,
    redis_db=REDIS_DB)
publisher.run()
