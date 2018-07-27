from nimbus.worker.context import ctx_request
from nimbus.worker.crypto import WorkerSecurityManager
from nimbus.worker.worker import Worker

worker = Worker(
    connect_control='tcp://127.0.0.1:5001',
    connect_response='tcp://127.0.0.1:5002',
    identity='Worker-1',
    security_manager=WorkerSecurityManager(
        connection_secret_key='keys/connection/worker/worker.key_secret',
        connection_broker_public_key='keys/connection/broker/broker.key',
        message_secret_key='keys/message/worker/secret.pem',
        message_public_keys='keys/message/worker/public',
    )
)


@ctx_request.route(endpoint='endpoint2', methods=['GET'])
def def_for_endpoint(message):
    print(message.method)
    print(message.endpoint)
    print(message.parameters)
    print(message.data)
    return 'OK ' + ctx_request.endpoint_for('def_for_endpoint') + ' ' + str(message.data[b'counter']), 200


worker.run()
