from nimbus.worker.context import ctx_request
from nimbus.worker.worker import Worker

worker = Worker(connect_control='tcp://127.0.0.1:5001',
                connect_response='tcp://127.0.0.1:5002')


@ctx_request.route(endpoint='endpoint2', methods=['GET'])
def def_for_endpoint(message):
    print(message.method)
    print(message.endpoint)
    print(message.parameters)
    print(message.data)
    return 'OK ' + ctx_request.endpoint_for('def_for_endpoint') + ' ' + str(message.data[b'counter']), 200


worker.run()
