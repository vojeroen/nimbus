from nimbus.worker import Worker

worker = Worker()


@worker.route(endpoint='endpoint2', methods=['GET'])
def def_for_endpoint(message):
    print(message.method)
    print(message.endpoint)
    print(message.parameters)
    print(message.data)
    return 'OK ' + worker.endpoint_for('def_for_endpoint') + ' ' + str(message.data['counter']), 200


worker.run()
