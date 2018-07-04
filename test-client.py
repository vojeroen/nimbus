from nimbus.client import Client

client = Client(connect='tcp://127.0.0.1:5003')

for cnt in range(10):
    response = client.send_and_recv(method='GET',
                                    endpoint='endpoint2',
                                    parameters={'a': 1},
                                    data={'counter': cnt})
    print(response)
