from nimbus.client import Client

client = Client()

for cnt in range(10):
    response = client.send_and_recv(method='GET',
                                    endpoint='endpoint2',
                                    parameters={'a': 1},
                                    data={'counter': cnt})
    print(response)
