import zmq

from nimbus import config


def run():
    zmq_context = zmq.Context.instance()

    zmq_worker_url = 'tcp://{}:{}'.format(config.cparser.get('crm', 'worker_hostname'),
                                          config.cparser.get('crm', 'worker_port'))
    zmq_api_url = 'tcp://{}:{}'.format(config.cparser.get('crm', 'api_hostname'),
                                       config.cparser.get('crm', 'api_port'))

    frontend = zmq_context.socket(zmq.XREP)
    frontend.bind(zmq_api_url)

    backend = zmq_context.socket(zmq.XREQ)
    backend.bind(zmq_worker_url)

    try:
        zmq.device(zmq.QUEUE, frontend, backend)

    except Exception as e:
        print(e)
        print("bringing down zmq device")
    finally:
        pass
        frontend.close()
        backend.close()
