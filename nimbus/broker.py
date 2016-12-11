import zmq

from nimbus.settings import ZMQ_API_URL, ZMQ_WORKER_URL


def run():
    zmq_context = zmq.Context.instance()

    frontend = zmq_context.socket(zmq.XREP)
    frontend.bind(ZMQ_API_URL)

    backend = zmq_context.socket(zmq.XREQ)
    backend.bind(ZMQ_WORKER_URL)

    try:
        zmq.device(zmq.QUEUE, frontend, backend)

    except Exception as e:
        print(e)
        print("bringing down zmq device")
    finally:
        pass
        frontend.close()
        backend.close()
