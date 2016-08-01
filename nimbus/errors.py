class RouteNotFound(Exception):
    pass


class RouteAlreadyExists(Exception):
    pass


class MessageAlreadyProcessed(Exception):
    pass


class MessageNotComplete(Exception):
    pass


class PayloadNotComplete(Exception):
    pass


class PayloadNotCorrect(Exception):
    pass


class InstanceExists(Exception):
    pass


class UnknownError(Exception):
    pass
