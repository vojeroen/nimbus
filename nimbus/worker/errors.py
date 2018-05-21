from requests import codes


class EndpointNotFound(Exception):
    pass


class MessageAlreadyProcessed(Exception):
    pass


class MessageNotComplete(Exception):
    pass


class MessageNotCorrect(Exception):
    pass


class DataNotComplete(Exception):
    pass


class DataNotCorrect(Exception):
    pass


class DataTypeNotCorrect(Exception):
    pass


class InstanceExists(Exception):
    pass


class UnknownError(Exception):
    pass


class WrongDataTypeForField(Exception):
    pass


class EndpointDoesNotExist(LookupError):
    pass


# external errors
class RequestError(LookupError):
    status_code = codes.SERVER_ERROR


class ObjectDoesNotExist(RequestError):
    status_code = codes.NOT_FOUND


class MultipleObjectsFound(RequestError):
    pass


class MissingParameter(RequestError):
    status_code = codes.BAD_REQUEST


class WrongEndpoint(RequestError):
    status_code = codes.SERVER_ERROR


class WrongMethod(RequestError):
    status_code = codes.SERVER_ERROR
