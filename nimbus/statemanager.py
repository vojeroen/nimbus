from nimbus.helpers.timestamp import get_utc_int


class ConnectionStateManager:
    """
    Keep track of the state of every worker.
    """

    def __init__(self, seconds_before_contact_check, seconds_before_disconnect):
        self._last_contact = dict()
        self._checking_connection = dict()
        self._seconds_before_contact_check = seconds_before_contact_check
        self._seconds_before_disconnect = seconds_before_disconnect

    def contact_from(self, connection_id):
        try:
            del self._checking_connection[connection_id]
        except KeyError:
            pass
        self._last_contact[connection_id] = get_utc_int()

    def disconnect(self, connection_id):
        try:
            del self._checking_connection[connection_id]
        except KeyError:
            pass
        try:
            del self._last_contact[connection_id]
        except KeyError:
            pass

    def get_connections_to_ping(self):
        connection_ids = [connection_id
                          for connection_id, last_contact in self._last_contact.items()
                          if get_utc_int() - last_contact > self._seconds_before_contact_check
                          and connection_id not in self._checking_connection]
        for connection_id in connection_ids:
            del self._last_contact[connection_id]
            self._checking_connection[connection_id] = get_utc_int()
        return connection_ids

    def get_connections_to_disconnect(self):
        connection_ids = [connection_id
                          for connection_id, check_time in self._checking_connection.items()
                          if get_utc_int() - check_time > self._seconds_before_disconnect]
        for connection_id in connection_ids:
            del self._checking_connection[connection_id]
        return connection_ids
