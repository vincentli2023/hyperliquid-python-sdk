class Error(Exception):
    pass


class ClientError(Error):
    def __init__(self, status_code, error_code, error_message, header, error_data=None):
        self.status_code = status_code
        self.error_code = error_code
        self.error_message = error_message
        self.header = header
        self.error_data = error_data


class ServerError(Error):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


class WebsocketPostError(Error):
    """A {"method": "post"} request could not be completed over the websocket (not ready, send failed,
    timed out, or the connection closed while waiting). On timeout the action's outcome is unknown."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message
