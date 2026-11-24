from __future__ import annotations


class ConnectorError(Exception):
    pass


class ValidationError(ConnectorError):
    pass


class ObservationError(ConnectorError):
    pass


class TransportError(ConnectorError):
    pass


class ActionError(ConnectorError):
    pass