"""CSFS exception hierarchy."""


class CSFSError(Exception):
    """Base exception for all CSFS errors."""


class ConnectorError(CSFSError):
    """Raised when a data provider connector fails."""

    def __init__(self, provider: str, message: str) -> None:
        self.provider = provider
        super().__init__(f"[{provider}] {message}")


class RateLimitError(ConnectorError):
    """Raised when a provider rate-limits us."""


class DataFormatError(ConnectorError):
    """Raised when provider response doesn't match expected format."""


class StoreError(CSFSError):
    """Raised when the data store layer fails."""
