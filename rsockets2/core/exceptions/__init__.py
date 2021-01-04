

from rsockets2.core.frames.error import ErrorCodes, ErrorFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType


class RSocketError(BaseException):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ApplicationError(RSocketError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class CanceledError(RSocketError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RSocketProtocolError(RSocketError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def to_exception(data: bytes) -> RSocketError:
    if FrameHeader.stream_id_type_and_flags(data)[1] != FrameType.ERROR:
        raise RuntimeError('Passed data does not encode and ErrorFrame!')
    error_code = ErrorFrame.error_code(data)
    msg = ErrorFrame.error_data(data)
    if error_code == ErrorCodes.APPLICATION_ERROR:
        return ApplicationError(msg)
    elif error_code == ErrorCodes.CANCELED:
        return CanceledError(msg)
    else:
        return RSocketProtocolError(msg)
