from .common import FrameType
import struct
from enum import IntEnum


class ErrorCodes(IntEnum):
    RESERVED = 0x00000000
    INVALID_SETUP = 0x00000001
    UNSUPPORTED_SETUP = 0x00000002
    REJECTED_SETUP = 0x00000003
    REJECTED_RESUME = 0x00000004
    CONNECTION_ERROR = 0x00000101
    CONNECTION_CLOSE = 0x00000102
    APPLICATION_ERROR = 0x00000201
    REJECTED = 0x00000202
    CANCELED = 0x00000203
    INVALID = 0x00000204
    UNKNOWN_ERROR = 0xEFFFFFFF
    RESERVED_2 = 0xFFFFFFFF


class ErrorFrame(object):

    def __init__(self):
        super().__init__()

        self.error_code = ErrorCodes.RESERVED
        self.error_data = bytes(0)
        self.stream_id = 0

    @staticmethod
    def from_data(stream_id: int, flags: int, full_data: bytes):
        frame = ErrorFrame()

        data_read = 6
        frame.stream_id = stream_id
        try:
            value, = struct.unpack_from(">I", full_data, data_read)
            frame.error_code = ErrorCodes(value)
        except ValueError:
            frame.error_code = ErrorCodes.UNKNOWN_ERROR
        data_read += 4
        frame.error_data = full_data[data_read:].decode('UTF-8')
        return frame

    def to_bytes(self):
        raise NotImplementedError()
