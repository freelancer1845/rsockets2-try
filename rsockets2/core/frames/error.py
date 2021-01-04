from enum import IntEnum

from .frame_header import FrameHeader
from .segmented_frame import FrameSegments


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


class ErrorFrame(FrameHeader):

    @staticmethod
    def is_ignore_if_not_understood(buffer: bytes) -> bool:
        raise NotImplementedError('Error frames cannot be ignored')

    @staticmethod
    def is_metdata_present(buffer: bytes) -> bool:
        raise NotImplementedError('Error frames have no metdata')

    @staticmethod
    def error_code(buffer: bytes) -> ErrorCodes:
        return ErrorCodes(ErrorFrame._decode_integer(buffer, 6)[0])

    @staticmethod
    def error_data(buffer: bytes) -> str:
        return buffer[10:].decode('UTF-8')

    @staticmethod
    def _set_error_code(buffer: bytearray, error_code: ErrorCodes):
        ErrorFrame._encode_integer(buffer, 6, error_code)

    @staticmethod
    def create_new(stream_id: int, error_code: ErrorCodes, error_data: str) -> FrameSegments:
        buffer = bytearray(6 + 4)
        ErrorFrame.encode_frame_header(buffer, stream_id, 0x0B, False, False)
        ErrorFrame._set_error_code(buffer, error_code)
        frame = FrameSegments()
        frame.append_fragment(buffer)
        frame.append_fragment(error_data.encode('UTF-8'))
        return frame
