from .segmented_frame import FrameSegments
from .frame_header import FrameHeader, FrameType


class RequestNFrame(FrameHeader):

    @staticmethod
    def request_n(buffer: bytes) -> int:
        return FrameHeader._decode_integer(buffer, 6)[0]

    @staticmethod
    def _set_request_n(buffer: bytes, n: int):
        FrameHeader._encode_integer(buffer, 6, n)

    @staticmethod
    def create_new(stream_id: int, request_n: int) -> FrameSegments:
        header = bytearray(10)
        RequestNFrame.encode_frame_header(
            header, stream_id, FrameType.REQUEST_N, False, False)
        RequestNFrame._set_request_n(header, request_n)
        frame = FrameSegments()
        frame.append_fragment(header)
        return frame
