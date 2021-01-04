

from .segmented_frame import FrameSegments
from typing import Optional
from .frame_header import FrameHeader


class LeaseFrame(FrameHeader):

    @staticmethod
    def time_to_live(buffer: bytes) -> int:
        return LeaseFrame._decode_integer(buffer, 6)[0]

    @staticmethod
    def number_of_requests(buffer: bytes) -> int:
        return LeaseFrame._decode_integer(buffer, 10)[0]

    @staticmethod
    def metadata(buffer: bytes) -> memoryview:
        return memoryview(buffer)[14:]

    @staticmethod
    def _set_time_to_live(buffer: bytes, time_to_live: int):
        LeaseFrame._encode_integer(buffer, 6, time_to_live)

    @staticmethod
    def _set_number_of_requests(buffer: bytes, number_of_requests: int):
        LeaseFrame._encode_integer(buffer, 10, number_of_requests)

    @staticmethod
    def create_new(time_to_live: int, number_of_requests: int, metadata: Optional[bytes]) -> FrameSegments:
        header = bytearray(14)
        LeaseFrame.encode_frame_header(
            header, 0, 0x02, False, metadata != None)

        LeaseFrame._set_time_to_live(header, time_to_live)
        LeaseFrame._set_number_of_requests(header, number_of_requests)

        segments = FrameSegments()
        segments.append_fragment(header)
        if metadata != None:
            segments.append_fragment(metadata)
        return segments
