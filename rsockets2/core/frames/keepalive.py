from typing import Optional

from .frame_header import FrameHeader
from .segmented_frame import FrameSegments


class KeepaliveFrame(FrameHeader):

    @staticmethod
    def respond_with_keepalive(buffer: bytes) -> bool:
        return buffer[5] >> 7 & 1 == 1

    @staticmethod
    def last_received_position(buffer: bytes) -> int:
        return KeepaliveFrame._decode_long(buffer, 6)[0]

    @staticmethod
    def data(buffer: bytes) -> memoryview:
        return memoryview(buffer)[14:]

    @staticmethod
    def _set_respond_with_keepalive(buffer: bytearray, value: bool):
        if value:
            buffer[5] |= 1 << 7

    @staticmethod
    def _set_last_received_position(buffer: bytearray, position: int):
        FrameHeader._encode_long(buffer, 6, position)

    @staticmethod
    def create_new(respond_with_keepalive: bool, last_received_position: int, data: Optional[bytes]) -> FrameSegments:
        header = bytearray(14)
        KeepaliveFrame.encode_frame_header(header, 0, 0x03, False, False)
        KeepaliveFrame._set_respond_with_keepalive(
            header, respond_with_keepalive)
        KeepaliveFrame._set_last_received_position(
            header, last_received_position)
        frame = FrameSegments()
        frame.append_fragment(header)
        if data != None:
            frame.append_fragment(data)

        return frame
