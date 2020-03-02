from .common import FrameType
import struct


class CancelFrame(object):

    def __init__(self):
        super().__init__()
        self.stream_id = 0

    @staticmethod
    def from_data(stream_id: int, flags: int, full_data: bytes):
        frame = CancelFrame()
        frame.stream_id = stream_id
        return frame

    def to_bytes(self):
        bufferSize = 6
        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.CANCEL << 10
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        return data
