from .common import FrameType
import struct


class RequestNFrame(object):

    def __init__(self):
        super().__init__()

        self.stream_id = 0
        self.n = 0

    @staticmethod
    def from_data(stream_id: int, flags: int, full_data: bytes):
        frame = RequestNFrame()

        data_read = 6
        frame.stream_id = stream_id
        frame.n = struct.unpack_from(">I", full_data, data_read)
        return frame

    def to_bytes(self):
        if self.stream_id == 0:
            raise ValueError("Stream ID must be set!")

        bufferSize = 10

        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.REQUEST_N << 10
        type_and_flags = type_and_flags
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2
        struct.pack_into(">I", data, dataWritten, self.n)
        dataWritten += 4
        return data
