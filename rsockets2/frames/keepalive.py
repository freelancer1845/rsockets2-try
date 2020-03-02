from .common import FrameType
import struct


class KeepAliveFrame(object):

    def __init__(self):
        super().__init__()
        self.respond_flag = False
        self.last_received_position = 0
        self.data = bytes(0)

    @staticmethod
    def from_data(stream_id: int, flags: int, full_data: bytes):
        frame = KeepAliveFrame()

        data_read = 6
        frame.respond_flag = flags >> 7 & 1 == 1
        frame.last_received_position = struct.unpack_from(">Q", full_data, data_read)
        data_read += 8
        frame.data = full_data[data_read:]
        return frame

    def to_bytes(self):
        bufferSize = 14 + len(self.data)
        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, 0)
        dataWritten = 4
        type_and_flags = FrameType.KEEPALIVE << 10
        if self.respond_flag:
            type_and_flags |= (1 << 7)
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2
        struct.pack_into(">Q", data, dataWritten, self.last_received_position)
        dataWritten += 8
        data[dataWritten:] = self.data
        return data
