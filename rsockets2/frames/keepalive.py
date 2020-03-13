from .common import FrameType
import struct
from .frame_abc import Frame_ABC
from abc import abstractmethod


class KeepAliveFrame(Frame_ABC):

    def __init__(self):
        super().__init__()
        self.respond_flag = False
        self.last_received_position = 0
        self.data = bytes(0)

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = KeepAliveFrame()

        data_read = 6
        frame.respond_flag = flags >> 7 & 1 == 1
        frame.last_received_position, = struct.unpack_from(
            ">Q", full_data, data_read)
        data_read += 8
        frame.data = full_data[data_read:]
        return frame

    def __len__(self):
        return 14 + len(self.data)

    def to_bytes(self):
        bufferSize = len(self)
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
