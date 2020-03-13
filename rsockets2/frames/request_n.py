from .common import FrameType
import struct
from .frame_abc import Frame_ABC
from abc import abstractmethod


class RequestNFrame(Frame_ABC):

    def __init__(self):
        super().__init__()

        self.n = 0

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = RequestNFrame()

        data_read = 6
        frame.stream_id = stream_id
        frame.n = struct.unpack_from(">I", full_data, data_read)
        return frame

    def __len__(self):
        return 10

    def to_bytes(self):
        if self.stream_id == 0:
            raise ValueError("Stream ID must be set!")

        data = bytearray(len(self))

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.REQUEST_N << 10
        type_and_flags = type_and_flags
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        dataWritten += 2
        struct.pack_into(">I", data, dataWritten, self.n)
        dataWritten += 4
        return data
