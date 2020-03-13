from .common import FrameType
import struct
from .frame_abc import Frame_ABC
from abc import abstractmethod


class CancelFrame(Frame_ABC):

    def __init__(self):
        super().__init__()

    @classmethod
    def from_data(cls, stream_id: int, flags: int, full_data: bytes):
        frame = CancelFrame()
        frame.stream_id = stream_id
        return frame

    def __len__(self):
        return 6

    def to_bytes(self):
        bufferSize = len(self)
        data = bytearray(bufferSize)

        struct.pack_into(">I", data, 0, self.stream_id)
        dataWritten = 4
        type_and_flags = FrameType.CANCEL << 10
        struct.pack_into(">H", data, dataWritten, type_and_flags)
        return data
