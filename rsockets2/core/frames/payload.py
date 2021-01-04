from rsockets2.core.frames.frame_header import FrameType
from typing import Optional

from .data_frame import DataFrame
from .segmented_frame import FrameSegments


class PayloadFrame(DataFrame):

    @staticmethod
    def next(buffer: bytes) -> bool:
        return buffer[5] >> 5 & 1 == 1

    @staticmethod
    def _set_next(buffer: bytes, value: bool):
        if value:
            buffer[5] |= 1 << 5

    @staticmethod
    def data(buffer: bytes) -> memoryview:
        return DataFrame.data(buffer, 6)

    @staticmethod
    def metadata(buffer: bytes) -> memoryview:
        return DataFrame.metadata(buffer, 6)

    @staticmethod
    def create_new(
            stream_id: int,
            fragment_follows: bool,
            complete: bool,
            next: bool,
            metadata: Optional[bytes],
            data: Optional[bytes]) -> FrameSegments:
        if metadata != None:
            header = bytearray(9)
            DataFrame._set_metadata_length(header, len(metadata), 6)
        else:
            header = bytearray(6)
        PayloadFrame.encode_frame_header(
            header, stream_id, FrameType.PAYLOAD, False, metadata != None)
        PayloadFrame._set_fragment_follows(header, fragment_follows)
        PayloadFrame._set_complete(header, complete)
        PayloadFrame._set_next(header, next)
        frame = FrameSegments()
        frame.append_fragment(header)
        if metadata != None:
            frame.append_fragment(metadata)
        if data != None:
            frame.append_fragment(data)
        return frame
