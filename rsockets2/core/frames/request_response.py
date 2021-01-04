from typing import Optional

from .data_frame import DataFrame
from .segmented_frame import FrameSegments


class RequestResponseFrame(DataFrame):

    @staticmethod
    def metadata(buffer: bytes) -> memoryview:
        return DataFrame.metadata(buffer, 6)

    @staticmethod
    def data(buffer: bytes) -> memoryview:
        return DataFrame.data(buffer, 6)

    @staticmethod
    def create_new(stream_id: int, fragment_follows: bool, metadata: Optional[bytes], data: Optional[bytes]) -> FrameSegments:
        if metadata != None:
            header = bytearray(9)
            DataFrame._set_metadata_length(header, len(metadata), 6)
        else:
            header = bytearray(6)
        RequestResponseFrame.encode_frame_header(
            header, stream_id, 0x04, False, metadata != None)
        RequestResponseFrame._set_fragment_follows(header, fragment_follows)
        frame = FrameSegments()
        frame.append_fragment(header)
        if metadata != None:
            frame.append_fragment(metadata)
        if data != None:
            frame.append_fragment(data)

        return frame
