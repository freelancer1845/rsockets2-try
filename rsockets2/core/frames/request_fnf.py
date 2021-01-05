from rsockets2.core.types import RequestPayloadTypes
from .segmented_frame import FrameSegments
from typing import Optional
from .data_frame import DataFrame


class RequestFNFFrame(DataFrame):

    @staticmethod
    def metadata(buffer: bytes) -> memoryview:
        return DataFrame.metadata(buffer, 6)

    @staticmethod
    def data(buffer: bytes) -> memoryview:
        return DataFrame.data(buffer, 6)

    @staticmethod
    def create_new(stream_id: int, fragment_follows: bool, metadata: RequestPayloadTypes, data: RequestPayloadTypes) -> FrameSegments:
        if metadata != None:
            header = bytearray(9)
            DataFrame._set_metadata_length(header, len(metadata), 6)
        else:
            header = bytearray(6)
        RequestFNFFrame.encode_frame_header(
            header, stream_id, 0x05, False, metadata != None)
        RequestFNFFrame._set_fragment_follows(header, fragment_follows)
        frame = FrameSegments()
        frame.append_fragment(header)
        if metadata != None:
            frame.append_fragment(metadata)
        if data != None:
            frame.append_fragment(data)

        return frame
