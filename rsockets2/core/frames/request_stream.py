from rsockets2.core.types import RequestPayloadTypes
from typing import Optional

from .data_frame import DataFrame
from .segmented_frame import FrameSegments


class RequestStreamFrame(DataFrame):

    @staticmethod
    def initial_request_n(buffer: bytes) -> int:
        return DataFrame._decode_integer(buffer, 6)[0]

    @staticmethod
    def metadata(buffer: bytes) -> memoryview:
        return DataFrame.metadata(buffer, 10)

    @staticmethod
    def data(buffer: bytes) -> memoryview:
        return DataFrame.data(buffer, 10)

    @staticmethod
    def _set_initial_request_n(buffer: bytes, n: int):
        DataFrame._encode_integer(buffer, 6, n)

    @staticmethod
    def create_new(
        stream_id: int,
        fragment_follows: bool,
        initial_request_n: int,
        metadata: RequestPayloadTypes,
        data: RequestPayloadTypes
    ) -> FrameSegments:
        if metadata != None:
            header = bytearray(13)
            DataFrame._set_metadata_length(header, len(metadata), 10)
        else:
            header = bytearray(10)
        RequestStreamFrame.encode_frame_header(
            header, stream_id, 0x06, False, metadata != None)
        RequestStreamFrame._set_fragment_follows(header, fragment_follows)
        RequestStreamFrame._set_initial_request_n(header, initial_request_n)
        frame = FrameSegments()
        frame.append_fragment(header)
        if metadata != None:
            frame.append_fragment(metadata)
        if data != None:
            frame.append_fragment(data)

        return frame
