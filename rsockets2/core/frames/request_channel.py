from typing import Optional
from .data_frame import DataFrame
from .segmented_frame import FrameSegments


class RequestChannelFrame(DataFrame):

    @staticmethod
    def metadata(buffer: bytes) -> memoryview:
        return DataFrame.metadata(buffer, 10)

    @staticmethod
    def data(buffer: bytes) -> memoryview:
        return DataFrame.data(buffer, 10)


    @staticmethod
    def initial_request_n(buffer: bytes) -> int:
        return DataFrame._decode_integer(buffer, 6)[0]



    @staticmethod
    def _set_initial_request_n(buffer: bytes, n: int):
        DataFrame._encode_integer(buffer, 6, n)

    @staticmethod
    def create_new(
            stream_id: int,
            fragment_follows: bool,
            complete: bool,
            initial_request_n: int,
            metadata: Optional[bytes],
            data: Optional[bytes]) -> FrameSegments:
        if metadata != None:
            header = bytearray(13)
            DataFrame._set_metadata_length(header, len(metadata), 10)
        else:
            header = bytearray(10)
        RequestChannelFrame.encode_frame_header(
            header, stream_id, 0x07, False, metadata != None)
        RequestChannelFrame._set_fragment_follows(header, fragment_follows)
        RequestChannelFrame._set_complete(header, complete)
        RequestChannelFrame._set_initial_request_n(header, initial_request_n)
        frame = FrameSegments()
        frame.append_fragment(header)
        if metadata != None:
            frame.append_fragment(metadata)
        if data != None:
            frame.append_fragment(data)

        return frame