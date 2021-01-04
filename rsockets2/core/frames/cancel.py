
from .frame_header import FrameHeader
from .segmented_frame import FrameSegments


class CancelFrame(FrameHeader):

    @staticmethod
    def create_new(stream_id: int) -> FrameSegments:
        header = bytearray(6)
        CancelFrame.encode_frame_header(header, stream_id, 0x09, False, False)
        frame = FrameSegments()
        frame.append_fragment(header)
        return frame
