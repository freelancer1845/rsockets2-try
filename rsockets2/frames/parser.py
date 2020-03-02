
from .setup import SetupFrame
from .error import ErrorFrame
from .common import FrameType
from .payload import Payload
from .request_response import RequestResponse
from .request_stream import RequestStream
from .keepalive import KeepAliveFrame
from .request_n import RequestNFrame
from .request_fnf import RequestFNF
import struct
from typing import Union


class FrameParser(object):

    def parseFrame(self, data: bytes, stream_id_required=True):

        stream_id, frame_type, flags = self._parse_frame_header(
            data, stream_id_required)
        if frame_type == FrameType.SETUP:
            return SetupFrame.from_data(stream_id, flags, data)
        if frame_type == FrameType.PAYLOAD:
            return Payload.from_data(stream_id, flags, data)
        elif frame_type == FrameType.ERROR:
            return ErrorFrame.from_data(stream_id, flags, data)
        elif frame_type == FrameType.REQUEST_RESPONSE:
            return RequestResponse.from_data(stream_id, flags, data)
        elif frame_type == FrameType.REQUEST_STREAM:
            return RequestStream.from_data(stream_id, flags, data)
        elif frame_type == FrameType.KEEPALIVE:
            return KeepAliveFrame.from_data(stream_id, flags, data)
        elif frame_type == FrameType.REQUEST_N:
            return RequestNFrame.from_data(stream_id, flags, data)
        elif frame_type == FrameType.REQUEST_FNF:
            return RequestFNF.from_data(stream_id, flags, data)
        else:
            raise ValueError("Unsupported Frame Type: {}".format(frame_type))

    # def toFrame(self, frame: Union[SetupFrame]):

    #     if isinstance(frame, SetupFrame):

    def _parse_frame_header(self, data: bytes, stream_id_required):
        if stream_id_required == True:
            stream_id, type_and_flags = struct.unpack_from(
                '>IH', data, 0)
            stream_id = stream_id & ~(1 << 31)
            frame_type = FrameType((type_and_flags >> 10) & 0x3F)
            flags = type_and_flags & 0x3FF
            return stream_id, frame_type, type_and_flags
        else:
            type_and_flags, = struct.unpack_from(
                '>H', data, 0)
            frame_type = FrameType((type_and_flags >> 10) & 0x3F)
            flags = type_and_flags & 0x3FF
            return None, frame_type, type_and_flags
