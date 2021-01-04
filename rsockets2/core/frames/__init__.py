from .frame_header import FrameType, FrameHeader
from .setup import SetupFrame
from .keepalive import KeepaliveFrame
from .error import ErrorFrame, ErrorCodes
from .request_response import RequestResponseFrame
from .request_stream import RequestStreamFrame
from .payload import PayloadFrame
from .request_n import RequestNFrame
from .request_fnf import RequestFNFFrame
from .cancel import CancelFrame
from .lease import LeaseFrame
from .segmented_frame import FrameSegments


FRAME_MAP = {
    FrameType.SETUP: SetupFrame,
    FrameType.LEASE: LeaseFrame,
    FrameType.KEEPALIVE: KeepaliveFrame,
    FrameType.REQUEST_RESPONSE: RequestResponseFrame,
    FrameType.REQUEST_FNF: RequestFNFFrame,
    FrameType.PAYLOAD: PayloadFrame,
    FrameType.ERROR: ErrorFrame,
}
