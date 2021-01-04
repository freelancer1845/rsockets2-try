from ..frames import FrameSegments, FrameType
from rx.core.typing import Observable


class DefaultConnection(object):

    def queue_frame(self, frame: FrameSegments):
        pass

    def listen_on_stream(self, stream_id: int) -> Observable[bytes]:
        pass
