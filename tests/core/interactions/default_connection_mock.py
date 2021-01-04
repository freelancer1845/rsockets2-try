from rsockets2.core.frames.segmented_frame import FrameSegments
from rx.core.typing import Observable
from rsockets2.core.connection import DefaultConnection
import rx.operators as op
from rsockets2.core.frames import FrameHeader
from rx.subject.subject import Subject

class DefaultConnectionMock(DefaultConnection):

    def __init__(self) -> None:
        self.receiver_stream = Subject()
        self.sender_stream = Subject()

    def listen_on_stream(self, stream_id: int) -> Observable[bytes]:
        return self.receiver_stream.pipe(op.filter(lambda data: FrameHeader.stream_id_type_and_flags(data)[0] == stream_id))

    def queue_frame(self, frame: FrameSegments):
        self.sender_stream.on_next(frame.reduce())

    def listen_on_sender_stream(self, stream_id: int) -> Observable[bytes]:
        return self.sender_stream.pipe(op.filter(lambda data: FrameHeader.stream_id_type_and_flags(data)[0] == stream_id))

    def queue_on_receiver_stream(self, frame: FrameSegments):
        self.receiver_stream.on_next(frame.reduce())
