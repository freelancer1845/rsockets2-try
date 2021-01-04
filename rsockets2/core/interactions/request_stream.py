

from rsockets2.core.frames.request_n import RequestNFrame
from typing import Optional
from rsockets2.core.frames.request_stream import RequestStreamFrame
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rx.core.observer.observer import Observer
from rx.core.typing import Observable
from rx.subject.subject import Subject
from rsockets2.core.types import RequestPayload, ResponsePayload
from rsockets2.core.connection import default_connection
from ..frames import ErrorFrame
import rx
import rx.operators as op


def local_request_stream(
        con: default_connection,
        stream_id: int,
        data: RequestPayload,
        initial_requests: int = 2 ** 31 - 1,
        requester: Optional[Observable[int]] = None) -> Observable[ResponsePayload]:
    def observable(observer: Observer, scheduler):

        unsubscribe = Subject()

        def map_next(data: bytes):
            frame_type = FrameHeader.stream_id_type_and_flags(data)[1]

            if frame_type == FrameType.PAYLOAD:
                if PayloadFrame.next(data):
                    if PayloadFrame.is_metdata_present(data):
                        observer.on_next((PayloadFrame.metadata(
                            data), PayloadFrame.data(data)))
                    else:
                        observer.on_next((None, PayloadFrame.data(data)))
                if PayloadFrame.complete(data):
                    unsubscribe.on_next(0)
                    observer.on_completed()

            elif frame_type == FrameType.CANCEL:
                unsubscribe.on_next(0)
                observer.on_error(ValueError('Stream Canceled by other side'))

            elif frame_type == FrameType.ERROR:
                unsubscribe.on_next(0)
                observer.on_error(ValueError(
                    f'Error: {ErrorFrame.error_code(data)}. Message: {ErrorFrame.error_data(data)}'))

            else:
                unsubscribe.on_next(0)
                observer.on_error(RuntimeError(
                    f'RSocket Protocol Error. Request Response cannot handle FrameTyp: {frame_type}'))

        disposable = con.listen_on_stream(stream_id).pipe(op.take_until(
            unsubscribe)).subscribe(lambda x: map_next(x), lambda err: unsubscribe.on_next(0), lambda: unsubscribe.on_next(0))

        frame = RequestStreamFrame.create_new(
            stream_id,
            False,
            initial_requests,
            data[0], data[1]
        )

        con.queue_frame(frame)

        def request_more(n: int):
            frame = RequestNFrame.create_new(stream_id, n)
            con.queue_frame(frame)

        if requester != None:
            requester.pipe(op.take_until(unsubscribe)).subscribe(
                on_next=lambda x: request_more(x))

        return disposable

    return rx.create(observable)
