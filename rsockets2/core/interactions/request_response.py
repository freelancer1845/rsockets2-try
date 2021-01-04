

from rx.subject.subject import Subject
from rsockets2.core.frames.error import ErrorFrame
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.types import RequestPayload, ResponsePayload
from rx.core.typing import Observable, Observer
from ..frames import RequestResponseFrame
from ..connection import DefaultConnection

import rx
import rx.operators as op


def local_request_response(con: DefaultConnection, stream_id: int, data: RequestPayload) -> Observable[ResponsePayload]:
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

        con.listen_on_stream(stream_id).pipe(op.take_until(
            unsubscribe)).subscribe(lambda x: map_next(x))

        frame = RequestResponseFrame.create_new(
            stream_id,
            False,
            data[0], data[1]
        )
        con.queue_frame(frame)

    return rx.create(observable)
