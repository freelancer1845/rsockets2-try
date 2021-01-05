

import threading
from typing import Callable, Union
import rx
import rx.operators as op
from rx.subject.asyncsubject import AsyncSubject
from rsockets2.core.exceptions import to_exception
from rsockets2.core.frames.cancel import CancelFrame
from rsockets2.core.frames.error import ErrorCodes, ErrorFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.interactions.request_stream import (ForeignRequestLogic, RequestWithAnswerLogic,
                                                        optional_schedule_on)
from rsockets2.core.types import RequestPayload, ResponsePayload
from rx.core.typing import Observable, Observer, Scheduler
from rx.subject.subject import Subject

from ..connection import DefaultConnection
from ..frames import RequestResponseFrame



def foreign_request_response(
        connection: DefaultConnection,
        request_frame: Union[bytes, bytearray, memoryview],
        handler: Callable[[RequestPayload], Observable[ResponsePayload]]) -> Observable:

    stream_id = FrameHeader.stream_id_type_and_flags(request_frame)[0]
    if RequestResponseFrame.is_metdata_present(request_frame):
        payload = (RequestResponseFrame.metadata(request_frame),
                   RequestResponseFrame.data(request_frame))
    else:
        payload = (None, RequestResponseFrame.data(request_frame))

    return handler(payload).pipe(
        op.take_until(
            connection.listen_on_stream(stream_id, FrameType.CANCEL)
        ),
        op.do(
            ForeignRequestLogic(connection, stream_id)
        )
    )


def local_request_response(con: DefaultConnection, data: RequestPayload) -> Observable[ResponsePayload]:
    def observable(observer: Observer, scheduler):

        logic = RequestWithAnswerLogic(observer)
        stream_id = con.stream_id_generator.new_stream_id()

        def finally_action():
            con.stream_id_generator.free_stream_id(stream_id)
            if logic.complete.is_set() == False:
                con.queue_frame(CancelFrame.create_new(stream_id))
        disposable = con.listen_on_stream(stream_id).pipe(
            op.take_until(logic.unsubscribe),
            op.finally_action(lambda: finally_action())
        ).subscribe(logic)

        frame = RequestResponseFrame.create_new(
            stream_id,
            False,
            data[0], data[1]
        )
        con.queue_frame(frame)
        return disposable

    return rx.create(observable)
