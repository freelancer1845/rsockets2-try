

import rx
import rx.operators as op
from rsockets2.core.exceptions import to_exception
from rsockets2.core.frames.cancel import CancelFrame
from rsockets2.core.frames.error import ErrorFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.interactions.request_stream import (RequestWithAnswerLogic,
                                                        optional_schedule_on)
from rsockets2.core.types import RequestPayload, ResponsePayload
from rx.core.typing import Observable, Observer
from rx.subject.subject import Subject

from ..connection import DefaultConnection
from ..frames import RequestResponseFrame


def local_request_response(con: DefaultConnection, stream_id: int, data: RequestPayload) -> Observable[ResponsePayload]:
    def observable(observer: Observer, scheduler):

        logic = RequestWithAnswerLogic(observer)

        def cancel_signal():
            if logic.complete.is_set() == False:
                con.queue_frame(CancelFrame.create_new(stream_id))

        disposable = con.listen_on_stream(stream_id).pipe(
            optional_schedule_on(scheduler),
            op.take_until(logic.unsubscribe),
            op.finally_action(lambda: cancel_signal())
        ).subscribe(logic)

        frame = RequestResponseFrame.create_new(
            stream_id,
            False,
            data[0], data[1]
        )
        con.queue_frame(frame)
        return disposable

    return rx.create(observable)
