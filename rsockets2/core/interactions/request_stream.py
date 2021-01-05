

from ast import Bytes
import threading
from rsockets2.core.exceptions import to_exception
from rsockets2.core.frames.request_n import RequestNFrame
from typing import Optional
from rsockets2.core.frames.request_stream import RequestStreamFrame
from rsockets2.core.frames.payload import PayloadFrame
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rx.core.observer.observer import Observer
from rx.core.typing import Observable, Scheduler
from rx.subject.subject import Subject
from rsockets2.core.types import RequestPayload, ResponsePayload
from rsockets2.core.connection import DefaultConnection
from ..frames import ErrorFrame, CancelFrame
import rx
import rx.operators as op


class RequestWithAnswerLogic(Observer):

    def __init__(self, observer: Observer) -> None:
        super().__init__()
        self.observer = observer
        self.unsubscribe = Subject()
        self.complete = threading.Event()

    def tear_down(self):
        self.complete.set()
        self.unsubscribe.on_next(0)

    def on_next(self, data: bytes) -> None:
        frame_type = FrameHeader.stream_id_type_and_flags(data)[1]

        if frame_type == FrameType.PAYLOAD:
            if PayloadFrame.next(data):
                if PayloadFrame.is_metdata_present(data):
                    self.observer.on_next((PayloadFrame.metadata(
                        data), PayloadFrame.data(data)))
                else:
                    self.observer.on_next((None, PayloadFrame.data(data)))
            if PayloadFrame.complete(data):
                self.tear_down()
                self.observer.on_completed()

        elif frame_type == FrameType.CANCEL:
            self.tear_down()
            self.observer.on_error(ValueError('Stream Canceled by other side'))

        elif frame_type == FrameType.ERROR:
            self.tear_down()
            self.observer.on_error(to_exception(data))

        else:
            self.tear_down()
            self.observer.on_error(RuntimeError(
                f'RSocket Protocol Error. Request Response cannot handle FrameTyp: {frame_type}'))

    def on_error(self, error: Exception) -> None:
        self.unsubscribe.on_next(0)
        self.observer.on_error(error)

    def on_completed(self) -> None:
        self.unsubscribe.on_next(0)


def optional_schedule_on(scheduler: Optional[Scheduler]):
    if scheduler == None:
        return op.pipe()
    else:
        return op.observe_on(scheduler)


def local_request_stream(
        con: DefaultConnection,
        data: RequestPayload,
        initial_requests: int = 2 ** 31 - 1,
        requester: Optional[Observable[int]] = None) -> Observable[ResponsePayload]:
    def observable(observer: Observer, scheduler):

        logic = RequestWithAnswerLogic(observer)
        stream_id = con.stream_id_generator.new_stream_id()

        def finally_action():
            if logic.complete.is_set() == False:
                con.queue_frame(CancelFrame.create_new(stream_id))
            con.stream_id_generator.free_stream_id(stream_id)

        disposable = con.listen_on_stream(stream_id).pipe(
            op.take_until(logic.unsubscribe),
            op.finally_action(lambda: finally_action())
        ).subscribe(logic)

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
            requester.pipe(
                optional_schedule_on(scheduler),
                op.take_until(logic.unsubscribe)
            ).subscribe(
                on_next=lambda x: request_more(x))
        return disposable

    return rx.create(observable)
