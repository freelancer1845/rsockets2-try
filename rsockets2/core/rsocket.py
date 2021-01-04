import logging
from rsockets2.core.frames.request_response import RequestResponseFrame
from rsockets2.core.connection.default_connection import DefaultConnection
import rx

from rx.core.typing import Observable

from .types import RequestPayload, ResponsePayload


class RSocket(object):
    log = logging.getLogger('rsocket.core.RSocket')

    def __init__(self) -> None:
        self._con: DefaultConnection

    def request_response(self, data: RequestPayload) -> Observable[ResponsePayload]:
        def observable(observer):

            

            frame = RequestResponseFrame.create_new(
                self._new_stream_id(),
                False,
                data[0], data[1]
            )

            self._con.queue_frame(frame)

        return rx.create(observable)

    def request_fnf(self, data: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def request_stream(self, data: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def request_channel(self, data: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def _new_stream_id(self) -> int:
        return 0
