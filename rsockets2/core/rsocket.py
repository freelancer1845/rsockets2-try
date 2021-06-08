import logging
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.connection.keeapalive import KeepaliveSupport
from typing import Optional
from rsockets2.core.frames.request_response import RequestResponseFrame
from rsockets2.core.connection.default_connection import DefaultConnection
from .interactions.request_response import foreign_request_response, local_request_response
from .interactions.request_stream import local_request_stream, foreign_request_stream
from .interactions.fire_and_forget import foreign_fire_and_forget, local_fire_and_forget
import rx
from threading import RLock
import rx.operators as op
from rx.subject.asyncsubject import AsyncSubject
from rx.core.typing import Observable, OnNext, Scheduler

from .types import RSocketHandler, RequestPayload, ResponsePayload


class RSocket(object):
    '''
        The Socket Object assumes negotiation has already taken place and thus can be used right away
    '''
    log = logging.getLogger('rsocket.core.RSocket')

    def __init__(self,
                 connection: DefaultConnection,
                 handler: RSocketHandler,
                 keepalive: KeepaliveSupport,
                 scheduler: Scheduler,
                 is_server_socket: bool,
                 metadata_mime_type: str,
                 data_mime_type: str
                 ) -> None:
        self._con = connection
        self._con.connection_error_obs.subscribe(lambda x: self.dispose())
        self.metadata_mime_type = metadata_mime_type
        self.data_mime_type = data_mime_type
        self._is_server_socket = is_server_socket
        self._scheduler = scheduler
        self._setup_keepalive(keepalive)
        self._keepalive = keepalive
        self._connection_level_error = AsyncSubject()
        self._dispose = AsyncSubject()
        self._setup_handler(handler)

    def request_response(self, data: RequestPayload) -> Observable[ResponsePayload]:

        finish_signal = {}
        if isinstance(data, tuple) == False:
            data = (None, data)
        return rx.merge(
            local_request_response(self._con, data).pipe(op.observe_on(self._scheduler),
                                                         op.subscribe_on(
                                                             self._scheduler),
                                                         op.concat(
                                                             rx.of(finish_signal))
                                                         ),
            self._connection_level_error
        ).pipe(op.take_while(lambda x: x is not finish_signal))

    def request_fnf(self, data: RequestPayload) -> None:
        if isinstance(data, tuple) == False:
            data = (None, data)
        local_fire_and_forget(
            self._con, data)

    def request_stream(self, data: RequestPayload, initial_requests: int = 2 ** 31 - 1,
                       requester: Optional[Observable[int]] = None) -> Observable[ResponsePayload]:
        finish_signal = {}
        if isinstance(data, tuple) == False:
            data = (None, data)
        return rx.merge(
            local_request_stream(self._con, data, initial_requests, requester).pipe(
                op.observe_on(self._scheduler),
                op.subscribe_on(self._scheduler),
                op.concat(
                    rx.of(finish_signal))
            ),
            self._connection_level_error
        ).pipe(op.take_while(lambda x: x is not finish_signal))

    def request_channel(self, data: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def _setup_handler(self, handler: RSocketHandler):
        handler.set_rsocket(self)
        self._con.listen_on_stream(frame_type=FrameType.REQUEST_RESPONSE).pipe(
            op.observe_on(self._scheduler),
            op.flat_map(lambda request_frame: foreign_request_response(
                self._con,
                request_frame,
                handler.on_request_response
            ))
        ).subscribe()
        self._con.listen_on_stream(frame_type=FrameType.REQUEST_STREAM).pipe(
            op.observe_on(self._scheduler),
            op.flat_map(lambda request_frame: foreign_request_stream(
                self._con,
                request_frame,
                handler.on_request_stream
            ))
        ).subscribe()
        self._con.listen_on_stream(frame_type=FrameType.REQUEST_FNF).pipe(
            op.observe_on(self._scheduler),
            op.flat_map(lambda request_frame: foreign_fire_and_forget(
                request_frame,
                handler.on_request_fnf
            ))
        ).subscribe()

    def _tear_down(self):
        self._connection_level_error.on_next(ValueError('Connection Disposed'))
        self._dispose.on_next(1)
        self._dispose.on_completed()
        self._keepalive.stop()
        self._con.dispose()

    def _setup_keepalive(self, keepalive: KeepaliveSupport):
        self.log.debug('Setting up keepalive...')
        keepalive.start()

    def dispose(self):
        self._tear_down()

    @property
    def on_dispose(self) -> Observable[int]:
        return self._dispose
