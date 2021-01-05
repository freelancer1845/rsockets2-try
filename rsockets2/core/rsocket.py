import logging
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rsockets2.core.connection.keeapalive import KeepaliveSupport
from typing import Optional
from rsockets2.core.frames.request_response import RequestResponseFrame
from rsockets2.core.connection.default_connection import DefaultConnection
from .interactions.request_response import local_request_response
from .interactions.request_stream import local_request_stream
from .interactions.fire_and_forget import local_fire_and_forget
import rx
from threading import RLock
import rx.operators as op
from rx.subject.asyncsubject import AsyncSubject
from rx.core.typing import Observable, Scheduler

from .types import RequestPayload, ResponsePayload


class RSocket(object):
    '''
        The Socket Object assumes negotiation has already taken place and thus can be used right away
    '''
    log = logging.getLogger('rsocket.core.RSocket')

    def __init__(self,
                 connection: DefaultConnection,
                 keepalive: KeepaliveSupport,
                 scheduler: Scheduler,
                 is_server_socket: bool) -> None:
        self._con = connection
        self._is_server_socket = is_server_socket
        self._scheduler = scheduler
        self._setup_keepalive(keepalive)
        self._keepalive = keepalive
        self._connection_level_error = AsyncSubject()
        self._dispose = AsyncSubject()

    def request_response(self, data: RequestPayload) -> Observable[ResponsePayload]:
        return rx.merge(
            local_request_response(self._con, data).pipe(op.observe_on(self._scheduler),
                                                         op.subscribe_on(self._scheduler)),
            self._connection_level_error
        )

    def request_fnf(self, data: RequestPayload) -> None:
        local_fire_and_forget(
            self._con, data)

    def request_stream(self, data: RequestPayload, initial_requests: int = 2 ** 31 - 1,
                       requester: Optional[Observable[int]] = None) -> Observable[ResponsePayload]:
        return rx.merge(
            local_request_stream(self._con, data, initial_requests, requester).pipe(
                op.observe_on(self._scheduler),
                op.subscribe_on(self._scheduler)
            ),
            self._connection_level_error
        )

    def request_channel(self, data: RequestPayload) -> Observable[ResponsePayload]:
        pass

    def _tear_down(self):
        self._connection_level_error.on_next(ValueError('Connection Disposed'))
        self._dispose.on_next(1)
        self._dispose.on_completed()
        self._keepalive.stop()
        self._con.dispose()

    def _setup_keepalive(self, keepalive: KeepaliveSupport):
        self.log.debug('Setting up keepalive...')
        keepalive.start()

    @property
    def on_dispose(self) -> Observable[int]:
        return self._dispose
