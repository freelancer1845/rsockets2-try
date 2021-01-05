from rsockets2.core.types import RSocketHandler, RequestPayload
from rsockets2.core.connection.default_connection import DefaultConnection
from rsockets2.core.connection.keeapalive import KeepaliveSupport
from rsockets2.core.factories.setup_config import RSocketSetupConfig
from rsockets2.core.transport.abstract_transport import AbstractTransport
from rx.core.typing import Observable, Scheduler
from rsockets2.core.rsocket import RSocket
from rx.scheduler.threadpoolscheduler import ThreadPoolScheduler
import rx
import rx.operators as op

import logging

log = logging.getLogger('rsockets2.core.factories.RSocketClient')


class NoopHandler(RSocketHandler):

    def on_request_channel(self):
        raise NotImplementedError('Cannot Handle')

    def on_request_fnf(self, payload: RequestPayload) -> None:
        pass

    def on_request_response(self, payload):
        raise NotImplementedError('Cannot Handle')

    def on_request_stream(self, payload: RequestPayload, initial_requests: int, requests: Observable[int]):
        raise NotImplementedError('Cannot Handle')


def rsocket_client(
    transport: AbstractTransport,
    setup_config: RSocketSetupConfig,
    handler: RSocketHandler = NoopHandler(),
    scheduler: Scheduler = ThreadPoolScheduler(5)
) -> Observable[RSocket]:

    def subscriber(observer, subscribe_scheduler):
        try:
            connection = DefaultConnection(transport, scheduler, False)
            log.debug('Negotiating connection: ' + str(setup_config))
            connection.negotiate_client(setup_config)
            log.debug('Connection negotiated')
            keepalive = KeepaliveSupport(
                connection, setup_config.time_between_keepalive, setup_config.max_lifetime)
            rsocket = RSocket(
                connection,
                handler,
                keepalive,
                scheduler,
                False)
            observer.on_next(rsocket)
            observer.on_completed()
        except Exception as err:
            observer.on_error(err)

    return rx.create(subscriber).pipe(op.observe_on(scheduler))
