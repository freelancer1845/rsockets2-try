from __future__ import annotations
import rx
import rx.core
import rx.disposable
import rx.scheduler
import rx.scheduler.scheduler
import rx.operators as op
from .common import RSocketConfig
from .transport import AbstractTransport
from . import RSocketClient
import logging
from .messages import RMessageClient
import typing
import threading


class RSocketClientFactory(object):

    def __init__(self):
        super().__init__()
        self._config = RSocketConfig()
        self._transport = None
        self._scheduler = None
        self._auto_reconnect = False
        self._message_client = False
        self.log = logging.getLogger('rsockets2.RSocketClientFactory')

    def with_config(self, config: RSocketConfig) -> RSocketClientFactory:
        self._config = config
        return self

    def with_transport(self, transport: AbstractTransport) -> RSocketClientFactory:
        self._transport = transport
        return self

    def with_scheduler(self, scheduler: rx.scheduler.scheduler.Scheduler):
        self._scheduler = scheduler
        return self

    def with_auto_reconnect(self):
        """
            This configuration returns a stream/flux of RSocketClients when beeing build.
            If a connection fails a new client is created.
            You can use this Observable in combinattion with switch_map to realize automatically reconnecting streams.
        """
        self._auto_reconnect = True
        return self

    def build(self) -> rx.Observable[RSocketClient]:
        if self._transport is None:
            raise ValueError("You must provide a viable transport!")

        if self._scheduler is None:
            self._scheduler = rx.scheduler.ThreadPoolScheduler(20)

        if self._auto_reconnect == True:

            return self._create_auto_reconnecting_client()
        else:
            return rx.from_callable(lambda: self._create_single_shot_client(), self._scheduler)

    def buildMessageingSocket(self) -> rx.Observable[RMessageClient]:
        self._message_client = True
        return self.build()

    def _create_single_shot_client(self) -> typing.Union[RMessageClient, RSocketClient]:
        if self._message_client == True:
            self.log.debug(
                "Creating RMessageClient. Config: {}".format(self._config))
            client = RMessageClient(self._transport,
                                    self._config,
                                    self._scheduler)
            self.log.debug("RMessageClient created. Opening connection")
            client.open()
            self.log.debug("RMessageClient socket opened")
            return client

        else:
            self.log.debug(
                "Creating RSocketClient. Config: {}".format(self._config))
            client = RSocketClient(
                self._config, self._transport, self._scheduler)

            client.open()
            return client

    def _create_auto_reconnecting_client(self) -> Observable[RSocketClient]:
        def observable(observer: rx.core.Observer, scheduler) -> rx.disposable.Disposable:
            self.log.debug(
                "Creating RSocketFactory with support for automatic reconnecting. Config: {}".format(self._config))

            def flat_map_to_destroy_observer(client):
                if isinstance(client, RSocketClient):
                    return client._connection.destroy_observable()
                elif isinstance(client, RMessageClient):
                    return client.rsocket._connection.destroy_observable()

            def client_publisher(client):
                observer.on_next(client)

            disposable = rx.from_callable(
                lambda: self._create_single_shot_client(),
                self._scheduler
            ).pipe(
                op.observe_on(scheduler=self._scheduler),
                op.do_action(on_next=lambda client: client_publisher(client)),
                op.flat_map(
                    lambda client: flat_map_to_destroy_observer(client)),
                op.do_action(on_completed=lambda: self.log.debug(
                    "Trying to reconnect to rsocket server in 5 seconds...")),
                op.delay(op.timedelta(seconds=5)),
                op.repeat()
            ).subscribe()

            return disposable

        return rx.create(observable).pipe(op.observe_on(self._scheduler))
