from rx.subject.asyncsubject import AsyncSubject
from rsockets2.core.frames.keepalive import KeepaliveFrame
import threading
from typing import Optional, Union
from rsockets2.core.factories.setup_config import RSocketSetupConfig
from rsockets2.core.frames.frame_header import FrameHeader
from rx.core.observable.connectableobservable import ConnectableObservable
from rsockets2.core.transport.abstract_transport import AbstractTransport
from threading import RLock
from ..frames import FrameSegments, FrameType, SetupFrame
from rx.core.typing import Observable, Observer, Scheduler
import rx
import rx.operators as op
from ..exceptions import RSocketProtocolError, to_exception

import logging


class StreamPositionHolder(object):

    def __init__(self) -> None:
        self._last_received_position = 0
        self._send_position = 0
        self._recv_lock = RLock()
        self._send_lock = RLock()

    def add_received(self, frame: bytes):
        with self._recv_lock:
            self._last_received_position += len(frame)

    def add_send(self, frame: FrameSegments):
        with self._send_lock:
            self._send_position += frame.length

    @property
    def last_received_position(self):
        with self._recv_lock:
            return self._last_received_position

    @property
    def send_position(self):
        with self._send_lock:
            return self._send_position


class StreamIdGenerator(object):

    MAX_ID = 2 ** 31 - 1

    def __init__(self, for_server: bool) -> None:
        super().__init__()
        self.for_server = for_server
        self._reset_id()
        self._reserved_ids = []

        self._id_lock = RLock()

    def _reset_id(self):
        if self.for_server:
            self._last_id = 0
        else:
            self._last_id = -1

    def _increment_and_get(self) -> int:
        self._last_id += 2
        if self._last_id > StreamIdGenerator.MAX_ID:
            self._reset_id()
            self._last_id += 2

        return self._last_id

    def new_stream_id(self) -> int:
        with self._id_lock:
            self._increment_and_get()
            while self._last_id in self._reserved_ids:
                self._increment_and_get()
            self._reserved_ids.append(self._last_id)
            return self._last_id

    def free_stream_id(self, id: int):
        with self._id_lock:
            self._reserved_ids.remove(id)


class DefaultConnection(object):

    log = logging.getLogger(__name__)

    def __init__(self,
                 transport: AbstractTransport,
                 scheduler: Scheduler,
                 server_to_client: bool) -> None:
        self.stream_id_generator = StreamIdGenerator(server_to_client)
        self.stream_position = StreamPositionHolder()
        self._transport = transport
        self._disposer = AsyncSubject()
        self._scheduler = scheduler

        def receive_subscriber(observer: Observer, scheduler):

            def recv_callback(frame: bytearray):
                stream_id, frame_type, flags = FrameHeader.stream_id_type_and_flags(
                    frame)
                self.log.debug(
                    f'Received Frame -> Stream Id: {stream_id} - {str(FrameType(frame_type))} - Flags: {bin(flags)}')
                self.stream_position.add_received(frame)
                observer.on_next(frame)

            self._transport.set_on_receive_callback(recv_callback)

        self._receiver: ConnectableObservable = rx.merge(
            rx.create(receive_subscriber),
            self._disposer
        ).pipe(op.observe_on(self._scheduler), op.publish())

    def _handle_transport_error(self, err: Exception):
        self.log.error(f'Transport failed {str(err)}')
        self.dispose()
        # On error is enough
        self._transport.set_on_error_callback(
            lambda x: self.log.debug(f'Additional errors: {str(x)}'))

    def _activate(self):
        self._transport.set_on_error_callback(self._handle_transport_error)
        self._receiver.connect()

    def negotiate_client(self, setup_config: RSocketSetupConfig):
        connection_established = threading.Event()
        connection_error = None

        def connection_establish_hook(data: Union[bytes, Exception]):
            nonlocal connection_error
            if isinstance(data, Exception):
                pass
            else:
                frame_type = FrameHeader.stream_id_type_and_flags(data)[1]
                if frame_type == FrameType.KEEPALIVE:
                    connection_established.set()
                    self._activate()
                elif frame_type == FrameType.ERROR:
                    connection_error = to_exception(data)
                    connection_established.set()
                else:
                    connection_error = RSocketProtocolError(
                        'Unexpected frame received after connection setup')
                    connection_established.set()
        self._transport.open()

        setup_frame = SetupFrame.create_new(
            setup_config.major_version,
            setup_config.minor_version,
            setup_config.time_between_keepalive,
            setup_config.max_lifetime,
            setup_config.token,
            setup_config.metadata_encoding_mime_type,
            setup_config.data_encoding_mime_type,
            setup_config.metadata_payload,
            setup_config.data_payload
        )
        segments = FrameSegments()
        segments.append_fragment(setup_frame)
        self._transport.set_on_error_callback(connection_establish_hook)
        self._transport.set_on_receive_callback(connection_establish_hook)
        self._transport.send_frame(segments)
        self._transport.send_frame(KeepaliveFrame.create_new(
            True, self.stream_position.last_received_position, bytes(0)))
        if connection_established.wait(5.0) == False:
            raise RSocketProtocolError(
                'Server Failed to answer in 5 seconds... Connection could not be established')
        else:
            if connection_error != None:
                self._transport.close()
                raise connection_error

    def queue_frame(self, frame: FrameSegments):
        stream_id, frame_type, flags = FrameHeader.stream_id_type_and_flags(
            frame[0])
        self.log.debug(
            f'Sending Frame -> Stream Id: {stream_id} - {str(FrameType(frame_type))} - Flags: {bin(flags)}')
        self._transport.send_frame(frame)
        self.stream_position.add_send(frame)

    def listen_on_stream(self, stream_id: Optional[int] = None, frame_type: Optional[FrameType] = None) -> Observable[bytes]:
        if stream_id == None:
            return self._receiver.pipe(
                op.filter(lambda frame: FrameHeader.stream_id_type_and_flags(
                    frame)[1] == frame_type)
            )
        elif frame_type == None:
            return self._receiver.pipe(
                op.filter(lambda frame: FrameHeader.stream_id_type_and_flags(
                    frame)[0] == stream_id)
            )
        else:
            return self._receiver

    def dispose(self):
        # self._disposer.on_next(0)
        self._disposer.on_completed()
        self._transport.close()
