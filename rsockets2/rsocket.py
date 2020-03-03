from .socket import RTcpSocket, Socket_ABC, RWebsocketSocket
from enum import Enum, auto
import rsockets2.frames as frames
import rx
import rx.scheduler
import rx.subject
import rx.operators as op
import logging
import threading
import time
import typing
import rsockets2.handler as handler
import rsockets2.executor as executors
import sys

MAJOR_VERSION = 1
MINOR_VERSION = 0


class SocketType(Enum):
    TCP_SOCKET = auto()
    WEBSOCKET = auto()


def _throw_error(error):
    raise error


def _wrap_throw_error_frame(frame: frames.ErrorFrame):
    raise Exception(
        'Application Error. Message: "{}"'.format(frame.error_data))


class RSocket(object):

    def __init__(self,
                 socket_type: SocketType,
                 keepalive=30000,
                 maxlive=10000,
                 meta_data_mime_type=b"message/x.rsocket.routing.v0",
                 data_mime_type=b"application/json",
                 *args,
                 **kwargs):
        super().__init__()
        self._log = logging.getLogger("rsockets2")

        self.socket: Socket_ABC = None
        self.socket_type = socket_type
        self.keepalive_time = keepalive
        self.max_lifetime = maxlive
        self.meta_data_mime_type = meta_data_mime_type
        self.data_mime_type = data_mime_type

        self._parser = frames.FrameParser()
        self._scheduler = rx.scheduler.ThreadPoolScheduler(20)

        self._last_stream_id = 0
        self._used_stream_ids = []
        self._stream_id_lock = threading.Lock()

        # Subjects
        self._keepalive_subject = rx.subject.Subject()
        self._connection_error_subject = rx.subject.Subject()
        self._payload_subject = rx.subject.Subject()
        self._application_error_subject = rx.subject.Subject()
        self._cancel_subject = rx.subject.Subject()
        self._socket_closed_subject = rx.subject.Subject()

        self._socket_closed_thrower = self._socket_closed_subject.pipe(
            op.map(lambda e: _throw_error(e)))

        self._fragmented_frames = {}

        # Callbacks
        self.on_request_response: typing.Callable[[
            frames.RequestResponse], rx.Observable] = None
        self.on_request_stream: typing.Callable[[
            frames.RequestStream], rx.Observable] = None
        self.on_fire_and_forget: typing.Callable[[
            frames.RequestFNF], None] = None

        # keeaplive
        self._keepalive_sender = threading.Thread(
            name="Keepalive-Support", target=self._keepalive_runner, daemon=True)
        self._keepalive_run = False
        self._setup_basic_observer()

        if socket_type == SocketType.TCP_SOCKET:
            if 'hostname' not in kwargs:
                raise ValueError('Parameter "hostname" missing')
            if 'port' not in kwargs:
                raise ValueError('Parameter "port" missing')
            self.hostname = kwargs['hostname']
            self.port = kwargs['port']
        if socket_type == SocketType.WEBSOCKET:
            if 'url' not in kwargs:
                raise ValueError('Parameter "url" missing (websocket)')
            self.url = kwargs['url']

    def open(self):
        if self.socket_type == SocketType.TCP_SOCKET:
            self._createTcpSocket()
        if self.socket_type == SocketType.WEBSOCKET:
            self._createWebsocket()
        self._negotiate()

    def close(self):
        self._keepalive_run = False
        self.socket.close()

    def request_response(self, meta_data, data) -> rx.Observable:

        request = frames.RequestResponse()
        request.stream_id = self._get_new_stream_id()
        if isinstance(meta_data, str):
            request.meta_data = meta_data.encode('UTF-8')
        else:
            request.meta_data = meta_data
        if isinstance(data, str):
            request.request_data = data.encode('UTF-8')
        else:
            request.request_data = data
        if self.meta_data_mime_type == b"message/x.rsocket.routing.v0":
            request.meta_data = bytearray(meta_data)
            request.meta_data.insert(0, len(request.meta_data))

        return executors.request_response_executor(self.socket, request, self._payload_subject).pipe(self._errors_and_teardown(request.stream_id))

    def request_stream(self, meta_data, data) -> rx.Observable:
        request = frames.RequestStream()
        request.stream_id = self._get_new_stream_id()
        request.initial_request = 100000
        if isinstance(meta_data, str):
            request.meta_data = meta_data.encode('UTF-8')
        else:
            request.meta_data = meta_data
        if isinstance(data, str):
            request.request_data = data.encode('UTF-8')
        else:
            request.request_data = data
        if self.meta_data_mime_type == b"message/x.rsocket.routing.v0":
            request.meta_data = bytearray(meta_data)
            request.meta_data.insert(0, len(request.meta_data))

        return executors.request_stream_executor(self.socket, request, self._payload_subject).pipe(self._errors_and_teardown(request.stream_id))

    def fire_and_forget(self, meta_data, data) -> rx.Observable:
        def action():
            frame = frames.RequestFNF()
            frame.meta_data = meta_data
            frame.request_data = data
            frame.stream_id = self._get_new_stream_id()
            self.socket.send_frame(frame.to_bytes())
        return rx.from_callable(lambda: action(), scheduler=self._scheduler).pipe(op.ignore_elements())

    def _negotiate(self):
        setupFrame = frames.SetupFrame()

        setupFrame.major_version = MAJOR_VERSION
        setupFrame.minor_version = MINOR_VERSION
        setupFrame.keepalive_time = self.keepalive_time
        setupFrame.max_lifetime = self.max_lifetime
        setupFrame.meta_data_mime_type = self.meta_data_mime_type
        setupFrame.data_mime_type = self.data_mime_type
        setupFrame.honors_lease = False

        self.socket.send_frame(setupFrame.to_bytes())
        self._keepalive_run = True
        self._keepalive_sender.start()

        def on_complete():
            self._keepalive_run = False
        self._socket_closed_subject.pipe(op.take(1)).subscribe(
            on_completed=lambda: on_complete())

    def _keepalive_runner(self):
        while self._keepalive_run:
            frame = frames.KeepAliveFrame()
            frame.last_received_position = self.socket.get_recv_position()
            frame.respond_flag = True
            frame.data = bytes(0)
            self.socket.send_frame(frame.to_bytes())
            time.sleep(self.keepalive_time / 1000.0 * 0.45)

    def _createTcpSocket(self):
        tcpSocket = RTcpSocket()
        tcpSocket.set_receive_handler(self._handleFrame)
        tcpSocket.set_socket_closed_handler(self._handle_socket_closed)
        tcpSocket.connect(self.hostname, self.port)
        self.socket = tcpSocket

    def _createWebsocket(self):
        websocket = RWebsocketSocket(self.url)
        websocket.set_receive_handler(self._handleFrame)
        websocket.set_socket_closed_handler(self._handle_socket_closed)
        websocket.open()
        self.socket = websocket

    def _handleFrame(self, rawFrame: bytes):
        frame = self._parser.parseFrame(data=rawFrame)
        if isinstance(frame, frames.KeepAliveFrame):
            self._keepalive_subject.on_next(frame)
        elif isinstance(frame, frames.ErrorFrame):

            if frame.error_code == frames.ErrorCodes.APPLICATION_ERROR:
                self._application_error_subject.on_next(frame)
            else:
                self._connection_error_subject.on_next(frame)
        elif isinstance(frame, frames.RequestResponse):
            if self.on_request_response == None:
                self._log.error(
                    "No Request Response Handler created. This should be reported to the requester!")
            else:
                observable = self.on_request_response(frame)
                if isinstance(observable, rx.Observable) == False:
                    raise ValueError(
                        "Request Response Handler must return an Observable!")
                observable.pipe(op.observe_on(self._scheduler),
                                op.take_until(self._cancel_subject.pipe(
                                    op.filter(lambda f: f.stream_id == frame.stream_id)))
                                ).subscribe(
                    handler.RequestResponseHandler(frame.stream_id, self.socket))
        elif isinstance(frame, frames.RequestStream):
            if self.on_request_stream == None:
                self._log.error(
                    "No Request Stream Handler created. This should be reported to the requester!")
            elif frame.initial_request < 2**31 - 1:
                errorframe = frames.ErrorFrame()
                errorframe.stream_id = frame.stream_id
                errorframe.error_code = frames.ErrorCodes.APPLICATION_ERROR
                errorframe.error_data = b"Currently does not support Backpressure. Initial Requests must be 2^31"
                self.socket.send_frame(errorframe.to_bytes())
            else:
                observable = self.on_request_stream(frame)
                if isinstance(observable, rx.Observable) == False:
                    raise ValueError(
                        "Request Stream Handler must return an Observable!")
                observable.pipe(
                    op.subscribe_on(self._scheduler),
                    op.take_until(self._cancel_subject.pipe(
                        op.filter(lambda f: f.stream_id == frame.stream_id)))
                ).subscribe(
                    handler.RequestStreamHandler(frame.stream_id, self.socket))

        elif isinstance(frame, frames.Payload):
            if frame.follows == True:
                if frame.stream_id not in self._fragmented_frames:
                    self._fragmented_frames = []
                self._fragmented_frames.append(frame)
            else:
                if frame.stream_id not in self._fragmented_frames:
                    self._payload_subject.on_next(frame)
                else:
                    fragmented_frames = self._fragmented_frames[frame.stream_id]
                    del self._fragmented_frames[frames.stream_id]

        elif isinstance(frame, frames.RequestNFrame):
            errorframe = frames.ErrorFrame()
            errorframe.stream_id = frame.stream_id
            errorframe.error_code = frames.ErrorCodes.APPLICATION_ERROR
            errorframe.error_data = b"Currently does not support Backpressure."
            self.socket.send_frame(errorframe.to_bytes())

        elif isinstance(frame, frames.RequestFNF):
            def runner(eventloop, state):
                self.on_fire_and_forget(frame)
            self._scheduler.invoke_action(runner, state=None)

        elif isinstance(frame, frames.CancelFrame):
            self._cancel_subject.on_next(frame)

    def _setup_basic_observer(self):

        def errorSubscriber(error: frames.ErrorFrame):
            self._log.error('Received ErrorFrame. Code: {} Message: {}'.format(
                error.error_code, error.error_data))
            if error.stream_id == 0:
                self._handleStreamZeroErrors(error)

        self._connection_error_subject.pipe(op.observe_on(
            self._scheduler)).subscribe(on_next=errorSubscriber)

        def keep_alive_subscriber(keepalive: frames.KeepAliveFrame):
            # self._log.debug("Received Keepalive frame. Respondflag: {}".format(
            #     keepalive.respond_flag))
            if keepalive.respond_flag == True:
                answer = frames.KeepAliveFrame()
                answer.last_received_position = self.socket.get_recv_position()
                answer.respond_flag = False
                answer.data = keepalive.data
                self.socket.send_frame(answer.to_bytes())

        self._keepalive_subject.pipe(op.observe_on(self._scheduler)).subscribe(
            on_next=keep_alive_subscriber)

    def _errors_and_teardown(self, stream_id):
        def final_action():
            self._used_stream_ids.remove(stream_id)
        application_error = self._application_error_subject.pipe(
            op.filter(lambda error: error.stream_id == stream_id),
            op.map(_wrap_throw_error_frame),
        )

        return rx.pipe(
            op.observe_on(self._scheduler),
            op.take_until(self._socket_closed_subject),
            op.materialize(),
            # Throws error if socket closes
            op.merge(self._socket_closed_thrower),
            # Throws error on Application error for this stream
            op.merge(application_error),
            op.dematerialize(),
            op.finally_action(
                lambda: final_action()),
            op.subscribe_on(self._scheduler),
        )

    def _handle_socket_closed(self, error):
        if error != None:
            self._log.error(
                "Underlying Socket closed because of error{}".format(error))
            self._socket_closed_subject.on_next(error)
        else:
            self._socket_closed_subject.on_next(
                Exception("Socket closed without providing error message"))

    def _handleStreamZeroErrors(self, error: frames.ErrorFrame):
        self._log.debug(
            "Error with StreamID=0. This will most likely close the connect!")

    def _get_new_stream_id(self) -> int:
        self._stream_id_lock.acquire(blocking=True)
        while True:
            if self._last_stream_id == 0:
                stream_id = 1
                self._last_stream_id = 1
            else:
                self._last_stream_id += 2
                stream_id = self._last_stream_id
            if stream_id not in self._used_stream_ids:
                self._used_stream_ids.append(stream_id)
                break
        self._stream_id_lock.release()

        return stream_id
