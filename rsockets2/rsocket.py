from .socket import RTcpSocket, Socket_ABC
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

MAJOR_VERSION = 1
MINOR_VERSION = 0


class SocketType(Enum):
    TCP_SOCKET = auto()


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
        self._scheduler = rx.scheduler.ThreadPoolScheduler(10)

        self._last_stream_id = 0
        self._used_stream_ids = []
        self._stream_id_lock = threading.Lock()

        # Subjects
        self._keepalive_subject = rx.subject.Subject()
        self._connection_error_subject = rx.subject.Subject()
        self._payload_subject = rx.subject.Subject()
        self._application_error_subject = rx.subject.Subject()

        self._fragmented_frames = {}

        # Callbacks

        # The callback takes the Request_Response frame and must return an Observable emitting 'bytes' objects
        self.on_request_response: typing.Callable[[
            frames.Payload], rx.Observable] = None

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

    def open(self):
        if self.socket_type == SocketType.TCP_SOCKET:
            self._createTcpSocket()
        self._negotiate()

    def close(self):
        self._keepalive_run = True
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

        def subscription(observer, scheduler):
            if scheduler is None:
                scheduler = self._scheduler

            stream_subscription = self._payload_subject.pipe(
                op.observe_on(self._scheduler),
                op.filter(lambda payload: payload.stream_id ==
                          request.stream_id),
                op.take(1),
                op.map(lambda payload: payload.payload),
                op.observe_on(scheduler),
                op.finally_action(
                    lambda: self._used_stream_ids.remove(request.stream_id))
            ).subscribe(on_next=lambda x: observer.on_next(x), on_error=lambda err: observer.on_error(err), on_completed=lambda: observer.on_completed())

            def error_handler(error):
                observer.on_error(
                    Exception("ErrorMessage: {}".format(error.error_data)))
                stream_subscription.dispose()
            self._application_error_subject.pipe(
                op.observe_on(self._scheduler),
                op.filter(lambda error: error.stream_id == request.stream_id),
                op.take(1),
                op.observe_on(scheduler),
            ).subscribe(on_next=lambda x: error_handler(x), on_error=lambda x: print(x))
            self.socket.send_frame(request.to_bytes())

        return rx.create(subscription)

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

        def subscription(observer, scheduler):
            if scheduler is None:
                scheduler = self._scheduler

            stream_subscription = self._payload_subject.pipe(
                op.observe_on(self._scheduler),
                op.filter(lambda payload: payload.stream_id ==
                          request.stream_id),
                op.take_while(
                    lambda payload: payload.complete == False, inclusive=True),
                op.filter(lambda payload: payload.next_present == True),
                op.map(lambda payload: payload.payload),
                op.finally_action(
                    lambda: self._used_stream_ids.remove(request.stream_id)),
                op.observe_on(scheduler)
            ).subscribe(on_next=lambda x: observer.on_next(x), on_error=lambda err: observer.on_error(err), on_completed=lambda: observer.on_completed())

            def error_handler(error):
                observer.on_error(
                    Exception("ErrorMessage: {}".format(error.error_data)))
                stream_subscription.dispose()
            self._application_error_subject.pipe(
                op.observe_on(self._scheduler),
                op.filter(lambda error: error.stream_id == request.stream_id),
                op.take(1),
                op.observe_on(scheduler),
            ).subscribe(on_next=error_handler, on_error=lambda x: print(x))

            self.socket.send_frame(request.to_bytes())
        return rx.create(subscription)

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
        tcpSocket.connect(self.hostname, self.port)
        self.socket = tcpSocket

    def _handleFrame(self, rawFrame: bytes):
        frame = self._parser.parseFrame(data=rawFrame)
        if isinstance(frame, frames.KeepAliveFrame):
            self._keepalive_subject.on_next(frame)
        if isinstance(frame, frames.ErrorFrame):
            if frame.error_code == frames.ErrorCodes.APPLICATION_ERROR:
                self._application_error_subject.on_next(frame)
            else:
                self._connection_error_subject.on_next(frame)
        if isinstance(frame, frames.RequestResponse):
            if self.on_request_response == None:
                self._log.error(
                    "No Request Response Handler created. This should be reported to the requester!")
            else:
                def on_next(value):
                    answer = frames.Payload()
                    answer.stream_id = frame.stream_id
                    answer.follows = False
                    answer.complete = True
                    answer.next_present = True
                    answer.payload = value
                    answer.meta_data = bytes(0)
                    self.socket.send_frame(answer.to_bytes())

                def on_error(value):
                    error = frames.ErrorFrame()
                    error.stream_id = frame.stream_id
                    error.error_code = frames.ErrorCodes.APPLICATION_ERROR
                    if isinstance(value, Exception):
                        error.error_data = str(value).encode("ASCII")
                    else:
                        error.error_data = value
                    self.socket.send_frame(error.to_bytes())

                def on_complete():
                    pass

                self.on_request_response(frame).pipe(op.observe_on(self._scheduler)).subscribe(
                    on_next=on_next, on_error=on_error, on_completed=on_complete)
        if isinstance(frame, frames.Payload):
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
