from .common import RSocketConfig
from .transport import AbstractTransport
from .connection import ClientConnection, ResumableClientConnection
from .handler import request_response_pipe, request_stream_pipe
import rsockets2.executor as executors
import rsockets2.frames as frames
import rx
import rx.operators as op
import rx.scheduler
import logging


class RSocketClient(object):

    def __init__(self,
                 config: RSocketConfig,
                 transport: AbstractTransport,
                 default_scheduler: rx.scheduler.scheduler.Scheduler = rx.scheduler.ThreadPoolScheduler(
                     100)
                 ):
        super().__init__()

        self._log = logging.getLogger("rsockets2.RSocketClient")
        if config.resume_support == True:
            self._connection = ResumableClientConnection(transport, config)
        else:
            self._connection = ClientConnection(transport, config)
        self.on_request_response: typing.Callable[[
            frames.RequestResponse], rx.Observable] = None
        self.on_request_stream: typing.Callable[[
            frames.RequestStream], rx.Observable] = None
        self.on_fire_and_forget: typing.Callable[[
            frames.RequestFNF], None] = None

        self._scheduler = default_scheduler

        self._setup_request_handler()

    def open(self):
        self._connection.open()

    def close(self):
        self._connection.close()

    def request_response(self, meta_data, data) -> rx.Observable:

        request = frames.RequestResponse()
        request.stream_id = self._connection.get_new_stream_id()
        if isinstance(meta_data, str):
            request.meta_data = meta_data.encode('UTF-8')
        else:
            request.meta_data = meta_data
        if isinstance(data, str):
            request.request_data = data.encode('UTF-8')
        else:
            request.request_data = data

        return executors.request_response_executor(self._connection, request).pipe(
            op.observe_on(self._scheduler),
            self._errors_and_teardown(request.stream_id),
            op.subscribe_on(self._scheduler)
        )

    def request_stream(self, meta_data, data) -> rx.Observable:
        request = frames.RequestStream()
        request.stream_id = self._connection.get_new_stream_id()
        request.initial_request = 100000
        if isinstance(meta_data, str):
            request.meta_data = meta_data.encode('UTF-8')
        else:
            request.meta_data = meta_data
        if isinstance(data, str):
            request.request_data = data.encode('UTF-8')
        else:
            request.request_data = data

        return executors.request_stream_executor(self._connection, request).pipe(
            op.observe_on(self._scheduler),
            self._errors_and_teardown(request.stream_id),
            op.subscribe_on(self._scheduler)
        )

    def fire_and_forget(self, meta_data, data) -> rx.Observable:
        def action():
            frame = frames.RequestFNF()
            frame.meta_data = meta_data
            frame.request_data = data
            frame.stream_id = self._connection.get_new_stream_id()
            self._connection.queue_frame(frame)
        return rx.from_callable(lambda: action(), scheduler=self._scheduler).pipe(op.ignore_elements())

    def _setup_request_handler(self):
        def on_next(frame: frames.Frame_ABC):
            if isinstance(frame, frames.RequestFNF):
                self._fire_and_forget_listener(frame)
            elif isinstance(frame, frames.RequestResponse):
                self._request_response_listener(frame)
            elif isinstance(frame, frames.RequestStream):
                self._request_stream_listener(frame)
            else:
                pass

        self._connection.recv_observable().pipe(op.observe_on(self._scheduler)
                                                ).subscribe(on_next=lambda x: on_next(x))

    def _request_response_listener(self, request):
        if self.on_request_response == None:
            self._log.debug(
                "Received Request Response but no handler registered!")
            self._connection.queue_frame(frames.ErrorFrame.from_info(
                "No Request Response Handler!", stream_id=request.stream_id))
        self.on_request_response(request).pipe(
            op.observe_on(self._scheduler),
            request_response_pipe(
                request.stream_id, self._connection)).subscribe(scheduler=self._scheduler)

    def _request_stream_listener(self, request):
        if self.on_request_response == None:
            self._log.debug(
                "Received Request Stream but no handler registered!")
            self._connection.queue_frame(frames.ErrorFrame.from_info(
                "No Request Stream Handler!", stream_id=request.stream_id))
        self.on_request_response(request).pipe(
            op.observe_on(self._scheduler),
            request_stream_pipe(
                request.stream_id, self._connection)).subscribe(scheduler=self._scheduler)

    def _fire_and_forget_listener(self, request):
        if self.on_request_response == None:
            self._log.debug(
                "Received Fire and Forget but no handler registered!")
        self.on_fire_and_forget(request)

    def _errors_and_teardown(self, stream_id):

        def _wrap_throw_error_frame(frame: frames.ErrorFrame):
            raise Exception(
                'Application Error. Message: "{}"'.format(frame.error_data))

        application_error = self._connection.recv_observable_filter_type(frames.ErrorFrame).pipe(
            op.filter(lambda error: error.stream_id == stream_id),
            op.map(_wrap_throw_error_frame),
        )

        def final_action():
            self._connection.free_stream_id(stream_id)

        return rx.pipe(
            op.materialize(),
            # Throws error on Application error for this stream
            op.merge(application_error),
            op.dematerialize(),
            op.finally_action(
                lambda: final_action()),
        )
