from .common import RSocketConfig
from .transport import AbstractTransport
from .connection import ClientConnection
import rsockets2.executor as executors
import rsockets2.frames as frames
import rx
import rx.operators as op


class RSocketClient(object):

    def __init__(self, config: RSocketConfig, transport: AbstractTransport):
        super().__init__()

        self._connection = ClientConnection(transport, config)

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

        return executors.request_response_executor(self._connection, request).pipe(self._errors_and_teardown(request.stream_id))

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
  
        return executors.request_stream_executor(self._connection, request).pipe(self._errors_and_teardown(request.stream_id))

    def fire_and_forget(self, meta_data, data) -> rx.Observable:
        def action():
            frame = frames.RequestFNF()
            frame.meta_data = meta_data
            frame.request_data = data
            frame.stream_id = self._connection.get_new_stream_id()
            self._connection.queue_frame(frame)
        return rx.from_callable(lambda: action(), scheduler=self._scheduler).pipe(op.ignore_elements())

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
