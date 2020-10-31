
import rx
import rx.operators as op

import rsockets2.frames as frames

from ..connection import AbstractConnection

import logging

log = logging.getLogger('rsockets2.handle.request_stream')


def request_stream_pipe(stream_id: int, connection: AbstractConnection):
    def on_next(value):
        if isinstance(value, tuple):
            meta_data = value[0]
            data = value[1]
        else:
            meta_data = bytes(0)
            data = value
        answer = frames.Payload()
        answer.stream_id = stream_id
        answer.follows = False
        answer.complete = False
        answer.next_present = True
        answer.payload = data
        answer.meta_data = meta_data
        connection.queue_frame(answer)

    def on_error(error):
        log.debug(error, exc_info=True)
        error_frame = frames.ErrorFrame()
        error_frame.stream_id = stream_id
        error_frame.error_code = frames.ErrorCodes.APPLICATION_ERROR
        if isinstance(error, Exception):
            error_frame.error_data = str(error).encode("ASCII")
        else:
            error_frame.error_data = error
        connection.queue_frame(error_frame)

    def on_completed():
        answer = frames.Payload()
        answer.stream_id = stream_id
        answer.follows = False
        answer.complete = True
        answer.next_present = False
        answer.payload = bytes(0)
        answer.meta_data = bytes(0)
        connection.send_frame(answer)

    return rx.pipe(
        op.take_until(
            connection.recv_observable_filter_type(frames.CancelFrame).pipe(
                op.filter(lambda f: f.stream_id == stream_id),
            )
        ),
        op.take_until(
            connection.destroy_observable()
        ),
        op.do_action(on_next=on_next, on_error=on_error,
                     on_completed=on_completed)
    )
