import rx
import rx.operators as op

import rsockets2.frames as frames

from ..connection import AbstractConnection


def request_response_pipe(stream_id: int, connection: AbstractConnection):
    is_complete = False
    def on_next(value):
        nonlocal is_complete
        if isinstance(value, tuple):
            meta_data = value[0]
            data = value[1]
        else:
            meta_data = bytes(0)
            data = value
        answer = frames.Payload()
        answer.stream_id = stream_id
        answer.follows = False
        answer.complete = True
        answer.next_present = True
        answer.payload = data
        answer.meta_data = meta_data
        if is_complete == True:
            raise RuntimeError("Request Response interaction completed before handling on_next value. This is a race condition!")
        is_complete = True
        connection.queue_frame(answer)

    def on_error(error):
        error_frame = frames.ErrorFrame()
        error_frame.stream_id = stream_id
        error_frame.error_code = frames.ErrorCodes.APPLICATION_ERROR
        if isinstance(error, Exception):
            error_frame.error_data = str(error).encode("ASCII")
        else:
            error_frame.error_data = error
        connection.queue_frame(error_frame)

    def on_completed():
        nonlocal is_complete
        if is_complete == False:
            answer = frames.Payload()
            answer.stream_id = stream_id
            answer.follows = False
            answer.complete = True
            answer.next_present = False
            connection.queue_frame(answer)


    return rx.pipe(
        op.take_until(
            connection.recv_observable_filter_type(frames.CancelFrame).pipe(
                op.filter(lambda f: f.stream_id == stream_id),
            ),

        ),
        op.take_until(
            connection.destroy_observable()
        ),
        op.do_action(on_next=on_next, on_error=on_error, on_completed=on_completed)
    )
