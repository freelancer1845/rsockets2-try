from ..connection import AbstractConnection
import rx
import rx.core
import rx.operators as op
import functools
import rx.scheduler
import threading
from rsockets2.frames import RequestResponse, RequestStream, Payload


def _request_response_executor(
        connection: AbstractConnection,
        frame: RequestResponse,
        map_to_payload,
        is_single_element,
        observer, scheduler):

    if scheduler is None:
        scheduler = rx.scheduler.ThreadPoolScheduler(max_workers=20)
    response_obs = connection.recv_observable_filter_type(Payload).pipe(
        op.observe_on(scheduler),
        op.filter(lambda payload: payload.stream_id == frame.stream_id),
    )

    if is_single_element == True:
        response_obs = response_obs.pipe(op.take(1))

    if map_to_payload == True:
        response_obs = response_obs.pipe(op.map(lambda data: data.payload))

    disposable = response_obs.subscribe(on_next=lambda x: observer.on_next(
        x), on_error=lambda err: observer.on_error(err), on_completed=lambda: observer.on_completed())
    connection.queue_frame(frame)
    return lambda: disposable.dispose()


def request_stream_executor(connection: AbstractConnection, frame: RequestStream, map_to_payload=True) -> rx.core.Observable:
    return rx.create(functools.partial(_request_response_executor, connection, frame, map_to_payload, False))


def request_response_executor(connection: AbstractConnection, frame: RequestResponse, map_to_payload=True) -> rx.core.Observable:
    return rx.create(functools.partial(_request_response_executor, connection, frame, map_to_payload, True))
