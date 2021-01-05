
from rsockets2.core.frames.frame_header import FrameHeader, FrameType
from rx.core.typing import Observable
from .default_connection import DefaultConnection

import threading
import logging
from ..frames import KeepaliveFrame
import rx
import rx.operators as op
import time
from queue import Empty, Queue


class KeepaliveSupport(object):

    count = 0

    def __init__(self, connection: DefaultConnection, keepalive_time: int, max_lifetime: int):
        super().__init__()
        self._connection = connection
        self._keepalive_time = keepalive_time
        self._max_lifetime = max_lifetime
        self._running = False
        self._opposite_keepalive_requests = Queue()
        self._log = logging.getLogger(
            "rosckets2.core.connection.KeepaliveSupport")

    def _run(self):
        try:
            time_last_keepalive_send = time.time() * 1000. - self._keepalive_time
            while self._running:

                now = time.time() * 1000.
                if now - time_last_keepalive_send > self._keepalive_time * 0.8:
                    frame = KeepaliveFrame.create_new(
                        True, self._connection.stream_position.last_received_position, None)
                    self._connection.queue_frame(frame)
                    time_last_keepalive_send = now
                try:
                    opp_keepalive = self._opposite_keepalive_requests.get(
                        False)
                    if KeepaliveFrame.respond_with_keepalive(opp_keepalive):
                        answer = KeepaliveFrame.create_new(
                            False, self._connection.stream_position.last_received_position, KeepaliveFrame.data(opp_keepalive))
                        self._connection.queue_frame(answer)
                except Empty:
                    pass
                time.sleep(self._keepalive_time / 20. / 1000.)

        except Exception as exception:
            self._log.debug(
                "Keepalive support crashed. Exception: {}".format(str(exception)))

    def start(self):
        self._running = True
        self.count += 1
        self._connection.listen_on_stream(0).pipe(
            op.filter(lambda frame: FrameHeader.stream_id_type_and_flags(
                frame)[1] == FrameType.KEEPALIVE)
        ).subscribe(on_next=lambda x: self._opposite_keepalive_requests.put(x))
        self._runner = threading.Thread(
            name="RSocket-Keepalive-Support-{}".format(self.count), daemon=True, target=self._run)
        self._runner.start()

    def stop(self):
        self._running = False
