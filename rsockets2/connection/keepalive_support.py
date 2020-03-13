import threading
import logging
from .abstract_connection import AbstractConnection
from ..frames import KeepAliveFrame
from ..common import RSocketConfig
import rx
import rx.operators as op
import time

class KeepaliveSupport(object):

    count = 0

    def __init__(self, connection: AbstractConnection, config: RSocketConfig):
        super().__init__()
        self._connection = connection
        self._config = config
        self.count += 1
        self._runner = threading.Thread(
            name="RSocket-Keepalive-Support-{}".format(self.count), daemon=True, target=self._run)
        self._running = False
        self._log = logging.getLogger("rosckets2.connection.KeepaliveSupport")

    def _run(self):
        try:
            while self._running:
                frame = KeepAliveFrame()
                frame.last_received_position = self._connection.last_received_position
                frame.respond_flag = True
                frame.data = bytes(0)
                self._connection.queue_frame(frame)
                time.sleep(self._config.keepalive_time / 1000.0 * 0.45)

        except Exception as exception:
            self._log.debug(
                "Keepalive support crashed. Exception: {}".format(exception))

    def start(self):
        self._running = True
        self._runner.start()

    def stop(self):
        self._running = False

    def keepalive_operator(self):
        """
            Must be used in a pipe that provides KeepaliveFrames!
            Will throw timeout exception if keepalive frames are not being send or responded properly.
            Will automatically answer keepalive messages from the other side
        """
        def handle_frame(frame: KeepAliveFrame):
            if not isinstance(frame, KeepAliveFrame):
                raise ValueError(
                    "The keepalive operator can only handle KeepAliveFrames!!!")
            if frame.respond_flag == True:
                answer = KeepAliveFrame()
                answer.last_received_position = self._connection.last_received_position
                answer.respond_flag = False
                answer.data = frame.data
                self._connection.queue_frame(answer)

        def handle_error():
            self._log.debug("Stopping keepalive thread because of error!")
            self._running = False

        def handle_complete():
            self._log.debug(
                "Stopping keepalive thread because keepalive frame stream completed")
            self._running = False
        return rx.pipe(
            op.map(lambda frame: handle_frame(frame)),
            op.timeout(op.timedelta(milliseconds=self._config.keepalive_time)),
            op.do_action(on_error=lambda err: handle_error(),
                         on_completed=lambda: handle_complete())
        )
