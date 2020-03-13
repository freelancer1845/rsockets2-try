from .abstract_connection import AbstractConnection
from ..transport import AbstractTransport
from ..frames import SetupFrame, Frame_ABC
from ..common import RSocketConfig
from .keepalive_support import KeepaliveSupport
import threading
import rx
import rx.subject
from queue import PriorityQueue
import logging

class PriorityEntry(object):

    def __init__(self, priority, data):
        self.data = data
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority

class ClientConnection(AbstractConnection):

    def __init__(self, transport: AbstractTransport, config: RSocketConfig):
        super().__init__()
        self._log = logging.getLogger("rsockets2.connection.ClientConnection")
        self._transport = transport
        self._config = config
        self._keepalive_support: KeepaliveSupport = None

        self._send_thread = threading.Thread(
            name="RSocket-Client-Send-Thread", daemon=True, target=self._send_loop)
        self._recv_thread = threading.Thread(
            name="RSocket-Client-Recv-Thread", daemon=True, target=self._recv_loop)

        self._running = False

        self._send_queue = PriorityQueue()
        self._recv_subject = rx.subject.Subject()

    def open(self):
        self._running = True

        self._transport.connect()
        self._recv_thread.start()
        self._negotiate_connection()
        self._send_thread.start()

    def _negotiate_connection(self):
        setupFrame = SetupFrame.from_config(self._config)

        self._transport.send_frame(setupFrame)

        self._keepalive_support = KeepaliveSupport(self, self._config)
        self._keepalive_support.start()

    def queue_frame(self, frame: Frame_ABC):
        if frame.stream_id == 0:
            self._send_queue.put(PriorityEntry(10, frame))
        else:
            self._send_queue.put(PriorityEntry(100, frame))

    def recv_observable(self):
        return self._recv_subject

    def close(self):
        self._running = False
        if self._keepalive_support != None:
            self._keepalive_support.stop()

        self._transport.disconnect()

    def _send_loop(self):
        try:
            while self._running:
                data = self._send_queue.get().data

                self.increase_send_position(len(data))
                self._transport.send_frame(data)

        except Exception as err:
            if self._running == True:
                self._log.debug(
                    "Error in send_loop: {}".format(err), exc_info=True)
            else:
                self._log.error(
                    "Error in send_loop: {}".format(err), exc_info=True)

    def _recv_loop(self):
        try:
            while self._running:
                frame = self._transport.recv_frame()
                self.increase_recv_position(len(frame))
                self._recv_subject.on_next(frame)

        except Exception as err:
            self._recv_subject.on_error(err)
            if self._running == True:
                self._log.debug(
                    "Error in recv_loop: {}".format(err), exc_info=True)
            else:
                self._log.error(
                    "Error in recv_loop: {}".format(err), exc_info=True)
