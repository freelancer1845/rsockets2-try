
import queue
import typing
from rsockets2.frames import Frame_ABC, KeepAliveFrame
from rsockets2.frames import FrameParser
from abc import ABC, abstractmethod
import threading
import logging


class Socket_ABC(ABC):

    def __init__(self, resume_support_enabled=False):
        super().__init__()
        self.resume_support_enabled = resume_support_enabled

        def callback_throw(message: str):
            def _inner(*args, **kwargs):
                raise Exception(message)

            return _inner
        self._log = logging.getLogger("rsockets2.socket.socket_abc")
        self._receive_handler_bytes = callback_throw(
            "Receive Handler Not Set!")
        self._socket_closed_handler = callback_throw(
            "Socket Closed Handler Not Set!")

        self._parser = FrameParser()
        self._send_position = 0
        self._recv_position = 0
        self._foreing_send_position = 0

        self._send_queue = queue.PriorityQueue(1000)
        self._data_send_store = {}

        self.__running = True
        self._send_thread = threading.Thread(
            name="RSocket-Send-Thread", daemon=True, target=self._send_loop)
        self._send_thread.start()

    def send_frame(self, frame: Frame_ABC):
        if frame.stream_id == 0:
            self._send_queue.put((10, frame.to_bytes()))
        else:
            self._send_queue.put((100, frame.to_bytes()))

    @abstractmethod
    def _send_frame_internal(self, data: bytes):
        raise NotImplementedError()

    def _receive_wrapper(self, data: bytes):
        self._recv_position += len(data)
        frame = self._parser.parseFrame(data)

        if isinstance(frame, KeepAliveFrame):
            self._foreing_send_position = frame.last_received_position
        return frame

    def set_receive_handler(self, callback: typing.Callable[[Frame_ABC], None]):
        # Wrap the callback into custom handler, so  that it receives the frame instead of the bytes
        self._receive_handler_bytes = lambda data: callback(
            self._receive_wrapper(data))

    def set_socket_closed_handler(self, callback):
        self._socket_closed_handler = callback

    def close(self):
        self.__running = False
        self._close_internal()

    @abstractmethod
    def _close_internal(self):
        raise NotImplementedError()

    def get_recv_position(self) -> int:
        return self._recv_position

    def _send_loop(self):
        try:
            while self.__running:
                data = self._send_queue.get()[1]

                self._send_frame_internal(data)

                self._send_position += len(data)
                if self.resume_support_enabled == True:
                    self._data_send_store[self._send_position] = data

                    self._clear_data_send_store()

        except Exception as err:
            if self.__running == True:
                self._log.debug(
                    "Error in send_loop: {}".format(err), exc_info=True)
            else:
                self._log.error(
                    "Error in send_loop: {}".format(err), exc_info=True)

    def _clear_data_send_store(self):
        keys_to_remove = []
        for key in self._data_send_store.keys():
            if key < self._foreing_send_position:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            del self._data_send_store[key]