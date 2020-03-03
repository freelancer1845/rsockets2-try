
import queue
import typing


class Socket_ABC:

    def __init__(self):
        super().__init__()
        def callback_throw(message: str):
            def _inner(*args, **kwargs):
                raise Exception(message)

            return _inner

        self._receive_handler = callback_throw("Receive Handler Not Set!")
        self._socket_closed_handler = callback_throw(
            "Socket Closed Handler Not Set!")

        self._send_position = 0
        self._recv_position = 0

    def send_frame(self, data: bytes):
        pass

    def set_receive_handler(self, callback):
        self._receive_handler = callback

    def set_socket_closed_handler(self, callback):
        self._socket_closed_handler = callback

    def close(self):
        pass

    def get_recv_position(self) -> int:
        return self._recv_position
