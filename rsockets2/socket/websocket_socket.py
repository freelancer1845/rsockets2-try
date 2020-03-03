from .socket_abc import Socket_ABC
from abc import abstractmethod
import asyncio
import websocket
import typing
import logging
import threading


class RWebsocketSocket(Socket_ABC):

    def __init__(self, url: str):
        super().__init__()
        self._ws: websocket.WebSocketApp
        self.url = url

        self._receive_handler = None
        self._log = logging.getLogger("rsockets2.socket.websocket")
        self._runner: threading.Thread
        self._send_position = 0
        self._recv_position = 0
        self._send_lock = threading.Lock()
        self._recv_position_lock = threading.Lock()

        self._message_buffer = []

    def open(self):
        if self._receive_handler == None:
            raise ValueError("Receive Handler must be set!")

        open_event = threading.Event()

        def on_open(ws):
            open_event.set()

        self._ws = websocket.WebSocketApp(
            url=self.url,
            on_data=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=on_open,

        )
        self._runner = threading.Thread(
            name="RSocket-Websocket", target=self._ws.run_forever, daemon=True)
        self._runner.start()

        open_event.wait(2.0)

    @abstractmethod
    def send_frame(self, data):
        try:
            self._send_lock.acquire()
            self._send_position += len(data)
            self._ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)
        finally:
            self._send_lock.release()

    @abstractmethod
    def set_receive_handler(self, callback: typing.Callable[[bytes], None]):
        self._receive_handler = callback

    @abstractmethod
    def close(self):
        self._ws.close()

    @abstractmethod
    def get_recv_position(self):
        return self._recv_position

    def _on_message(self, message, data_type, cont_flag):

        if cont_flag == False:
            self._message_buffer.append(message)
        else:
            if len(self._message_buffer) > 0:
                final_message = bytes(0)
                for msg in self._message_buffer:
                    final_message += msg
                self._message_buffer.clear()
            else:
                final_message = message
            try:
                self._recv_position_lock.acquire()
                self._recv_position += len(final_message)
            finally:
                self._recv_position_lock.release()
            self._receive_handler(final_message)

    def _on_error(self, ws, error):
        self._log.error("Unexpected error on websocket {}".format(error))

    def _on_close(self, ws):
        self._log.info("Websocket closed {}")
