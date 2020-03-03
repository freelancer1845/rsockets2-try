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

        self._log = logging.getLogger("rsockets2.socket.websocket")
        self._runner: threading.Thread
        self._send_lock = threading.Lock()
        self._recv_position_lock = threading.Lock()

        self._message_buffer = []


    def open(self):
        if self._receive_handler == None:
            raise ValueError("Receive Handler must be set!")

        open_event = threading.Event()
        connect_error = None

        def on_open(ws: websocket.WebSocket):
            open_event.set()

        def on_error(ws, error):
            nonlocal connect_error
            connect_error = error
            open_event.set()

        def on_close(ws):
            pass

        self._ws = websocket.WebSocketApp(
            url=self.url,
            on_data=self._on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,

        )


        self._runner = threading.Thread(
            name="RSocket-Websocket", target=self._ws.run_forever, daemon=True)
        self._runner.start()

        open_event.wait(30.0)
        if connect_error != None:
            raise connect_error
        self._ws.on_error = self._on_error
        self._ws.on_close = self._on_close

    @abstractmethod
    def send_frame(self, data):
        try:
            self._send_lock.acquire()
            self._send_position += len(data)
            self._ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)
        finally:
            self._send_lock.release()

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

    def _on_error(self, error):
        self._log.error("Unexpected error on websocket {}".format(error), exc_info=error)

    def _on_close(self):
        self._log.info("Websocket closed")
        self._socket_closed_handler(None)
