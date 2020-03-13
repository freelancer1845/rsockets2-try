from .abstract_transport import AbstractTransport
from queue import Queue
import logging
import websocket
import threading


class WebsocketTransport(AbstractTransport):

    def __init__(self, url: str):
        super().__init__()
        self._log = logging.getLogger("rsockets2.transport.WebsocketTransport")
        self._url = url
        self._ws: websocket.WebSocketApp
        self._message_buffer = []

        self._recv_queue = Queue(1000)

        self._last_error = None
        self._closed = False
        self._runner: threading.Thread

    def connect(self):
        self._log.debug("Trying to open Websocket at '{}'".format(self._log))

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

        ws = websocket.WebSocketApp(
            url=self.url,
            on_data=self._on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )

        self._runner = threading.Thread(
            name="RSocket-Websocket", target=ws.run_forever, daemon=True)
        self._runner.start()

        open_event.wait(30.0)
        if connect_error != None:
            raise connect_error
        self._ws = ws
        self._ws.on_error = self._on_error
        self._ws.on_close = self._on_close

    def disconnect(self):
        if self._ws == None:
            raise ValueError(
                "Trying to disconnect a websocket that never successfully connected!")
        self._ws.close()

    def _on_error(self, error):
        self._log.debug("Error in Websocket. {}".format(error))
        self._last_error = error

    def _on_close(self):
        self._closed = True

    def _send_bytes(self, frameBytes):
        self._check_closed_and_error()
        self._ws.send(frameBytes, opcode=websocket.ABNF.OPCODE_BINARY)

    def _recv_bytes(self):
        self._check_closed_and_error()
        data = self._recv_queue.get(block=True)
        return data

    def _check_closed_and_error(self):
        if self._ws == None:
            raise ValueError(
                "Trying to access a websocket that never successfully connected!")
        if self._closed == True and self._last_error == None:
            raise ConnectionError("Websocket closed. Unknown Error.")
        elif self._last_error != None:
            raise ConnectionError(
                "Websocket closed. Error: {}".format(self._last_error))

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

            self._recv_queue.put(final_message)
