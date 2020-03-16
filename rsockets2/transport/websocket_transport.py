from .abstract_transport import AbstractTransport
from queue import Queue
import logging
import websocket
import threading
import time


class WebsocketTransport(AbstractTransport):

    def __init__(self, url: str):
        super().__init__()
        self._log = logging.getLogger("rsockets2.transport.WebsocketTransport")
        self._url = url
        self._message_buffer = []
        self._ws: websocket.WebSocket = None

        self._recv_queue = Queue(1000)

        self._last_error = None
        self._closed = False
        self._runner: threading.Thread

    def connect(self):
        self._log.debug("Trying to open Websocket at '{}'".format(self._url))

        ws = websocket.WebSocket(enable_multithread=False)
        self._ws = ws

        self._ws.connect(self._url)

    def disconnect(self):
        if self._ws == None:
            raise ValueError(
                "Trying to disconnect a websocket that never successfully connected!")
        try:
            self._ws.close()
        except Exception as error:
            self._log.error("Execption while disconnecting", exc_info=True)
            raise error

    def _on_error(self, error):
        self._log.debug("Error in Websocket. {}".format(error))
        self._last_error = error

    def _on_close(self):
        self._closed = True

    def _send_bytes(self, frameBytes):
        try:
            self._ws.send_binary(frameBytes)
        except websocket.WebSocketException as error:
            raise ConnectionError(error)
        # self._check_closed_and_error()
        # self._ws.send(frameBytes, opcode=websocket.ABNF.OPCODE_BINARY)

    def _recv_bytes(self):
        try:
            code, frame = self._ws.recv_data_frame()

            if code == websocket.ABNF.OPCODE_BINARY or code == websocket.ABNF.OPCODE_TEXT:
                return frame.data
            else:
                raise ConnectionError("Websocket error. Code: {}".format(code))
        except websocket.WebSocketTimeoutException as error:
            raise TimeoutError(error)
        except websocket.WebSocketException as error:
            raise ConnectionError(error)
        # self._check_closed_and_error()
        # data = self._recv_queue.get(block=True)
        # return data

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
