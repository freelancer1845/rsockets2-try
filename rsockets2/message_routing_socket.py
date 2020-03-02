from rsockets2 import SocketType, RSocket
import rsockets2.frames as frames
import logging
import json
import typing
import rx
import rx.operators as op


class RMessageSocket(object):

    def __init__(self,
                 socket_type: SocketType,
                 keepalive=30000,
                 maxlive=10000,
                 data_mime_type=b"application/json",
                 *args,
                 **kwargs):
        super().__init__()

        self._log = logging.getLogger("rsockets2.message")

        self.rsocket = RSocket(socket_type, keepalive, maxlive,
                               b"message/x.rsocket.routing.v0", data_mime_type, *args, **kwargs)

        self.decoder = lambda x: x
        self.encoder = lambda x: x

        if data_mime_type == b"application/json":
            self.decoder = lambda x: json.loads(x, encoding='UTF-8')
            self.encoder = lambda x: json.dumps(x).encode('UTF-8')

        self._stream_handler = {}
        self._request_handler = {}
        self._fire_and_forget_handler = {}

        self._setup_handler()

    def open(self):
        self.rsocket.open()

    def close(self):
        self.rsocket.close()

    def register_stream_handler(self, route: str, stream_creater: typing.Callable[[typing.Union[bytes, typing.Dict]], rx.Observable]):
        self._stream_handler[route] = stream_creater

    def register_request_handler(self, route: str, stream_creater: typing.Callable[[typing.Union[bytes, typing.Dict]], rx.Observable]):
        self._request_handler[route] = stream_creater

    def register_fire_and_forget_handler(self, route: str, handler: typing.Callable[[typing.Union[bytes, typing.Dict]], rx.Observable]):
        self._fire_and_forget_handler[route] = handler

    def request_response(self, route: str, data: typing.Union[bytes, typing.Dict] = None) -> rx.Observable:
        data = self._check_data_none(data)
        return self.rsocket.request_response(meta_data=route.encode('ASCII'), data=self.encoder(data))

    def request_stream(self, route: str, data: typing.Union[bytes, typing.Dict] = None) -> rx.Observable:
        data = self._check_data_none(data)
        return self.rsocket.request_stream(meta_data=route.encode('ASCII'), data=self.encoder(data))

    def fire_and_forget(self, route: str, data: typing.Union[bytes, typing.Dict] = None) -> rx.Observable:
        data = self._check_data_none(data)
        return self.rsocket.fire_and_forget(meta_data=route.encode('ASCII'), data=self.encoder(data))

    def _setup_handler(self):
        self.rsocket.on_fire_and_forget = self._on_fire_and_forget
        self.rsocket.on_request_stream = self._on_request_stream
        self.rsocket.on_request_response = self._on_request_response

    def _on_fire_and_forget(self, frame: frames.RequestFNF):
        route_name = self._get_route_name(frame.meta_data)
        if route_name in self._fire_and_forget_handler:
            self._fire_and_forget_handler[route_name](
                self.decoder(frame.request_data))
        else:
            self._log.warn(
                "Received FNF for Unknown Destination '{}'".format(route_name))

    def _on_request_stream(self, frame: frames.RequestStream):
        route_name = self._get_route_name(frame.meta_data)
        if route_name in self._stream_handler:
            return self._stream_handler[route_name](self.decoder(frame.request_data)).pipe(op.map(self.encoder))
        else:
            return rx.throw(Exception("Unknown Destination '{}'".format(route_name)))

    def _on_request_response(self, frame: frames.RequestResponse):
        route_name = self._get_route_name(frame.meta_data)
        if route_name in self._request_handler:
            return self._request_handler[route_name](
                self.decoder(frame.request_data)).pipe(op.map(self.encoder))
        else:
            return rx.throw(Exception("Unknown Destination '{}'".format(route_name)))

    def _get_route_name(self, meta_data: bytes):
        routeLength = meta_data[0]
        return meta_data[1:(routeLength + 1)].decode('UTF-8')

    def _check_data_none(self, data):
        if data == None:
            if self.rsocket.data_mime_type == b'application/json':
                data = {}
            else:
                data = bytes(0)

        return data
