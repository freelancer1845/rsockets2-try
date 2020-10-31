from rsockets2.transport import AbstractTransport
from ..rsocket_client import RSocketClient
from ..common import RSocketConfig
import rsockets2.frames as frames
import logging
import json
import typing
import rx
import rx.operators as op
from .serialization import json_decoder, json_encoder


class RMessageClient(object):

    def __init__(self,
                 transport: AbstractTransport,
                 config: RSocketConfig,
                 default_scheduler: rx.scheduler.scheduler.Scheduler = None
                 ):
        super().__init__()

        self._log = logging.getLogger("rsockets2.RMessageClient")

        self.rsocket = RSocketClient(config, transport, default_scheduler)

        self.decoder = lambda x: x
        self.encoder = lambda x: x

        if config.data_mime_type == b"application/json":
            self.decoder = json_decoder
            self.encoder = json_encoder

        self._stream_handler = {}
        self._request_handler = {}
        self._fire_and_forget_handler = {}

        self._setup_handler()

    def open(self):
        self.rsocket.open()

    def close(self):
        self.rsocket.close()

    def register_stream_handler(self, route: str, stream_creater: typing.Callable[[typing.Union[bytes, typing.Dict]], rx.Observable], try_decode_request: bool = True):
        self._stream_handler[route] = [stream_creater, try_decode_request]

    def register_request_handler(self, route: str, stream_creater: typing.Callable[[typing.Union[bytes, typing.Dict]], rx.Observable], try_decode_request: bool = True):
        self._request_handler[route] = [stream_creater, try_decode_request]

    def register_fire_and_forget_handler(self, route: str, handler: typing.Callable[[typing.Union[bytes, typing.Dict]], rx.Observable], try_decode_request: bool = True):
        self._fire_and_forget_handler[route] = [handler, try_decode_request]

    def request_response(self, route: str, data: typing.Union[bytes, typing.Dict] = None, try_decode: bool = True) -> rx.Observable:
        data = self._check_data_none(data)
        if try_decode == True:
            decoder = self.decoder
        else:
            def decoder(x): return x
        return self.rsocket.request_response(meta_data=self._encode_route_name(route), data=self.encoder(data)).pipe(op.map(lambda response: decoder(response)))

    def request_stream(self, route: str, data: typing.Union[bytes, typing.Dict] = None, try_decode: bool = True) -> rx.Observable:
        data = self._check_data_none(data)
        if try_decode == True:
            decoder = self.decoder
        else:
            def decoder(x): return x
        return self.rsocket.request_stream(meta_data=self._encode_route_name(route), data=self.encoder(data)).pipe(op.map(lambda item: decoder(item)))

    def fire_and_forget(self, route: str, data: typing.Union[bytes, typing.Dict] = None) -> rx.Observable:
        data = self._check_data_none(data)
        return self.rsocket.fire_and_forget(meta_data=self._encode_route_name(route), data=self.encoder(data))

    def _setup_handler(self):
        self.rsocket.on_fire_and_forget = self._on_fire_and_forget
        self.rsocket.on_request_stream = self._on_request_stream
        self.rsocket.on_request_response = self._on_request_response

    def _on_fire_and_forget(self, frame: frames.RequestFNF):
        route_name = self._get_route_name(frame.meta_data)
        if route_name in self._fire_and_forget_handler:
            try:
                handler = self._fire_and_forget_handler[route_name]
                if handler[1] == True:
                    return handler[0](self.decoder(frame.request_data))
                else:
                    return handler[0](frame.request_data)

            except Exception as err:
                self._log.warn(
                    "Failed to decode Fire And Forget frame: {}".format(err), exc_info=True)

        else:
            self._log.warn(
                "Received FNF for Unknown Destination '{}'".format(route_name))

    def _on_request_stream(self, frame: frames.RequestStream):
        route_name = self._get_route_name(frame.meta_data)
        if route_name in self._stream_handler:
            handler = self._stream_handler[route_name]
            if handler[1] == True:
                return handler[0](self.decoder(frame.request_data)).pipe(op.map(lambda x: self.encoder(x)))
            else:
                return handler[0](frame.request_data).pipe(op.map(lambda x: self.encoder(x)))
        else:
            return rx.throw(Exception("Unknown Destination '{}'".format(route_name)))

    def _on_request_response(self, frame: frames.RequestResponse):
        route_name = self._get_route_name(frame.meta_data)
        if route_name in self._request_handler:
            handler = self._request_handler[route_name]
            if handler[1] == True:
                return handler[0](self.decoder(frame.request_data)).pipe(op.map(self.encoder))
            else:
                return handler[0](frame.request_data).pipe(op.map(self.encoder))
        else:
            return rx.throw(Exception("Unknown Destination '{}'".format(route_name)))

    def _get_route_name(self, meta_data: bytes):

        routeLength = meta_data[0]
        return meta_data[1:(routeLength + 1)].decode('UTF-8')

    def _encode_route_name(self, route_name: str) -> bytes:
        meta_data = bytearray(route_name.encode('ASCII'))
        meta_data.insert(0, len(meta_data))
        return meta_data

    def _check_data_none(self, data):
        if data == None:
            data = {}

        return data
