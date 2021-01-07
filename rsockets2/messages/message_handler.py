
from typing import List
from rsockets2.core.rsocket import RSocket
from rsockets2.messages.router import RSocketMessageRouter
from rx.core.typing import Observable
from rsockets2.core.types import RSocketHandler, ReadBuffer, RequestPayload, ResponsePayload
import rx
from rsockets2.extensions.wellknown_mime_types import WellknownMimeTypeList, WellknownMimeType
from rsockets2.core.exceptions import ApplicationError, RSocketError, RSocketProtocolError
import rsockets2.extensions.routing_metadata as routing
import rsockets2.extensions.composite_metadata as composite
import logging


class RSocketMessageHandler(RSocketHandler):

    log = logging.getLogger(__name__)

    router = RSocketMessageRouter()

    tag_getter = None

    def on_request_fnf(self, payload: RequestPayload) -> None:
        return super().on_request_fnf(payload)

    def on_request_response(self, payload: RequestPayload) -> Observable[ResponsePayload]:
        def deferred(scheduler):
            tags = self.tag_getter(payload[0])
            self.log.debug(
                'Handling Request Response with given route tags: ' + str(tags))
            handler = self.router.get_request_response_routes(tags)
            if len(handler) == 0:
                return rx.throw(ApplicationError('No handler for route tags: ' + str(tags)))
            if len(handler) > 1:
                self.log.error(
                    'More than one request_response handler was resolved for the given route tags')
                return rx.throw(ApplicationError('More than one request_response handler was resolved for the given route tags'))
            return handler[0](payload)
        return rx.defer(deferred)

    def on_request_stream(self, payload: RequestPayload, initial_requests: int, requests: Observable[int]) -> Observable[ResponsePayload]:
        def deferred(scheduler):
            tags = self.tag_getter(payload[0])
            self.log.debug(
                'Handling Request Stream with given route tags: ' + str(tags))
            handler = self.router.get_request_stream_routes(tags)
            if len(handler) == 0:
                return rx.throw(ApplicationError('No handler for route tags: ' + str(tags)))
            if len(handler) > 1:
                self.log.error(
                    'More than one request_stream handler was resolved for the given route tags')
                return rx.throw(ApplicationError('More than one request_response handler was resolved for the given route tags'))
            return handler[0](payload, initial_requests, requests)
        return rx.defer(deferred)

    def on_request_channel(self):
        raise NotImplementedError()

    def request_response(self, route_tags: List[str], payload: RequestPayload) -> Observable[ResponsePayload]:

        payload = self._encode_route_tags(route_tags, payload)

        def deferred(scheduler):
            return self.rsocket.request_response(payload)
        return rx.defer(deferred)

    def request_stream(self, route_tags: List[str], payload: RequestPayload, initial_requests=2 ** 31 - 1, requester=rx.empty()) -> Observable[ResponsePayload]:
        payload = self._encode_route_tags(route_tags, payload)

        def deferred(scheduler):
            return self.rsocket.request_stream(payload, initial_requests, requester)
        return rx.defer(deferred)

    def _encode_route_tags(self, route_tags: List[str], payload: RequestPayload):
        if self.rsocket.metadata_mime_type == WellknownMimeTypeList.Message_XRsocketRoutingV0.str_value:
            if payload[0] != None and len(payload[0] > 0):
                raise RSocketError(
                    f'Provided metadata would be overwritten. Use Metadata Mime {WellknownMimeTypeList.Message_XRsocketRoutingV0.str_value}')
            else:
                return (routing.encode_route_tags((route_tags,)), payload[1])
        else:
            if payload[0] == None:
                return (composite.encode_as_composite_metadata(
                    WellknownMimeTypeList.Message_XRsocketRoutingV0, routing.encode_route_tags(route_tags)), payload[1])
            else:
                return (composite.extend_composite_metadata(
                    payload[0], WellknownMimeTypeList.Message_XRsocketRoutingV0, routing.encode_route_tags(route_tags)), payload[1])

    def set_rsocket(self, rsocket: RSocket):
        if rsocket.metadata_mime_type == WellknownMimeTypeList.Message_XRsocketRoutingV0.str_value:
            self._setup_tag_getter(
                WellknownMimeTypeList.Message_XRsocketRoutingV0)
        elif rsocket.metadata_mime_type == WellknownMimeTypeList.Message_XRsocketCompositeMetadataV0.str_value:
            self._setup_tag_getter(
                WellknownMimeTypeList.Message_XRsocketCompositeMetadataV0)
        else:
            raise RSocketProtocolError(
                'RSocketMessageHandler only support "message/x.rsocket.routing" or "message/x.rsocket.composite.metadata"')
        return super().set_rsocket(rsocket)

    def _setup_tag_getter(self, mime_type: WellknownMimeType):
        if mime_type == WellknownMimeTypeList.Message_XRsocketRoutingV0:
            self.tag_getter = routing.decode_route_tags
        elif mime_type == WellknownMimeTypeList.Message_XRsocketCompositeMetadataV0:
            def getter(metadata: ReadBuffer):
                composite_metadata = composite.decode_composite_metadata(
                    metadata)
                routes = []
                for data in composite_metadata:
                    if data[0] == WellknownMimeTypeList.Message_XRsocketRoutingV0.str_value:
                        routes = routing.decode_route_tags(data[1])
                        break
                return routes
            self.tag_getter = getter
        else:
            raise RSocketProtocolError(
                'RSocketMessageHandler only support "message/x.rsocket.routing" or "message/x.rsocket.composite.metadata"')
