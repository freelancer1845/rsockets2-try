

from rsockets2.core.types import RequestPayload, ResponsePayload
from typing import Callable, Dict, List
from rx.core.typing import Observable


def default_tag_matcher(tags: List[str], tag: str) -> bool:
    for to_match in tags:
        if tag == to_match:
            return True


class RSocketMessageRouter(object):

    def __init__(self, tag_matcher: Callable[[List[str], str], bool] = default_tag_matcher) -> None:
        super().__init__()
        self._response_routes = {}
        self._stream_routes = {}
        self._fnf_routes = {}
        self._channel_routes = {}
        self._tag_matcher = tag_matcher

    def _find_routes(self, tags: List[str], registry: Dict) -> List:
        result = []
        for item in registry.items():
            if self._tag_matcher(tags, item[0]):
                result.append(item[1])
        return result

    def get_request_response_routes(self, tags: List[str]) -> List[Callable[[
            RequestPayload], Observable[ResponsePayload]]]:
        return self._find_routes(tags, self._response_routes)

    def get_request_stream_routes(self, tags: List[str]) -> List[Callable[[
            RequestPayload, int, Observable[int]], Observable[ResponsePayload]]]:
        return self._find_routes(tags, self._stream_routes)

    def get_request_fnf_routes(self, tags: List[str]):
        return self._find_routes(tags, self._fnf_routes)

    def get_request_channel_routes(self, tags: List[str]):
        return self._find_routes(tags, self._channel_routes)

    def register_request_response_route(self, tag: str, route_handler: Callable[[
            RequestPayload], Observable[ResponsePayload]]):
        """
            tag: str (Associated Tag, unique)
            Route Handler:
            request_payload: [ReadableBuffer, ReadableBuffer] -> Observable[RequestResponse]
        """
        self._response_routes[tag] = route_handler

    def register_request_stream_route(self, tag: str, route_handler:  Callable[[
            RequestPayload, int, Observable[int]], Observable[ResponsePayload]]):
        """
            tag: str (Associated Tag, unique)
            Route Handler:
            request_payload: [ReadableBuffer, ReadableBuffer], initial_requests: int, requester: Observable[int] -> Observable[RequestResponse]
        """
        self._stream_routes[tag] = route_handler

    def register_request_fnf_route(self, tag: str, route_handler: Callable[[RequestPayload], None]):
        """
            tag: str (Associated Tag, unique)
            Route Handler:
            request_payload: [ReadableBuffer, ReadableBuffer] -> None
        """
        self._fnf_routes[tag] = route_handler

    def register_request_channel_route(self, tag: str):
        pass
