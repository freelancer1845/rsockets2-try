
from rsockets2.extensions.wellknown_mime_types import WellknownMimeTypeList
from rx.core.typing import Observable
from rsockets2.core.types import RequestPayload, ResponsePayload
from typing import List
import unittest
import rx
import rx.subject.subject

from rsockets2.messages.message_handler import RSocketMessageHandler


class FluentRequestTest(unittest.TestCase):

    def test_route_and_message_handler_call(self):

        test_route = "hello/world"
        initial_requests = 24215
        requester = rx.subject.subject.Subject()

        called_route = ""
        called_payload = None
        called_initial_request = 0
        called_requester = None

        class RSocketMock(object):

            metadata_mime_type = WellknownMimeTypeList.Message_XRsocketCompositeMetadataV0.str_value

        class MockHandler(RSocketMessageHandler):
            def __init__(self) -> None:
                super().__init__()
                self.rsocket = RSocketMock()

            def request_response(self, route_tags: List[str], payload: RequestPayload) -> Observable[ResponsePayload]:
                nonlocal called_route
                nonlocal called_payload
                called_route = route_tags
                called_payload = payload
                return rx.empty()

            def request_stream(self, route_tags: List[str], payload: RequestPayload, initial_requests, requester) -> Observable[ResponsePayload]:
                nonlocal called_route
                nonlocal called_payload
                nonlocal called_initial_request
                nonlocal called_requester
                called_route = route_tags
                called_payload = payload
                called_initial_request = initial_requests
                called_requester = requester
                return rx.empty()

            def request_fnf(self, route_tags: List[str], payload: RequestPayload) -> None:
                nonlocal called_route
                nonlocal called_payload
                called_route = route_tags
                called_payload = payload

        mock_handler = MockHandler()

        request = mock_handler.route(test_route)
        request.data(test_route.encode('UTF-8')).mono(lambda x: int(x))

        self.assertEqual(test_route, called_route)
        self.assertEqual(called_payload[1], test_route.encode('UTF-8'))

        called_route = ""
        called_payload = None
        request.data(test_route.encode(
            'UTF-8')).initial_requests(initial_requests).requester(requester).flux(lambda x: int(x))
        self.assertEqual(test_route, called_route)
        self.assertEqual(called_payload[1], test_route.encode('UTF-8'))
        self.assertEqual(initial_requests, called_initial_request)
        self.assertEqual(requester, called_requester)

        request.data(test_route.encode('UTF-8')).send()

        self.assertEqual(test_route, called_route)
        self.assertEqual(called_payload[1], test_route.encode('UTF-8'))
