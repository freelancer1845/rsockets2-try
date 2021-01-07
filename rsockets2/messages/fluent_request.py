
from dataclasses import dataclass


from rx.core.typing import Observable
from rsockets2.extensions.composite_metadata import extend_composite_metadata
from rsockets2.extensions.wellknown_mime_types import WellknownMimeType, WellknownMimeTypeList
from typing import Any, Callable, Generic, TypeVar, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from rsockets2.messages.message_handler import RSocketMessageHandler
from rsockets2.core.types import RequestPayload, RequestPayloadTypes

import rx.operators as op
import rx
T = TypeVar('T')


@dataclass()
class FluentRequest(object):
    _message_handler: 'RSocketMessageHandler'
    _route: str
    _metadata = bytes(0)
    _data: RequestPayloadTypes = None
    _freeze_metadata: bool = False
    _initial_requests: int = 2 ** 31 - 1
    _requester: Observable[int] = rx.empty()

    def data(self, obj: RequestPayloadTypes) -> 'FluentRequest':
        self._data = obj
        return self

    def add_metadata(self, mime_type: Union[str, WellknownMimeType], metadata: RequestPayloadTypes) -> 'FluentRequest':
        if self._freeze_metadata == True:
            raise ValueError(
                f'To send metadata with a message request you need to use {WellknownMimeTypeList.Message_XRsocketCompositeMetadataV0.str_value} as metadata mime type at connection level')
        self._metadata = extend_composite_metadata(
            self._metadata, mime_type, metadata)

    def initial_requests(self, n: int) -> 'FluentRequest':
        self._initial_requests = n
        return self

    def requester(self, obs: Observable[int]) -> 'FluentRequest':
        self._requester = obs
        return self

    def mono(self, mapper: Callable[[RequestPayload], T] = rx.pipe()) -> Observable[T]:
        return self._message_handler.request_response(
            self._route,
            (self._metadata, self._data)
        ).pipe(op.map(lambda x: mapper(x)))

    def flux(self, mapper: Callable[[RequestPayload], T] = rx.pipe()) -> Observable[T]:
        return self._message_handler.request_stream(
            self._route,
            (self._metadata, self._data),
            self._initial_requests,
            self._requester
        ).pipe(op.map(lambda x: mapper(x)))

    def send(self) -> None:
        self._message_handler.request_fnf(
            self._route,
            (self._metadata, self._data)
        )
