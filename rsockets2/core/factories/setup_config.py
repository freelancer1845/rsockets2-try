from dataclasses import dataclass
from typing import Optional


@dataclass
class RSocketSetupConfig(object):
    metadata_encoding_mime_type: str = "application/octet-stream"
    data_encoding_mime_type: str = "application/octet-stream"
    metadata_payload: Optional[bytes] = None
    data_payload: Optional[bytes] = None
    time_between_keepalive: int = 5000
    max_lifetime: int = 30000
    major_version: int = 1
    minor_version: int = 0
    token: Optional[bytes] = None
