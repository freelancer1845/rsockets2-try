import enum
from rsockets2.common import WellknownEnum


class WellknownAuthenticationTypes(WellknownEnum):

    Simple = 0x00
    Bearer = 0x01
