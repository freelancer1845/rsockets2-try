import enum
from typing import Union


class WellknownEnumMeta(enum.EnumMeta):

    def __contains__(cls, member: Union[str, int]) -> bool:
        if member in cls.__members__:
            return True
        if member in [v.value for v in cls.__members__.values()]:
            return True
        return False


class WellknownEnum(enum.IntEnum, metaclass=WellknownEnumMeta):
    pass

