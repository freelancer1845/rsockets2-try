import json
import typing


def json_decoder(json_string) -> typing.Dict:
    if json_string == None:
        return {}
    if len(json_string) == 0:
        return {}
    return json.loads(json_string, encoding='UTF-8')


def json_encoder(object_dict: typing.Dict):
    if object_dict == None:
        return b'{}'
    return json.dumps(object_dict).encode('UTF-8')
