from dataclasses import dataclass
from rsockets2.common.enum_extensions import WellknownEnum


@dataclass
class WellknownMimeType(object):
    name: str
    value: int


class WellknownMimeTypes():
    Application_Avro = WellknownMimeType("application/avro", 0x00)
    Application_Cbor = 0x01
    Application_Graphql = 0x02
    Application_Gzip = 0x03
    Application_Javascript = 0x04
    Application_Json = 0x05
    Application_OctetStream = 0x06
    Application_Pdf = 0x07
    Application_VndApacheThriftBinary = 0x08
    Application_VndGoogleProtobuf = 0x09
    Application_Xml = 0x0A
    Application_Zip = 0x0B
    Audio_Aac = 0x0C
    Audio_Mp3 = 0x0D
    Audio_Mp4 = 0x0E
    Audio_Mpeg3 = 0x0F
    Audio_Mpeg = 0x10
    Audio_Ogg = 0x11
    Audio_Opus = 0x12
    Audio_Vorbis = 0x13
    Image_Bmp = 0x14
    Image_Gif = 0x15
    Image_HeicSequence = 0x16
    Image_Heic = 0x17
    Image_HeifSequence = 0x18
    Image_Heif = 0x19
    Image_Jpeg = 0x1A
    Image_Png = 0x1B
    Image_Tiff = 0x1C
    Multipart_Mixed = 0x1D
    Text_Css = 0x1E
    Text_Csv = 0x1F
    Text_Html = 0x20
    Text_Plain = 0x21
    Text_Xml = 0x22
    Video_H264 = 0x23
    Video_H265 = 0x24
    Video_VP8 = 0x25
    Application_XHessian = 0x26
    Application_XJavaObject = 0x27
    Application_CloudeventsJson = 0x28
    Application_XCapnp = 0x29
    Application_XFlatbuffers = 0x2A
    Message_XRsocketMimeTypeV0 = 0x7A
    Message_XRsocketAcceptMimeTypesV0 = 0x7b
    Message_XRsocketAuthenticationV0 = 0x7C
    Message_XRsocketTracingZipkinV0 = 0x7D
    Message_XRsocketRoutingV0 = WellknownMimeType(
        "message/x.rsocket.routing.v0", 0x7E)
    Message_XRsocketCompositeMetadataV0 = WellknownMimeType(
        "message/x.rsocket.composite-metadata.v0", 0x7F)

    @classmethod
    def __contains__(cls):
        pass


WELLKNOWN_MIME_TYPE_VALUE = {
    WellknownMimeTypes.Message_XRsocketCompositeMetadataV0: "message/x.rsocket.composite-metadata.v0",
    WellknownMimeTypes.Message_XRsocketRoutingV0: "message/x.rsocket.routing.v0"
}
