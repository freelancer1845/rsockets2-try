

class RSocketConfig(object):
    def __init__(self):
        super().__init__()
        self.major_version = 1
        self.minor_version = 0
        self.keepalive_time = 30000
        self.max_liftime = 100000
        self.meta_data_mime_type = b"message/x.rsocket.routing.v0"
        self.data_mime_type = b"application/json"
        self.resume_identification_token = None
        self.honors_lease = False
