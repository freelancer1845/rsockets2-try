from rsockets2.extensions.security.wellknown_auth_types import WellknownAuthenticationTypes
import unittest
from rsockets2.extensions.security.authentication import Authentication


class AuthenticationTest(unittest.TestCase):

    def test_handles_all_readable_buffer_types(self):
        auth_payload = "HelloWorld".encode('UTF-8')
        auth_type = 'my_type'
        buffer = Authentication.create_new(
            auth_type, auth_payload)

        self.assertEqual(Authentication.auth_type(buffer),
                         auth_type)
        self.assertEqual(Authentication.auth_payload(buffer), auth_payload)

        buffer = memoryview(buffer)
        self.assertEqual(Authentication.auth_type(buffer),
                        auth_type)
        self.assertEqual(Authentication.auth_payload(buffer), auth_payload)
