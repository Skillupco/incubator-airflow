import unittest
from airflow.contrib.hooks.valohai_hook import ValohaiHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

CONN_ID = 'valohai_conn_test'
HOST = 'https://www.example.com'


class TestValohaiHook(unittest.TestCase):

    @mock.patch('airflow.contrib.hooks.valohai_hook.ValohaiHook.get_connection')
    def test_init(self, get_connection_mock):
        conn_mock = mock.Mock(extra_dejson=[], host=HOST)
        get_connection_mock.return_value = conn_mock

        hook = ValohaiHook(valohai_conn_id=CONN_ID)

        get_connection_mock.assert_called_with(CONN_ID)
        self.assertIs(hook.valohai_conn, conn_mock)
        self.assertEqual(hook.host, HOST)
