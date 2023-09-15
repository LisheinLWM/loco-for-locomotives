from unittest.mock import patch, MagicMock
from load import write_cancel_codes


def test_write_cancel_codes():

    fake_connection = MagicMock()

    fake_execute = fake_connection.cursor().execute

    write_cancel_codes(fake_connection, ['a', 'b', 'c'])
