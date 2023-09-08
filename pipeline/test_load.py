from unittest.mock import patch, MagicMock
from load import write_cancel_codes, switch_between_schemas


# def test_write_cancel_codes(cancel_codes_df):

#     fake_connection = MagicMock()

#     fake_execute = fake_connection.cursor().execute

#     write_cancel_codes(fake_connection, cancel_codes_df)

#     assert fake_execute.call_count == 1

def test_switch_between_schema(cancel_codes_df):

    fake_connection = MagicMock()

    fake_execute = fake_connection.cursor().execute

    switch_between_schemas(fake_connection, 'Lishein')

    assert fake_execute.call_count == 1
