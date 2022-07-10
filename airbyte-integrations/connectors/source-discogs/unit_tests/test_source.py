#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from pytest import fixture
from source_discogs.source import SourceDiscogs

@fixture
def config():
    return {}

def test_check_connection(mocker, requests_mock, config):
    source = SourceDiscogs()
    logger_mock =  MagicMock()
    config = {"release_id":"249504"} 
    requests_mock.get("https://dummy")
    assert source.check_connection(logger_mock, config) == (True, None)


#def test_streams(mocker):
#    source = SourceDiscogs()
#    config_mock = MagicMock()
#    streams = source.streams(config_mock)
#    expected_streams_number = 1
#    assert len(streams) == expected_streams_number
