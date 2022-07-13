#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from pytest import fixture
from source_discogs.source import SourceDiscogs


@fixture
def config():
    return {"release_id": "249504", "curr_abbr": "ZAR"}


def test_check_connection(mocker, requests_mock, config):
    source = SourceDiscogs()
    logger_mock = MagicMock()
    requests_mock.get("https://api.discogs.com/releases/249504?release_id=249504&curr_abbr=ZAR", json=config)
    assert source.check_connection(logger_mock, config) == (True, None)


def test_streams(mocker):
    source = SourceDiscogs()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
