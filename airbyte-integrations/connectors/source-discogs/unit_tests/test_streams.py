#/
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import requests
import requests_mock
import fixture
import pytest
from source_discogs.source import Release

@pytest.fixture(name="config")
def config_fixture():
    config = {"release_id":"249504"}
    return config

@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(Release, "path", "v0/example_endpoint")
    mocker.patch.object(Release, "primary_key", "test_primary_key")
    mocker.patch.object(Release, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = Release(MagicMock())
    # TODO: replace this with your input parameters
    inputs = {"stream_slice": None, "stream_state": MagicMock(), "next_page_token": None}
    # TODO: replace this with your expected request parameters
    expected_params = {}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class):
    stream = Release(MagicMock())
    # TODO: replace this with your input parameters
    inputs = {"response": MagicMock()}
    # TODO: replace this with your expected next page token
    expected_token = None
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class):
    stream = Release(MagicMock())
    inputs = {"config": MagicMock()}
    expected_parsed_object = {"id": "249504" }
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = Release(MagicMock())
    inputs = { "stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {"User-Agent": "AirbyteConnector/0.1 +https://airbyte.com"}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = Release(MagicMock())
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = Release(MagicMock())
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = Release(MagicMock())
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
