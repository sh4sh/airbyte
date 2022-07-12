#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode

from airbyte_cdk.sources.streams.http.auth import NoAuth
# Don't need authentication yet, but maybe later
# from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

# Abstract base class meant to contain all the common functionality at the API level e.g: the API base URL,
# pagination strategy, parsing responses etc..


class DiscogsStream(HttpStream, ABC):

    url_base = "https://api.discogs.com/"

    def next_page_token(
            self,
            response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None
        # return {'page': response.json()['page'] + 1}
        # Not all endpoints offer pagination so we should leave this empty for now

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    # All requests need a User-Agent or Discogs will return 404
    def request_headers(
            self,
            **kwargs
    ) -> Mapping[str, Any]:
        return {"User-Agent": "AirbyteConnector/0.1 +https://airbyte.com"}

    def parse_response(
            self,
            response: requests.Response,
            **kwargs
    ) -> Iterable[Mapping]:
        #        yield {}
        response_json = response.json()
        yield response_json

# The Release resource endpoint will pull all metadata associated with a particular physical or digital object released by one or more Artists.
# release_id: The Release ID e.g. 249504
# curr_abbr: Currency for marketplace data. Defaults to the authenticated users currency.
class Release(DiscogsStream):

    primary_key = "id"

    def __init__(
            self,
            config: Mapping[str, Any],
            **kwargs
    ):
        super().__init__()
        self.release_id = config['release_id']
        self.curr_abbr = config['curr_abbr']

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params["release_id"] = self.release_id
        params["curr_abbr"] = self.curr_abbr
        return params

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        # /releases/{release_id}{?curr_abbr}
        return "releases/" + self.release_id


# Source
class SourceDiscogs(AbstractSource):
    def check_connection(
        self,
        logger: AirbyteLogger,
        config: Mapping[str, Any]
    ) -> Tuple[bool, Any]:
        logger.info("Checking Discogs API connection...")
        try:
            args = {"authenticator": NoAuth(), "config": config}
            release_stream = Release(**args)
            release_items = release_stream.read_records(sync_mode=SyncMode.full_refresh)
            item = next(release_items)
            logger.info(f"Successfully connected to Discogs API. Pulled one release with title: {item['title']}")
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(
        self,
        config: Mapping[str, Any]
    ) -> List[Stream]:
        args = {"authenticator": NoAuth(), "config": config}
        return [
            Release(**args)
        ]
