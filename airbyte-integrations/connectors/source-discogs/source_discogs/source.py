#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth
# Don't need authentication yet, but maybe later
# from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

# Basic full refresh stream


class DiscogsStream(HttpStream, ABC):

    url_base = "https://api.discogs.com/"

    primary_key = None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return {'page': response.json()['pagination.page'] + 1}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # Discogs default pagination is 50, max is 100
        # Do I even need pagination if we're just looking up one release? hmm
        return {"per_page": 10}

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        # All requests need a User-Agent or Discogs will return 404
        return {"User-Agent": "AirbyteConnector/0.1 +https://airbyte.com"}

    def parse_response(
            self,
            response: requests.Response,
            **kwargs
    ) -> Iterable[Mapping]:
        #        yield {}
        return [response.json()]


class Release(DiscogsStream):

    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs):
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
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        logger.info("Checking Discogs API connection...")
        try:
            url = "https://api.discogs.com/releases/" + config["release_id"]
            headers = {
                # All requests need a User-Agent or Discogs will return 404
                "User-Agent": "AirbyteConnector/0.1 +https://airbyte.com"}
            requests.get(url, headers=headers)
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth()
#        return [Customers(authenticator=auth), Employees(authenticator=auth)]
        return [Release(authenticator=auth, config=config)]
