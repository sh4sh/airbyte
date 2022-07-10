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
        # Discogs default pagination is 50, max is 100
        # response body contains pagination.page object with page count
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # set per_page param here
        return {}
    
    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return ""

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


class Customers(DiscogsStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "customer_id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "customers"


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
