from __future__ import annotations  # noqa: D100

import decimal
import logging
import time
import typing as t
from http import HTTPStatus

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    import requests


class AvomaStream(RESTStream):
    """Avoma stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url")

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("api_key", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {}

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and whether they are fatal or retriable.

        Args:
            response: A :class:`requests.Response` object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.
        """
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        # rate limits: https://dev694.avoma.com/#section/Introduction/Rate-Limits
        # These limits define the number of requests you can make within a specific time
        # period. We Rate Limit our APIs to 60 request in 1 minute period.
        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            msg = self.response_error_message(response)
            sleep_seconds = 60 + 10  # add 10 seconds buffer
            logging.info(f"Rate limit reached. Sleeping for {sleep_seconds} seconds")  # noqa: LOG015, G004
            time.sleep(sleep_seconds)
            raise RetriableAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
