"""Avoma tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_avoma import streams


class TapAvoma(Tap):
    """Avoma tap class."""

    name = "tap-avoma"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="API Key",
            description="The API key to authenticate against the API service",
        ),
        th.Property(
            "from_date",
            th.DateTimeType(nullable=True),
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "to_date",
            th.DateTimeType(nullable=True),
            required=True,
            description="The latest record date to sync",
        ),
        th.Property(
            "page_size",
            th.IntegerType(nullable=True),
            default=100,
            description="The number of records to fetch per page",
        ),
        th.Property(
            "recording_duration__gte",
            th.IntegerType(nullable=True),
            default=60,
            description=(
                "To retrieve meetings with recording duration(in seconds) greater than"
                "or equal to given value. Sending this filter will"
                "by default exclude meetings without recordings."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            required=True,
            title="API URL",
            default="https://api.avoma.com",
            description="The url for the API service",
        ),
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap-avoma>/<tap-version>'"
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AvomaStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.MeetingsStream(self),
            streams.TranscriptionsStream(self),
        ]


if __name__ == "__main__":
    TapAvoma.cli()
