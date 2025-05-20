from __future__ import annotations  # noqa: D100

import typing as t
from urllib.parse import parse_qs, urlparse

from singer_sdk import typing as th

from tap_avoma.client import AvomaStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class MeetingsStream(AvomaStream):
    """Define custom stream."""

    name = "meetings"
    path = "/v1/meetings"
    replication_key = "start_at"
    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.next"  # noqa: S105
    is_sorted = True
    schema = th.PropertiesList(
        th.Property(
            "attendees",
            th.ArrayType(
                th.ObjectType(
                    th.Property("email", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("uuid", th.StringType),
                    th.Property("response_status", th.StringType),
                )
            ),
        ),
        th.Property("audio_ready", th.BooleanType),
        th.Property(
            "call_details",
            th.ObjectType(
                th.Property("external_id", th.StringType),
                th.Property("frm", th.StringType),
                th.Property("to", th.StringType),
            ),
        ),
        th.Property("created", th.DateTimeType),
        th.Property("duration", th.NumberType),
        th.Property("end_at", th.DateTimeType),
        th.Property("is_call", th.BooleanType),
        th.Property("is_internal", th.BooleanType),
        th.Property("is_private", th.BooleanType),
        th.Property("modified", th.DateTimeType),
        th.Property("notes_ready", th.BooleanType),
        th.Property("organizer_email", th.StringType),
        th.Property(
            "outcome",
            th.ObjectType(
                th.Property("label", th.StringType),
                th.Property("uuid", th.StringType),
            ),
        ),
        th.Property("processing_status", th.StringType),
        th.Property(
            "purpose",
            th.ObjectType(
                th.Property("label", th.StringType),
                th.Property("uuid", th.StringType),
            ),
        ),
        th.Property("recording_uuid", th.StringType),
        th.Property("start_at", th.DateTimeType),
        th.Property("state", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("transcript_ready", th.BooleanType),
        th.Property("transcription_uuid", th.StringType),
        th.Property(
            "type",
            th.ObjectType(
                th.Property("label", th.StringType),
                th.Property("uuid", th.StringType),
            ),
        ),
        th.Property("url", th.StringType),
        th.Property("uuid", th.StringType),
        th.Property("video_ready", th.BooleanType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            parsed_url = urlparse(next_page_token)
            params = parse_qs(parsed_url.query)
        else:
            start = self.get_starting_timestamp(context)
            if start:
                params["from_date"] = start  # result from last processed record
            else:
                params["from_date"] = self.config.get("from_date")
            params["to_date"] = self.config.get("to_date")
            params["page_size"] = self.config.get("page_size")
            params["recording_duration__gte"] = self.config.get(
                "recording_duration__gte"
            )
            params["o"] = "start_at"  # ascending
        return params

    def get_child_context(
        self,
        record: dict,
        context: Context | None,  # noqa: ARG002
    ) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "meeting_uuid": record["uuid"],
            "meeting_start_at": record["start_at"],
        }


class TranscriptionsStream(AvomaStream):
    """Define custom stream."""

    name = "transcriptions"
    path = "/v1/transcriptions"
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = MeetingsStream
    state_partitioning_keys: t.ClassVar[list[str]] = []
    ignore_parent_replication_key = False
    schema = th.PropertiesList(
        th.Property(
            "transcript",
            th.ArrayType(
                th.ObjectType(
                    th.Property("speaker_id", th.IntegerType),
                    th.Property("speaker_distance", th.NumberType),
                    th.Property("timestamps", th.ArrayType(th.NumberType)),
                    th.Property("transcript", th.StringType),
                )
            ),
        ),
        th.Property(
            "speakers",
            th.ArrayType(
                th.ObjectType(
                    th.Property("email", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("is_rep", th.BooleanType),
                    th.Property("id", th.IntegerType),
                )
            ),
        ),
        th.Property("transcription_vtt_url", th.StringType),
        th.Property("uuid", th.StringType),
        th.Property("meeting_uuid", th.StringType),
        th.Property("meeting_start_at", th.DateTimeType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401, ARG002
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if context:
            params["meeting_uuid"] = context["meeting_uuid"]
        return params

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Post-process the row."""
        row = super().post_process(row, context)
        if "uuid" not in row:
            # filter out meetings with no transcriptions
            return None
        if context:
            row["meeting_uuid"] = context["meeting_uuid"]
            row["meeting_start_at"] = context["meeting_start_at"]
        return row
