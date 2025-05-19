"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_avoma.tap import TapAvoma

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapAvoma = get_tap_test_class(
    tap_class=TapAvoma,
    config=SAMPLE_CONFIG,
)
