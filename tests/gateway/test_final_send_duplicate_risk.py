"""B5 (fork 2026-11-13, DPMS 561213): a normal final send can only duplicate what the stream
consumer actually delivered. Production 2026-11-09..11 logged 12 duplicate-send warnings with
zero real duplicates — every one of them from a consumer that had delivered nothing.
"""
from gateway.run_turn import _final_send_duplicate_risk


class _Consumer:
    def __init__(self, already_sent: bool):
        self.already_sent = already_sent


def test_a_consumer_that_delivered_frames_can_duplicate():
    assert _final_send_duplicate_risk(_Consumer(True)) is True


def test_an_idle_consumer_cannot_duplicate():
    assert _final_send_duplicate_risk(_Consumer(False)) is False


def test_a_consumer_without_the_attribute_is_treated_as_idle():
    assert _final_send_duplicate_risk(object()) is False
