"""B2 (fork 2026-11-13, DPMS 561213): tool_call arguments cut by the model's output cap must
be reported as a truncation with the remedy, not as generic "unrepairable" corruption.

Production 2026-11-10..11: 14 such events; one review-report task failed a dozen rounds
because the log never said why the arguments were empty.
"""
import logging

from agent.message_sanitization import _repair_tool_call_arguments


def test_truncated_arguments_name_the_truncation_and_the_remedy(caplog):
    truncated = '{"path": "/tmp/report.html", "content": "half a docum'

    with caplog.at_level(logging.WARNING, logger="agent.message_sanitization"):
        repaired = _repair_tool_call_arguments(truncated, "write_file")

    assert repaired == "{}"
    messages = [r.getMessage() for r in caplog.records]
    assert any("Truncated tool_call arguments for write_file" in m for m in messages)
    assert any("output cap" in m for m in messages)
    assert any("smaller arguments" in m for m in messages)


def test_arguments_that_are_malformed_but_not_truncated_keep_the_generic_message(caplog):
    malformed = '{"a": 1, "b": }'

    with caplog.at_level(logging.WARNING, logger="agent.message_sanitization"):
        repaired = _repair_tool_call_arguments(malformed, "terminal")

    messages = [r.getMessage() for r in caplog.records]
    assert not any("Truncated tool_call arguments" in m for m in messages)
    # Whatever the repair outcome, the caller always gets valid JSON.
    import json
    json.loads(repaired)
