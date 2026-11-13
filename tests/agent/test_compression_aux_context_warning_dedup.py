"""B8 (fork 2026-11-13, DPMS 561213): the "aux compression model context below threshold"
warning is a per-session event, so a gateway serving many sessions wrote the identical line
on every session start — 36 WARNING lines over three production days, a quarter of the log.
Warn once per (model, context, threshold) per process; repeats go to DEBUG.
"""
import logging

from agent.conversation_compression import (
    _WARNED_AUX_CONTEXT_FINGERPRINTS,
    _warn_aux_context_mismatch,
)


def test_the_same_mismatch_warns_once_per_process(caplog):
    _WARNED_AUX_CONTEXT_FINGERPRINTS.clear()

    with caplog.at_level(logging.DEBUG, logger="agent.conversation_compression"):
        _warn_aux_context_mismatch("glm-5-turbo-zp", 202752, 350000, 202752)
        _warn_aux_context_mismatch("glm-5-turbo-zp", 202752, 350000, 202752)
        _warn_aux_context_mismatch("glm-5-turbo-zp", 202752, 350000, 202752)

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    debugs = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(warnings) == 1
    assert "auto-lowered session threshold" in warnings[0].getMessage()
    assert len(debugs) == 2
    assert all("already warned" in r.getMessage() for r in debugs)


def test_a_different_mismatch_warns_again(caplog):
    _WARNED_AUX_CONTEXT_FINGERPRINTS.clear()

    with caplog.at_level(logging.WARNING, logger="agent.conversation_compression"):
        _warn_aux_context_mismatch("glm-5-turbo-zp", 202752, 350000, 202752)
        _warn_aux_context_mismatch("other-model", 100000, 350000, 100000)

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warnings) == 2
