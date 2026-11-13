"""B4 (fork 2026-11-13, DPMS 561213): a kill aimed at the gateway must never be auto-approved
by the guardian LLM — production 2026-11-11 smart-approved and ran one (exit -15), which
takes the gateway down on a stray pgrep match in an unattended session.
"""
import pytest

from tools import approval


def _spec():
    return approval._GateSpec(
        noun="command",
        transport=False,
        user_approved=False,
        redact_cli=False,
        pending_keys=True,
        notify_failed="failed",
        gateway_refused="refused",
        transport_denied="denied",
        cli_timeout="timeout",
        cli_denied="denied",
        smart_log="smart {command} {description} {session_key}",
    )


@pytest.mark.parametrize(
    "command,pattern_key",
    [
        ("pkill -f hermes-gateway", "kill hermes/gateway process (self-termination)"),
        ("killall -f gateway/run.py", "kill hermes/gateway process (self-termination)"),
        ("kill $(pgrep -f hermes)", "kill process via pgrep/pidof expansion (self-termination)"),
        ("kill `pidof hermes`", "kill process via backtick pgrep/pidof expansion (self-termination)"),
    ],
)
def test_self_termination_commands_are_never_smart_approved(monkeypatch, command, pattern_key):
    asked = []

    def _verdict(*args, **kwargs):
        asked.append(args)
        return "approve"

    monkeypatch.setattr(approval, "_smart_verdict", _verdict)

    result, smart_denied = approval._smart_gate(
        _spec(), command, pattern_key, pattern_key, [], "session-1", human_present=False,
    )

    # No decision: the command falls through to the explicit human-approval path.
    assert result is None
    assert smart_denied is False
    # The guardian LLM was never even consulted for a self-termination command.
    assert asked == []


def test_an_ordinary_dangerous_command_still_reaches_the_guardian(monkeypatch):
    asked = []

    def _verdict(*args, **kwargs):
        asked.append(args)
        return "approve"

    monkeypatch.setattr(approval, "_smart_verdict", _verdict)

    result, _denied = approval._smart_gate(
        _spec(), "rm -rf /tmp/scratch", "dangerous command", "dangerous command", [],
        "session-1", human_present=False,
    )

    assert len(asked) == 1
    assert result is not None and result.get("smart_approved") is True
