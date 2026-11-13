"""B3 (fork 2026-11-13, DPMS 561213): the batch validator's "needs a 'name'" error must teach
the correct operation shape — the bare message failed across three days (production
2026-11-09..11: 8 calls, some turns retrying the same malformed op three times).
"""
from tools.skill_manager_batch import _validate_batch_ops


def _tool_error(message, success=False):
    return message


def test_missing_name_error_echoes_the_operation_and_a_correct_shape():
    names, error = _validate_batch_ops([{"action": "patch"}], None, _tool_error)

    assert names is None
    assert error is not None
    assert "needs a 'name'" in error
    assert "Correct shape" in error
    # The offending operation is echoed so the model can see what it sent.
    assert '"action": "patch"' in error


def test_a_named_operation_is_accepted():
    names, error = _validate_batch_ops(
        [{"action": "patch", "name": "kbs", "old_string": "a", "new_string": "b"}],
        None, _tool_error,
    )

    assert error is None
    assert names == ["kbs"]
