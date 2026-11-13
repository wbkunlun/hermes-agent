"""B1a (fork 2026-11-13, DPMS 561213): every continuation attempt burned the output budget
on reasoning and produced no visible text — the turn used to end on the canned sentinel
("no visible text was produced"), throwing the round away. The configured fallback model
must get the turn first.

Production 2026-11-11: three turns delivered a 274-char apology instead of the work.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from agent.error_classifier import FailoverReason


def _agent(*, fallback_available: bool = True):
    agent = MagicMock()
    agent._vprint = MagicMock()
    agent._flush_status_buffer = MagicMock()
    agent._cleanup_task_resources = MagicMock()
    agent._persist_session = MagicMock()
    agent._build_assistant_message = MagicMock(return_value={"role": "assistant", "content": None})
    agent._session_messages = []
    agent._ephemeral_reasoning_off = False
    agent._fallback_chain = ["fallback-model"] if fallback_available else []
    agent._fallback_index = 0
    agent._try_activate_fallback = MagicMock(return_value=fallback_available)
    agent.log_prefix = ""
    agent.model = "main-model"
    agent.provider = "custom"
    agent.api_mode = "chat_completions"
    agent.quiet_mode = True
    agent.max_iterations = 50
    agent.max_tokens = 4096
    agent._strip_think_blocks = MagicMock(return_value="")
    # The transport must hand the response back unchanged, else the mock's MagicMock
    # "tool_calls" makes the harness take the truncated-tool-call branch.
    agent._get_transport = MagicMock(
        return_value=SimpleNamespace(normalize_response=lambda response: response)
    )
    # A real window size would make the "prompt filled the window" branch fire instead.
    agent.context_compressor = SimpleNamespace(context_length=0)
    return agent


def _response():
    return SimpleNamespace(
        id="resp-1",
        _dropped_tool_names=None,
        choices=[SimpleNamespace(
            index=0,
            message=SimpleNamespace(role="assistant", content=None, tool_calls=None,
                                    reasoning_content="reasoning only, no visible text"),
            finish_reason="length",
        )],
    )


def _run(agent, *, length_continue_retries: int = 4):
    from agent.turn_truncation import recover_from_truncation

    messages = [{"role": "user", "content": "write the long report"}]
    retry = MagicMock()
    verdict = recover_from_truncation(
        agent, _response(), "length", retry,
        messages=messages, conversation_history=None, api_kwargs={},
        api_call_count=length_continue_retries, effective_task_id=None, current_turn_user_idx=0,
        length_continue_retries=length_continue_retries, truncated_response_parts=[],
        truncated_tool_call_retries=0, retry_count=0, compression_attempts=0,
    )
    return verdict, retry, messages


def test_no_visible_text_hands_the_turn_to_the_fallback_model():
    agent = _agent()

    verdict, retry, _messages = _run(agent)

    agent._try_activate_fallback.assert_called_once()
    assert agent._try_activate_fallback.call_args.kwargs["reason"] is FailoverReason.incomplete_response
    # "break" lets the loop re-issue the call; the rebuilt messages restart on the fallback model.
    assert verdict.action == "break"
    assert retry.restart_with_rebuilt_messages is True


def test_without_a_fallback_the_sentinel_still_ends_the_turn():
    agent = _agent(fallback_available=False)

    verdict, _retry, _messages = _run(agent)

    agent._try_activate_fallback.assert_not_called()
    assert verdict.action == "return"
    assert "No visible answer was produced" in (verdict.result or {}).get("final_response", "")
