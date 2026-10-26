"""Post-redelivery correction of a settled-failed cron delivery (fork 2026-10-26, batch 3).

The WeCom dead-window chain books ``delivery_failed`` the moment the drain's send
fails (at-most-once, correct AT THAT INSTANT) — but the adapter's parked-group
redelivery may deliver the same content minutes later. These tests pin the
double-fenced correction: only a completed attempt with delivery_outcome exactly
``failed`` flips, the job record must still describe this execution with a
delivery-shaped failure, and a late settle never stomps a corrected record back
to failed.
"""

import sqlite3

from cron import executions, jobs
from cron.delivery_correction import correct_failed_delivery
from cron.delivery_outcome import _settle_job_record


def _make_job(tmp_path, monkeypatch) -> str:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    return jobs.create_job(prompt="p", schedule="every 1h", deliver="wecom:fixture")["id"]


def _failed_delivery(job_id: str, error: str = "Group proactive send failed: errcode=846609") -> str:
    """One completed run whose drain send failed: ledger failed, job delivery_failed."""
    execution_id = executions.create_execution(job_id, source="test")["id"]
    assert executions.finish_execution(
        execution_id, success=True, delivery_outcome="queued") is not None
    assert executions.settle_delivery_outcome(execution_id, "failed") is True

    def apply(all_jobs, _i, job):
        job["last_status"] = "delivery_failed"
        job["last_delivery_error"] = error
        job["last_error"] = None
        job.setdefault("canonical_completions", []).append(execution_id)
        jobs.save_jobs(all_jobs)
        return True

    assert jobs._with_job(job_id, apply, missing=False)
    return execution_id


def test_correction_flips_ledger_and_job_record(tmp_path, monkeypatch):
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = _failed_delivery(job_id)

    assert correct_failed_delivery(execution_id) is True
    assert executions.latest_execution(job_id)["delivery_outcome"] == "delivered"
    saved = jobs.get_job(job_id)
    assert saved["last_status"] == "ok"
    assert saved["last_delivery_error"] is None


def test_correction_is_idempotent(tmp_path, monkeypatch):
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = _failed_delivery(job_id)

    assert correct_failed_delivery(execution_id) is True
    assert correct_failed_delivery(execution_id) is False  # no longer a failed row
    assert jobs.get_job(job_id)["last_status"] == "ok"


def test_correction_leaves_a_failed_run_alone(tmp_path, monkeypatch):
    """A run that actually failed is not a delivery failure — never rewrite it."""
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = executions.create_execution(job_id, source="test")["id"]
    executions.finish_execution(
        execution_id, success=False, error="the agent run blew up", delivery_outcome="failed")

    assert correct_failed_delivery(execution_id) is False
    assert executions.latest_execution(job_id)["delivery_outcome"] == "failed"


def test_correction_leaves_a_queued_row_alone(tmp_path, monkeypatch):
    """A delivery still queued (no terminal outcome yet) is not a failure to correct."""
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = executions.create_execution(job_id, source="test")["id"]
    executions.finish_execution(execution_id, success=True, delivery_outcome="queued")

    assert correct_failed_delivery(execution_id) is False
    assert executions.latest_execution(job_id)["delivery_outcome"] == "queued"


def test_correction_respects_the_job_completion_fence(tmp_path, monkeypatch):
    """A later run owns the job record now — the correction must not rewrite it."""
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = _failed_delivery(job_id)

    def apply(all_jobs, _i, job):
        job["canonical_completions"].append("a-later-execution")
        jobs.save_jobs(all_jobs)
        return True

    assert jobs._with_job(job_id, apply, missing=False)

    assert correct_failed_delivery(execution_id) is False
    assert jobs.get_job(job_id)["last_status"] == "delivery_failed"


def test_correction_never_rewrites_a_failed_run_record(tmp_path, monkeypatch):
    """The record now describes a FAILED run (``last_error`` set) — not ours to rewrite."""
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = _failed_delivery(job_id)

    def apply(all_jobs, _i, job):
        job["last_error"] = "the agent run blew up"
        jobs.save_jobs(all_jobs)
        return True

    assert jobs._with_job(job_id, apply, missing=False)

    assert correct_failed_delivery(execution_id) is False
    assert jobs.get_job(job_id)["last_status"] == "delivery_failed"


def test_late_settle_never_stomps_a_corrected_record(tmp_path, monkeypatch):
    """The redelivery superseded the failure — a late settle must not re-fail the job."""
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = _failed_delivery(job_id)

    assert correct_failed_delivery(execution_id) is True
    _settle_job_record(job_id, execution_id, "late failure error")

    saved = jobs.get_job(job_id)
    assert saved["last_status"] == "ok"
    assert saved["last_delivery_error"] is None


def test_correction_is_best_effort_on_bookkeeping_failure(tmp_path, monkeypatch):
    """A bookkeeping failure must never undo the at-most-once send that just succeeded."""
    job_id = _make_job(tmp_path, monkeypatch)
    execution_id = _failed_delivery(job_id)

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(executions, "correct_failed_delivery_outcome", boom)
    assert correct_failed_delivery(execution_id) is False


def _target(job):
    """A minimal ``_TargetDelivery`` shape for ``_live_route_metadata`` (wecom target)."""
    from types import SimpleNamespace

    return SimpleNamespace(
        job=job, platform="wecom", platform_name="wecom", chat_id="g1", thread_id=None,
        transport=None, runtime_adapter=None, loop=None, notify_delivery=True,
        origin={}, origin_target=False, origin_user_id=None, is_dm_target=False,
        is_relay=False,
        mirror_text="", mirror_this_target=False, in_channel_surface=False,
        inchannel_continuable=False, opened_thread_id=None,
    )


def test_live_route_metadata_carries_the_execution_id():
    """The cron live lane threads the execution id to the adapter so a parked
    redelivery that succeeds later can correct the settled-failed row (batch 3)."""
    from cron.scheduler_delivery import _live_route_metadata

    _, route_metadata, _ = _live_route_metadata(_target({"id": "j1", "execution_id": "exec-9"}))

    assert route_metadata["execution_id"] == "exec-9"
    assert route_metadata["job_id"] == "j1"


def test_live_route_metadata_omits_the_key_without_an_execution_id():
    """Non-cron sends park without a correction state — no id, no key."""
    from cron.scheduler_delivery import _live_route_metadata

    _, route_metadata, _ = _live_route_metadata(_target({"id": "j1"}))

    assert "execution_id" not in route_metadata
    assert route_metadata["job_id"] == "j1"
