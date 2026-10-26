"""Correct a cron delivery record after a late parked redelivery delivered (fork 2026-10-26, batch 3).

The WeCom dead-window chain books ``delivery_failed`` the moment the drain's send hits
the dead window (at-most-once, correct AT THAT INSTANT) — but the adapter's parked-group
redelivery may deliver the SAME content minutes later (timed retry / inbound ⏰ flush).
Without this module ``hermes cron list`` keeps showing a failure that no longer is one
(2026-10-26 17:31 incident: the report delivered at 17:32:16 under a ``delivery_failed``
stamp).

The correction is fenced twice: the ledger row must be a COMPLETED attempt with
delivery_outcome exactly ``failed`` (``executions.correct_failed_delivery_outcome``),
and the job record must still describe this execution (canonical_completions tail) with
a delivery-shaped failure (``last_error is None`` — the run itself succeeded; a record
that has since become a failed RUN is not ours to rewrite). ``settle_queued_delivery``
never stomps a corrected record back (the guard in delivery_outcome._settle_job_record).

Scope: only the cron live lane threads an execution id into the adapter today —
standalone/relay sends park without one and keep the failed stamp (no regression).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def correct_failed_delivery(execution_id: str) -> bool:
    """Flip a settled-failed delivery to delivered (ledger + job record). Best-effort:
    a bookkeeping failure must never undo the at-most-once send that just succeeded."""
    try:
        return _correct(str(execution_id))
    except Exception:
        logger.warning("Cron delivery %s: post-redelivery correction failed", execution_id, exc_info=True)
        return False


def _correct(execution_id: str) -> bool:
    from cron.executions import correct_failed_delivery_outcome

    job_id = correct_failed_delivery_outcome(execution_id)
    if not job_id:
        return False
    return _correct_job_record(job_id, execution_id)


def _correct_job_record(job_id: str, execution_id: str) -> bool:
    from cron.jobs import _with_job, save_jobs

    def apply(all_jobs, _i, job):
        completions = job.get("canonical_completions") or []
        if not completions or completions[-1] != execution_id:
            return False
        if job.get("last_error") is not None:
            return False  # failure-notice guard: the record now describes a failed RUN
        if job.get("last_status") == "ok" and not job.get("last_delivery_error"):
            return True  # already corrected (or settled ok)
        job["last_status"] = "ok"
        job["last_delivery_error"] = None
        save_jobs(all_jobs)
        return True

    return bool(_with_job(job_id, apply, missing=False))
