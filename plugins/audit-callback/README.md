# audit-callback

Reports agent execution events to the platform Agent Execution Audit API
(`POST /api/v1/agent/audit`, v0.5 spec). Fire-and-forget: a bounded queue +
single daemon worker ship events in batches (`IngestBatchRequest`, up to 100
items per POST); reporting failure never blocks the agent loop.

## What gets reported (reporting rules)

Work order 2026-09-28 改动4: these rules used to live only in
`plugin.yaml`'s description; they are documentation, kept here.

| Source tool | Reported? | event_type / action | risk_level |
|---|---|---|---|
| `terminal` — hardline or dangerous classification | ✅ always | `command` / `command.exec` | critical (hardline) / high (dangerous) |
| `terminal` — blocked at ANY severity (allowlist denial etc.) | ✅ always (unauthorized attempt) | `command` | medium (escalated from info by the block) |
| `terminal` — benign, completed | ❌ unless `HERMES_AUDIT_REPORT_ALL_COMMANDS=1` → info | `command` | info |
| `execute_code` | ✅ every call | `command` / `code.exec` | high (spawn/exec indicators) else medium |
| `computer_use` | ✅ every call | `command` / `desktop.control` | medium |
| `skill_view` | ✅ every load | `skill` / `skill.invoke` | medium (failed load → decision=blocked) |
| `skill_manage` (install/update/enable/…) | ✅ always | `skill` / `skill.invoke` | medium |
| `write_file` / `patch` | ✅ always | `command` / `file.write` | info; **empty/whitespace write escalates to medium** (truncation channel when `rm` is whitelisted out) |
| everything else (`read_file`, web tools, …) | ❌ | — | — |

Notes:

- Records are built at `post_tool_call`, so they carry post-execution
  signals (`exit_code`, `duration_ms`, `decision` — prefers the
  machine-readable `status:"blocked"` from the result payload).
- `actor` is server-overridden from auth (sandbox id) and is NOT sent;
  `username` carries the real triggering user from the gateway session
  context. ⚠️ Known gap (work order background): slash-command turns and
  a2a-toolset turns may reach the hook with an empty session context →
  empty `username`. Platform Option A (`HERMES_AUDIT_REPORT_ALL_COMMANDS=1`)
  only widens *which* commands are reported; it does not fix `username`.

## Configuration

| Env | Meaning |
|---|---|
| `HERMES_AUDIT_CALLBACK_URL` | Intake URL override. Wins when set. |
| `CONTROL_PLANE_URL` | When the override is absent, intake derives as `<CONTROL_PLANE_URL>/api/v1/agent/audit`. Unset both → plugin is a no-op. |
| `CONTROL_PLANE_AUTH` | Auth header, passed through verbatim (already carries `Bearer `). |
| `HERMES_AUDIT_TOKEN` | Legacy fallback credential; wrapped as `Bearer <token>`. |
| `HERMES_AUDIT_REPORT_ALL_COMMANDS` | `1` = also report every `terminal` command at `info`. Platform-side opt-in (work order Option A). |
| `HERMES_AUDIT_ENV` | Optional `env` tag attached to every event (e.g. `staging`). |
| `SANDBOX_ID` | When set, promoted to `resource_type=sandbox` / `resource_id`. |

## Failure behavior (work order 2026-09-28 改动3)

Retries: transient network errors and 5xx retry 3 attempts (0.5s/1.0s
backoff); 409-style per-item duplicates are success; other 4xx are terminal.
When a batch is dropped — retries exhausted, terminal 4xx, or an unretriable
POST error — the worker now logs one WARNING:

```
audit-callback: dropped N events after retries (last status=..., url=...)
```

so silent loss is visible in `agent.log` while remaining non-blocking. Clear
the drop count by recovering the control plane; the worker auto-resumes.
