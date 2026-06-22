# Platform plugins — fork-maintained bundled adapters

This directory holds **bundled-plugin wrappers** around the
fork-maintained `gateway/platforms/*.py` adapters in the
[wbkunlun/hermes-agent](https://github.com/wbkunlun/hermes-agent) fork.

## Why thin wrappers?

Upstream NousResearch/hermes-agent is migrating platform adapters to
the `plugins/platforms/<name>/` bundled-plugin slot (see
`plugins/platforms/raft/`, `photon/`, `homeassistant/`, etc.). The fork
follows the same convention so:

- The fork's adapter inventory is discoverable alongside upstream plugins.
- `gateway.multiplex_profiles`, `hermes model picker`, and the desktop
  GUI's "API keys" / "Accounts" tabs pick up the fork's adapters through
  the same `register(ctx)` mechanism upstream uses.
- Future upstream refactors (e.g. moving `gateway/platforms/wecom.py`
  into a bundled plugin) merge cleanly because the fork has already
  adopted the same shape.

The fork's **`gateway/platforms/<name>.py` files are preserved
verbatim** — every production-tested wecom / feishu / telegram fix lives
there and continues to be the import target for `tools.send_message_tool`,
`gateway.run`, and any other internal caller. The plugin files here are
**thin re-exports**, not replacements.

## Layout

Each plugin is one directory:

```
plugins/platforms/<name>/
├── __init__.py     # PEP 562 lazy import + register(ctx) entry point
├── adapter.py      # `from gateway.platforms.<name> import *` re-export
└── plugin.yaml     # plugin manifest (kind: platform, label, requires_env)
```

## Fork-maintained plugins (v0.17.0)

| Plugin | Canonical adapter | Fork divergence vs upstream |
|---|---|---|
| `wecom`        | `gateway/platforms/wecom.py` (1792 lines)        | 8 production fixes (asyncio.Lock, 846609 retry, env fallback, race guards, singletons) |
| `wecom_callback` | `gateway/platforms/wecom_callback.py` (425 lines) | defusedxml, multi-app support |
| `dingtalk`     | `gateway/platforms/dingtalk.py` (1503 lines)      | — (synced with upstream) |
| `email`        | `gateway/platforms/email.py` (883 lines)         | 154 lines fork divergence |
| `feishu`       | `gateway/platforms/feishu.py` (5213 lines)       | 73 lines + helpers (feishu_comment, feishu_comment_rules, feishu_meeting_invite) |
| `matrix`       | `gateway/platforms/matrix.py` (4108 lines)       | 1858 lines fork divergence |
| `slack`        | `gateway/platforms/slack.py` (3815 lines)        | 347 lines fork divergence |
| `sms`          | `gateway/platforms/sms.py` (379 lines)           | — (synced with upstream) |
| `telegram`     | `gateway/platforms/telegram.py` (6888 lines)     | 900 lines + helpers (telegram_network) |
| `whatsapp`     | `gateway/platforms/whatsapp.py` (1193 lines)     | 407 lines fork divergence |

The `wecom_callback` plugin is registered separately under the
`wecom_callback` key (self-built-app HTTP transport) and ships with the
wecom plugin at `plugins/platforms/wecom/` because they share the
crypto helper.

## How a fork adapter becomes a plugin

Run the generator once after each upstream merge:

```bash
python scripts/_gen_platform_plugins.py           # generate all 8
python scripts/_gen_platform_plugins.py --check   # dry-run report
python scripts/_gen_platform_plugins.py feishu    # generate one
```

The generator parses `gateway/platforms/<name>.py` for the class name,
requirements check function, and `Platform.<X>` enum reference, then
emits the three plugin files. It is idempotent and overwrites.

To add a brand-new fork adapter (not in `gateway/platforms/` yet):

1. Create `gateway/platforms/<name>.py` with `<Name>Adapter` class
   inheriting `BasePlatformAdapter` and a `check_<name>_requirements()`
   function.
2. Register `Platform.<NAME>` in `gateway/config.py`.
3. Run `python scripts/_gen_platform_plugins.py <name>` to mint the
   plugin directory.
4. Update `hermes_cli/gateway.py` setup wizard and
   `tools/send_message_tool.py` `_send_<name>()` if needed (existing
   upstream slots will pick up the platform via the plugin registry).

## Production-fix preservation contract

The plugin layer is **non-invasive**: zero production code lives in
`plugins/platforms/`. Every `adapter.py` is a single `from
gateway.platforms.<name> import *` line. This means:

- A wecom fix in `gateway/platforms/wecom.py` becomes effective
  immediately for both `from gateway.platforms.wecom import …` AND
  `from plugins.platforms.wecom import …` callers — same class object,
  no duplication.
- An upstream refactor that touches `gateway/platforms/<name>.py` is
  automatically picked up by the plugin layer with zero plugin-side
  changes.
- The fork's `wechat` / `feishu` / `dingtalk` production deployments
  can roll back a bad merge by `git checkout <last-known-good-sha> --
  gateway/platforms/` — the plugin layer follows the import path.

## Verify a fresh clone

```bash
# All fork-maintained plugin imports succeed
python -c "
import importlib
for n in ['wecom','dingtalk','email','feishu','matrix','slack','sms','telegram','whatsapp']:
    importlib.import_module(f'plugins.platforms.{n}')
print('all 9 plugins importable')
"

# Identity preservation (gateway path == plugin path)
python -c "
import importlib, gateway.platforms as g
PLUGINS = ['dingtalk','email','feishu','matrix','slack','sms','telegram','whatsapp']
CLASS = {'dingtalk':'DingTalkAdapter','email':'EmailAdapter','feishu':'FeishuAdapter','matrix':'MatrixAdapter','slack':'SlackAdapter','sms':'SmsAdapter','telegram':'TelegramAdapter','whatsapp':'WhatsAppAdapter'}
for n,c in CLASS.items():
    gw = importlib.import_module(f'gateway.platforms.{n}')
    pl = importlib.import_module(f'plugins.platforms.{n}')
    assert getattr(gw,c) is getattr(pl,c), f'{n} identity broken'
print('identity preserved for all 8 adapters')
"
```