"""feishu adapter — thin re-export wrapper.

Full implementation lives in ``gateway/platforms/feishu.py`` so all
fork-specific production fixes are preserved verbatim. This module
exists so the bundled-plugin slot at
``plugins/platforms/feishu/adapter.py`` has a real module to point at
when ``register(ctx)`` is called.
"""

from __future__ import annotations

from gateway.platforms.feishu import *  # noqa: F401,F403
from gateway.platforms.feishu_comment import *  # noqa: F401,F403

from gateway.platforms.feishu_comment_rules import *  # noqa: F401,F403

from gateway.platforms.feishu_meeting_invite import *  # noqa: F401,F403

