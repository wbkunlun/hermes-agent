"""email adapter — thin re-export wrapper.

Full implementation lives in ``gateway/platforms/email.py`` so all
fork-specific production fixes are preserved verbatim. This module
exists so the bundled-plugin slot at
``plugins/platforms/email/adapter.py`` has a real module to point at
when ``register(ctx)`` is called.
"""

from __future__ import annotations

from gateway.platforms.email import *  # noqa: F401,F403

