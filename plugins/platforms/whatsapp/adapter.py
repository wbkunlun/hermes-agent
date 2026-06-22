"""whatsapp adapter — thin re-export wrapper.

Full implementation lives in ``gateway/platforms/whatsapp.py`` so all
fork-specific production fixes are preserved verbatim. This module
exists so the bundled-plugin slot at
``plugins/platforms/whatsapp/adapter.py`` has a real module to point at
when ``register(ctx)`` is called.
"""

from __future__ import annotations

from gateway.platforms.whatsapp import *  # noqa: F401,F403

