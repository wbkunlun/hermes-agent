"""WeCom env -> gateway config bridge (``PlatformEntry.env_overrides``).

FORK(wbkunlun) 2026-10-10: the ``wecom_callback`` sibling platform was removed from
this deployment family (no fallback interface), so the upstream ``CALLBACK_*``
blocks (which reference the deleted ``Platform.WECOM_CALLBACK`` enum member at
module level) are dropped too — keeping them would fail the whole wecom plugin
load ("type object 'Platform' has no attribute 'WECOM_CALLBACK'").
"""

from gateway.config import Platform
from gateway.platform_env import EnvCred, int_or

ENV_ENABLE_CREDENTIALS = ("WECOM_BOT_ID", "WECOM_SECRET")
ENV_OVERRIDES = (
    EnvCred(
        Platform.WECOM, ("WECOM_BOT_ID", "WECOM_SECRET"),
        fixed=(("bot_id", "WECOM_BOT_ID"), ("secret", "WECOM_SECRET")),
        optional=(("websocket_url", "WECOM_WEBSOCKET_URL"),),
        home="WECOM_HOME_CHANNEL",
    ),
)

# Authorization env names (``PlatformEntry`` fields). Core reads them from this file when the
# live entry lacks them: a user plugin overriding this platform, safe mode, the plugin disabled.
AUTHZ = {
    "allowed_users_env": "WECOM_ALLOWED_USERS",
    "allow_all_env": "WECOM_ALLOW_ALL_USERS",
    "own_policy_env": ("WECOM_DM_POLICY", "WECOM_GROUP_POLICY"),
    "allowlist_suppresses_pairing": True,
}
