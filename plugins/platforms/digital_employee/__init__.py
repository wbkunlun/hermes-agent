"""企微「数字员工」平台插件。

平台侧（Dify HTTP 节点）负责企微收发与会话历史，我们提供同步 HTTP 接口当
"大脑"：收 ``POST /chat`` → 跑一轮 agent → 在同一响应里回 ``message_list``。
超时的迟到结果改用企微发消息 API 主动推送。文件/图片暂不支持。

设计文档：docs/design/2026-11-23-digital-employee-platform-plugin.md
"""

from plugins.platforms.digital_employee.adapter import DigitalEmployeeAdapter, register

__all__ = ["DigitalEmployeeAdapter", "register"]
