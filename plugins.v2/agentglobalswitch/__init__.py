"""
全局智能助手开关（自建）

提供一个可在企业微信/Telegram 等客户端菜单中一键触发的命令，用于切换
MoviePilot 的「全局 AI 智能体」开关（``AI_AGENT_GLOBAL``）：

- 命令 ``/agent``：在「开启 / 关闭」之间切换，并回复切换后的当前状态。

设计要点：
- ``AI_AGENT_GLOBAL`` 属于 Settings 源（持久化到 app.env）。
  ``settings.update_setting()`` 会同时更新内存与 app.env 文件，
  因此切换立即生效，且重启后保留。
- Chain 运行时通过 ``lambda: build_chain_runtime_config(settings)`` 实时读取
  settings，所以内存更新会被后续消息链路直接感知，无需重启 MP。
"""

from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.message import Message
from app.schemas.types import EventType

# 目标系统配置项
_TARGET_KEY = "AI_AGENT_GLOBAL"


class AgentGlobalSwitch(_PluginBase):
    """全局智能助手开关插件。"""

    # 插件名称
    plugin_name = "全局智能助手开关"
    # 插件描述
    plugin_desc = "提供一键命令，切换 MoviePilot 的全局 AI 智能体开关（AI_AGENT_GLOBAL），并回复当前状态。"
    # 插件图标
    plugin_icon = (
        "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/agent.png"
    )
    # 插件版本
    plugin_version = "1.0.2"
    # 插件作者
    plugin_author = "local"
    # 作者主页
    author_url = "https://github.com/jxxghp/MoviePilot"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "agentglobalswitch_"
    # 插件标签
    plugin_label = "智能体"
    # 插件排序
    plugin_order = 99
    # 认证等级
    auth_level = 1

    # 是否启用
    _enabled = False

    def init_plugin(self, config: Optional[dict] = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        if not config:
            return
        self._enabled = bool(config.get("enabled"))

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/agent",
                "event": EventType.PluginAction,
                "desc": "切换全局智能助手",
                "category": "智能体",
                "data": {"action": "toggle_agent_global"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return []

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """返回插件配置表单与默认配置。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "开启后 /ai 命令可用",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "发送 /agent 可在「开启 / 关闭全局 AI 智能体」之间切换，"
                                                "并回复当前状态。该开关切换后立即生效，无需重启。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面。"""
        if not self._enabled:
            return None
        state = self._current_state()
        state_text = "未知" if state is None else ("已开启" if state else "已关闭")
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": f"当前全局智能助手状态：{state_text}",
                },
            }
        ]

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        return None

    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event = None) -> None:
        """处理插件命令事件。"""
        if not self._enabled or not event or not event.event_data:
            return
        if event.event_data.get("action") != "toggle_agent_global":
            return
        logger.info("【全局智能助手】收到命令，开始切换 ...")
        after, error = self._toggle()
        if after is None:
            self._send_notify(
                title="【全局智能助手】",
                text=error or "切换失败，请检查日志",
            )
            return
        state_text = "已开启" if after else "已关闭"
        self._send_notify(
            title="【全局智能助手】",
            text=f"全局 AI 智能体：{state_text}",
        )

    @staticmethod
    def _current_state() -> Optional[bool]:
        """读取当前全局智能助手开关状态，读取失败返回 None。"""
        try:
            return bool(getattr(settings, _TARGET_KEY, False))
        except Exception as err:
            logger.error(f"【全局智能助手】读取 {_TARGET_KEY} 失败：{err}")
            return None

    @staticmethod
    def _toggle() -> Tuple[Optional[bool], str]:
        """切换全局智能助手开关，返回 (切换后状态, 错误信息)。"""
        current = AgentGlobalSwitch._current_state()
        if current is None:
            return None, "读取当前状态失败，请检查日志"
        target = not current
        try:
            success, message = settings.update_setting(_TARGET_KEY, target)
        except Exception as err:
            logger.error(f"【全局智能助手】写入 {_TARGET_KEY} 失败：{err}")
            return None, f"写入失败：{err}"
        if success is False:
            return None, f"切换失败：{message or '未知原因'}"
        after = AgentGlobalSwitch._current_state()
        logger.info(f"【全局智能助手】{_TARGET_KEY} 已切换为 {after}")
        return after, ""

    def _send_notify(self, title: str, text: str) -> None:
        """发送插件通知，且不附加插件页跳转链接。"""
        try:
            self.chain.post_message(
                Message(
                    mtype=NotificationType.Plugin,
                    title=title,
                    text=text,
                    link=None,
                )
            )
        except Exception as err:
            logger.error(f"【全局智能助手】发送通知失败：{err}")
