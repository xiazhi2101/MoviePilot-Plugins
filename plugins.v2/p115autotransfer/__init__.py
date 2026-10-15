"""
115 网盘自动整理（自建）

从 P115StrmHelper 中抽取「网盘整理」核心能力，独立成一个轻量插件：

- 触发方式可切换：
  * 115 生活事件监听（实时，需 115 Cookie）
  * 定时轮询目录比对（无需生活事件，有延迟）
- 命中整理目录下的媒体文件后，调用 MoviePilot 的 TransferChain.do_transfer()
  按 MP 的目录/重命名规则整理入库（不生成 STRM，不做媒体服务器刷新）。
- 提供手动整理命令 /p115_transfer <路径>；不带参数时整理默认目录（manual_transfer_dir，默认 /最近接收）。

磁力离线自动整理（成人内容专用流程）：
- 命令 /ol <磁力链接>：提交 115 离线下载到 /云下载
- 自动识别：直接发送整条为 magnet:/ed2k: 的消息即可提交（配置项 auto_magnet，默认开）
- 定时轮询离线任务状态（clouddownload_task_list），完成后：
  * 只保留体积最大的视频文件（正片），删除其余全部文件
  * 移动到 /影视库/小姐姐们/<正片名>/<正片名>.<ext>
  * 清理 /云下载 下遗留空目录
  * 通知用户（含大小）

与上游 P115StrmHelper 完全脱钩，不受其升级影响。
"""

import re
from pathlib import Path
from queue import Empty, Queue
from threading import Event as ThreadEvent, Thread
from time import sleep, time
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger
from p115client import P115Client, check_response
from p115client.tool.iterdir import iter_files
from p115client.tool.life import BEHAVIOR_TYPE_TO_NAME, iter_life_behavior_once

from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.context import MediaInfo, TorrentInfo
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import FileItem, NotificationType, TransferInfo
from app.schemas.message import Message
from app.schemas.types import EventType, MediaType


class P115AutoTransfer(_PluginBase):
    # 插件名称
    plugin_name = "115网盘自动整理"
    # 插件描述
    plugin_desc = "监听 115 网盘目录（生活事件或定时轮询），自动调用 MP 整理规则入库；支持 /ol 磁力离线自动整理（仅处理本插件提交的离线任务）。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/115.png"
    # 插件版本
    plugin_version = "1.11.0"
    # 插件作者
    plugin_author = "local"
    # 作者主页
    author_url = "https://github.com/jxxghp/MoviePilot"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "p115autotransfer_"
    # 是否允许配置
    plugin_config = True
    # 是否仅管理员可用
    plugin_admin_only = True

    # 生活事件监听线程
    _life_thread: Optional[Thread] = None
    _stop_event: Optional[ThreadEvent] = None
    # 轮询线程
    _poll_thread: Optional[Thread] = None
    # 115 客户端
    _client: Optional[P115Client] = None
    # 已处理文件 ID 缓存（轮询模式去重）
    _seen_ids: set = set()
    # 轮询模式下已扫描过的目录 ID -> 上次扫描时间
    _scanned_dirs: Dict[str, float] = {}
    # 已提交的离线任务 hash -> 提交时间（用于超时告警）
    _offline_tasks: Dict[str, float] = {}
    # 整理提交串行队列：待提交整理的 (fileid, FileItem)，防止一次性打爆 115 接口
    _transfer_queue: Optional[Queue] = None
    _transfer_worker: Optional[Thread] = None
    # 队列已登记但尚未提交的文件 ID（避免重复入队）
    _queued_fileids: set = set()
    # 整理后待复查的源目录路径 -> 首次登记时间（用于延迟复查空目录）
    _pending_cleanup: Dict[str, float] = {}
    # 目录事件待整理队列：目录路径 -> 登记时间（目录刚建时内部文件可能尚未落盘，需延迟复查）
    # 目录事件待整理队列：目录路径 -> (首次登记时间, 上次复查时间)
    _pending_folders: Dict[str, tuple] = {}

    def __init__(self):
        super().__init__()
        self._enabled: bool = False
        self._notify: bool = False
        # 过程通知开关（默认关闭）：仅控制"开始整理/已提交 N 个文件/已提交离线"
        # 这类过程反馈；整理完成、任务失败、离线完成等结果与异常通知不受影响。
        self._notify_progress: bool = False
        self._cookies: Optional[str] = None
        self._storage: str = "u115"
        self._trigger_mode: str = "life"  # life / poll
        self._transfer_paths: Optional[str] = None
        self._poll_cron: str = "*/5 * * * *"
        self._min_file_size: int = 0  # MB
        # 默认纳入 new_folder：分享转存/整包转存常只产生目录事件，不补会导致漏整理
        self._event_modes: List[str] = ["upload_file", "receive_files", "move_file", "new_folder"]
        # 目录事件实时整理：目录事件到达即扫描，内部文件未落盘时按短间隔快速重试
        self._folder_delay_sec: int = 0
        # 空目录快速重试间隔（秒），配合 1 分钟定时任务实现准实时
        self._folder_retry_sec: int = 5
        # 目录事件延迟复查最大轮次，超出后不再重复登记
        self._folder_max_retry: int = 24
        self._last_error: Optional[str] = None
        # 磁力离线自动整理
        self._magnet_enabled: bool = True
        self._magnet_offline_dir: str = "/云下载"
        self._magnet_target_dir: str = "/影视库/小姐姐们"
        # 手动整理命令的默认目录：菜单点击（无参数）时直接整理该目录
        self._manual_transfer_dir: str = "/最近接收"
        # 自动识别磁力链接：整条消息以 magnet:/ed2k: 开头时自动提交离线下载
        self._auto_magnet: bool = True
        self._magnet_cron: str = "*/2 * * * *"
        self._magnet_timeout_min: int = 180
        # 整理后清理空目录
        self._cleanup_empty_after_transfer: bool = True
        self._cleanup_delay_min: int = 3
        # 整理提交节流：串行队列逐个提交，避免一次性打爆 115 接口触发风控
        self._transfer_throttle_enabled: bool = True
        # 每个文件提交后的等待间隔（秒）
        self._transfer_interval_sec: int = 3
        # 每个扫描批次最多提交的文件数（0 表示不限制）
        self._transfer_batch_max: int = 10
        # 成人内容入库（绕过 MP 识别，直接按番号入库）
        self._adult_enabled: bool = True
        self._adult_library_dir: str = "/影视库/小姐姐们"
        self._adult_media_server: str = ""

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", False)
            self._notify_progress = config.get("notify_progress", False)
            self._cookies = config.get("cookies") or None
            self._storage = config.get("storage") or "u115"
            self._trigger_mode = config.get("trigger_mode") or "life"
            self._transfer_paths = config.get("transfer_paths") or None
            self._poll_cron = config.get("poll_cron") or "*/5 * * * *"
            self._min_file_size = int(config.get("min_file_size") or 0)
            modes = config.get("event_modes")
            if isinstance(modes, list) and modes:
                self._event_modes = modes
            elif isinstance(modes, str) and modes.strip():
                self._event_modes = [m.strip() for m in modes.split(",") if m.strip()]
            # 目录事件实时整理参数
            self._folder_delay_sec = int(config.get("folder_delay_sec") or 0)
            self._folder_retry_sec = int(config.get("folder_retry_sec") or 5)
            self._folder_max_retry = int(config.get("folder_max_retry") or 24)
            # 磁力离线
            self._magnet_enabled = config.get("magnet_enabled", True)
            self._magnet_offline_dir = (config.get("magnet_offline_dir") or "/云下载").strip()
            self._magnet_target_dir = (config.get("magnet_target_dir") or "/影视库/小姐姐们").strip()
            self._manual_transfer_dir = (config.get("manual_transfer_dir") or "/最近接收").strip() or "/最近接收"
            self._auto_magnet = bool(config.get("auto_magnet", True))
            self._magnet_cron = config.get("magnet_cron") or "*/2 * * * *"
            self._magnet_timeout_min = int(config.get("magnet_timeout_min") or 180)
            # 整理后清理空目录
            self._cleanup_empty_after_transfer = config.get("cleanup_empty_after_transfer", True)
            self._cleanup_delay_min = int(config.get("cleanup_delay_min") or 3)
            # 整理提交节流
            self._transfer_throttle_enabled = config.get("transfer_throttle_enabled", True)
            self._transfer_interval_sec = max(int(config.get("transfer_interval_sec") or 3), 0)
            self._transfer_batch_max = max(int(config.get("transfer_batch_max") or 10), 0)
            # 成人内容入库
            self._adult_enabled = config.get("adult_enabled", True)
            self._adult_library_dir = (config.get("adult_library_dir") or "/影视库/小姐姐们").strip()
            self._adult_media_server = (config.get("adult_media_server") or "").strip()

        # 先停掉旧任务
        self.stop_service()

        if not self._enabled:
            return

        if not self._cookies:
            self._last_error = "未配置 115 Cookie，插件无法工作"
            logger.error(f"【网盘整理】{self._last_error}")
            return

        if not self._transfer_paths and not self._magnet_enabled:
            self._last_error = "未配置整理目录，插件无法工作"
            logger.error(f"【网盘整理】{self._last_error}")
            return

        self._last_error = None
        self._client = P115Client(self._cookies, app="android")

        if self._trigger_mode == "life":
            self._start_life_thread()
        else:
            self._start_poll_thread()

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        # 停止生活事件线程
        if self._stop_event:
            self._stop_event.set()
        if self._life_thread and self._life_thread.is_alive():
            self._life_thread.join(timeout=5)
        self._life_thread = None
        self._stop_event = None
        # 停止轮询线程
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5)
        self._poll_thread = None
        # 整理提交线程：置空引用，worker 为 daemon 且队列持久，重载后由守护任务重建
        self._transfer_worker = None
        self._queued_fileids.clear()
        self._client = None

    # ------------------------------------------------------------------ #
    # 配置 / 命令 / 服务
    # ------------------------------------------------------------------ #
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify_progress",
                                            "label": "过程通知",
                                            "hint": "开启后额外发送「开始整理 / 已提交 N 个文件 / 已提交离线」等过程反馈",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "trigger_mode",
                                            "label": "触发方式",
                                            "items": [
                                                {
                                                    "title": "115 生活事件监听（实时）",
                                                    "value": "life",
                                                },
                                                {
                                                    "title": "定时轮询目录（有延迟）",
                                                    "value": "poll",
                                                },
                                            ],
                                        },
                                    }
                                ],
                            },
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cookies",
                                            "label": "115 Cookie",
                                            "placeholder": "UID=xxx; CID=xxx; SEID=xxx; KID=xxx",
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
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "transfer_paths",
                                            "label": "整理目录",
                                            "rows": 4,
                                            "placeholder": "每行一个 115 网盘目录，例如：\n/最近接收\n/待整理",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "poll_cron",
                                            "label": "轮询 Cron（仅轮询模式）",
                                            "placeholder": "*/5 * * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_file_size",
                                            "label": "最小文件大小(MB)",
                                            "placeholder": "0 表示不限制",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "storage",
                                            "label": "存储模块",
                                            "items": [
                                                {"title": "115网盘(u115)", "value": "u115"},
                                                {
                                                    "title": "115网盘Plus",
                                                    "value": "115网盘Plus",
                                                },
                                            ],
                                        },
                                    }
                                ],
                            },
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "event_modes",
                                            "label": "监听事件类型（仅事件模式）",
                                            "multiple": True,
                                            "chips": True,
                                            "items": [
                                                {
                                                    "title": "上传文件/目录",
                                                    "value": "upload_file",
                                                },
                                                {
                                                    "title": "接收文件（转存）",
                                                    "value": "receive_files",
                                                },
                                                {
                                                    "title": "移动文件/目录",
                                                    "value": "move_file",
                                                },
                                                {
                                                    "title": "新建目录（整包转存）",
                                                    "value": "new_folder",
                                                },
                                                {
                                                    "title": "复制文件",
                                                    "value": "copy_file",
                                                },
                                                {
                                                    "title": "复制文件夹",
                                                    "value": "copy_folder",
                                                },
                                            ],
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
                                        "component": "VDivider",
                                        "props": {"class": "my-4"},
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
                                            "text": "磁力离线自动整理：发送 /ol <磁力链接>，插件提交 115 离线下载，完成后只保留正片并移动到目标目录。",
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "magnet_enabled",
                                            "label": "启用磁力离线整理",
                                            "hint": "只处理通过 /ol 或直接发送磁力链接提交的任务；其他来源（如 NextFind）转存到离线目录的文件不会被整理",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "auto_magnet",
                                            "label": "自动识别磁力链接",
                                            "hint": "开启后，直接发送整条为 magnet:/ed2k: 的消息即可自动提交离线，无需 /ol 前缀",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "magnet_offline_dir",
                                            "label": "离线保存目录",
                                            "placeholder": "/云下载",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "magnet_target_dir",
                                            "label": "正片目标目录",
                                            "placeholder": "/影视库/小姐姐们",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "magnet_cron",
                                            "label": "离线监控 Cron",
                                            "placeholder": "*/2 * * * *",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "magnet_timeout_min",
                                            "label": "离线超时告警(分钟)",
                                            "placeholder": "180",
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
                                        "component": "VDivider",
                                        "props": {"class": "my-4"},
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
                                            "text": "手动整理目录：菜单点击 /p115_transfer（不带参数）时直接整理该目录；带参数时仍按参数路径整理。",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "manual_transfer_dir",
                                            "label": "手动整理默认目录",
                                            "placeholder": "/最近接收",
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
                                        "component": "VDivider",
                                        "props": {"class": "my-4"},
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
                                            "text": "整理后清理空目录：整理完成后延迟复查源目录，若目录内已无任何文件（只剩空文件夹），则删除该目录。",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "cleanup_empty_after_transfer",
                                            "label": "整理后清理空目录",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cleanup_delay_min",
                                            "label": "复查延迟(分钟)",
                                            "placeholder": "3",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "folder_retry_sec",
                                            "label": "空目录重试间隔(秒)",
                                            "placeholder": "5",
                                            "hint": "目录事件到达时若内部文件未落盘，按此间隔快速重试",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "folder_max_retry",
                                            "label": "空目录最大重试次数",
                                            "placeholder": "24",
                                            "hint": "重试间隔 × 次数 为总等待上限",
                                        },
                                    }
                                ],
                            },
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
                                            "text": "整理提交节流：批量转存时逐条串行提交整理，避免短时间内大量 115 接口调用触发「访问上限」风控（触发后全体接口冷却 1 小时）。",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "transfer_throttle_enabled",
                                            "label": "启用整理提交节流",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "transfer_interval_sec",
                                            "label": "每条提交间隔(秒)",
                                            "placeholder": "3",
                                            "hint": "建议 3~5 秒；越小越快但越容易触发风控",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "transfer_batch_max",
                                            "label": "单批最大提交数",
                                            "placeholder": "10",
                                            "hint": "0 表示不限制",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "adult_enabled",
                                            "label": "成人内容直接入库",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "adult_library_dir",
                                            "label": "成人内容库目录",
                                            "placeholder": "/影视库/小姐姐们",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "adult_media_server",
                                            "label": "媒体服务器(可空)",
                                            "placeholder": "留空则使用默认媒体服务器",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": self._enabled,
            "notify": self._notify,
            "notify_progress": self._notify_progress,
            "cookies": self._cookies,
            "storage": self._storage,
            "trigger_mode": self._trigger_mode,
            "transfer_paths": self._transfer_paths,
            "poll_cron": self._poll_cron,
            "min_file_size": self._min_file_size,
            "event_modes": self._event_modes,
            "folder_delay_sec": self._folder_delay_sec,
            "folder_retry_sec": self._folder_retry_sec,
            "folder_max_retry": self._folder_max_retry,
            "magnet_enabled": self._magnet_enabled,
            "magnet_offline_dir": self._magnet_offline_dir,
            "magnet_target_dir": self._magnet_target_dir,
            "manual_transfer_dir": self._manual_transfer_dir,
            "auto_magnet": self._auto_magnet,
            "magnet_cron": self._magnet_cron,
            "magnet_timeout_min": self._magnet_timeout_min,
            "cleanup_empty_after_transfer": self._cleanup_empty_after_transfer,
            "cleanup_delay_min": self._cleanup_delay_min,
            "transfer_throttle_enabled": self._transfer_throttle_enabled,
            "transfer_interval_sec": self._transfer_interval_sec,
            "transfer_batch_max": self._transfer_batch_max,
            "adult_enabled": self._adult_enabled,
            "adult_library_dir": self._adult_library_dir,
            "adult_media_server": self._adult_media_server,
        }

    def get_page(self) -> Optional[List[dict]]:
        return None

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/p115_transfer",
                "event": EventType.PluginAction,
                "desc": "手动整理 115 网盘指定目录（不带参数则整理默认目录）",
                "category": "网盘整理",
                "data": {"action": "p115_manual_transfer"},
            },
            {
                "cmd": "/ol",
                "event": EventType.PluginAction,
                "desc": "提交磁力链接到 115 离线下载，完成后自动整理正片",
                "category": "网盘整理",
                "data": {"action": "p115_offline_magnet"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        services: List[Dict[str, Any]] = []
        if self._trigger_mode == "poll" and self._poll_cron:
            services.append(
                {
                    "id": "P115AutoTransfer_poll",
                    "name": "115网盘目录轮询整理",
                    "trigger": CronTrigger.from_crontab(self._poll_cron),
                    "func": self.poll_once,
                    "kwargs": {},
                }
            )
        # 事件模式下的线程守护
        if self._trigger_mode == "life":
            services.append(
                {
                    "id": "P115AutoTransfer_guard",
                    "name": "115生活事件线程守护",
                    "trigger": CronTrigger.from_crontab("* * * * *"),
                    "func": self.check_life_guard,
                    "kwargs": {},
                }
            )
        # 磁力离线任务监控
        if self._magnet_enabled and self._magnet_cron:
            services.append(
                {
                    "id": "P115AutoTransfer_magnet",
                    "name": "115磁力离线任务监控",
                    "trigger": CronTrigger.from_crontab(self._magnet_cron),
                    "func": self.check_offline_tasks,
                    "kwargs": {},
                }
            )
        # 整理后空目录复查
        if self._cleanup_empty_after_transfer:
            services.append(
                {
                    "id": "P115AutoTransfer_cleanup",
                    "name": "115整理后空目录清理",
                    "trigger": CronTrigger.from_crontab("* * * * *"),
                    "func": self.check_pending_cleanup,
                    "kwargs": {},
                }
            )
        # 目录事件快速重试（目录刚建时内部文件可能尚未落盘）
        if self._trigger_mode == "life" and "new_folder" in self._event_modes:
            services.append(
                {
                    "id": "P115AutoTransfer_folder",
                    "name": "115目录事件快速整理",
                    "trigger": CronTrigger.from_crontab("*/1 * * * *"),
                    "func": self.check_pending_folders,
                    "kwargs": {},
                }
            )
        # 离线任务白名单清理（防止长期运行后 _offline_tasks 膨胀）
        services.append(
            {
                "id": "P115AutoTransfer_offline_prune",
                "name": "115磁力离线白名单清理",
                "trigger": CronTrigger.from_crontab("0 3 * * *"),
                "func": self.prune_offline_whitelist,
                "kwargs": {},
            }
        )
        # 整理提交线程守护（提交线程意外退出时自动重启）
        if self._transfer_throttle_enabled:
            services.append(
                {
                    "id": "P115AutoTransfer_transfer_guard",
                    "name": "115整理提交线程守护",
                    "trigger": CronTrigger.from_crontab("* * * * *"),
                    "func": self.check_transfer_guard,
                    "kwargs": {},
                }
            )
        return services

    # ------------------------------------------------------------------ #
    # 事件处理（手动命令）
    # ------------------------------------------------------------------ #
    @eventmanager.register(EventType.PluginAction)
    def on_plugin_action(self, event: Event):
        if not event:
            return
        event_data = event.event_data or {}
        action = event_data.get("action")
        if action == "p115_manual_transfer":
            self._handle_manual_transfer(event_data)
        elif action == "p115_offline_magnet":
            self._handle_offline_magnet(event_data)

    @eventmanager.register(EventType.UserMessage)
    def on_user_message(self, event: Event = None):
        """监听未被宿主命令消费的用户消息，自动识别整条为磁力/ed2k 链接的消息。

        仅当整条消息（去除首尾空格）以 ``magnet:`` 或 ``ed2k:`` 开头时处理，
        避免正文中偶然出现的链接造成误触发。可在插件配置中关闭该行为。
        """
        if not self._enabled or not self._auto_magnet or not self._magnet_enabled:
            return
        if not event or not event.event_data:
            return
        text = str(event.event_data.get("text") or "").strip()
        if not text:
            return
        if not re.match(r"^(magnet:|ed2k:)", text, re.IGNORECASE):
            return
        logger.info("【磁力离线】检测到用户消息中的磁力/ed2k 链接，自动提交离线任务")
        if not self._ensure_client():
            return
        Thread(
            target=self.submit_offline,
            args=(text,),
            daemon=True,
        ).start()

    def _handle_manual_transfer(self, event_data: dict):
        arg_str = (event_data.get("arg_str") or "").strip()
        if not arg_str:
            # 菜单点击不带参数时，直接整理默认目录（配置项 manual_transfer_dir，默认 /最近接收）
            arg_str = self._manual_transfer_dir or "/最近接收"
        if not self._ensure_client():
            return
        if self._notify_progress:
            self._send_notify(
                title="【网盘整理】",
                text=f"开始手动整理：{arg_str}",
            )
        Thread(
            target=self.transfer_path,
            args=(arg_str,),
            daemon=True,
        ).start()

    def _handle_offline_magnet(self, event_data: dict):
        arg_str = (event_data.get("arg_str") or "").strip()
        if not arg_str:
            self._send_notify(
                title="【磁力离线】",
                text="请提供磁力链接，例如：/ol magnet:?xt=urn:btih:xxxx",
            )
            return
        if not self._magnet_enabled:
            self._send_notify(
                title="【磁力离线】",
                text="磁力离线整理功能未启用",
            )
            return
        if not self._ensure_client():
            return
        Thread(
            target=self.submit_offline,
            args=(arg_str,),
            daemon=True,
        ).start()

    def _send_notify(self, title: str, text: str) -> None:
        """发送插件通知，且不附加插件页跳转链接。

        基类 ``post_message`` 在未传 link 时会自动补上
        ``#/plugins?tab=installed&id=<类名>``，用户不需要这条「点击查看」，
        因此这里直接走 chain 发送，显式将 link 置空。
        """
        try:
            self.chain.post_message(
                Message(
                    mtype=NotificationType.Plugin,
                    title=title,
                    text=text,
                    link=None,
                )
            )
        except Exception as e:
            logger.error(f"【网盘整理】发送通知失败：{e}")

    def _ensure_client(self) -> bool:
        if self._client:
            return True
        if not self._cookies:
            self._send_notify(
                title="【网盘整理】",
                text="未配置 115 Cookie，无法执行",
            )
            return False
        self._client = P115Client(self._cookies, app="android")
        return True

    # ------------------------------------------------------------------ #
    # 磁力离线：提交
    # ------------------------------------------------------------------ #
    def submit_offline(self, magnet: str):
        """提交磁力链接到 115 离线下载"""
        magnet = magnet.strip().strip("<>")
        if not re.match(r"^(magnet:|ed2k:|https?://|ftp://)", magnet, re.IGNORECASE):
            self._send_notify(
                title="【磁力离线】",
                text=f"链接格式不支持：{magnet[:80]}",
            )
            return
        try:
            cid = self._get_cid(self._magnet_offline_dir)
            if cid is None or cid < 0:
                self._send_notify(
                    title="【磁力离线】",
                    text=f"离线保存目录不存在：{self._magnet_offline_dir}",
                )
                return
            resp = self._client.clouddownload_task_add_url(
                {"url": magnet, "wp_path_id": int(cid)}
            )
            check_response(resp)
            info_hash = resp.get("info_hash") or resp.get("hash") or ""
            if info_hash:
                self._offline_tasks[str(info_hash).lower()] = time()
            logger.info(f"【磁力离线】已提交离线任务 hash={info_hash} -> {self._magnet_offline_dir}")
            # /ol 属用户主动发起的单次操作，其过程与结果通知一律保留
            self._send_notify(
                title="【磁力离线】",
                text=f"已提交离线下载\n保存目录：{self._magnet_offline_dir}\n完成后将自动整理正片",
            )
        except Exception as e:
            logger.error(f"【磁力离线】提交失败: {e}", exc_info=True)
            self._send_notify(
                title="【磁力离线】",
                text=f"提交离线失败：{e}",
            )

    # ------------------------------------------------------------------ #
    # 磁力离线：监控与整理
    # ------------------------------------------------------------------ #
    def check_offline_tasks(self):
        """定时检查离线任务，完成则整理。

        仅处理**本插件主动提交**的离线任务：以 ``self._offline_tasks`` 中记录的
        info_hash 白名单为准。NextFind/其他来源自行提交到 /云下载 的任务不会被
        命中，从而避免误整理（例如把 NF 转存的剧集正片当成磁力离线成果）。
        """
        if not self._enabled or not self._magnet_enabled:
            return
        # 白名单为空时无任何待处理任务，直接返回，不触碰 /云下载
        if not self._offline_tasks:
            return
        if not self._ensure_client():
            return
        try:
            resp = self._client.clouddownload_task_list(
                {"page": 1, "page_size": 50, "stat": 0}
            )
            check_response(resp)
        except Exception as e:
            logger.error(f"【磁力离线】获取任务列表失败: {e}", exc_info=True)
            return

        tasks = resp.get("tasks") or []
        now = time()
        for task in tasks:
            try:
                info_hash = str(task.get("info_hash") or task.get("hash") or "").lower()
                # 白名单：只处理本插件通过 /ol（或自动识别磁力消息）提交的任务
                if not info_hash or info_hash not in self._offline_tasks:
                    continue
                status = task.get("status")
                display_status = task.get("display_status") or ""
                name = task.get("name") or ""
                # 已完成
                if status == 2 or display_status == "finished":
                    if self._is_task_processed(info_hash):
                        continue
                    self._process_finished_task(task)
                    self._mark_task_processed(info_hash)
                    self._offline_tasks.pop(info_hash, None)
                # 失败
                elif isinstance(status, int) and 1 <= status <= 9:
                    if self._is_task_processed(info_hash):
                        continue
                    self._mark_task_processed(info_hash)
                    self._offline_tasks.pop(info_hash, None)
                    self._send_notify(
                        title="【磁力离线】",
                        text=f"离线任务失败：{name}\n状态：{task.get('status_text') or status}",
                    )
                # 超时告警
                elif info_hash in self._offline_tasks:
                    elapsed = (now - self._offline_tasks[info_hash]) / 60
                    if elapsed >= self._magnet_timeout_min:
                        self._offline_tasks.pop(info_hash, None)
                        self._send_notify(
                            title="【磁力离线】",
                            text=f"离线任务长时间未完成（{int(elapsed)} 分钟）：{name}\n进度：{task.get('display_percent')}%",
                        )
            except Exception as e:
                logger.error(f"【磁力离线】处理任务失败: {e}", exc_info=True)

    def prune_offline_whitelist(self):
        """清理超过超时阈值仍未完成的离线任务白名单记录，避免长期膨胀。

        仅清除监控记录，不代表删除 115 云端任务。
        """
        if not self._offline_tasks:
            return
        now = time()
        ttl = max(self._magnet_timeout_min, 60) * 60
        stale = [h for h, t in self._offline_tasks.items() if (now - t) > ttl]
        for h in stale:
            self._offline_tasks.pop(h, None)
        if stale:
            logger.info(f"【磁力离线】已清理 {len(stale)} 条过期离线监控记录")

    def _process_finished_task(self, task: Dict[str, Any]):
        """处理已完成的离线任务：保留正片、移动、清理"""
        name = task.get("name") or ""
        del_path = (task.get("del_path") or "").strip()
        if not del_path:
            logger.warning(f"【磁力离线】任务缺少 del_path，跳过：{name}")
            return
        # 落盘目录（del_path 形如 "SNOS-406-C/" 或 "xxx.iso"）
        rel = del_path.rstrip("/")
        src_path = f"{self._magnet_offline_dir.rstrip('/')}/{rel}"
        logger.info(f"【磁力离线】开始整理：{src_path}")

        try:
            from app.modules.filemanager.storages.u115 import U115Pan

            pan = U115Pan()
            src_item = pan.get_item(Path(src_path))
            if not src_item:
                logger.warning(f"【磁力离线】源路径不存在：{src_path}")
                self._send_notify(
                    title="【磁力离线】",
                    text=f"离线已完成但未找到文件：{src_path}",
                )
                return

            # 收集目录内所有文件
            if src_item.type == "dir":
                files = self._collect_files(pan, src_item)
            else:
                files = [src_item]

            if not files:
                logger.warning(f"【磁力离线】目录内无文件：{src_path}")
                return

            # 正片 = 体积最大的视频文件
            videos = [f for f in files if self._is_video(f)]
            if not videos:
                # 无视频文件，取体积最大的文件兜底
                videos = sorted(files, key=lambda x: x.size or 0, reverse=True)[:1]
            main = max(videos, key=lambda x: x.size or 0)
            main_name = self._clean_main_name(main.basename or Path(main.name).stem)

            # 删除其余全部文件
            deleted = 0
            for f in files:
                if f.fileid == main.fileid:
                    continue
                try:
                    if pan.delete(f):
                        deleted += 1
                except Exception as e:
                    logger.warning(f"【磁力离线】删除失败 {f.path}: {e}")

            # 目标目录
            target_root = pan.get_item(Path(self._magnet_target_dir))
            if not target_root:
                logger.error(f"【磁力离线】目标目录不存在：{self._magnet_target_dir}")
                self._send_notify(
                    title="【磁力离线】",
                    text=f"目标目录不存在：{self._magnet_target_dir}",
                )
                return

            # 创建 <正片名> 子目录
            sub_dir = pan.get_item(Path(f"{self._magnet_target_dir.rstrip('/')}/{main_name}"))
            if not sub_dir:
                sub_dir = pan.create_folder(target_root, main_name)
            if not sub_dir:
                logger.error(f"【磁力离线】创建目标子目录失败：{main_name}")
                return

            # 移动正片
            # 移动正片（文件名同样使用清洗后的番号，保留原扩展名）
            ext = Path(main.name).suffix or ""
            new_file_name = f"{main_name}{ext}"
            ok = pan.move(main, Path(sub_dir.path), new_file_name)
            if not ok:
                logger.error(f"【磁力离线】移动正片失败：{main.path} -> {sub_dir.path}")
                self._send_notify(
                    title="【磁力离线】",
                    text=f"移动正片失败：{main.name}",
                )
                return

            final_path = f"{sub_dir.path.rstrip('/')}/{new_file_name}"
            size_mb = round((main.size or 0) / 1024 / 1024, 1)
            logger.info(f"【磁力离线】整理完成：{final_path}（{size_mb} MB，删除 {deleted} 个文件）")

            # 清理离线目录下遗留空目录
            self._cleanup_empty_dirs(pan)
            # 登记源目录复查（正片已移走，若只剩空文件夹则删除）
            self._schedule_cleanup(src_path)

            # 成人内容：直接入库（绕过 MP 识别），并触发媒体服务器扫描
            library_path = final_path
            if self._adult_enabled:
                try:
                    library_path = self._adult_library_transfer(pan, main, main_name, ext)
                except Exception as e:
                    logger.error(f"【磁力离线】成人内容入库失败: {e}", exc_info=True)

            # /ol 属用户主动发起的单次操作，整理完成通知始终发送
            self._send_notify(
                title="【磁力离线】整理完成",
                text=(
                    f"正片：{main_name}\n"
                    f"大小：{size_mb} MB\n"
                    f"路径：{library_path}\n"
                    f"已删除推广/多余文件：{deleted} 个"
                ),
            )
        except Exception as e:
            logger.error(f"【磁力离线】整理失败: {e}", exc_info=True)
            self._send_notify(
                title="【磁力离线】",
                text=f"整理失败：{name}\n{e}",
            )

    def _adult_library_transfer(self, pan, main: FileItem, main_name: str, ext: str) -> str:
        """成人内容入库收尾：确认目录并触发媒体服务器扫描。

        不走 MP 识别/整理（成人内容识别会失败）。注意正片已在调用方移动到
        ``<成人库目录>/<番号>/``，此处**不可再次移动**——重复移动会因源文件
        已不在原路径而误报「移动失败」。这里只负责目录校验与刷新媒体服务器。
        """
        lib_root_path = (self._adult_library_dir or "/影视库/小姐姐们").rstrip("/")
        lib_root = pan.get_item(Path(lib_root_path))
        if not lib_root:
            logger.error(f"【磁力离线】成人库目录不存在：{lib_root_path}")
            return main.path

        # 目标子目录 <番号>
        target_dir = pan.get_item(Path(f"{lib_root_path}/{main_name}"))
        if not target_dir:
            target_dir = pan.create_folder(lib_root, main_name)
        if not target_dir:
            logger.error(f"【磁力离线】创建成人库子目录失败：{main_name}")
            return main.path

        new_file_name = f"{main_name}{ext}"
        final_path = f"{target_dir.path.rstrip('/')}/{new_file_name}"
        logger.info(f"【磁力离线】成人内容已入库：{final_path}")

        # 触发媒体服务器扫描
        self._refresh_media_server(lib_root_path)
        return final_path

    def _refresh_media_server(self, path: str):
        """触发媒体服务器扫描指定路径（失败不影响主流程）"""
        try:
            from app.helper.mediaserver import MediaServerHelper
            from app.schemas.mediaserver import RefreshMediaItem

            name_filters = [self._adult_media_server] if self._adult_media_server else None
            services = MediaServerHelper().get_services(name_filters=name_filters)
            if not services:
                logger.info("【磁力离线】未获取到媒体服务器实例，跳过扫描")
                return
            item = RefreshMediaItem(
                title=Path(path).name,
                type=MediaType.UNKNOWN,
                target_path=Path(path),
            )
            for name, service in services.items():
                try:
                    if service.instance.is_inactive():
                        logger.warning(f"【磁力离线】媒体服务器 {name} 未连接，跳过")
                        continue
                    if hasattr(service.instance, "refresh_library_by_items"):
                        service.instance.refresh_library_by_items([item])
                    elif hasattr(service.instance, "refresh_root_library"):
                        service.instance.refresh_root_library()
                    else:
                        logger.warning(f"【磁力离线】{name} 不支持刷新")
                        continue
                    logger.info(f"【磁力离线】已触发 {name} 扫描：{path}")
                except Exception as e:
                    logger.warning(f"【磁力离线】触发 {name} 扫描失败: {e}")
        except Exception as e:
            logger.warning(f"【磁力离线】媒体服务器扫描调用失败: {e}")

    def _collect_files(self, pan, dir_item: FileItem) -> List[FileItem]:
        """递归收集目录下所有文件"""
        result: List[FileItem] = []
        try:
            children = pan.list(dir_item) or []
        except Exception as e:
            logger.warning(f"【磁力离线】列目录失败 {dir_item.path}: {e}")
            return result
        for child in children:
            if child.type == "dir":
                result.extend(self._collect_files(pan, child))
            else:
                result.append(child)
        return result

    def _cleanup_empty_dirs(self, pan):
        """清理离线目录下遗留的空目录"""
        root = pan.get_item(Path(self._magnet_offline_dir))
        if not root:
            return
        try:
            children = pan.list(root) or []
        except Exception as e:
            logger.warning(f"【磁力离线】清理空目录失败: {e}")
            return
        for child in children:
            if child.type != "dir":
                continue
            try:
                sub = pan.list(child) or []
                if not sub:
                    pan.delete(child)
                    logger.info(f"【磁力离线】已删除空目录：{child.path}")
            except Exception as e:
                logger.warning(f"【磁力离线】删除空目录失败 {child.path}: {e}")

    # ------------------------------------------------------------------ #
    # 整理后空目录清理
    # ------------------------------------------------------------------ #
    def _schedule_cleanup(self, path: str):
        """整理完成后登记源目录，延迟复查是否可删除"""
        if not self._cleanup_empty_after_transfer or not path:
            return
        try:
            self._pending_cleanup[path] = time()
            logger.info(f"【网盘整理】已登记空目录复查：{path}")
        except Exception as e:
            logger.warning(f"【网盘整理】登记空目录复查失败 {path}: {e}")

    def _schedule_folder(self, folder_path: Path):
        """登记目录到快速重试队列（内部文件尚未落盘时使用）"""
        key = str(folder_path)
        if key in self._pending_folders:
            return
        # 结构：(首次登记时间, 上次复查时间)
        self._pending_folders[key] = (time(), 0.0)
        logger.info(f"【网盘整理】目录暂为空，登记快速重试：{key}")

    def _count_folder_media(self, cid: int) -> int:
        """统计目录下直属媒体文件数量（不递归、不遍历全盘）。

        115 原生 fs_files 返回字段：n=名称，fc=1 文件 / 0 目录。
        """
        count = 0
        try:
            resp = self._client.fs_files({"cid": int(cid), "show_dir": 1, "limit": 1000, "offset": 0})
            data = resp.get("data") or []
            for item in data:
                try:
                    if int(item.get("fc") or 0) == 0:
                        # 目录项跳过
                        continue
                except (TypeError, ValueError):
                    continue
                name = str(item.get("n") or "")
                if not name:
                    continue
                if self._is_media_file(Path(name)):
                    count += 1
        except Exception as e:
            logger.warning(f"【网盘整理】统计目录媒体文件失败 cid={cid}: {e}")
        return count

    def _try_transfer_folder(self, cid: int, folder_path: Path):
        """目录已落盘则整理，否则登记快速重试。"""
        if not self._client:
            return
        try:
            if self._count_folder_media(int(cid)) > 0:
                self._transfer_folder(int(cid), folder_path)
                self._pending_folders.pop(str(folder_path), None)
            else:
                self._schedule_folder(folder_path)
        except Exception as e:
            logger.error(f"【网盘整理】尝试整理目录失败 {folder_path}: {e}", exc_info=True)
            self._schedule_folder(folder_path)

    def check_pending_folders(self):
        """定时复查：目录内文件落盘后立即整理（短间隔快速重试）"""
        if not self._enabled or not self._pending_folders:
            return
        if not self._ensure_client():
            return
        now = time()
        interval = max(1, int(self._folder_retry_sec))
        max_wait = interval * max(1, int(self._folder_max_retry))
        for path, meta in list(self._pending_folders.items()):
            first_seen, last_check = meta
            if now - last_check < interval:
                continue
            waited = now - first_seen
            if waited >= max_wait:
                logger.warning(f"【网盘整理】目录等待 {int(waited)} 秒仍无媒体文件，停止跟踪：{path}")
                self._pending_folders.pop(path, None)
                continue
            try:
                cid = self._get_cid(path)
                if cid is None or cid < 0:
                    logger.warning(f"【网盘整理】待整理目录已不存在，停止跟踪：{path}")
                    self._pending_folders.pop(path, None)
                    continue
                media_count = self._count_folder_media(int(cid))
                if media_count > 0:
                    logger.info(f"【网盘整理】目录已落盘 {media_count} 个媒体文件，开始整理：{path}")
                    self._transfer_folder(int(cid), Path(path))
                    self._pending_folders.pop(path, None)
                else:
                    self._pending_folders[path] = (first_seen, now)
            except Exception as e:
                logger.error(f"【网盘整理】复查待整理目录失败 {path}: {e}", exc_info=True)
                self._pending_folders[path] = (first_seen, now)

    def check_pending_cleanup(self):
        """定时复查：整理后源目录若已无文件（只剩空文件夹）则删除"""
        if not self._enabled or not self._cleanup_empty_after_transfer:
            return
        if not self._pending_cleanup:
            return
        if not self._ensure_client():
            return
        now = time()
        delay_sec = max(0, self._cleanup_delay_min) * 60
        for path, ts in list(self._pending_cleanup.items()):
            if now - ts < delay_sec:
                continue
            try:
                self._cleanup_source_dir(path)
            except Exception as e:
                logger.warning(f"【网盘整理】复查空目录失败 {path}: {e}")
            finally:
                self._pending_cleanup.pop(path, None)

    def _cleanup_source_dir(self, path: str):
        """若目录内已无任何文件（只剩空文件夹），删除该目录"""
        from app.modules.filemanager.storages.u115 import U115Pan

        pan = U115Pan()
        item = pan.get_item(Path(path))
        if not item:
            logger.info(f"【网盘整理】源目录已不存在，跳过：{path}")
            return
        if item.type != "dir":
            return
        files = self._collect_files(pan, item)
        if files:
            logger.info(f"【网盘整理】源目录仍有 {len(files)} 个文件，保留：{path}")
            return
        if pan.delete(item):
            logger.info(f"【网盘整理】已删除整理后空目录：{path}")
        else:
            logger.warning(f"【网盘整理】删除空目录失败：{path}")

    def _is_task_processed(self, info_hash: str) -> bool:
        if not info_hash:
            return False
        try:
            data = self.get_data("offline_processed") or []
            return info_hash in data
        except Exception:
            return False

    def _mark_task_processed(self, info_hash: str):
        if not info_hash:
            return
        try:
            data = self.get_data("offline_processed") or []
            if info_hash not in data:
                data.append(info_hash)
                # 只保留最近 500 条
                self.save_data("offline_processed", data[-500:])
        except Exception as e:
            logger.warning(f"【磁力离线】记录已处理任务失败: {e}")

    @staticmethod
    def _is_video(item: FileItem) -> bool:
        ext = (item.extension or "").lower()
        return f".{ext}" in settings.RMT_MEDIAEXT

    # ------------------------------------------------------------------ #
    # 生活事件监听
    # ------------------------------------------------------------------ #
    def _start_life_thread(self):
        self._stop_event = ThreadEvent()
        self._life_thread = Thread(
            target=self._life_loop,
            args=(self._stop_event,),
            daemon=True,
        )
        self._life_thread.start()
        logger.info("【网盘整理】生活事件监听线程已启动")

    def check_life_guard(self):
        """守护：线程意外退出时重启"""
        if not self._enabled or self._trigger_mode != "life":
            return
        if self._life_thread and self._life_thread.is_alive():
            return
        logger.warning("【网盘整理】生活事件线程已退出，正在重启")
        self._start_life_thread()

    def _life_loop(self, stop_event: ThreadEvent):
        from_id = 0
        while not stop_event.is_set():
            try:
                for event in iter_life_behavior_once(
                    self._client,
                    from_id=from_id,
                    type="",
                    cooldown=5,
                    app="web",
                ):
                    if stop_event.is_set():
                        return
                    try:
                        event_id = int(event.get("id") or 0)
                        if event_id > from_id:
                            from_id = event_id
                        event_name = event.get("event_name") or BEHAVIOR_TYPE_TO_NAME.get(
                            event.get("type"), ""
                        )
                        if self._event_modes and event_name not in self._event_modes:
                            continue
                        self._handle_life_event(event)
                    except Exception as e:
                        logger.error(f"【网盘整理】处理生活事件失败: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"【网盘整理】生活事件监听异常: {e}", exc_info=True)
                self._last_error = str(e)
                sleep(30)

    def _handle_life_event(self, event: Dict[str, Any]):
        """处理单条生活事件"""
        file_id = event.get("file_id")
        if not file_id:
            return
        file_category = event.get("file_category")
        # 获取文件/目录路径
        try:
            from p115client.tool.attr import get_path

            path = get_path(self._client, int(file_id), app="android")
        except Exception as e:
            logger.warning(f"【网盘整理】获取路径失败 file_id={file_id}: {e}")
            return
        if not path:
            return
        file_path = Path(str(path))
        if not self._in_transfer_paths(file_path):
            return
        if file_category == 0:
            # 目录：先立即扫一次（多数转存此时文件已落盘），
            # 若仍是空目录则登记到快速重试队列，由分钟级定时任务补扫
            self._try_transfer_folder(int(file_id), file_path)
        else:
            # 文件：直接整理
            self._transfer_single_file(event, file_path)

    # ------------------------------------------------------------------ #
    # 轮询模式
    # ------------------------------------------------------------------ #
    def _start_poll_thread(self):
        # 轮询由 scheduler 触发 poll_once，这里无需线程
        logger.info("【网盘整理】轮询模式已启用，由定时任务触发")

    def poll_once(self):
        """轮询一次：扫描整理目录，整理新出现的媒体文件"""
        if not self._enabled or self._trigger_mode != "poll":
            return
        if not self._ensure_client():
            return
        for path in self._iter_transfer_paths():
            try:
                self.transfer_path(path, poll=True)
            except Exception as e:
                logger.error(f"【网盘整理】轮询整理 {path} 失败: {e}", exc_info=True)

    # ------------------------------------------------------------------ #
    # 整理核心
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # 整理提交节流队列（防止一次性打爆 115 接口导致风控）
    # ------------------------------------------------------------------ #
    def _submit_transfer(self, fileitem: FileItem, file_path: Path) -> bool:
        """把整理任务提交到串行队列。

        直接调用 ``TransferChain().do_transfer()`` 会在批量转存场景下短时间
        向 115 发起数百次接口调用，触发「访问上限」整站风控。这里改为入队，
        由单线程 worker 按 ``transfer_interval_sec`` 间隔逐个提交。

        节流关闭时退化为原行为（同步直接提交）。
        """
        if not self._transfer_throttle_enabled:
            TransferChain().do_transfer(fileitem=fileitem)
            logger.info(f"【网盘整理】{file_path} 加入整理队列（节流已关闭，直接提交）")
            return True
        fileid = str(fileitem.fileid or "")
        if fileid and fileid in self._queued_fileids:
            logger.debug(f"【网盘整理】{file_path} 已在提交队列中，跳过")
            return False
        self._ensure_transfer_worker()
        if fileid:
            self._queued_fileids.add(fileid)
        self._transfer_queue.put((fileitem, str(file_path)))
        logger.info(f"【网盘整理】{file_path} 已入整理队列（待提交 {self._transfer_queue.qsize()}）")
        return True

    def _ensure_transfer_worker(self):
        """确保串行提交线程已启动"""
        if self._transfer_queue is None:
            self._transfer_queue = Queue()
        if self._transfer_worker and self._transfer_worker.is_alive():
            return
        self._transfer_worker = Thread(
            target=self._transfer_worker_loop,
            name="P115AutoTransfer-transfer-worker",
            daemon=True,
        )
        self._transfer_worker.start()
        logger.info("【网盘整理】整理提交线程已启动")

    def _transfer_worker_loop(self):
        """串行消费整理队列，逐个提交并保持最小间隔"""
        while True:
            try:
                fileitem, path = self._transfer_queue.get(timeout=60)
            except Empty:
                continue
            except Exception as e:
                logger.error(f"【网盘整理】整理队列异常: {e}", exc_info=True)
                continue
            try:
                TransferChain().do_transfer(fileitem=fileitem)
                logger.info(f"【网盘整理】{path} 已提交整理（剩余 {self._transfer_queue.qsize()}）")
            except Exception as e:
                logger.error(f"【网盘整理】提交整理失败 {path}: {e}", exc_info=True)
            finally:
                fid = str(fileitem.fileid or "")
                if fid:
                    self._queued_fileids.discard(fid)
                try:
                    self._transfer_queue.task_done()
                except Exception:
                    pass
            if self._transfer_interval_sec:
                sleep(self._transfer_interval_sec)

    def check_transfer_guard(self):
        """定时守护：提交线程意外退出时自动重启（并定期重置队列去重集合）"""
        if not self._transfer_throttle_enabled or not self._enabled:
            return
        if self._transfer_worker and not self._transfer_worker.is_alive():
            logger.warning("【网盘整理】整理提交线程已退出，正在重启")
            self._ensure_transfer_worker()
        elif not self._transfer_worker:
            self._ensure_transfer_worker()

    def transfer_path(self, path: str, poll: bool = False):
        """整理指定 115 网盘目录"""
        if not self._client:
            return
        try:
            cid = self._get_cid(path)
        except Exception as e:
            logger.error(f"【网盘整理】获取目录 ID 失败 {path}: {e}")
            return
        if cid is None or cid < 0:
            logger.warning(f"【网盘整理】目录不存在或无法访问: {path}")
            return
        count = 0
        for item in iter_files(
            self._client,
            cid=int(cid),
            with_path=True,
            order="user_ptime",
            max_workers=None,
            with_ancestors=True,
            cooldown=2,
            use_media_api=False,
        ):
            try:
                item_path = item.get("path")
                if not item_path:
                    continue
                file_path = Path(str(item_path))
                if not self._in_transfer_paths(file_path):
                    continue
                if item.get("is_dir"):
                    continue
                if not self._is_media_file(file_path):
                    continue
                if poll:
                    fid = str(item.get("id"))
                    if fid in self._seen_ids:
                        continue
                    self._seen_ids.add(fid)
                if self._min_file_size and (item.get("size") or 0) < self._min_file_size * 1024 * 1024:
                    continue
                fileitem = self._build_fileitem(item, file_path)
                self._submit_transfer(fileitem, file_path)
                count += 1
                logger.info(f"【网盘整理】{file_path} 加入整理队列")
            except Exception as e:
                logger.error(f"【网盘整理】处理文件失败: {e}", exc_info=True)
        if count:
            self._schedule_cleanup(path)
        if count and self._notify_progress:
            self._send_notify(
                title="【网盘整理】",
                text=f"{path} 已提交 {count} 个文件整理",
            )

    def _transfer_folder(self, cid: int, folder_path: Path):
        """整理目录下的媒体文件（受 transfer_batch_max 限制单批提交数）"""
        count = 0
        batch_max = self._transfer_batch_max
        for item in iter_files(
            self._client,
            cid=int(cid),
            with_path=True,
            order="user_ptime",
            max_workers=None,
            with_ancestors=True,
            cooldown=2,
            use_media_api=False,
        ):
            try:
                item_path = item.get("path")
                if not item_path:
                    continue
                file_path = Path(str(item_path))
                if not self._in_transfer_paths(file_path):
                    continue
                if item.get("is_dir"):
                    continue
                if not self._is_media_file(file_path):
                    continue
                if self._min_file_size and (item.get("size") or 0) < self._min_file_size * 1024 * 1024:
                    continue
                fileitem = self._build_fileitem(item, file_path)
                self._submit_transfer(fileitem, file_path)
                count += 1
                logger.info(f"【网盘整理】{file_path} 加入整理队列")
            except Exception as e:
                logger.error(f"【网盘整理】处理文件失败: {e}", exc_info=True)
            if batch_max and count >= batch_max:
                logger.info(
                    f"【网盘整理】{folder_path} 已达单批上限 {batch_max} 个，剩余文件留待下次扫描"
                )
                break
        if count and self._notify_progress:
            self._send_notify(
                title="【网盘整理】",
                text=f"{folder_path} 已提交 {count} 个文件整理",
            )

    def _transfer_single_file(self, event: Dict[str, Any], file_path: Path):
        """整理单个文件"""
        if not self._is_media_file(file_path):
            return
        if self._min_file_size and (event.get("file_size") or 0) < self._min_file_size * 1024 * 1024:
            return
        name, basename = self._inject_parent_media_id(file_path)
        fileitem = FileItem(
            storage=self._storage,
            fileid=str(event.get("file_id")),
            parent_fileid=str(event.get("parent_id")),
            path=file_path.as_posix(),
            type="file",
            name=name,
            basename=basename,
            extension=file_path.suffix[1:].lower(),
            size=event.get("file_size"),
            pickcode=event.get("pick_code"),
            modify_time=event.get("update_time"),
        )
        self._submit_transfer(fileitem, file_path)

    # ------------------------------------------------------------------ #
    # 工具
    # ------------------------------------------------------------------ #
    def _build_fileitem(self, item: Dict[str, Any], file_path: Path) -> FileItem:
        name, basename = self._inject_parent_media_id(file_path)
        return FileItem(
            storage=self._storage,
            fileid=str(item.get("id")),
            parent_fileid=str(item.get("parent_id")),
            path=file_path.as_posix(),
            type="file",
            name=name,
            basename=basename,
            extension=file_path.suffix[1:].lower(),
            size=item.get("size"),
            pickcode=item.get("pickcode"),
            modify_time=item.get("ctime") or item.get("user_ptime"),
        )

    @staticmethod
    def _extract_media_id_from_dir(dir_name: str) -> Optional[str]:
        """从目录名提取 TMDB ID。

        转存目录形如 ``片名 (年份) {tmdb-1785448}``，但文件名常常不含 ID，
        导致 MP 仅凭文件名识别失败。这里把目录名里的 ID 提取出来复用。
        支持 ``{tmdb-123}``、``{tmdbid-123}``、``[tmdb-123]``、``(tmdb-123)`` 等写法。
        """
        if not dir_name:
            return None
        for pattern in (
            r"[{\[]\s*tmdb(?:id)?[=\-]\s*(\d+)\s*[}\]]",
            r"\(\s*tmdb(?:id)?[=\-]\s*(\d+)\s*\)",
        ):
            m = re.search(pattern, dir_name, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

    def _inject_parent_media_id(self, file_path: Path) -> Tuple[str, str]:
        """若文件自身不含媒体 ID 而父目录含有，则把 ID 附加到名称上。

        仅在文件名本身没有 tmdb 标记时注入，避免覆盖已有身份，
        注入格式 ``{tmdbid-xxx}`` 为 MP 识别器明确支持的标签。
        """
        name = file_path.name
        basename = file_path.stem
        try:
            # 文件名已带 ID 时不处理
            if re.search(r"tmdb(?:id)?[=\-]\s*\d+", basename, re.IGNORECASE):
                return name, basename
            parent_name = file_path.parent.name if file_path.parent else ""
            media_id = self._extract_media_id_from_dir(parent_name)
            if not media_id:
                return name, basename
            new_basename = f"{basename}{{tmdbid-{media_id}}}"
            logger.info(
                f"【网盘整理】从父目录补全媒体 ID：{basename} -> {new_basename}"
            )
            return f"{new_basename}{file_path.suffix}", new_basename
        except Exception as e:
            logger.warning(f"【网盘整理】补全媒体 ID 失败 {file_path}: {e}")
            return name, basename

    def _is_media_file(self, file_path: Path) -> bool:
        suffix = file_path.suffix.lower()
        if suffix in settings.RMT_MEDIAEXT:
            return True
        if suffix in settings.RMT_SUBEXT:
            return True
        if suffix in settings.RMT_AUDIOEXT:
            return True
        return False

    def _iter_transfer_paths(self) -> List[str]:
        if not self._transfer_paths:
            return []
        return [p.strip() for p in self._transfer_paths.split("\n") if p.strip()]

    def _in_transfer_paths(self, file_path: Path) -> bool:
        for path in self._iter_transfer_paths():
            if self._has_prefix(file_path, path):
                return True
        return False

    @staticmethod
    def _has_prefix(full_path: Path, prefix_path: str) -> bool:
        try:
            full = Path(full_path).parts
            prefix = Path(prefix_path).parts
            if len(prefix) > len(full):
                return False
            return full[: len(prefix)] == prefix
        except Exception:
            return False

    # 常见推广域名/站点前缀，如 489155.com@、abc.net@
    _PROMO_PREFIX_RE = re.compile(
        r"^\s*(?:[0-9a-z\-]+\.(?:com|net|org|cc|tv|me|xyz|top|info|club|vip|app|site|online|la|io|us|cn|co)"
        r"(?:\.[a-z]{2,4})?)\s*[@\-_]\s*",
        re.IGNORECASE,
    )
    # 番号主体：字母数字组合（如 BACJ-192、SNOS-406-C、SSIS00123）
    _CODE_RE = re.compile(r"[A-Za-z]{2,10}[-_]?\d{2,6}(?:[-_][A-Za-z0-9]{1,4})*")

    def _clean_main_name(self, name: str) -> str:
        """清洗正片文件名，提取纯番号。

        例：489155.com@BACJ-192-U -> BACJ-192
        """
        if not name:
            return name
        raw = name.strip()
        # 去掉扩展名残留
        raw = re.sub(r"\.(mp4|mkv|avi|wmv|mov|ts|m2ts|iso|rmvb|flv|webm)$", "", raw, flags=re.IGNORECASE)
        # 去掉推广域名前缀
        cleaned = self._PROMO_PREFIX_RE.sub("", raw).strip()
        # 去掉常见噪声标记
        cleaned = re.sub(r"[\[\]【】()（）]", " ", cleaned)
        cleaned = re.sub(r"\b(?:hhd800|h265|h264|hevc|x264|x265|4k|1080p|720p|uncensored|leak|full|hd)\b",
                         " ", cleaned, flags=re.IGNORECASE)
        cleaned = cleaned.strip(" -_.")
        # 提取番号主体
        m = self._CODE_RE.search(cleaned)
        if m:
            code = m.group(0).upper().replace("_", "-")
            # 归一化：字母与数字之间补连字符（SSIS00123 -> SSIS-00123）
            code = re.sub(r"^([A-Z]+)(\d)", r"\1-\2", code)
            # 去掉末尾的 -U 标记（Uncensored 噪声）
            code = re.sub(r"-U$", "", code)
            return code
        # 无法识别番号时，返回去前缀后的结果
        return cleaned or raw

    def _get_cid(self, path: str) -> Optional[int]:
        """通过路径获取 115 目录 ID"""
        try:
            resp = self._client.fs_dir_getid(Path(path).as_posix())
            check_response(resp)
            raw = resp.get("id", -1)
            try:
                return int(raw)
            except (TypeError, ValueError):
                logger.warning(f"【网盘整理】fs_dir_getid 返回非法 id: {raw!r}")
                return None
        except Exception as e:
            logger.warning(f"【网盘整理】fs_dir_getid 失败 {path}: {e}")
            return None

