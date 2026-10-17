"""NextFind 控制器插件：通过 Agent 对话控制 NextFind（NF）的 API 能力。

2026-08-29 增强：详情页展示 NF 订阅列表（分电视剧/电影，仿 MP 订阅布局，日志折叠查看）；
监听 MP 订阅添加事件自动同步到 NF 订阅列表。
2026-08-29 合并 NfAutoSubscribe：新增「自动订阅」功能——轮询入库历史，对指定频道
（爱影/影巢等全量转存频道）转存入库的新资源自动在 NF 创建订阅（默认仅电视剧），
详情页增加自动订阅历史海报卡片（含订阅时间与连载进度）。
2026-10-17 从 NfController(自建) 移植 `/nf <关键词>` 一键获取命令（搜索→选最高画质→
转存 /最近接收→延迟触发整理），避免两个插件同名类 NfController 冲突导致命令丢失。
"""

import json
import re
from datetime import datetime
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple

from app.core.event import eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import FileItem
from app.schemas import Event
from app.schemas.types import EventType, NotificationType
from app.utils.http import RequestUtils


from .agenttool import (
    NextFindCreateDirectoryTool,
    NextFindDeleteEpisodeTool,
    NextFindDeleteMovieTool,
    NextFindDeleteSeasonTool,
    NextFindDirectoriesTool,
    NextFindFillMissingTool,
    NextFindHdhiveUnlockTool,
    NextFindHistoryTool,
    NextFindIgnoredEpisodesTool,
    NextFindLocalLibraryTool,
    NextFindLogsTool,
    NextFindPreviewTool,
    NextFindQuotaTool,
    NextFindResourcesSearchTool,
    NextFindSearchTool,
    NextFindShieldSearchTool,
    NextFindSubscribeAddTool,
    NextFindSubscribeRemoveTool,
    NextFindSubscriptionsTool,
    NextFindTransferTool,
)


class NextFindController(_PluginBase):
    """NextFind 控制器插件。"""

    plugin_name = "NextFind 控制器"
    plugin_desc = "通过 Agent 对话控制 NextFind（NF）的搜索、订阅、转存、额度查询等 API 能力；并可自动订阅指定频道转存入库的新资源。"
    plugin_icon = "nextfindcontroller.png"
    plugin_version = "1.6.0"
    plugin_label = "智能体,资源管理"
    plugin_author = "local"
    plugin_config_prefix = "nextfindcontroller_"
    # 是否由 /nf 主动投递 P115AutoTransfer 定向整理（默认关闭，整理交给 P115AutoTransfer 常规监听）
    _nf_dispatch_target = False
    plugin_order = 100
    auth_level = 1

    _enabled = False
    _api_url = ""
    _api_key = ""
    # 自动订阅（轮询入库历史）配置
    _auto_sub = True
    _auto_sub_interval = 30
    _only_tv = True
    _source_keywords: List[str] = []
    # /nf 转存目标目录（默认 /最近接收，可由使用者自行指定）
    _nf_target_folder = "/最近接收"
    # 自动订阅数据键（从 NfAutoSubscribe 迁移沿用）
    _last_id_key = "last_history_id"
    _history_key = "nf_sub_history"
    _first_enabled_key = "first_enabled_at"
    _max_history_pages = 5

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        self._api_url = ""
        self._api_key = ""
        self._auto_sub = True
        self._auto_sub_interval = 30
        self._only_tv = True
        self._source_keywords = []
        self._nf_target_folder = "/最近接收"
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._api_url = str(config.get("api_url") or "").rstrip("/")
        self._api_key = str(config.get("api_key") or "")
        self._auto_sub = bool(config.get("auto_sub", True))
        try:
            self._auto_sub_interval = max(5, int(config.get("auto_sub_interval") or 30))
        except (TypeError, ValueError):
            self._auto_sub_interval = 30
        self._only_tv = bool(config.get("only_tv", True))
        raw = str(config.get("source_keywords") or "爱影,影巢")
        self._source_keywords = [
            k.strip() for k in raw.replace("，", ",").split(",") if k.strip()
        ]
        self._nf_target_folder = self._normalize_nf_target(
            config.get("nf_target_folder") or "/最近接收"
        )
        # 记录自动订阅首次启用时间（用于详情页补录启用后创建的订阅）
        if self._enabled and self._auto_sub and not self.get_data(self._first_enabled_key):
            self.save_data(self._first_enabled_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/nf",
                "event": EventType.PluginAction,
                "desc": "NextFind 一键获取",
                "category": "网盘整理",
                "data": {"action": "nf_get"},
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
                        "component": "VSwitch",
                        "props": {
                            "model": "enabled",
                            "label": "启用插件"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "api_url",
                            "label": "NextFind API 地址",
                            "placeholder": "https://your-server.example.com/api/openapi"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "api_key",
                            "label": "NextFind API Key",
                            "type": "password"
                        }
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "auto_sub",
                            "label": "自动订阅（轮询入库历史）"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "auto_sub_interval",
                            "label": "自动订阅检查间隔（分钟，最小 5）",
                            "placeholder": "30"
                        }
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "only_tv",
                            "label": "仅订阅电视剧（电影跳过）"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "source_keywords",
                            "label": "自动订阅频道关键字（逗号分隔，留空则不限频道）",
                            "placeholder": "爱影,影巢"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "nf_target_folder",
                            "label": "/nf 转存目录",
                            "placeholder": "/最近接收",
                            "hint": "执行 /nf 时资源转存到该 115 网盘目录；留空默认 /最近接收。建议填被自动整理插件监控的目录",
                            "persistent-hint": True
                        }
                    }
                ]
            }
        ], {
            "enabled": False,
            "api_url": "https://your-server.example.com/api/openapi",
            "api_key": "",
            "auto_sub": True,
            "auto_sub_interval": 30,
            "only_tv": True,
            "source_keywords": "爱影,影巢",
            "nf_target_folder": "/最近接收"
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面：自动订阅历史 + NF 订阅列表（分电视剧/电影）+ 折叠式日志。"""
        if not self._enabled:
            return None
        # 补录自动订阅历史（历史为空时从 openapi 来源补录）
        self._backfill_history()
        # 订阅列表
        subs, subs_error = self._fetch_nf_subscriptions()
        tv_subs = [s for s in subs if s.get("media_type") == "tv"]
        movie_subs = [s for s in subs if s.get("media_type") == "movie"]
        in_lib_count = sum(1 for s in subs if s.get("is_in_library"))
        subscribing_count = len(subs) - in_lib_count
        tv_in_lib = sum(1 for s in tv_subs if s.get("is_in_library"))
        movie_in_lib = sum(1 for s in movie_subs if s.get("is_in_library"))
        # 日志（折叠查看）
        log_rows, log_error, fetch_time = self._fetch_nf_logs(lines=80)
        # NF 服务 host（海报地址基础）
        nf_host = self._api_url.rsplit("/api", 1)[0] if self._api_url else ""

        def stat_card(title: str, value: str, color: str) -> dict:
            """顶部统计卡片。"""
            return {
                "component": "VCol",
                "props": {"cols": 6, "sm": 6, "md": 3},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "color": color, "class": "pa-2"},
                        "content": [
                            {"component": "VCardSubtitle", "props": {"class": "pb-0 text-caption"}, "text": title},
                            {"component": "VCardTitle", "props": {"class": "text-h6 pt-1"}, "text": value}
                        ]
                    }
                ]
            }

        def poster_url(sub: dict) -> str:
            """拼接订阅海报地址。"""
            pp = sub.get("poster_path") or sub.get("poster") or ""
            if not pp:
                return ""
            if pp.startswith("http"):
                return pp
            return f"{nf_host}{pp}" if nf_host else ""

        def sub_card(sub: dict) -> dict:
            """订阅卡片：海报 + 标题 + 年份/集数/状态。"""
            title = sub.get("title") or "未知"
            tmdb = sub.get("tmdb_id")
            href = ""
            if tmdb:
                href = (
                    f"https://www.themoviedb.org/tv/{tmdb}"
                    if sub.get("media_type") == "tv"
                    else f"https://www.themoviedb.org/movie/{tmdb}"
                )
            title_node = {
                "component": "a" if href else "span",
                "props": {
                    "href": href,
                    "target": "_blank",
                    "class": "text-decoration-none"
                } if href else {},
                "text": title
            }
            # 副信息
            info_parts = []
            if sub.get("year"):
                info_parts.append(str(sub["year"]))
            if sub.get("media_type") == "tv":
                if sub.get("total_episodes"):
                    info_parts.append(f"全 {sub['total_episodes']} 集")
                if sub.get("local_episodes") is not None:
                    info_parts.append(f"已有 {sub['local_episodes']}")
            if sub.get("is_in_library"):
                info_parts.append("已入库")
            elif sub.get("status"):
                info_parts.append(str(sub["status"]))
            poster = poster_url(sub)
            poster_node = {
                "component": "VImg",
                "props": {
                    "src": poster,
                    "height": 114,
                    "width": 76,
                    "aspect-ratio": "2/3",
                    "class": "rounded flex-shrink-0",
                    "cover": True
                }
            } if poster else {
                "component": "div",
                "props": {
                    "class": "d-flex align-center justify-center rounded bg-surface-variant text-caption text-medium-emphasis flex-shrink-0",
                    "style": {"width": "76px", "height": "114px"}
                },
                "text": "无海报"
            }
            return {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "h-100"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "d-flex flex-nowrap ga-2 pa-2"},
                        "content": [
                            poster_node,
                            {
                                "component": "div",
                                "props": {"class": "min-w-0 flex-grow-1"},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "text-subtitle-2 font-weight-bold text-truncate mb-1"},
                                        "content": [title_node]
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "d-flex flex-wrap ga-1"},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {"size": "x-small", "variant": "tonal",
                                                         "color": "teal" if sub.get("is_in_library") else "indigo"},
                                                "text": "已入库" if sub.get("is_in_library") else "订阅中"
                                            }
                                        ]
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "text-caption text-medium-emphasis mt-1 text-truncate"},
                                        "text": " · ".join(info_parts) if info_parts else ""
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }

        def sub_window(items: List[dict]) -> dict:
            """订阅卡片分页网格（每页 12 条）。"""
            if not items:
                return {
                    "component": "div",
                    "props": {"class": "text-center pa-4 text-medium-emphasis"},
                    "text": "暂无订阅"
                }
            page_size = 12
            pages = [items[i:i + page_size] for i in range(0, len(items), page_size)]
            page_items = []
            for idx, page_records in enumerate(pages, start=1):
                page_items.append({
                    "component": "VWindowItem",
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "grid gap-2 grid-info-card"},
                            "content": [sub_card(s) for s in page_records]
                        }
                    ]
                })
            return {
                "component": "VWindow",
                "props": {"show-arrows": "hover"},
                "content": page_items
            }

        # 日志折叠区
        if log_error:
            log_content = [
                {"component": "VAlert", "props": {"type": "error", "text": f"获取 NextFind 日志失败：{log_error}"}}
            ]
        elif not log_rows:
            log_content = [
                {"component": "div", "props": {"class": "text-center pa-4 text-medium-emphasis"}, "text": "NextFind 暂无日志输出"}
            ]
        else:
            def log_line_node(line: str) -> dict:
                """单行日志节点，按级别着色。"""
                cls = "text-body-2"
                if re.search(r"\[(ERROR|CRITICAL)\]", line):
                    cls += " text-error"
                elif re.search(r"\[(WARN|WARNING)\]", line):
                    cls += " text-orange-darken-1"
                elif re.search(r"\[DEBUG\]", line):
                    cls += " text-medium-emphasis"
                return {
                    "component": "div",
                    "props": {
                        "class": cls,
                        "style": {
                            "font-family": "monospace",
                            "font-size": "12px",
                            "line-height": "1.5",
                            "white-space": "pre-wrap",
                            "word-break": "break-all"
                        }
                    },
                    "text": line
                }
            log_content = [
                {
                    "component": "div",
                    "props": {"class": "text-caption text-medium-emphasis mb-2"},
                    "text": f"获取时间：{fetch_time} · 最近 {len(log_rows)} 行"
                },
                {
                    "component": "div",
                    "props": {
                        "class": "bg-surface-variant rounded pa-2",
                        "style": {"max-height": "380px", "overflow-y": "auto"}
                    },
                    "content": [log_line_node(line) for line in log_rows]
                }
            ]

        # ============ 自动订阅历史（合并自 NfAutoSubscribe） ============
        auto_history = self.get_data(self._history_key) or {}
        auto_items = []
        if auto_history:
            nf_map = {}
            for s in subs:
                tid = str(s.get("tmdb_id") or "").strip()
                mtype = str(s.get("media_type") or "").strip()
                if tid and tid != "0":
                    nf_map[f"{tid}|{mtype}"] = s
            for key, info in sorted(
                auto_history.items(), key=lambda kv: kv[1].get("time") or "", reverse=True
            ):
                tid = str(info.get("tmdb_id") or "").strip()
                mtype = str(info.get("media_type") or "").strip()
                sub = nf_map.get(key) or {}
                status = sub.get("status") or "subscribing"
                total = int(sub.get("total_episodes") or 0)
                local = int(sub.get("local_episodes") or 0)
                in_lib = bool(sub.get("is_in_library"))
                if status == "cancelled":
                    progress = "已取消"
                elif status == "completed":
                    progress = "已完成"
                elif mtype == "movie":
                    progress = "已入库" if in_lib else "已订阅"
                elif total <= 0:
                    progress = "连载中"
                elif in_lib or local >= total:
                    progress = f"已全 {total} 集"
                elif local > 0:
                    progress = f"已更新 {local}/{total} 集"
                else:
                    progress = f"尚未下载 · 全 {total} 集"
                auto_state_map = {"subscribing": "订阅中", "completed": "已完成", "cancelled": "已取消"}
                auto_state_color = {"subscribing": "teal", "completed": "deep-purple", "cancelled": "grey"}
                auto_items.append({
                    "title": info.get("title") or sub.get("title") or "未知",
                    "media_type": "电影" if mtype == "movie" else "电视剧",
                    "state_text": auto_state_map.get(status, "订阅中"),
                    "state_color": auto_state_color.get(status, "teal"),
                    "progress": progress,
                    "time": info.get("time") or sub.get("created_at") or "",
                    "poster": poster_url(sub),
                    "tmdb": sub.get("tmdb_id") or tid,
                })
        auto_tv = sum(1 for i in auto_items if i.get("media_type") == "电视剧")
        auto_movie = sum(1 for i in auto_items if i.get("media_type") == "电影")
        # 分页信息（每页 12 条，展示在面板标题右侧）
        auto_pages = (len(auto_items) + 11) // 12
        tv_pages = (len(tv_subs) + 11) // 12
        movie_pages = (len(movie_subs) + 11) // 12

        def auto_card(item: dict) -> dict:
            """自动订阅卡片：海报 + 标题 + 类型/状态 + 进度 + 订阅时间。"""
            href = ""
            if item.get("tmdb"):
                href = (
                    f"https://www.themoviedb.org/tv/{item['tmdb']}"
                    if item.get("media_type") == "电视剧"
                    else f"https://www.themoviedb.org/movie/{item['tmdb']}"
                )
            title_node = {
                "component": "a" if href else "span",
                "props": {
                    "href": href,
                    "target": "_blank",
                    "class": "text-decoration-none"
                } if href else {},
                "text": item.get("title")
            }
            poster_node = {
                "component": "VImg",
                "props": {
                    "src": item.get("poster"),
                    "height": 114,
                    "width": 76,
                    "aspect-ratio": "2/3",
                    "class": "rounded flex-shrink-0",
                    "cover": True
                }
            } if item.get("poster") else {
                "component": "div",
                "props": {
                    "class": "d-flex align-center justify-center rounded bg-surface-variant text-caption text-medium-emphasis flex-shrink-0",
                    "style": {"width": "76px", "height": "114px"}
                },
                "text": "无海报"
            }
            return {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "h-100"},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "d-flex flex-nowrap ga-2 pa-2"},
                        "content": [
                            poster_node,
                            {
                                "component": "div",
                                "props": {"class": "min-w-0 flex-grow-1"},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "text-subtitle-2 font-weight-bold text-truncate mb-1"},
                                        "content": [title_node]
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "d-flex flex-wrap ga-1 mb-1"},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {"size": "x-small", "variant": "tonal", "color": "indigo"},
                                                "text": item.get("media_type")
                                            },
                                            {
                                                "component": "VChip",
                                                "props": {"size": "x-small", "variant": "tonal", "color": item.get("state_color")},
                                                "text": item.get("state_text")
                                            }
                                        ]
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "text-body-2 mb-1 text-truncate"},
                                        "text": item.get("progress")
                                    },
                                    {
                                        "component": "div",
                                        "props": {"class": "text-caption text-medium-emphasis"},
                                        "text": f"订阅时间：{item.get('time')}"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }

        def auto_window() -> dict:
            """自动订阅卡片分页网格（每页 12 条）。"""
            if not auto_items:
                return {
                    "component": "div",
                    "props": {"class": "text-center pa-4 text-medium-emphasis"},
                    "text": "暂无自动订阅记录"
                }
            page_size = 12
            pages = [auto_items[i:i + page_size] for i in range(0, len(auto_items), page_size)]
            page_nodes = []
            for idx, page_records in enumerate(pages, start=1):
                page_nodes.append({
                    "component": "VWindowItem",
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "grid gap-2 grid-info-card"},
                            "content": [auto_card(item) for item in page_records]
                        }
                    ]
                })
            return {
                "component": "VWindow",
                "props": {"show-arrows": "hover"},
                "content": page_nodes
            }

        auto_panel = {
            "component": "VExpansionPanel",
            "content": [
                {"component": "VExpansionPanelTitle", "text": f"自动订阅（{len(auto_items)}）· 电视剧 {auto_tv} / 电影 {auto_movie} · {auto_pages} 页"},
                {"component": "VExpansionPanelText", "content": [auto_window()]}
            ]
        }

        return [
            {
                "component": "VRow",
                "props": {"class": "mb-2"},
                "content": [
                    stat_card("NF 订阅", f"{len(subs)} 条", "primary"),
                    stat_card("电视剧", f"{len(tv_subs)} 条", "indigo"),
                    stat_card("电影", f"{len(movie_subs)} 条", "deep-orange"),
                    stat_card("已入库", f"{in_lib_count} 条", "teal"),
                ]
            },
            (
                {
                    "component": "VAlert",
                    "props": {"type": "error", "text": f"获取 NF 订阅列表失败：{subs_error}"}
                }
                if subs_error else {
                    "component": "VExpansionPanels",
                    "props": {"variant": "inset", "class": "mb-2"},
                    "content": [
                        auto_panel,
                        {
                            "component": "VExpansionPanel",
                            "content": [
                                {"component": "VExpansionPanelTitle", "text": f"电视剧订阅（{len(tv_subs)}）· 已入库 {tv_in_lib} / 订阅中 {len(tv_subs) - tv_in_lib} · {tv_pages} 页"},
                                {"component": "VExpansionPanelText", "content": [sub_window(tv_subs)]}
                            ]
                        },
                        {
                            "component": "VExpansionPanel",
                            "content": [
                                {"component": "VExpansionPanelTitle", "text": f"电影订阅（{len(movie_subs)}）· 已入库 {movie_in_lib} / 订阅中 {len(movie_subs) - movie_in_lib} · {movie_pages} 页"},
                                {"component": "VExpansionPanelText", "content": [sub_window(movie_subs)]}
                            ]
                        },
                        {
                            "component": "VExpansionPanel",
                            "content": [
                                {"component": "VExpansionPanelTitle", "text": "NF 系统日志（点击展开查看）"},
                                {"component": "VExpansionPanelText", "content": log_content}
                            ]
                        }
                    ]
                }
            )
        ]

    def _fetch_nf_subscriptions(self) -> Tuple[List[dict], str]:
        """获取 NF 订阅列表。"""
        if not self._api_url or not self._api_key:
            return [], "未配置 API 地址或 API Key"
        url = f"{self._api_url}/subscriptions"
        headers = {"X-API-Key": self._api_key}
        try:
            res = RequestUtils(headers=headers, timeout=15).get_res(url)
            if not res:
                return [], "NF API 请求失败（无响应）"
            if res.status_code != 200:
                return [], f"NF API 请求失败：HTTP {res.status_code}"
            data = res.json()
            if data.get("status") != "success":
                return [], f"NF API 返回异常：{str(data)[:150]}"
            payload = data.get("data")
            if isinstance(payload, list):
                return payload, ""
            return [], "订阅列表返回格式异常"
        except Exception as err:
            logger.error(f"获取 NF 订阅列表异常：{err}")
            return [], f"获取异常：{err}"

    def _fetch_nf_logs(self, lines: int = 80) -> Tuple[List[str], str, str]:
        """获取 NextFind 系统日志，返回 (日志行列表, 错误信息, 获取时间)。"""
        if not self._api_url or not self._api_key:
            return [], "未配置 API 地址或 API Key", ""
        url = f"{self._api_url}/logs"
        headers = {"X-API-Key": self._api_key}
        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            res = RequestUtils(headers=headers, timeout=10).get_res(
                url, params={"lines": lines}
            )
            if not res:
                return [], "NF API 请求失败（无响应）", fetch_time
            if res.status_code != 200:
                return [], f"NF API 请求失败：HTTP {res.status_code} {res.text[:150]}", fetch_time
            data = res.json()
            if data.get("status") != "success":
                return [], f"NF API 返回异常：{str(data)[:200]}", fetch_time
            payload = data.get("data")
            rows = []
            if isinstance(payload, list):
                rows = [str(x).rstrip("\n") for x in payload if str(x).strip()]
            elif isinstance(payload, str):
                rows = [line for line in payload.splitlines() if line.strip()]
            return rows, "", fetch_time
        except Exception as err:
            logger.error(f"获取 NF 日志异常：{err}")
            return [], f"获取 NF 日志异常：{err}", fetch_time

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        return None

    def get_service(self) -> List[Dict[str, Any]]:
        """返回插件定时服务。"""
        if not self._auto_sub:
            return []
        return [
            {
                "id": "NfAutoSubscribeCheck",
                "name": "NF 入库自动订阅检查",
                "trigger": "interval",
                "func": self.check_new_subscriptions,
                "kwargs": {"seconds": self._auto_sub_interval * 60}
            }
        ]

    # ==================== 自动订阅（入库历史轮询） ====================

    def check_new_subscriptions(self) -> None:
        """执行自动订阅检查：轮询入库历史，对指定频道转存入库的新资源自动在 NF 创建订阅。"""
        if not self._enabled or not self._auto_sub:
            return
        if not self._api_url or not self._api_key:
            logger.warn("NextFind 控制器未配置 API 地址或 API Key，自动订阅跳过")
            return
        try:
            subscriptions, subs_err = self._fetch_nf_subscriptions()
            if subs_err:
                logger.error(f"获取 NF 订阅列表失败，自动订阅本次执行中止：{subs_err}")
                return
            # 构建已订阅集合：tmdb_id -> media_type 集合
            subscribed: Dict[str, set] = {}
            for item in subscriptions:
                tmdb_id = str(item.get("tmdb_id") or "").strip()
                media_type = str(item.get("media_type") or "").strip()
                if tmdb_id and tmdb_id != "0":
                    subscribed.setdefault(tmdb_id, set()).add(media_type)
            logger.info(f"NF 当前订阅条目：{len(subscriptions)}，已订阅媒体数：{len(subscribed)}")

            # 读取游标并拉取入库历史（多页）
            last_id = int(self.get_data(self._last_id_key) or 0)
            history = self._get_history()
            if history is None:
                logger.error("获取 NF 入库历史失败，自动订阅本次执行中止")
                return
            logger.info(f"NF 入库历史拉取 {len(history)} 条（游标 {last_id}）")
            # 筛选新记录（id > last_id），按 tmdb_id 去重，应用类型与频道过滤
            new_items = []
            seen: set = set()
            max_id = last_id
            filtered = {"tv": 0, "channel": 0, "invalid": 0}
            for h in history:
                hid = int(h.get("id") or 0)
                if hid > max_id:
                    max_id = hid
                if hid <= last_id:
                    continue
                tmdb_id = str(h.get("tmdb_id") or "").strip()
                media_type = str(h.get("media_type") or "").strip()
                title = str(h.get("title") or "").strip()
                if not tmdb_id or tmdb_id == "0" or not media_type or not title:
                    filtered["invalid"] += 1
                    continue
                # 仅电视剧
                if self._only_tv and media_type not in ("tv", "电视剧"):
                    filtered["tv"] += 1
                    continue
                # 频道过滤
                if self._source_keywords:
                    source = self._extract_source(h)
                    if not source or not any(k in source for k in self._source_keywords):
                        filtered["channel"] += 1
                        continue
                key = f"{tmdb_id}|{media_type}"
                if key in seen:
                    continue
                seen.add(key)
                new_items.append({"tmdb_id": tmdb_id, "media_type": media_type, "title": title})
            logger.info(
                f"入库历史新增记录 {len(new_items)} 个（去重后，跳过：电影 {filtered['tv']}、"
                f"频道不符 {filtered['channel']}、无效 {filtered['invalid']}）"
            )

            # 逐个检查并订阅
            added = 0
            skipped = 0
            failed = 0
            for item in new_items:
                tmdb_id = item["tmdb_id"]
                media_type = item["media_type"]
                # 已订阅则跳过
                if tmdb_id in subscribed and media_type in subscribed[tmdb_id]:
                    skipped += 1
                    continue
                ok = self._add_subscribe(tmdb_id, media_type, item["title"])
                if ok:
                    added += 1
                    # 记录创建历史（首次创建时间）
                    self._record_subscribe(tmdb_id, media_type, item["title"])
                    # 加入已订阅集合，避免同批重复
                    subscribed.setdefault(tmdb_id, set()).add(media_type)
                else:
                    failed += 1

            # 更新游标
            if max_id > last_id:
                self.save_data(self._last_id_key, max_id)
            logger.info(
                f"NF 自动订阅检查完成：新增 {added}，已订阅跳过 {skipped}，失败 {failed}，"
                f"游标更新至 {max_id}"
            )
            # 有新增或失败时发送通知
            if added or failed:
                self.post_message(
                    mtype=NotificationType.Subscribe,
                    title="NF 自动订阅",
                    text=f"新增订阅 {added} 个，跳过 {skipped} 个，失败 {failed} 个"
                )
        except Exception as err:
            logger.error(f"NF 自动订阅检查异常：{err}")

    def _extract_source(self, h: dict) -> str:
        """从历史记录提取频道来源（source 字段，缺失时兜底 movie_attributes_json）。"""
        source = str(h.get("source") or "").strip()
        if source:
            return source
        attrs = h.get("movie_attributes_json")
        if attrs:
            try:
                if isinstance(attrs, str):
                    attrs = json.loads(attrs)
                if isinstance(attrs, dict):
                    source = str(attrs.get("source_display") or attrs.get("recovered_channel_name") or "").strip()
            except (TypeError, ValueError):
                pass
        return source

    def _get_history(self) -> Optional[List[dict]]:
        """获取 NF 入库历史（最多 5 页 × 50 条，按 id 降序）。"""
        all_items: List[dict] = []
        for page in range(1, self._max_history_pages + 1):
            url = f"{self._api_url}/history"
            headers = {"X-API-Key": self._api_key}
            try:
                res = RequestUtils(headers=headers).get_res(
                    url, params={"page": page, "page_size": 50}
                )
                if not res:
                    logger.error("NF 入库历史请求失败（无响应）")
                    if page == 1:
                        return None
                    break
                if res.status_code != 200:
                    logger.error(f"NF 入库历史请求失败：HTTP {res.status_code}")
                    if page == 1:
                        return None
                    break
                data = res.json()
                if data.get("status") != "success":
                    logger.error(f"NF 入库历史返回异常：{data}")
                    if page == 1:
                        return None
                    break
                items = data.get("data") or []
                all_items.extend(items)
                if len(items) < 50:
                    break
            except Exception as err:
                logger.error(f"获取 NF 入库历史异常：{err}")
                if page == 1:
                    return None
                break
        return all_items

    def _add_subscribe(self, tmdb_id: str, media_type: str, title: str) -> bool:
        """调用 NF API 创建订阅。"""
        url = f"{self._api_url}/subscriptions/add"
        headers = {"X-API-Key": self._api_key, "Content-Type": "application/json"}
        body = {"tmdb_id": tmdb_id, "media_type": media_type, "title": title}
        try:
            res = RequestUtils(headers=headers).post_res(url, json=body)
            if not res:
                logger.error(f"创建 NF 订阅失败（无响应）：{title} tmdb={tmdb_id}")
                return False
            if res.status_code != 200:
                logger.error(f"创建 NF 订阅失败：HTTP {res.status_code} {title} tmdb={tmdb_id}")
                return False
            data = res.json()
            if data.get("status") != "success":
                logger.error(f"创建 NF 订阅返回异常：{data} {title} tmdb={tmdb_id}")
                return False
            logger.info(f"已在 NF 创建订阅：{title} tmdb={tmdb_id} type={media_type}")
            return True
        except Exception as err:
            logger.error(f"创建 NF 订阅异常：{err} {title} tmdb={tmdb_id}")
            return False

    def _record_subscribe(self, tmdb_id: str, media_type: str, title: str) -> None:
        """记录自动创建的 NF 订阅（已存在则保留首次创建时间）。"""
        key = f"{tmdb_id}|{media_type}"
        history = self.get_data(self._history_key) or {}
        if key in history:
            return
        history[key] = {
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "title": title,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.save_data(self._history_key, history)

    def _backfill_history(self) -> None:
        """补录启用期间自动创建的订阅：openapi 来源且创建时间晚于插件首次启用时间。"""
        history = self.get_data(self._history_key) or {}
        if history:
            return
        first_at = str(self.get_data(self._first_enabled_key) or "").strip()
        if not first_at:
            return
        subscriptions, subs_err = self._fetch_nf_subscriptions()
        if subs_err or not subscriptions:
            return
        changed = False
        for s in subscriptions:
            if str(s.get("source") or "") != "openapi":
                continue
            created = str(s.get("created_at") or "").strip()
            if not created or created < first_at:
                continue
            tmdb_id = str(s.get("tmdb_id") or "").strip()
            media_type = str(s.get("media_type") or "").strip()
            title = str(s.get("title") or "").strip()
            if not tmdb_id or tmdb_id == "0" or not media_type:
                continue
            key = f"{tmdb_id}|{media_type}"
            if key in history:
                continue
            history[key] = {
                "tmdb_id": tmdb_id,
                "media_type": media_type,
                "title": title,
                "time": created
            }
            changed = True
        if changed:
            self.save_data(self._history_key, history)

    # ==================== 事件处理 ====================

    def _extract_sub_identity(self, event_data: dict) -> Optional[tuple]:
        """从订阅事件提取 (tmdb_id, nf_type)；无法识别返回 None。"""
        info = event_data.get("subscribe_info") or {}
        tmdb_id = info.get("tmdbid") or info.get("tmdb_id")
        media_type = info.get("type") or ""
        if not tmdb_id:
            # 从 mediainfo 兜底（SubscribeComplete 事件携带）
            mi = event_data.get("mediainfo") or {}
            tmdb_id = mi.get("tmdb_id")
            media_type = mi.get("type") or ""
        if not tmdb_id:
            return None
        if media_type in ("电视剧", "tv", "TV"):
            nf_type = "tv"
        elif media_type in ("电影", "movie", "MOVIE"):
            nf_type = "movie"
        else:
            return None
        return str(tmdb_id), nf_type

    @eventmanager.register(EventType.SubscribeAdded)
    def handle_subscribe_added(self, event: Event = None) -> None:
        """MP 订阅添加后自动同步到 NF 订阅列表。"""
        if not self._enabled:
            return
        if not event or not event.event_data:
            return
        try:
            identity = self._extract_sub_identity(event.event_data)
            if not identity:
                logger.debug("订阅事件缺少身份信息，跳过 NF 同步")
                return
            tmdb_id, nf_type = identity
            title = ((event.event_data.get("subscribe_info") or {}).get("name")) or ""
            # 幂等检查：NF 已有该订阅则跳过
            subs, err = self._fetch_nf_subscriptions()
            if subs:
                for s in subs:
                    if str(s.get("tmdb_id")) == tmdb_id and s.get("media_type") == nf_type:
                        logger.info(f"NF 已有该订阅，跳过同步：{title} (tmdb={tmdb_id})")
                        return
            if err:
                logger.warn(f"查询 NF 订阅列表失败（{err}），直接尝试添加")
            # 调用 NF 添加订阅
            result = self._request("POST", "/subscriptions/add", body={
                "tmdb_id": tmdb_id,
                "media_type": nf_type,
                "title": title,
            })
            logger.info(f"MP 订阅已同步到 NF：{title} (tmdb={tmdb_id}) - {str(result)[:150]}")
        except Exception as err:
            logger.error(f"MP 订阅同步 NF 异常：{err}")

    @eventmanager.register(EventType.SubscribeComplete)
    def handle_subscribe_complete(self, event: Event = None) -> None:
        """MP 订阅完成后自动删除 NF 对应订阅。"""
        if not self._enabled:
            return
        if not event or not event.event_data:
            return
        try:
            identity = self._extract_sub_identity(event.event_data)
            if not identity:
                logger.debug("订阅完成事件缺少身份信息，跳过 NF 删除")
                return
            tmdb_id, nf_type = identity
            result = self._request("POST", "/subscriptions/remove", body={
                "tmdb_id": tmdb_id,
                "media_type": nf_type,
            })
            logger.info(f"MP 订阅完成，已删除 NF 订阅：tmdb={tmdb_id} type={nf_type} - {str(result)[:120]}")
        except Exception as err:
            logger.error(f"订阅完成删除 NF 订阅异常：{err}")

    @eventmanager.register(EventType.SubscribeDeleted)
    def handle_subscribe_deleted(self, event: Event = None) -> None:
        """MP 订阅删除时同步删除 NF 对应订阅。"""
        if not self._enabled:
            return
        if not event or not event.event_data:
            return
        try:
            identity = self._extract_sub_identity(event.event_data)
            if not identity:
                logger.debug("订阅删除事件缺少身份信息，跳过 NF 删除")
                return
            tmdb_id, nf_type = identity
            result = self._request("POST", "/subscriptions/remove", body={
                "tmdb_id": tmdb_id,
                "media_type": nf_type,
            })
            logger.info(f"MP 订阅已删除，同步删除 NF 订阅：tmdb={tmdb_id} type={nf_type} - {str(result)[:120]}")
        except Exception as err:
            logger.error(f"订阅删除同步 NF 异常：{err}")

    def get_agent_tools(self) -> List[type]:
        """返回插件提供的 Agent 工具列表。"""
        return [
            NextFindSearchTool,
            NextFindSubscriptionsTool,
            NextFindSubscribeAddTool,
            NextFindSubscribeRemoveTool,
            NextFindQuotaTool,
            NextFindHistoryTool,
            NextFindResourcesSearchTool,
            NextFindTransferTool,
            NextFindDirectoriesTool,
            NextFindLocalLibraryTool,
            NextFindLogsTool,
            NextFindFillMissingTool,
            NextFindIgnoredEpisodesTool,
            NextFindShieldSearchTool,
            NextFindPreviewTool,
            NextFindHdhiveUnlockTool,
            NextFindCreateDirectoryTool,
            NextFindDeleteEpisodeTool,
            NextFindDeleteSeasonTool,
            NextFindDeleteMovieTool,
        ]

    # ==================== NF API 调用方法 ====================

    def _request(self, method: str, path: str, params: dict = None, body: dict = None) -> str:
        """发起 NF API 请求并返回格式化结果。"""
        if not self._api_url or not self._api_key:
            return "NextFind 控制器未配置 API 地址或 API Key"
        url = f"{self._api_url}{path}"
        headers = {
            "X-API-Key": self._api_key,
            "Content-Type": "application/json",
        }
        try:
            if method.upper() == "GET":
                res = RequestUtils(headers=headers).get_res(url, params=params)
            elif method.upper() == "POST":
                res = RequestUtils(headers=headers).post_res(url, json=body)
            elif method.upper() == "DELETE":
                res = RequestUtils(headers=headers).delete_res(url, params=params)
            else:
                return f"不支持的请求方法：{method}"
            if not res:
                return "NF API 请求失败（无响应）"
            if res.status_code != 200:
                return f"NF API 请求失败：HTTP {res.status_code} {res.reason}"
            try:
                data = res.json()
            except Exception:
                return f"NF API 返回非 JSON：{res.text[:500]}"
            return json.dumps(data, ensure_ascii=False, indent=2)
        except Exception as err:
            logger.error(f"NextFind API 请求异常：{err}")
            return f"NextFind API 请求异常：{err}"

    async def tool_search(self, query: str, media_type: str = None) -> str:
        """全局搜索影片或剧集。"""
        params = {"query": query}
        if media_type:
            params["type"] = media_type
        return self._request("GET", "/search", params=params)

    async def tool_subscriptions(self) -> str:
        """获取订阅列表。"""
        return self._request("GET", "/subscriptions")

    async def tool_subscribe_add(self, tmdb_id: str, media_type: str, title: str) -> str:
        """添加订阅。"""
        return self._request("POST", "/subscriptions/add", body={
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "title": title,
        })

    async def tool_subscribe_remove(self, tmdb_id: str, media_type: str) -> str:
        """取消订阅。"""
        return self._request("POST", "/subscriptions/remove", body={
            "tmdb_id": tmdb_id,
            "media_type": media_type,
        })

    async def tool_quota(self) -> str:
        """查询额度与积分。"""
        return self._request("GET", "/quota")

    async def tool_history(self, page: int = 1, page_size: int = 20) -> str:
        """查询转存历史。"""
        return self._request("GET", "/history", params={"page": page, "page_size": page_size})

    async def tool_resources_search(self, tmdb_id: str, media_type: str, season: int = None, episode: int = None) -> str:
        """搜索网盘与种子资源。"""
        params = {"tmdb_id": tmdb_id, "media_type": media_type}
        if season is not None:
            params["season"] = season
        if episode is not None:
            params["episode"] = episode
        return self._request("GET", "/resources/search", params=params)

    async def tool_transfer(self, slug: str, target_folder: str = None) -> str:
        """一键转存到网盘。"""
        body = {"slug": slug}
        if target_folder:
            body["target_folder"] = target_folder
        return self._request("POST", "/transfer", body=body)

    async def tool_directories(self, cid: str = "0") -> str:
        """查询网盘目录。"""
        return self._request("GET", "/directories", params={"cid": cid})

    async def tool_local_library(self, status_filter: str = None) -> str:
        """查询本地库状态。"""
        params = {}
        if status_filter:
            params["status_filter"] = status_filter
        return self._request("GET", "/local_library/filter", params=params)

    async def tool_logs(self, lines: int = 50) -> str:
        """获取系统日志。"""
        return self._request("GET", "/logs", params={"lines": lines})

    async def tool_fill_missing(self, tmdb_id: str, media_type: str, title: str) -> str:
        """触发补缺集搜索。"""
        return self._request("POST", "/media/fill_missing", body={
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "title": title,
        })

    async def tool_ignored_episodes(self, tmdb_id: str, season: int) -> str:
        """切换忽略季状态。"""
        return self._request("POST", "/ignored_episodes/toggle", body={
            "tmdb_id": tmdb_id,
            "season": season,
        })

    async def tool_shield_search(self, sha1: str = None, mediasource_id: str = None, tmdb_id: str = None) -> str:
        """神盾模式查询。"""
        params = {}
        if sha1:
            params["sha1"] = sha1
        if mediasource_id:
            params["mediasource_id"] = mediasource_id
        if tmdb_id:
            params["tmdb_id"] = tmdb_id
        return self._request("GET", "/shield/search", params=params)

    async def tool_preview(self, slug: str) -> str:
        """触发探针解包。"""
        return self._request("POST", "/preview", body={"slug": slug})

    async def tool_hdhive_unlock(self, id: str, media_type: str) -> str:
        """HDHive 积分解锁。"""
        return self._request("POST", "/hdhive/unlock", body={"id": id, "type": media_type})

    async def tool_create_directory(self, parent_cid: str, name: str) -> str:
        """创建网盘目录。"""
        return self._request("POST", "/directories", body={"parent_cid": parent_cid, "name": name})

    async def tool_delete_episode(self, tmdb_id: str, season: int, episode: int) -> str:
        """静默删除指定集。"""
        return self._request("DELETE", "/media/episode", params={
            "tmdb_id": tmdb_id, "season": season, "episode": episode
        })

    async def tool_delete_season(self, tmdb_id: str, season: int) -> str:
        """静默删除整季。"""
        return self._request("DELETE", "/media/season", params={
            "tmdb_id": tmdb_id, "season": season
        })

    async def tool_delete_movie(self, tmdb_id: str) -> str:
        """静默删除电影。"""
        return self._request("DELETE", "/media/movie", params={"tmdb_id": tmdb_id})

    # ------------------------------------------------------------------ #
    # `/nf <关键词>` 一键获取（2026-10-17 从 NfController(自建) 移植）
    # ------------------------------------------------------------------ #
    # 画质评分表：分辨率 > 色彩 > 音频 > 片源 > 体积
    _RES_SCORE = [("2160", 4), ("4k", 4), ("1080", 3), ("720", 2), ("480", 1)]
    _COLOR_SCORE = [("dovi", 3), ("dv", 3), ("hdr", 2), ("sdr", 1)]
    _AUDIO_SCORE = [
        ("truehd", 4), ("atmos", 4),
        ("dts-hd", 3), ("dtshd", 3), ("dts.ma", 3), ("dtsma", 3),
        ("lpcm", 3), ("flac", 3),
        ("dts", 2), ("ac3", 2), ("eac3", 2), ("ddp", 2), ("aac", 2),
    ]
    _SRC_SCORE = [
        ("remux", 5), ("bluray", 4), ("blu-ray", 4),
        ("web-dl", 3), ("webdl", 3), ("hdtv", 2),
    ]

    @staticmethod
    def _score_text(text: str, table: List[Tuple[str, int]]) -> int:
        """在文本中按表取最高分（未命中为 0）。"""
        low = str(text or "").lower()
        best = 0
        for token, score in table:
            if token in low and score > best:
                best = score
        return best

    def _resource_quality_key(self, res: dict) -> Tuple[int, int, int, int, float]:
        """生成资源画质排序键（越大越好）。"""
        attrs = res.get("movie_attributes") or {}
        audio_parts = [
            res.get("audio"),
            attrs.get("audio") if isinstance(attrs, dict) else None,
            res.get("remark"),
            res.get("name"),
            res.get("title"),
        ]
        audio_text = " ".join(str(p) for p in audio_parts if p)
        name_text = " ".join(str(res.get(k) or "") for k in ("name", "title", "remark"))
        try:
            size = float(res.get("size") or 0)
        except (TypeError, ValueError):
            size = 0.0
        return (
            self._score_text(name_text, self._RES_SCORE),
            self._score_text(name_text, self._COLOR_SCORE),
            self._score_text(audio_text, self._AUDIO_SCORE),
            self._score_text(name_text, self._SRC_SCORE),
            size,
        )

    @staticmethod
    def _asyncio_run(coro):
        """在独立事件循环中运行协程（子线程内使用）。"""
        # 2026-10-17：宿主插件加载器会按名单清理插件模块里名为 asyncio/json/logger
        # 之类的全局名，直接引用 asyncio 会 NameError，这里从标准库延迟导入。
        import importlib

        _asyncio = importlib.import_module("asyncio")
        loop = _asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    @staticmethod
    def _extract_items(payload: str) -> List[dict]:
        """兼容多种返回结构，取出候选列表。"""
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            return []
        if isinstance(data, list):
            return [i for i in data if isinstance(i, dict)]
        if not isinstance(data, dict):
            return []
        for key in ("data", "list", "resources", "items", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return [i for i in value if isinstance(i, dict)]
            if isinstance(value, dict):
                for inner_key in ("list", "resources", "items", "results"):
                    if isinstance(value.get(inner_key), list):
                        return [i for i in value[inner_key] if isinstance(i, dict)]
        return []

    def _pick_best_resource(self, payload: str) -> Optional[str]:
        """从资源搜索返回中挑出画质最高的 slug。"""
        candidates = [r for r in self._extract_items(payload) if r.get("slug")]
        if not candidates:
            logger.warning("/nf 资源搜索无可用候选")
            return None
        best = max(candidates, key=self._resource_quality_key)
        logger.info(
            f"/nf 画质排序选中 slug={best.get('slug')} "
            f"key={self._resource_quality_key(best)} "
            f"name={best.get('name') or best.get('title')}"
        )
        return best.get("slug")

    @classmethod
    def _pick_media(cls, payload: str) -> Tuple[Optional[str], Optional[str], str]:
        """从媒体搜索返回中挑选最佳候选，返回 (tmdb_id, nf_type, title)。"""
        items = cls._extract_items(payload)
        first = items[0] if items else None
        if not first:
            return None, None, ""
        media_id = first.get("tmdb_id") or first.get("id")
        raw_type = str(first.get("media_type") or first.get("type") or "").lower()
        if raw_type in ("tv", "电视剧", "剧集", "series"):
            nf_type = "tv"
        elif raw_type in ("movie", "电影", "film"):
            nf_type = "movie"
        else:
            nf_type = "movie"
        title = first.get("title") or first.get("name") or ""
        return (str(media_id) if media_id else None), nf_type, str(title)

    @eventmanager.register(EventType.PluginAction)
    def handle_nf_command(self, event: Event) -> None:
        """`/nf <关键词>` 一键获取资源。

        零 token 完成「搜索媒体 → 搜索资源 → 选最高画质 → 转存 /最近接收 → 延迟整理」。
        """
        if not event or not event.event_data:
            return
        event_data = event.event_data
        if event_data.get("action") != "nf_get":
            return
        channel = event_data.get("channel")
        userid = event_data.get("user")
        if not self._enabled:
            self.post_message(
                channel=channel, title="NextFind 控制器未启用", userid=userid
            )
            return
        keyword = (event_data.get("arg_str") or "").strip()
        if not keyword:
            raw = event_data.get("text") or event_data.get("message") or ""
            keyword = re.sub(r"^\s*/nf\s*", "", str(raw)).strip()
        if not keyword:
            self.post_message(
                channel=channel, title="用法：/nf <影片或剧集名>", userid=userid
            )
            return
        Thread(
            target=self._run_nf_command,
            args=(keyword, channel, userid),
            daemon=True,
        ).start()

    @staticmethod
    def _normalize_nf_target(folder: str) -> str:
        """规范化 /nf 转存目录：补齐前导斜杠、去掉结尾斜杠，空值回退 /最近接收。"""
        value = str(folder or "").strip().replace("\\", "/")
        if not value:
            return "/最近接收"
        if not value.startswith("/"):
            value = "/" + value
        while len(value) > 1 and value.endswith("/"):
            value = value[:-1]
        return value

    def _run_nf_command(self, keyword: str, channel, userid) -> None:
        """`/nf` 主流程（子线程执行，避免阻塞事件分发）。"""
        try:
            self.post_message(
                channel=channel, title=f"【/nf】开始搜索：{keyword}", userid=userid
            )
            # 1. 搜索媒体
            search_raw = self._asyncio_run(self.tool_search(query=keyword))
            media_id, media_type, title = self._pick_media(search_raw)
            if not media_id:
                self.post_message(
                    channel=channel, title=f"【/nf】未找到媒体：{keyword}", userid=userid
                )
                return
            logger.info(f"/nf 识别媒体：{title} tmdb={media_id} type={media_type}")

            # 2. 搜索资源
            res_raw = self._asyncio_run(
                self.tool_resources_search(tmdb_id=media_id, media_type=media_type)
            )
            slug = self._pick_best_resource(res_raw)
            if not slug:
                self.post_message(
                    channel=channel, title=f"【/nf】{title} 无可用资源", userid=userid
                )
                return

            # 3. 转存到 /最近接收
            result = self._asyncio_run(
                self.tool_transfer(slug=slug, target_folder=self._nf_target_folder)
            )
            logger.info(f"/nf 转存结果：{str(result)[:200]}")
            self.post_message(
                channel=channel,
                title=f"【/nf】{title} 已转存 {self._nf_target_folder}，稍后触发整理",
                userid=userid,
            )

            # 4. 整理与源目录清理统一交给 P115AutoTransfer（2026-10-17 调整分工）
            #    NF 自身不再执行整理。以下投递代码保留，由 _nf_dispatch_target 开关控制；
            #    关闭时只记录日志，转存结果不受影响，/最近接收 由 P115AutoTransfer 正常监控。
            if self._nf_dispatch_target:
                import time as _time
                now = _time.time()
                try:
                    eventmanager.send_event(
                        EventType.PluginAction,
                        {
                            "action": "p115_target_transfer",
                            "tmdb_id": str(media_id),
                            "title": title,
                            "since": now,
                            "channel": channel,
                            "user": userid,
                            "arg_str": str(media_id),
                        },
                    )
                    logger.info(f"/nf 已请求 P115AutoTransfer 定向整理：{title} tmdb={media_id}")
                except Exception as err:
                    logger.warning(f"/nf 投递定向整理请求失败（忽略）：{err}")
            else:
                logger.info(
                    f"/nf 定向整理开关关闭，仅转存：{title} tmdb={media_id}（由 P115AutoTransfer 常规监听处理）"
                )
        except Exception as err:
            logger.error(f"/nf 执行异常：{err}", exc_info=True)
            self.post_message(
                channel=channel, title=f"【/nf】执行失败：{err}", userid=userid
            )

    # ------------------------------------------------------------------ #
    # 【已停用，保留参考】NF 自身整理/清理逻辑（2026-10-17 关闭，未删除）
    # 关闭原因：整理与源目录清理统一由 P115AutoTransfer 负责，避免双重整理与重复准入。
    # 如需恢复：把下面代码去掉行首注释，并把 _nf_dispatch_target 置为 False。
    # ------------------------------------------------------------------ #
    # def _iter_recent_videos(self, target_id: str) -> List[FileItem]:
    #         """列出 /最近接收 下的视频文件，优先保留与目标 TMDB ID 匹配的条目。"""
    #         import re as _re
    #    #         results: List[FileItem] = []
    #         try:
    #             matched: List[FileItem] = []
    #             fallback: List[FileItem] = []
    #             root = FileItem(storage="u115", path="/最近接收", type="dir")
    #             for item in self._list_storage(root):
    #                 files = self._walk_storage(item)
    #                 for f in files:
    #                     if f.extension and str(f.extension).lower() not in (
    #                         "mkv", "mp4", "ts", "avi", "mov", "wmv", "flv", "m2ts", "iso",
    #                     ):
    #                         continue
    #                     if (f.size or 0) < 1024 * 1024 * 100:
    #                         continue
    #                     fallback.append(f)
    #                     if _re.search(rf"tmdb-?{_re.escape(str(target_id))}\b", f.path or ""):
    #                         matched.append(f)
    #             return matched or fallback
    #         except Exception as err:
    #             logger.error(f"/nf 遍历 /最近接收 异常：{err}")
    #             return results
    #    # def _walk_storage(self, item: FileItem, depth: int = 0) -> List[FileItem]:
    #         """递归收集存储目录下的文件（限制 3 层，避免过量请求）。"""
    #         files: List[FileItem] = []
    #         if not item:
    #             return files
    #         if item.type == "file":
    #             return [item]
    #         if depth > 3:
    #             return files
    #         for child in self._list_storage(item):
    #             if child.type == "file":
    #                 files.append(child)
    #             else:
    #                 files.extend(self._walk_storage(child, depth + 1))
    #         return files
    #    # @staticmethod
    # def _list_storage(fileitem: FileItem) -> List[FileItem]:
    #         """通过 MoviePilot 存储链列出目录内容。"""
    #         from app.chain.storage import StorageChain
    #    #         try:
    #             return StorageChain().list_files(fileitem) or []
    #         except Exception as err:
    #             logger.warning(f"/nf 列出目录 {fileitem.path} 失败：{err}")
    #             return []

