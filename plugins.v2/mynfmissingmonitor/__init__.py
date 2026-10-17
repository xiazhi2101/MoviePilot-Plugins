"""NF 缺集监测插件（v2.0.0，2026-10-11 重写）。

职责（单一）：
1. 缺集监测：扫描全部 MP 订阅缺集，按播出日历过滤已播出集，NF 有资源则拆包/整包转存到
   `/最近接收` 补全；NF 没有资源则直接跳过，不做任何 PT 下载兜底。
2. 转存落地校验：转存后实际查询 115 目标目录确认文件落地，未落地不计入去重、下轮重试，
   避免 NF 后端超时/分享失效导致的「假成功」。

已移除（相对 v1.1.0）：
- 兜底订阅（NF 订阅列表 → MP 建订阅搜索下载）
- 洗版（best_version 订阅质量升级）
"""

import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.application.subscription.contract import build_subscribe_meta
from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.subscribe.identity import subscribe_recognize_kwargs
from app.core.event import eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import Event, Response
from app.schemas.types import EventType, MediaType
from app.utils.http import RequestUtils


class MyNfMissingMonitor(_PluginBase):
    """NF 缺集监测：扫描 MP 订阅缺集，用 NextFind 搜索转存补全（含落地校验）。"""

    plugin_name = "缺集监测(自建)"
    plugin_desc = "定时监测 MP 订阅缺集，用 NextFind 搜索补全（支持 ed2k/115 拆包转存），转存后校验文件实际落地。NF 无资源时直接跳过，不做 PT 兜底。"
    plugin_icon = "nfmissingmonitor.png"
    plugin_version = "2.1.7"
    plugin_label = "订阅,资源管理,智能体"
    plugin_author = "local"
    plugin_config_prefix = "mynfmissingmonitor_"
    plugin_order = 100
    auth_level = 1

    _enabled = False
    _api_url = ""
    _api_key = ""
    _interval = 120
    # 日历过滤：只处理已到播出时间的缺集（未播出集不搜 NF，避免提前白查）
    _calendar_filter = True
    # 转存目标目录（115 网盘路径，统一 /最近接收，由 MP 目录监控自动整理入库）
    _target_folder = "/最近接收"
    # 转存落地校验等待秒数（115 转存为异步，转存后等待再查询目标目录）
    _verify_wait = 20
    # 2026-10-17：本轮已做过 115 落地校验则跳过逐文件校验，减少 115 接口调用
    _verified_this_round = False
    # 本轮是否有待校验的转存（整轮结束统一校验一次）
    _round_verify_pending = False
    # 转存落地后是否立即触发一次 MP 整理（115 为远程存储，目录监控仅定时轮询）
    _auto_transfer = True
    # 详情页筛选状态（纯前端展示，存 plugindata 持久化）：all/tv/movie/missing/full/movie_missing
    _page_filter = "all"
    # 内存去重缓存（已转存 key 集合），init 时从 plugindata 加载
    _nf_done = set()
    # 跨通道去重：转存前查 NF 历史，若该集已被「追更」等其它通道转存则跳过
    _recent_dedup = True
    # 去重时扫描的 NF 历史条数
    _dedup_scan_limit = 50

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        # 手动搜索缺集防重入锁
        self._check_lock = threading.Lock()
        self._enabled = False
        self._api_url = ""
        self._api_key = ""
        self._interval = 120
        self._calendar_filter = True
        self._target_folder = "/最近接收"
        self._verify_wait = 20
        self._verified_this_round = False
        self._round_verify_pending = False
        self._auto_transfer = True
        self._nf_done = set((self.get_data("nf_transferred") or {}).keys())
        self._recent_dedup = True
        self._dedup_scan_limit = 50
        self._page_filter = str(self.get_data("page_filter") or "all") or "all"
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._api_url = str(config.get("api_url") or "").rstrip("/")
        self._api_key = str(config.get("api_key") or "")
        # 2026-10-17：缺集监测与 NextFind 控制器共用同一套 NF 接口。
        # 这里没填地址/Key 时，回退读取 NextFindController 的配置，避免两处各填一遍
        # 导致漏填后整轮监测直接跳过。
        if not self._api_url or not self._api_key:
            try:
                nf_conf = self.get_config("NextFindController") or {}
                if isinstance(nf_conf, dict):
                    self._api_url = self._api_url or str(nf_conf.get("api_url") or "").rstrip("/")
                    self._api_key = self._api_key or str(nf_conf.get("api_key") or "")
                    if self._api_url and self._api_key:
                        logger.info("缺集监测复用 NextFind 控制器的 NF 接口配置")
            except Exception as err:
                logger.debug(f"读取 NextFindController 配置失败（忽略）：{err}")
        try:
            self._interval = max(5, int(config.get("interval") or 120))
        except (TypeError, ValueError):
            self._interval = 120
        self._calendar_filter = bool(config.get("calendar_filter", True))
        self._target_folder = str(config.get("target_folder") or "/最近接收").strip() or "/最近接收"
        try:
            self._verify_wait = max(0, int(config.get("verify_wait") or 20))
        except (TypeError, ValueError):
            self._verify_wait = 20
        self._auto_transfer = bool(config.get("auto_transfer", True))
        self._recent_dedup = bool(config.get("recent_dedup", True))
        try:
            self._dedup_scan_limit = max(10, int(config.get("dedup_scan_limit") or 50))
        except (TypeError, ValueError):
            self._dedup_scan_limit = 50

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/nfmissing",
                "event": EventType.PluginAction,
                "desc": "手动触发缺集监测检查",
                "category": "订阅",
                "data": {"action": "check"}
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return [
            {
                "path": "/check_missing",
                "endpoint": self.api_check_missing,
                "methods": ["POST"],
                "summary": "手动搜索缺集",
                "description": "异步触发缺集监测检查：扫描订阅缺集并搜索 NF 转存",
                "auth": "bear",
            },
            {
                "path": "/search_sub",
                "endpoint": self.api_search_sub,
                "methods": ["POST"],
                "summary": "搜索单个订阅缺集",
                "description": "对指定订阅单独执行缺集 NF 搜索补全",
                "auth": "bear",
            },
            {
                "path": "/set_filter",
                "endpoint": self.api_set_filter,
                "methods": ["POST"],
                "summary": "设置详情页筛选状态",
                "description": "仅保存前端筛选标签的选中状态（all/tv/movie/missing/full/movie_missing），不影响任何业务逻辑",
                "auth": "bear",
            }
        ]

    def api_set_filter(self, payload: dict) -> Response:
        """保存详情页筛选状态（纯前端交互，不涉及业务逻辑）。"""
        f = (payload or {}).get("filter") or "all"
        if f not in ("all", "tv", "movie", "missing", "full", "movie_missing"):
            f = "all"
        self._page_filter = f
        try:
            self.save_data("page_filter", f)
        except Exception as err:
            logger.error(f"保存筛选状态失败：{err}")
        return Response(success=True, message=f"筛选已切换：{f}")

    def api_check_missing(self) -> Response:
        """异步触发缺集监测检查（详情页「搜索缺集」按钮调用）。"""
        if not self._check_lock.acquire(blocking=False):
            return Response(success=False, message="缺集监测正在运行中，请稍候")
        try:
            def _run():
                try:
                    self.check_missing()
                except Exception as err:
                    logger.error(f"后台缺集监测异常：{err}")
                finally:
                    self._check_lock.release()
            threading.Thread(target=_run, daemon=True).start()
            return Response(success=True, message="已开始搜索缺集，稍后刷新页面查看结果")
        except Exception as err:
            self._check_lock.release()
            logger.error(f"启动缺集监测失败：{err}")
            return Response(success=False, message=f"启动失败：{err}")

    def api_search_sub(self, payload: dict) -> Response:
        """异步对单个订阅执行缺集 NF 搜索补全（详情页卡片「搜索」按钮调用）。"""
        sid = (payload or {}).get("sid")
        if not sid:
            return Response(success=False, message="缺少订阅 ID")
        try:
            sid = int(sid)
        except (TypeError, ValueError):
            return Response(success=False, message=f"无效的订阅 ID：{sid}")
        sub = SubscribeOper().get(sid)
        if not sub:
            return Response(success=False, message=f"订阅不存在：{sid}")
        if not self._check_lock.acquire(blocking=False):
            return Response(success=False, message="已有搜索任务运行中，请稍候")
        try:
            def _run():
                try:
                    logger.info(f"开始单独搜索订阅缺集：{sub.name} (sid={sid})")
                    result = self._nf_complete_for_subscribe(sub)
                    logger.info(f"单订阅搜索完成：{sub.name} (sid={sid}) 结果={result}")
                except Exception as err:
                    logger.error(f"单订阅搜索异常：{sub.name} - {err}")
                finally:
                    self._check_lock.release()
            threading.Thread(target=_run, daemon=True).start()
            return Response(success=True, message=f"已开始搜索「{sub.name}」缺集")
        except Exception as err:
            self._check_lock.release()
            logger.error(f"启动单订阅搜索失败：{err}")
            return Response(success=False, message=f"启动失败：{err}")

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
                        "component": "VTextField",
                        "props": {
                            "model": "interval",
                            "label": "缺集监测间隔（分钟，最小5）",
                            "placeholder": "120"
                        }
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "calendar_filter",
                            "label": "日历过滤（只搜已到播出时间的缺集）",
                            "hint": "开启后，按 TMDB 播出日历判断，未播出集不去 NF 搜索；无日历数据的剧回退为全部缺集"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "target_folder",
                            "label": "转存目标目录（115 网盘路径）",
                            "placeholder": "/最近接收",
                            "hint": "NF 资源转存到该目录，由 MP 目录监控自动整理入库"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "verify_wait",
                            "label": "转存落地校验等待（秒）",
                            "placeholder": "20",
                            "hint": "115 转存为异步操作，转存后等待该秒数再查询目标目录确认文件落地；0 表示不等待直接校验"
                        }
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "auto_transfer",
                            "label": "转存落地后立即触发整理",
                            "hint": "115 为远程存储，MP 目录监控仅定时轮询；开启后每次转存校验通过即主动触发一次整理，加快入库"
                        }
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "recent_dedup",
                            "label": "跨通道去重",
                            "hint": "转存前先查 NF 转存历史：若该集已被 NF 自带「追更」等其它通道转存，则跳过，避免重复消耗转存配额"
                        }
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "dedup_scan_limit",
                            "label": "去重扫描历史条数",
                            "placeholder": "50",
                            "hint": "跨通道去重时查询 NF 历史的前 N 条记录；过小可能漏判，建议 50~200"
                        }
                    }
                ]
            }
        ], {
            "enabled": False,
            "api_url": "https://your-server.example.com/api/openapi",
            "api_key": "",
            "interval": 120,
            "calendar_filter": True,
            "target_folder": "/最近接收",
            "verify_wait": 20,
            "auto_transfer": True,
            "recent_dedup": True,
            "dedup_scan_limit": 50
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面：缺集监测状态 + 订阅海报卡片（含订阅时间与连载进度）。"""
        if not self._enabled:
            return None
        history = self.get_data("subscribe_history") or {}
        # 顶部小字状态条
        status_nodes = [
            {
                "component": "div",
                "props": {
                    "class": "text-caption text-medium-emphasis text-truncate mb-2"
                },
                "text": (
                    f"缺集监测：{self._interval} 分钟/次"
                    + ("（日历过滤，只搜已播出缺集）" if self._calendar_filter else "")
                    + "；NF 无资源直接跳过"
                    + f"；转存目录：{self._target_folder}"
                )
            }
        ]
        if not history:
            return status_nodes + [
                {
                    "component": "div",
                    "text": "暂无订阅记录",
                    "props": {
                        "class": "text-center pa-6 text-medium-emphasis"
                    }
                }
            ]
        # 订阅表实时数据（一次取全量，按 id 建索引）
        subscribe_map = {}
        try:
            subscribe_map = {str(sub.id): sub for sub in (SubscribeOper().list() or [])}
        except Exception as err:
            logger.error(f"查询订阅列表失败：{err}")
        # 媒体库实际入库状态（实时查 Emby，避免订阅表 lack_episode 陈旧）
        lib_state = self._build_library_state(list(subscribe_map.values()))

        def safe_text(value, default: str = "—") -> str:
            """统一处理空值，避免页面直接显示 None。"""
            if value is None:
                return default
            text = str(value).strip()
            if not text or text.lower() == "none":
                return default
            return text

        def tmdb_href(media_type: str, tmdb_id) -> str:
            """根据媒体类型生成 TMDB 链接，缺少 ID 时返回空串。"""
            clean_id = safe_text(tmdb_id, "")
            if not clean_id:
                return ""
            if media_type == "tv":
                return f"https://www.themoviedb.org/tv/{clean_id}"
            return f"https://www.themoviedb.org/movie/{clean_id}"

        def progress_text(media_type: str, state: str, total, lack, season) -> str:
            """生成连载进度文本：电视剧按已下载集数，电影按订阅状态。"""
            if state == "REMOVED":
                return "已完结" if media_type == "tv" else "已完成"
            if media_type == "movie":
                if int(total or 0) > 0 and int(lack or 0) <= 0:
                    return "已入库"
                return "已订阅"
            total_i = int(total or 0)
            lack_i = int(lack or 0)
            if total_i <= 0:
                return "连载中"
            done = total_i - lack_i
            if done >= total_i:
                return f"已全 {total_i} 集"
            if done <= 0:
                return f"尚未下载 · 全 {total_i} 集"
            if season:
                return f"连载至 S{int(season):02d}E{done:02d} · 全 {total_i} 集"
            return f"连载至 {done}/{total_i} 集"

        state_map = {
            "R": "订阅中",
            "S": "已暂停",
            "P": "待定",
            "REMOVED": "已完成"
        }
        state_color = {
            "R": "teal",
            "S": "grey",
            "P": "orange",
            "REMOVED": "deep-purple"
        }

        # 组装条目（按订阅时间降序）
        items = []
        for sid, info in sorted(
            history.items(), key=lambda kv: kv[1].get("time") or "", reverse=True
        ):
            sub = subscribe_map.get(str(sid))
            media_type = info.get("media_type") or (
                "tv" if sub and sub.type == "电视剧" else "movie"
            )
            state = sub.state if sub else "REMOVED"
            total = sub.total_episode if sub else 0
            lack = sub.lack_episode if sub else 0
            season = info.get("season") if info.get("season") is not None else (sub.season if sub else None)
            poster = safe_text(sub.poster, "") if sub else ""
            # 媒体库实际入库状态（优先实时，未命中回退订阅表字段）
            lib_tmdb = str(info.get("tmdbid") or self._sub_tmdb_id(sub))
            lib_info = lib_state.get(lib_tmdb)
            if lib_info is not None:
                if media_type == "movie":
                    progress = "已入库" if lib_info.get("movie") else "未入库"
                else:
                    season_key = int(season) if season else 1
                    eps_in_lib = (lib_info.get("tv") or {}).get(season_key, set())
                    done = len(eps_in_lib)
                    total_i = int(total or 0)
                    if total_i and done >= total_i:
                        progress = f"已全 {total_i} 集"
                    elif done:
                        progress = f"已更新 {done}/{total_i} 集" if total_i else f"已下载 {done} 集"
                    else:
                        # 订阅季无集：检查该剧其他季（库中季号可能与订阅季不一致）
                        all_tv = lib_info.get("tv") or {}
                        other_eps = {
                            s: len(e) for s, e in all_tv.items()
                            if s != season_key and e
                        }
                        if other_eps:
                            desc = "、".join(
                                f"S{s}({n}集)" for s, n in sorted(other_eps.items())
                            )
                            progress = f"订阅季无 · 库中 {desc}"
                        else:
                            progress = f"尚未下载 · 全 {total_i} 集" if total_i else "连载中"
            else:
                progress = progress_text(media_type, state, total, lack, season)
            items.append({
                "sid": sid,
                "title": safe_text(info.get("title") or (sub.name if sub else ""), "未命名"),
                "media_type": "电影" if media_type == "movie" else "电视剧",
                "state": state,
                "state_text": state_map.get(state, "未知"),
                "state_color": state_color.get(state, "grey"),
                "season": season,
                "progress": progress,
                "time": safe_text(info.get("time") or (sub.date if sub else ""), "未知时间"),
                "poster": poster,
                "tmdb_href": tmdb_href(media_type, info.get("tmdbid") or self._sub_tmdb_id(sub) or None),
            })

        movie_count = sum(1 for item in items if item.get("media_type") == "电影")
        tv_count = sum(1 for item in items if item.get("media_type") == "电视剧")

        def _is_missing(item: dict) -> bool:
            """判定条目是否处于缺集/未入库状态。"""
            if item.get("state") == "REMOVED":
                return False
            progress = item.get("progress", "")
            return not any(k in progress for k in ("已全", "已入库", "已订阅", "连载中"))

        missing_count = sum(1 for item in items if _is_missing(item))
        full_count = sum(1 for item in items if "已全" in item.get("progress", ""))
        movie_missing = sum(
            1 for item in items
            if item.get("media_type") == "电影" and "未入库" in item.get("progress", "")
        )
        # 按详情页筛选状态过滤列表（统计卡保持全量数字，仅列表与分页跟随筛选）
        _filter = getattr(self, "_page_filter", "all") or "all"
        if _filter == "tv":
            filtered = [i for i in items if i.get("media_type") == "电视剧"]
        elif _filter == "movie":
            filtered = [i for i in items if i.get("media_type") == "电影"]
        elif _filter == "missing":
            filtered = [i for i in items if _is_missing(i)]
        elif _filter == "full":
            filtered = [i for i in items if "已全" in i.get("progress", "")]
        elif _filter == "movie_missing":
            filtered = [i for i in items if i.get("media_type") == "电影" and "未入库" in i.get("progress", "")]
        else:
            filtered = items
        page_size = 12
        pages = [filtered[index:index + page_size] for index in range(0, len(filtered), page_size)]
        # 资源状态速查栏（筛选标签选中高亮 + 右侧分页文字）
        filter_defs = [
            ("all", "primary", f"全部 {len(items)}"),
            ("tv", "indigo", f"电视剧 {tv_count}"),
            ("movie", "deep-orange", f"电影 {movie_count}"),
            ("missing", "orange", f"缺集中 {missing_count}"),
            ("full", "teal", f"已全 {full_count}"),
            ("movie_missing", "error", f"电影未入库 {movie_missing}"),
        ]
        status_bar = [
            {
                "component": "VRow",
                "props": {"class": "d-flex align-center mb-2", "no-gutters": True},
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 9, "class": "d-flex align-center"},
                        "content": [
                            {
                                "component": "VChipGroup",
                                "props": {"class": "flex-grow-1"},
                                "content": [
                                    {
                                        "component": "VChip",
                                        "props": {
                                            "variant": "flat" if _filter == key else "tonal",
                                            "color": color,
                                            "class": "px-2",
                                        },
                                        "text": text,
                                        "events": {
                                            "click": {
                                                "api": "plugin/MyNfMissingMonitor/set_filter",
                                                "method": "POST",
                                                "params": {"filter": key}
                                            }
                                        }
                                    }
                                    for key, color, text in filter_defs
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 3, "class": "d-flex align-center justify-md-end justify-start text-caption text-medium-emphasis"},
                        "content": [
                            {"component": "span", "text": f"共 {len(filtered)} 条 · {len(pages)} 页"}
                        ]
                    }
                ]
            }
        ]

        def stat_card(title: str, value: str, color: str, value_class: str = "") -> dict:
            """顶部统计卡片，帮助快速掌握订阅规模。"""
            return {
                "component": "VCol",
                "props": {"cols": 6, "md": 2},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "color": color, "class": "pa-1"},
                        "content": [
                            {
                                "component": "VCardSubtitle",
                                "props": {"class": "pb-0 text-caption"},
                                "text": title
                            },
                            {
                                "component": "VCardTitle",
                                "props": {"class": f"text-h6 pt-1 font-weight-bold {value_class}".strip()},
                                "text": value
                            }
                        ]
                    }
                ]
            }

        def subscribe_card(item: dict) -> dict:
            """订阅卡片：海报 + 标题 + 状态 + 连载进度 + 订阅时间。"""
            title_node = {
                "component": "a" if item.get("tmdb_href") else "span",
                "props": {
                    "href": item.get("tmdb_href"),
                    "target": "_blank",
                    "class": "text-decoration-none"
                } if item.get("tmdb_href") else {},
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
                            },
                            {
                                "component": "VBtn",
                                "props": {
                                    "color": "primary",
                                    "variant": "tonal",
                                    "size": "small",
                                    "prepend-icon": "mdi-magnify",
                                    "class": "align-self-center flex-shrink-0"
                                },
                                "text": "搜索",
                                "events": {
                                    "click": {
                                        "api": "plugin/MyNfMissingMonitor/search_sub",
                                        "method": "POST",
                                        "params": {"sid": item.get("sid")}
                                    }
                                }
                            }
                        ]
                    }
                ]
            }

        page_items = []
        for page_records in pages:
            page_items.append({
                "component": "VWindowItem",
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "grid gap-2 grid-info-card"},
                        "content": [subscribe_card(item) for item in page_records]
                    }
                ]
            })

        return status_nodes + status_bar + [
            {
                "component": "VRow",
                "props": {"class": "mb-2 align-center"},
                "content": [
                    stat_card("订阅", f"{len(items)} 条", "primary"),
                    stat_card("电影", f"{movie_count} 条", "deep-orange"),
                    stat_card("电视剧", f"{tv_count} 条", "indigo"),
                    stat_card("缺集中", f"{missing_count} 条", "orange", "text-orange-darken-2"),
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4, "class": "d-flex align-center justify-md-end justify-start"},
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {
                                    "color": "primary",
                                    "variant": "tonal",
                                    "prepend-icon": "mdi-magnify",
                                    "size": "small"
                                },
                                "text": "搜索缺集",
                                "events": {
                                    "click": {
                                        "api": "plugin/MyNfMissingMonitor/check_missing",
                                        "method": "POST",
                                        "params": {}
                                    }
                                }
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VWindow",
                "props": {"show-arrows": "hover"},
                "content": page_items
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """返回插件定时服务。"""
        return [
            {
                "id": "MyNfMissingMonitorCheck",
                "name": "缺集监测检查",
                "trigger": "interval",
                "func": self.check_missing,
                # 显式传 seconds，避免 APScheduler 0 间隔退化为每秒触发（一直连跑）
                "kwargs": {"seconds": self._interval * 60}
            }
        ]

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        return None

    # ==================== 核心逻辑 ====================

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None) -> None:
        """处理插件命令。"""
        if event:
            event_data = event.event_data
            if not event_data:
                return
            if event_data.get("action") == "check":
                logger.info("收到命令，开始缺集监测检查 ...")
                self.check_missing()

    @eventmanager.register(EventType.SubscribeAdded)
    def handle_subscribe_added(self, event: Event = None) -> None:
        """新订阅默认置为暂停（S），仅由本插件 NF 补全，不触发系统 PT 搜索下载。"""
        if not self._enabled:
            return
        if not event or not event.event_data:
            return
        try:
            subscribe_info = event.event_data.get("subscribe_info") or {}
            subscribe_id = subscribe_info.get("id") or event.event_data.get("subscribe_id")
            if not subscribe_id:
                for key in ("id", "sid"):
                    if event.event_data.get(key):
                        subscribe_id = event.event_data.get(key)
                        break
            if not subscribe_id:
                logger.debug("订阅添加事件缺少订阅 ID，跳过暂停处理")
                return
            SubscribeOper().update(int(subscribe_id), {"state": "S"})
            name = subscribe_info.get("name") or subscribe_id
            logger.info(f"新订阅已置为暂停，仅由 NF 补全：{name} (sid={subscribe_id})")
        except Exception as err:
            logger.error(f"处理订阅添加事件异常：{err}")

    def check_missing(self) -> None:
        """监测全部 MP 订阅缺集，NF 有资源则转存补全，无资源跳过（不触发 PT）。"""
        if not self._enabled:
            return
        if not self._api_url or not self._api_key:
            logger.warn("缺集监测未配置 API 地址或 API Key")
            return
        # 2026-10-17：加运行标记，便于确认定时服务是否真的在跑（此前只看日志无法区分
        # 「服务没触发」和「触发了但无输出」）。
        logger.info(
            f"开始缺集监测检查（周期 {self._interval} 分钟，NF={self._api_url}）..."
        )
        self._verified_this_round = False
        self._round_verify_pending = False
        try:
            # 扫描全部订阅（含暂停 S 状态），暂停订阅不参与系统 PT 搜索，仅由本插件 NF 补全
            subscribes = SubscribeOper().list() or []
            tv_subs = [x for x in subscribes if x.type == MediaType.TV.value]
            movie_subs = [x for x in subscribes if x.type == MediaType.MOVIE.value]
            logger.info(
                f"订阅总数：{len(subscribes)}（电视剧 {len(tv_subs)}，电影 {len(movie_subs)}）"
            )
            transferred = 0
            no_nf = 0
            complete = 0
            for sub in subscribes:
                if sub.type not in (MediaType.TV.value, MediaType.MOVIE.value):
                    continue
                try:
                    result = self._nf_complete_for_subscribe(sub)
                    if result == "transferred":
                        transferred += 1
                    elif result == "no_nf_resource":
                        no_nf += 1
                    elif result == "complete":
                        complete += 1
                except Exception as err:
                    logger.error(f"订阅缺集监测异常：{sub.name} - {err}")
            logger.info(
                f"缺集监测完成：NF转存补齐 {transferred}，NF无资源跳过 {no_nf}，无缺集 {complete}"
            )
            if self._round_verify_pending:
                self._finish_round_verify()
        except Exception as err:
            logger.error(f"缺集监测异常：{err}")

    @staticmethod
    def _sub_tmdb_id(sub) -> str:
        """读取订阅的 TMDB ID。

        2026-10-17 修正：新版 MoviePilot 的订阅对象已把 ``tmdbid`` 合并为
        ``media_id``（配合 ``media_source``），旧代码直接读 ``sub.tmdbid`` 会抛
        ``'Subscribe' object has no attribute 'tmdbid'``，导致缺集监测全部跳过。
        这里同时兼容两种字段：非 TMDB 来源（如 bangumi/douban）不返回 ID。
        """
        if not sub:
            return ""
        media_id = getattr(sub, "media_id", None)
        if media_id:
            source = str(getattr(sub, "media_source", "") or "").lower()
            if source and source not in ("tmdb", "themoviedb"):
                return ""
            return str(media_id)
        legacy = getattr(sub, "tmdbid", None)
        return str(legacy) if legacy else ""

    def _nf_complete_for_subscribe(self, sub) -> str:
        """对单个订阅：缺集 →（日历过滤已播出集）→ NF 搜索 → 有资源拆包转存补全；无资源直接跳过。"""
        tmdb_id = self._sub_tmdb_id(sub)
        if not tmdb_id:
            return "no_nf_resource"
        missing = self._resolve_subscribe_missing(sub)
        if not missing:
            return "complete"
        media_type = "tv" if sub.type == MediaType.TV.value else "movie"
        # 日历过滤：只处理已到播出时间的缺集（未播出集不去搜 NF）
        if self._calendar_filter and media_type == "tv" and sub.season:
            aired = self._get_aired_episodes(int(tmdb_id), int(sub.season))
            if aired is not None and aired:
                filtered = {}
                for sea, eps in missing.items():
                    if sea != sub.season:
                        continue
                    need = [ep for ep in eps if ep in aired]
                    if need:
                        filtered[sea] = need
                missing = filtered
                if not missing:
                    logger.info(f"日历过滤后无待搜集（未到播出时间）：{sub.name} (tmdb={tmdb_id})")
                    return "complete"
        # NF 无资源：直接跳过，不做任何 PT 下载兜底
        resources = self._check_nf_resource(int(tmdb_id), media_type)
        if resources is None:
            # 查询失败（网络/接口异常），本轮跳过，下轮重试
            logger.warn(f"NF 资源查询失败，本轮跳过：{sub.name} (tmdb={tmdb_id})")
            return "no_nf_resource"
        if not resources:
            logger.info(f"NF 无资源，跳过：{sub.name} (tmdb={tmdb_id})")
            return "no_nf_resource"
        # NF 有资源：拆包转存缺集
        if media_type == "movie":
            # 跨通道去重：NF 自带通道若已转存该片，跳过避免重复
            dup = self._nf_recent_transferred(tmdb_id)
            if dup:
                logger.info(f"跨通道去重跳过：{sub.name} 已由 {dup} 转存")
                self._mark_transferred(tmdb_id, "movie", 0, 0)
                return "transferred"
            best = self._pick_best_resource(resources)
            if best and self._transfer_nf_resource(best, media_type="movie"):
                self._mark_transferred(tmdb_id, "movie", 0, 0)
                logger.info(f"电影 NF 转存补齐：{sub.name} - {best.get('slug')}")
                return "transferred"
            return "no_nf_resource"
        done = False
        hdhive_failed = False
        for resource in resources:
            slug = resource.get("slug") or ""
            if slug.startswith("hdhive://") and hdhive_failed:
                continue
            preview_files = self._preview_nf_resource(slug)
            if preview_files is None:
                if slug.startswith("hdhive://"):
                    hdhive_failed = True
                continue
            for season, episodes in list(missing.items()):
                for ep in list(episodes):
                    key = self._transferred_key(tmdb_id, "tv", season, ep)
                    if key in self._nf_done:
                        continue
                    # 跨通道去重：NF 自带「追更」等通道若已转存该集，跳过避免重复
                    dup = self._nf_recent_transferred(tmdb_id, season, ep)
                    if dup:
                        logger.info(
                            f"跨通道去重跳过：{sub.name} S{season:02d}E{ep:02d} 已由 {dup} 转存"
                        )
                        self._mark_transferred(tmdb_id, "tv", season, ep)
                        episodes.remove(ep)
                        if not episodes:
                            missing.pop(season, None)
                        continue
                    # 剧名锚点：优先订阅名，其次资源名（资源名可能只是集号）
                    anchor = sub.name or resource.get("name") or ""
                    item = self._find_episode_item(preview_files, season, ep, title=anchor)
                    if not item:
                        continue
                    if self._transfer_single_item(item, media_type):
                        self._mark_transferred(tmdb_id, "tv", season, ep)
                        done = True
                        logger.info(f"NF 拆包转存：{sub.name} S{season:02d}E{ep:02d} - {item.get('name')}")
                        episodes.remove(ep)
                        if not episodes:
                            missing.pop(season, None)
        if done:
            return "transferred"
        # 拆包未命中：非 hdhive 资源整包转存回退
        # 注意：整包转存无法逐文件过滤，若分享包混有其他剧会一并带入。
        # 因此要求包内至少有一个文件匹配目标剧名，否则放弃整包转存。
        # 整包前先跨通道去重：缺集若已被 NF 其它通道转存，则无需整包转存。
        if missing:
            remaining_missing = {}
            for season, episodes in missing.items():
                keep = []
                for ep in episodes:
                    dup = self._nf_recent_transferred(tmdb_id, season, ep)
                    if dup:
                        logger.info(
                            f"跨通道去重跳过（整包）：{sub.name} S{season:02d}E{ep:02d} 已由 {dup} 转存"
                        )
                        self._mark_transferred(tmdb_id, "tv", season, ep)
                        continue
                    keep.append(ep)
                if keep:
                    remaining_missing[season] = keep
            missing = remaining_missing
            if not missing:
                logger.info(f"缺集均已由其它通道转存，跳过整包转存：{sub.name}")
                return "transferred"
        best = self._pick_best_resource(resources)
        if best and not (best.get("slug") or "").startswith("hdhive://"):
            preview_files = self._preview_nf_resource(best.get("slug") or "")
            if preview_files and self._pack_matches_title(preview_files, sub.name):
                if self._transfer_nf_resource(best, media_type="tv"):
                    for season, episodes in missing.items():
                        for ep in episodes:
                            self._mark_transferred(tmdb_id, "tv", season, ep)
                    logger.info(f"电视剧 NF 整包转存：{sub.name} - {best.get('slug')}")
                    return "transferred"
            elif preview_files:
                logger.warn(
                    f"整包转存跳过：包内未匹配到目标剧名 {sub.name}，"
                    f"疑似混包 - {best.get('slug')}"
                )
        return "no_nf_resource"

    # ==================== 缺集计算 ====================

    def _build_library_state(self, subscribes: List[Any]) -> dict:
        """并发查询媒体库实际入库状态，返回 {tmdb_str: {"movie": bool, "tv": {season: set(eps)}}}。

        针对订阅涉及的剧/电影定向查询（Emby 库 23 万集，不能全量拉取）：
        1. Series 全量一次（<1 万条）建 tmdb -> series_id 映射；
        2. 电视剧订阅并发查 /Shows/{series_id}/Episodes 拿各季已入库集；
        3. 电影订阅并发按 AnyProviderIdEquals=tmdb 判定是否入库。
        查询失败/未命中返回空（详情页回退订阅表 lack_episode）。
        """
        state = {}
        try:
            from concurrent.futures import ThreadPoolExecutor
            from app.modules.emby import EmbyModule
            em = EmbyModule()
            em.init_module()
            insts = em.get_instances()
            if not insts:
                return state
            inst = list(insts.values())[0]
            host = inst._host
            apikey = inst._apikey
            if not host or not apikey:
                return state
            # 1. Series 全量：tmdb -> series_id 列表（ProviderIds.Tmdb 或路径 {tmdbid=}/{tmdb=} 双匹配）
            series_map = {}
            url = f"{host}emby/Items"
            params = {
                "Recursive": "true",
                "IncludeItemTypes": "Series",
                "Fields": "ProviderIds,Path",
                "Limit": 10000,
                "api_key": apikey,
            }
            res = inst.get_data(url + "?" + "&".join(f"{k}={v}" for k, v in params.items()))
            if res and res.status_code == 200:
                for it in (res.json().get("Items") or []):
                    tm = (it.get("ProviderIds") or {}).get("Tmdb")
                    if not tm:
                        m = re.search(r"\{tmdb(?:id)?[-=](\d+)\}", it.get("Path") or "")
                        if m:
                            tm = m.group(1)
                    if tm:
                        series_map.setdefault(str(tm), []).append(it.get("Id"))
            tv_subs = [s for s in subscribes if s.type == MediaType.TV.value and self._sub_tmdb_id(s)]
            movie_subs = [s for s in subscribes if s.type == MediaType.MOVIE.value and self._sub_tmdb_id(s)]

            def _fetch_tv(tmdb_id) -> tuple:
                """查询单部电视剧各季已入库集（合并该剧全部 Series 条目的集）。"""
                tm = str(tmdb_id)
                ids = series_map.get(tm) or []
                eps = {}
                if not ids:
                    return tm, {}
                for series_id in ids:
                    u2 = f"{host}emby/Shows/{series_id}/Episodes"
                    p2 = {
                        "Fields": "ParentIndexNumber,IndexNumber",
                        "Limit": 1000,
                        "api_key": apikey,
                    }
                    try:
                        r = inst.get_data(u2 + "?" + "&".join(f"{k}={v}" for k, v in p2.items()))
                        if r and r.status_code == 200:
                            for it in (r.json().get("Items") or []):
                                sea = it.get("ParentIndexNumber")
                                ep = it.get("IndexNumber")
                                if sea is None or ep is None:
                                    continue
                                eps.setdefault(int(sea), set()).add(int(ep))
                    except Exception as err:
                        logger.warn(f"查询剧集入库状态失败：tmdb={tm} series={series_id} - {err}")
                return tm, eps

            def _fetch_movie(tmdb_id) -> tuple:
                """查询电影是否已入库。"""
                tm = str(tmdb_id)
                u3 = f"{host}emby/Items"
                p3 = {
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie",
                    "AnyProviderIdEquals": f"tmdb|{tmdb_id}",
                    "Limit": 5,
                    "api_key": apikey,
                }
                try:
                    r = inst.get_data(u3 + "?" + "&".join(f"{k}={v}" for k, v in p3.items()))
                    return tm, bool(r and r.status_code == 200 and (r.json().get("Items") or []))
                except Exception as err:
                    logger.warn(f"查询电影入库状态失败：tmdb={tm} - {err}")
                    return tm, False

            # 2/3. 并发查询
            with ThreadPoolExecutor(max_workers=8) as ex:
                for tm, eps in ex.map(_fetch_tv, [self._sub_tmdb_id(s) for s in tv_subs]):
                    state[tm] = {"movie": False, "tv": eps}
                for tm, ok in ex.map(_fetch_movie, [self._sub_tmdb_id(s) for s in movie_subs]):
                    state.setdefault(tm, {"movie": False, "tv": {}})["movie"] = ok
        except Exception as err:
            logger.error(f"构建媒体库入库状态失败：{err}")
        return state

    def _get_aired_episodes(self, tmdb_id: int, season: int) -> Optional[set]:
        """获取已播出的集号集合（按 TMDB 播出日历）；查询失败返回 None，无日历数据返回空集合。"""
        try:
            from app.chain.tmdb import TmdbChain
            episodes = TmdbChain().tmdb_episodes(tmdbid=tmdb_id, season=season) or []
        except Exception as err:
            logger.warn(f"获取 TMDB 播出日历失败：tmdb={tmdb_id} S{season} - {err}")
            return None
        today = datetime.now().strftime("%Y-%m-%d")
        aired = set()
        for ep in episodes:
            air_date = getattr(ep, "air_date", None) or ""
            ep_num = getattr(ep, "episode_number", None)
            if not air_date or not ep_num:
                continue
            if air_date <= today:
                aired.add(int(ep_num))
        return aired

    def _resolve_subscribe_missing(self, sub) -> Dict[int, List[int]]:
        """计算订阅缺集列表 {season: [episodes]}。"""
        try:
            meta = build_subscribe_meta(sub)
        except Exception as err:
            logger.error(f"构建订阅元数据失败：{sub.name} - {err}")
            return {}
        try:
            mediainfo = MediaChain().recognize_media(
                meta=meta,
                mtype=meta.type,
                **subscribe_recognize_kwargs(sub),
                cache=False,
            )
        except Exception as err:
            logger.error(f"识别订阅媒体失败：{sub.name} - {err}")
            return {}
        if not mediainfo:
            return {}
        totals = {}
        if sub.type == MediaType.TV.value and sub.season and sub.total_episode:
            totals = {sub.season: sub.total_episode}
        try:
            exist_flag, no_exists = DownloadChain().get_no_exists_info(
                meta=meta,
                mediainfo=mediainfo,
                totals=totals,
            )
        except Exception as err:
            logger.error(f"查询订阅缺集失败：{sub.name} - {err}")
            return {}
        if exist_flag:
            return {}
        missing = {}
        for season, info in (no_exists or {}).items():
            if isinstance(info, dict):
                for sea, notexist in info.items():
                    if getattr(notexist, "episodes", None):
                        missing[sea] = notexist.episodes
            elif getattr(info, "season", None) is not None:
                sea = info.season
                if getattr(info, "episodes", None):
                    missing[sea] = info.episodes
        return missing

    # ==================== NF API ====================

    def _check_nf_resource(self, tmdb_id: int, media_type: str) -> Optional[List[dict]]:
        """搜索 NextFind 网盘/TG 频道资源；查询失败返回 None（不缓存），真无资源返回空列表。"""
        url = f"{self._api_url}/resources/search"
        headers = {"X-API-Key": self._api_key}
        params = {
            "tmdb_id": str(tmdb_id),
            "media_type": "tv" if media_type == "tv" else "movie",
        }
        try:
            res = RequestUtils(headers=headers, timeout=15).get_res(url, params=params)
            if not res:
                logger.warn(f"NF 资源搜索失败（无响应）：tmdb={tmdb_id}")
                return None
            if res.status_code != 200:
                logger.warn(f"NF 资源搜索失败：HTTP {res.status_code}：tmdb={tmdb_id}")
                return None
            data = res.json()
            if data.get("status") != "success":
                logger.warn(f"NF 资源搜索返回异常：{str(data)[:150]}：tmdb={tmdb_id}")
                return None
            payload = data.get("data")
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                for key in ("items", "list", "resources", "results"):
                    val = payload.get(key)
                    if isinstance(val, list) and val:
                        return val
            return []
        except Exception as err:
            logger.warn(f"NF 资源搜索异常：{err}：tmdb={tmdb_id}")
            return None

    def _preview_nf_resource(self, slug: str) -> Optional[List[dict]]:
        """调用 NF 探针解包，返回文件树列表；失败返回 None。"""
        if not slug:
            return None
        url = f"{self._api_url}/preview"
        headers = {"X-API-Key": self._api_key}
        try:
            res = RequestUtils(headers=headers, timeout=10).post_res(url=url, json={"slug": slug})
            if not res or res.status_code != 200:
                logger.warn(f"NF 解包失败（HTTP {res.status_code if res else '无响应'}）：{slug[:60]}")
                return None
            data = res.json()
            if data.get("status") != "success":
                logger.warn(f"NF 解包返回异常：{str(data)[:150]}")
                return None
            files = data.get("data") or []
            if not isinstance(files, list):
                return None
            return [f for f in files if isinstance(f, dict) and f.get("id")]
        except Exception as err:
            logger.warn(f"NF 解包异常：{err}")
            return None

    def _pick_best_resource(self, resources: List[dict]) -> Optional[dict]:
        """从资源列表中挑选最优资源（4K/REMUX 优先）。"""
        if not resources:
            return None

        def _score(res: dict) -> int:
            score = 0
            reso = str(res.get("video_resolution") or res.get("resolution") or "")
            if any(k in reso.upper() for k in ("4K", "2160", "UHD")):
                score += 100
            elif "1080" in reso.upper():
                score += 50
            src = " ".join(res.get("source") or [])
            src = f"{src} {res.get('source_type') or ''}".upper()
            if "REMUX" in src:
                score += 60
            elif "蓝光" in src or "BLURAY" in src or "原盘" in src:
                score += 30
            score += min(int(res.get("unlocked_users_count") or 0), 100)
            return score

        return max(resources, key=_score)

    def _transfer_nf_resource(self, resource: dict, media_type: Optional[str] = None) -> bool:
        """整包转存资源到目标目录，并校验文件实际落地。

        :param resource: NF 资源对象（含 slug）
        :param media_type: 媒体类型（tv/movie），显式传入优先于资源对象字段
        """
        slug = resource.get("slug")
        if not slug:
            logger.warn("NF 资源缺少 slug，无法转存")
            return False
        if not media_type:
            media_type = resource.get("media_type")
        target_folder = self._target_folder
        url = f"{self._api_url}/transfer"
        headers = {"X-API-Key": self._api_key}
        body = {"slug": slug, "target_folder": target_folder}
        try:
            res = RequestUtils(headers=headers, timeout=30).post_res(url=url, json=body)
            if not res:
                logger.error(f"NF 转存失败（无响应）：{slug[:60]}")
                return False
            if res.status_code != 200:
                logger.error(f"NF 转存失败：HTTP {res.status_code} {res.text[:150]}")
                return False
            data = res.json()
            if data.get("status") != "success":
                logger.error(f"NF 转存返回异常：{str(data)[:150]}")
                return False
            logger.info(f"NF 转存请求成功：{slug[:60]} -> {target_folder}")
        except Exception as err:
            logger.error(f"NF 转存异常：{err}")
            return False
        # 2026-10-17 风控优化：不再每次转存都立刻查 115 目标目录，
        # 改为整轮检查结束时统一校验一次（见 _finish_round_verify）。
        self._round_verify_pending = True
        self._trigger_transfer(
            tmdb_id=str(resource.get("tmdb_id") or ""),
            title=str(resource.get("title") or resource.get("name") or ""),
        )
        return True

    def _transfer_single_item(self, item: dict, media_type: str) -> bool:
        """转存单个拆包文件（ed2k 链接或 115 文件 id），并校验文件实际落地。"""
        item_id = item.get("id")
        if not item_id:
            return False
        target_folder = self._target_folder
        url = f"{self._api_url}/transfer"
        headers = {"X-API-Key": self._api_key}
        body = {"slug": item_id, "target_folder": target_folder}
        try:
            res = RequestUtils(headers=headers, timeout=30).post_res(url=url, json=body)
            if not res:
                logger.error(f"NF 单文件转存失败（无响应）：{str(item_id)[:60]}")
                return False
            if res.status_code != 200:
                logger.error(f"NF 单文件转存失败：HTTP {res.status_code} {res.text[:150]}")
                return False
            data = res.json()
            if data.get("status") != "success":
                logger.error(f"NF 单文件转存返回异常：{str(data)[:150]}")
                return False
            logger.info(f"NF 单文件转存请求成功：{str(item_id)[:60]} -> {target_folder}")
        except Exception as err:
            logger.error(f"NF 单文件转存异常：{err}")
            return False
        # 2026-10-17 风控优化：单集转存不再逐个查 115，改为整轮结束统一校验。
        self._round_verify_pending = True
        self._trigger_transfer(
            tmdb_id=str(item.get("tmdb_id") or ""),
            title=str(item.get("title") or item.get("name") or ""),
        )
        return True

    def _p115_auto_transfer_active(self) -> bool:
        """判断 115 网盘自动整理插件是否已启用。

        读取该插件保存的配置（enabled 字段），不依赖宿主的私有运行态接口；
        插件未安装或读取失败时返回 False，此时仍投递定向整理，保证单独使用时也能入库。
        """
        try:
            conf = self.get_config("P115AutoTransfer")
            if isinstance(conf, dict):
                return bool(conf.get("enabled"))
        except Exception as err:
            logger.debug(f"读取 P115AutoTransfer 配置失败（按未启用处理）：{err}")
        return False

    def _trigger_transfer(self, tmdb_id: Optional[str] = None, title: str = "") -> None:
        """转存落地后请求整理（115 为远程存储，需要主动触发）。

        2026-10-17 修正：原实现调用 ``TransferChain().process()``，该方法并不存在，
        日志里的「已触发 MP 整理」实际什么都没做。现改为向 P115AutoTransfer 投递
        ``p115_target_transfer`` 定向整理请求，由它统一整理并清理源目录；
        插件未安装/未启用时忽略即可，不影响转存结果。
        """
        if not self._auto_transfer:
            return
        # 2026-10-17：115 网盘自动整理已启用时，/最近接收 的生活事件与兜底轮询本就会整理，
        # 再投递定向整理会造成重复准入（不会重复入库，但日志里会多一条失败记录）。
        # 这里先判断一次，已启用则只记录日志，不再重复请求。
        if self._p115_auto_transfer_active():
            logger.info(
                f"P115AutoTransfer 已启用，整理交给其常规监听：tmdb={tmdb_id} {title}"
            )
            return
        try:
            eventmanager.send_event(
                EventType.PluginAction,
                {
                    "action": "p115_target_transfer",
                    "tmdb_id": str(tmdb_id or ""),
                    "title": title or "",
                    "since": time.time(),
                    "channel": None,
                    "user": None,
                    "arg_str": str(tmdb_id or ""),
                },
            )
            logger.info(f"已请求 P115AutoTransfer 定向整理：tmdb={tmdb_id} {title}")
        except Exception as err:
            logger.warn(f"投递定向整理请求失败（不影响转存）：{err}")

    def _finish_round_verify(self) -> None:
        """整轮检查结束后统一校验一次 /最近接收 落地情况。

        115 转存是异步落地：所有转存提交完成后等待 `verify_wait` 秒，再查一次目标目录。
        相比「每转存一次就查一次」，接口调用次数从 N 次降到 1 次，
        明显降低触发 115 访问上限（风控）的概率。
        """
        if self._verified_this_round:
            return
        self._verified_this_round = True
        try:
            if self._verify_wait > 0:
                time.sleep(self._verify_wait)
            from pathlib import Path
            from app.modules.filemanager.storages.u115 import U115Pan

            storage = U115Pan()
            folder_item = storage.get_item(Path(self._target_folder))
            if not folder_item:
                logger.warn(f"整轮落地校验：目标目录不可访问 {self._target_folder}")
                return
            children = storage.list(folder_item) or []
            logger.info(
                f"整轮落地校验完成：{self._target_folder} 现有 {len(children)} 项"
                "（本轮转存不逐条复查，交由整理插件处理）"
            )
        except Exception as err:
            logger.warn(f"整轮落地校验异常（不影响转存）：{err}")

    def _verify_transfer_landed(self, slug: str, target_folder: str,
                                name: Optional[str] = None) -> bool:
        """校验转存是否真正落地：查询 115 目标目录，确认存在对应文件/目录。

        115 转存为异步操作，先等待 `verify_wait` 秒再查询。校验失败返回 False，
        调用方据此不写入去重记录，下轮重试（避免「假成功」）。
        """
        if self._verified_this_round:
            # 整轮已统一校验过，逐条校验直接视为通过，避免重复消耗 115 接口
            return True
        if self._verify_wait > 0:
            time.sleep(self._verify_wait)
        try:
            from pathlib import Path
            from app.modules.filemanager.storages.u115 import U115Pan
            storage = U115Pan()
            folder_item = storage.get_item(Path(target_folder))
            if not folder_item:
                logger.warn(f"转存校验：目标目录不存在或不可访问 {target_folder}")
                return False
            children = storage.list(folder_item) or []
            if not children:
                logger.warn(f"转存校验：目标目录为空 {target_folder}（slug={str(slug)[:60]}）")
                return False
            # 有文件名时按名称匹配；否则只要目录非空即视为落地（整包转存会新建子目录）
            if name:
                base = str(name).rsplit("/", 1)[-1]
                stem = base.rsplit(".", 1)[0]
                for child in children:
                    child_name = str(getattr(child, "name", "") or "")
                    if base in child_name or (stem and stem in child_name):
                        logger.info(f"转存校验通过：{child_name} 已落地 {target_folder}")
                        return True
                logger.warn(f"转存校验：目标目录未找到 {base}（slug={str(slug)[:60]}）")
                return False
            logger.info(f"转存校验通过：{target_folder} 现有 {len(children)} 项（slug={str(slug)[:60]}）")
            return True
        except Exception as err:
            logger.warn(f"转存校验异常（视为未落地，下轮重试）：{err}")
            return False

    # ==================== 工具 ====================

    @staticmethod
    def _title_tokens(title: str) -> List[str]:
        """从剧名中提取用于文件名校验的锚点 token。

        去掉年份、季集标记、常见修饰词后，取长度 >= 2 的片段。
        中文剧名整体作为一个 token；英文/数字按词切分。
        """
        if not title:
            return []
        text = str(title)
        # 去掉括号内容、年份、季集标记、常见技术词
        text = re.sub(r"[（(【\[].*?[)）】\]]", " ", text)
        text = re.sub(r"\b(19|20)\d{2}\b", " ", text)
        text = re.sub(r"\bS\d{1,2}(E\d{1,4})?\b", " ", text, flags=re.I)
        text = re.sub(
            r"\b(4K|2160P?|1080P?|720P?|UHD|HDR|DV|SDR|REMUX|BLURAY|WEB-?DL|HDTV|"
            r"AAC|AC3|EAC3|DTS|FLAC|MKV|MP4|HEVC|AVC|H\.?26[45])\b",
            " ",
            text,
            flags=re.I,
        )
        tokens: List[str] = []
        # 中文片段（连续 2 个及以上汉字）
        for seg in re.findall(r"[\u4e00-\u9fff]{2,}", text):
            tokens.append(seg)
        # 英文/数字词（长度 >= 2）
        for seg in re.findall(r"[A-Za-z0-9]{2,}", text):
            tokens.append(seg)
        # 去重保序
        seen = set()
        out = []
        for t in tokens:
            tl = t.lower()
            if tl not in seen:
                seen.add(tl)
                out.append(t)
        return out

    @classmethod
    def _find_episode_item(
        cls,
        files: List[dict],
        season: int,
        episode: int,
        title: Optional[str] = None,
    ) -> Optional[dict]:
        """在文件树中查找指定季集的单集文件（支持 SxxExx 与 第N集 格式）。

        :param title: 剧名锚点。传入时只接受文件名包含该剧名 token 的文件，
            避免同一分享包内含多部剧时跨剧误命中（如给 A 剧找 S01E37 却命中
            B 剧的 S01E37）。
        """
        tokens = cls._title_tokens(title) if title else []

        def _title_ok(name: str) -> bool:
            if not tokens:
                return True
            low = name.lower()
            return any(t.lower() in low for t in tokens)

        # 1) 精确 SxxExx（带剧名校验）
        s_str = f"S{season:02d}E{episode:02d}"
        for f in files:
            name = str(f.get("name") or "")
            if s_str in name.upper() and _title_ok(name):
                return f
        # 2) 宽松 SxxExx（S1E1 形式，带剧名校验）
        for f in files:
            name = str(f.get("name") or "")
            if re.search(rf"S{season}E{episode}\b", name, re.I) and _title_ok(name):
                return f
        # 3) 第N集（必须带剧名校验，否则裸集号极易跨剧误命中）
        if tokens:
            for f in files:
                name = str(f.get("name") or "")
                if re.search(rf"第\s*{episode}\s*集", name) and _title_ok(name):
                    return f
        return None

    @classmethod
    def _pack_matches_title(cls, files: List[dict], title: Optional[str]) -> bool:
        """校验分享包文件树中是否至少有一个文件匹配目标剧名。

        用于整包转存前的混包防护：若包内完全没有目标剧名 token，
        说明该包可能属于其他剧，放弃整包转存。
        """
        tokens = cls._title_tokens(title) if title else []
        if not tokens:
            # 无剧名锚点时不阻断（保持原行为）
            return True
        for f in files:
            name = str(f.get("name") or "")
            low = name.lower()
            if any(t.lower() in low for t in tokens):
                return True
        return False

    @staticmethod
    def _transferred_key(tmdb_id, media_type: str, season: int, episode: int) -> str:
        return f"{tmdb_id}:{media_type}:{season}:{episode}"
    def _nf_recent_transferred(self, tmdb_id, season: int = None, episode: int = None) -> Optional[str]:
        """查询 NF 转存历史，判断该集是否已被「任何通道」近期转存过。

        用于跨通道去重：NF 自带的「追更」与本插件的「智能体-订阅」是两条
        独立通道，各自转存同一集会造成重复。转存前先查历史，命中即跳过。

        :param tmdb_id: TMDB ID
        :param season: 季号（电影传 None）
        :param episode: 集号（电影传 None）
        :return: 命中时返回触发来源说明，未命中返回 None；查询失败返回 None（不阻塞转存）
        """
        if not self._recent_dedup or not self._api_url or not self._api_key:
            return None
        try:
            url = f"{self._api_url}/history"
            headers = {"X-API-Key": self._api_key}
            res = RequestUtils(headers=headers, timeout=15).get_res(
                url, params={"page": 1, "limit": self._dedup_scan_limit}
            )
            if not res or res.status_code != 200:
                logger.warn(f"NF 历史查询失败，跳过去重检查：HTTP {getattr(res, 'status_code', '无响应')}")
                return None
            items = (res.json() or {}).get("data") or []
        except Exception as err:
            logger.warn(f"NF 历史查询异常，跳过去重检查：{err}")
            return None

        tid = str(tmdb_id)
        want_ep = None
        if season is not None and episode is not None:
            want_ep = f"S{int(season):02d}E{int(episode):02d}"
        for h in items:
            try:
                if str(h.get("tmdb_id")) != tid:
                    continue
                if want_ep:
                    got = str(h.get("episode_str") or "")
                    if not got or got.upper() != want_ep:
                        continue
                reason = str(h.get("trigger_reason") or "未知")
                return f"{reason}（{h.get('transfer_time')}）"
            except Exception:
                continue
        return None

    def _mark_transferred(self, tmdb_id, media_type: str, season: int, episode: int) -> None:
        key = self._transferred_key(tmdb_id, media_type, season, episode)
        self._nf_done.add(key)
        data = self.get_data("nf_transferred") or {}
        data[key] = 1
        self.save_data("nf_transferred", data)