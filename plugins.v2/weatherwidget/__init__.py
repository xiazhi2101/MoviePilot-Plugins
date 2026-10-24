import base64
import hmac
import os
import re
import shutil
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psutil
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from starlette.requests import Request
from starlette.responses import Response

from app.sdk.browser import launch_browser_context
from app.sdk.config import settings
from app.sdk.events import eventmanager, Event
from app.sdk.logging import logger
from app.sdk.plugins import PluginManager
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType
from app.sdk.network import RequestUtils
from app.application.messaging.image import (
    WECHAT_IMAGE_SIGNATURE_PURPOSE,
    build_wechat_image_url,
)
from app.application.security.url import SecurityUtils

SCREENSHOT_DEVICES = {
    "default": {
        "mobile": {
            "device": "iphone_13_pro_max",
            "size": {}
        },
        "desktop": {
            "device": "ipad_pro_11",
            "size": {'width': 740, 'height': 1024}
        }
    },
    "border": {
        "mobile": {
            "device": "iphone_13_pro_max",
            "size": {}
        },
        "desktop": {
            "device": "ipad_pro_11",
            "size": {}
        }
    }
}

# 截图设备参数保持为插件本地常量，避免依赖浏览器内核的设备注册表。
SCREENSHOT_DEVICE_CONTEXTS = {
    "iphone_13_pro_max": {
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 428, "height": 746},
        "device_scale_factor": 3,
        "is_mobile": True,
        "has_touch": True
    },
    "ipad_pro_11": {
        "user_agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 834, "height": 1194},
        "device_scale_factor": 2,
        "is_mobile": True,
        "has_touch": True
    }
}

# 可选的天气刷新周期（小时）。截图每次都要启动完整浏览器，默认值取 6 小时，
# 在天气时效和浏览器带来的内存、磁盘开销之间取平衡。
REFRESH_INTERVAL_HOURS = (1, 3, 6, 12, 24)
DEFAULT_REFRESH_INTERVAL = 6
# 不刷新：只保留天气通知，不再启动浏览器截图，仪表盘也不显示天气。
# 用显式取值而不是空值表示，避免与旧配置中缺失该字段（按默认周期处理）混淆。
REFRESH_DISABLED = 0
# 和风天气公共 API Host，未配置专属 Host 时使用；
# 新版控制台为每个账号分配的专属 Host（形如 xxxx.qweatherapi.com）需在插件配置中填写。
DEFAULT_QWEATHER_API_HOST = "https://devapi.qweather.com"
# 公共城市反查 Host，仅在未配置专属 Host 且天气 API 也使用公共 Host 时使用。
DEFAULT_QWEATHER_GEO_HOST = "https://geoapi.qweather.com"

# 浏览器关闭后等待其进程退出的最长秒数，进程退出后文件页才不再被映射、可以释放。
BROWSER_EXIT_TIMEOUT = 5

# 允许作为通知卡片封面的图片扩展名。
# 通知卡片封面：天气截图会发布到前端静态目录，供企业微信等外部服务免鉴权拉取。
# 插件动态 API 的返回值会被宿主统一序列化为 JSON，无法直接输出图片字节，
# 因此改用「复制截图到前端目录 + 直接给出文件 URL」的方式。
# 通知正文各字段使用的图标。
TIME_ICON = "⏰"
TEMP_ICON = "🌡️"
WEATHER_ICON = "☁️"
AIR_ICON = "🍃"
WIND_ICON = "🌬️"
HUMIDITY_ICON = "💧"
FEELS_LIKE_ICON = "🌡️"
VISIBILITY_ICON = "👁️"
PRECIP_ICON = "🌧️"
PRESSURE_ICON = "📊"
UV_ICON = "☀️"
# 天气概述段落的首行缩进（两个全角空格）。
ABSTRACT_INDENT = "　　"
# 网页抓取路径下「基本天气数据」标签到图标的映射，未命中时留空。
BASIC_LABEL_ICONS = {
    "相对湿度": HUMIDITY_ICON,
    "体感温度": FEELS_LIKE_ICON,
    "紫外线": UV_ICON,
    "能见度": VISIBILITY_ICON,
    "降水量": PRECIP_ICON,
    "大气压": PRESSURE_ICON,
}

COVER_IMAGE_SUFFIXES = (".jpg", ".png", ".gif", ".bmp", ".jpeg", ".webp")
# 发布到前端静态目录的封面文件名。
COVER_PUBLIC_FILENAME = "weather_cover.png"


def publish_weather_cover() -> Optional[str]:
    """
    把最新的桌面端天气截图发布到前端静态目录并返回其公网 URL。

    企业微信服务器拉取图片时不会携带 MoviePilot 登录态，而前端静态目录
    由 Nginx 直接提供、无需鉴权，因此外部服务可以稳定访问。

    :return: 可公开访问的封面 URL；无截图或发布失败时返回 None
    """
    try:
        data_path = Path(settings.PLUGIN_DATA_PATH) / "WeatherWidget" / "images"
        if not data_path.is_dir():
            return None
        candidates = [
            item
            for item in data_path.glob("weather_*_desktop_*.png")
            if item.is_file() and item.suffix.lower() in COVER_IMAGE_SUFFIXES
        ]
        if not candidates:
            return None
        image = max(candidates, key=lambda x: x.stat().st_mtime)

        frontend_path = Path(str(settings.FRONTEND_PATH or "").strip())
        if not frontend_path or not frontend_path.is_dir():
            return None
        target = frontend_path / COVER_PUBLIC_FILENAME
        shutil.copyfile(image, target)

        app_domain = str(settings.APP_DOMAIN or "").strip().rstrip("/")
        if not app_domain:
            return None
        return f"{app_domain}/{COVER_PUBLIC_FILENAME}"
    except Exception as err:
        logger.error(f"发布天气封面失败：{err}")
        return None


class WeatherWidget(_PluginBase):
    # region 全局定义

    # 插件名称
    plugin_name = "实时天气推送"
    # 插件描述
    plugin_desc = "定时推送实时天气卡片（上方天气截图、下方带图标正文），并在仪表盘中显示实时天气；支持官方 API 优先、失败自动回退网页抓取。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/InfinityPacer/MoviePilot-Plugins/main/icons/weatherwidget.png"
    # 插件版本
    plugin_version = "3.5.0"
    # 插件作者
    plugin_author = "local"
    # 作者主页
    author_url = "https://github.com/xiazhi2101/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "weatherwidget_"
    # 加载顺序
    plugin_order = 80
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    # enable
    _enabled = None
    # border
    _border = None
    # clear_cache
    _clear_cache = None
    # location
    _location = None
    # weather_url
    _weather_url = None
    # 启用自动主题
    _auto_theme_enabled = None
    # 自动高度
    _auto_height = None
    # use_dark_mode
    _use_dark_mode = None
    # adapt_mode
    _adapt_mode = None
    # component_size
    _component_size = None
    # weather_background
    _weather_background = None
    # weather_current_time
    _weather_current_time = None
    # weather_air_tag
    _weather_air_tag = None
    # weather_air_tag_background
    _weather_air_tag_background = None
    # location_url
    _location_url = None
    # 和风天气 API 密钥，优先使用插件配置，未配置时读取环境变量。
    _weather_api_key = ""
    # 环境变量密钥只供运行时使用，不应被写入插件配置。
    _weather_api_key_configured = False
    # 配置中原有的密钥值，用于未显式配置时原样保留。
    _weather_api_key_original = None
    # 当日预报数据缓存，供出行建议复用，避免重复请求接口。
    _forecast_daily = None
    # 是否在天气概述后追加出行建议。
    _travel_advice = True
    # 和风天气 API Host（新版控制台分配的专属域名，形如 xxxx.qweatherapi.com）
    _weather_api_host = ""
    # Host 是否由用户显式配置；未配置时保留原值，避免回退值覆盖用户配置。
    _weather_api_host_configured = False
    # 配置中原有的 Host 值，用于未显式配置时原样保留。
    _weather_api_host_original = None
    # 开启天气通知
    _weather_notify = None
    # 天气通知周期
    _weather_notify_cron = None
    # 消息类型
    _weather_notify_type = None
    # last_screenshot_time
    _last_screenshot_time = None
    # min_screenshot_span
    _min_screenshot_span = 5 * 60
    # 截图超时时间
    _screenshot_timeout = 2 * 60
    # 截图类型
    _screenshot_type = None
    # 天气刷新周期（小时），同时约束定时刷新和仪表盘触发的按需刷新
    _refresh_interval = DEFAULT_REFRESH_INTERVAL
    # 最近一次截图时解析到的日出、日落时间，按需刷新时据此在本地判断主题是否需要切换
    _sunrise_time = None
    _sunset_time = None
    # 定时器
    _scheduler = None
    # 退出事件
    _event = None

    # endregion

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._event = threading.Event()
        config = config or {}

        self._enabled = bool(config.get("enabled", False))
        self._border = config.get("border", False)
        self._clear_cache = config.get("clear_cache", False)
        self._location = config.get("location", "")
        self._location_url = config.get("location_url", "")
        configured_api_key = config.get("weather_api_key")
        self._weather_api_key_configured = bool(str(configured_api_key or "").strip())
        # 记录配置中原有的密钥值，供整存更新时原样保留，避免字段被抹掉。
        self._weather_api_key_original = configured_api_key
        self._weather_api_key = str(
            configured_api_key or os.getenv("QWEATHER_API_KEY", "")
        ).strip()
        # 自定义 API Host：新版和风控制台为每个账号分配专属域名，
        # 未配置时回退到公共 Host，以兼容旧版 Key。
        configured_host = str(config.get("weather_api_host") or os.getenv("QWEATHER_API_HOST", "")).strip()
        # 只有用户显式配置过才更新；未配置时保留原值，避免把回退值写回
        # 覆盖用户填写的专属 Host，也避免字段在整存更新中被抹掉。
        self._weather_api_host_configured = bool(configured_host)
        self._weather_api_host_original = config.get("weather_api_host")
        self._weather_api_host = self.__normalize_api_host(configured_host)
        self._weather_url = self.__get_weather_url()
        self._auto_theme_enabled = config.get("auto_theme_enabled", True)
        self._auto_height = config.get("auto_height", False)
        self._last_screenshot_time = None
        self._sunrise_time = None
        self._sunset_time = None
        self._use_dark_mode = self.__should_use_dark_mode()
        self._adapt_mode = config.get("adapt_mode", "compatibility")
        self._component_size = config.get("component_size", "mini")
        self._refresh_interval = self.__normalize_refresh_interval(config.get("refresh_interval"))
        self._weather_notify = bool(config.get("weather_notify", True))
        self._weather_notify_cron = config.get("weather_notify_cron")
        self._weather_notify_type = config.get("weather_notify_type", "Plugin")
        # 出行建议开关，默认开启；关闭后概述后不再追加建议。
        self._travel_advice = bool(config.get("travel_advice", True))
        self._screenshot_type = self.__get_screenshot_type()
        self._weather_background = config.get("weather_background",
                                              "linear-gradient(225deg, #fee5ca, #e9f0ff 55%, #dce3fb)")
        self._weather_current_time = config.get("weather_current_time",
                                                datetime.now(tz=pytz.timezone(settings.TZ)).strftime('%Y-%m-%d %H:%M'))
        self._weather_air_tag = config.get("weather_air_tag", " AQI 优 ")
        self._weather_air_tag_background = config.get("weather_air_tag_background", "#95B359")

        if self._clear_cache:
            self.__save_data({})
            self.__clear_image()

        self.__update_config()

        if not self._enabled:
            return

        if not self._location:
            logger.error("城市不能为空")
            return

        if not self.__is_refresh_enabled():
            logger.info("已关闭天气刷新，仅推送天气通知")
            return

        if not self.__check_image():
            logger.info("没有找到截图，立即运行一次截图任务")
            self.__add_screenshot_task()

    def get_state(self) -> bool:
        return bool(self._enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
            定义远程控制命令
            :return: 命令关键字、事件、描述、附带数据
            """
        return [
            {
                "cmd": "/weather_notify",
                "event": EventType.PluginAction,
                "desc": "实时天气",
                "category": "工具",
                "data": {"action": "weather_notify"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        不注册动态 API。

        插件动态 API 的返回值会被宿主统一序列化为 JSON，无法直接输出图片
        字节；通知卡片封面改用「发布截图到前端静态目录并给出文件 URL」的
        方式实现，因此这里无需注册接口。
        """
        return []

    def __get_total_elements(self, image: str, key: str = "mobile") -> List[dict]:
        """
        组装汇总元素
        """
        if self._border:
            return [
                {
                    'component': 'VCardItem',
                    'content': [
                        {
                            'component': 'VCardTitle',
                            'text': f"{self._location}"
                        }
                    ]
                },
                {
                    'component': 'VCardItem',
                    'props': {
                        'class': 'w-full',
                        'style': {
                            'position': 'relative',
                            'height': 'auto',
                            'background': f'{self._weather_background}',
                            'padding': "0"
                        }
                    },
                    'content': [
                        {
                            'component': 'a',
                            'props': {
                                'href': f'{self._weather_url}',
                                "target": "_blank"
                            },
                            'content': [
                                {
                                    'component': 'VImg',
                                    'props': {
                                        'src': f'{image}',
                                        'height': 'auto' if key == 'mobile' else (
                                            '264px' if self._adapt_mode == 'compatibility' else 'auto'),
                                        'max-width': '100%',
                                        'width': '100%'
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        else:
            return [
                {
                    'component': 'VCardItem',
                    'props': {
                        'class': 'w-full',
                        'style': {
                            'position': 'relative',
                            'height': 'auto',
                            'background': f'{self._weather_background}',
                            'padding': 0 if self._adapt_mode == 'compatibility'
                                            or key == 'mobile' or self._auto_height else '0 0 1.5rem',
                        }
                    },
                    'content': [
                        {
                            'component': 'a',
                            'props': {
                                'href': f'{self._weather_url}',
                                "target": "_blank"
                            },
                            'content': [
                                {
                                    'component': 'VImg',
                                    'props': {
                                        'src': f'{image}',
                                        'height': 'auto' if self._auto_height or key == 'mobile' else (
                                            '336px' if self._adapt_mode == 'compatibility' else '310px'),
                                        'max-width': '100%',
                                        'width': '100%',
                                        'cover': False if self._adapt_mode == 'compatibility' else True,
                                    }
                                },
                                {
                                    'component': 'VCardText',
                                    'props': {
                                        'class': 'v-card-text w-full flex flex-row justify-start items-start absolute '
                                                 'top-0 left-0 cursor-pointer'
                                    },
                                    'content': [
                                        {
                                            'component': 'VCardTitle',
                                            'props': {
                                                'class': 'mb-1 line-clamp-2 overflow-hidden text-ellipsis ...',
                                                'style': {
                                                    'color': 'rgb(231 227 252)'
                                                    if self._use_dark_mode else 'rgb(58 53 65 / 87%)',
                                                }
                                            },
                                            'text': self._location
                                        }
                                    ]
                                },
                                {
                                    'component': 'VCardText',
                                    'props': {
                                        'class': 'w-full flex flex-row justify-end items-start absolute '
                                                 'left-0 cursor-pointer',
                                        'style': {
                                            'top': '1rem',
                                            'font-size': '12px',
                                            'line-height': '12px',
                                            'color': 'rgb(231 227 252)'
                                            if self._use_dark_mode else 'rgb(58 53 65 / 87%)',
                                            'text-align': 'right'
                                        }
                                    },
                                    'text': self._weather_current_time,
                                },
                                {
                                    'component': 'p',
                                    'props': {
                                        'class': 'w-full flex flex-row justify-end items-start absolute '
                                                 'right-0 cursor-pointer',
                                        'style': {
                                            'top': '2.4rem',
                                            'display': 'inline-block',
                                            'width': '76px',
                                            'padding-top': '4px',
                                            'padding-bottom': '4px',
                                            'margin-right': '20px',
                                            'font-size': '15px',
                                            'line-height': '16px',
                                            'text-align': 'center',
                                            'white-space': 'nowrap',
                                            'border-radius': '14px',
                                            'color': 'white',
                                            "background-color": self._weather_air_tag_background
                                        }
                                    },
                                    'text': self._weather_air_tag,
                                }
                            ]
                        }
                    ]
                }
            ]

    def get_dashboard(self, key: str = None, user_agent: str = None) \
            -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """
        获取插件仪表盘页面，需要返回：1、仪表板cols配置字典；2、全局配置（自动刷新等）；2、仪表板页面元素配置json（含数据）
        1、col配置参考：
        {
            "cols": 12, "md": 6
        }
        2、全局配置参考：
        {
            "refresh": 10, // 自动刷新时间，单位秒
            "border": True, // 是否显示边框，默认True，为False时取消组件边框和边距，由插件自行控制
        }
        3、页面配置使用Vuetify组件拼装，参考：https://vuetifyjs.com/
        """
        # 根据UA获取设备类型
        key = self.__detect_device_type(user_agent=user_agent).lower()
        # 不刷新时旧截图会越来越旧，不再展示，避免误导
        refresh_enabled = self.__is_refresh_enabled()
        image = self.__get_weather_base64_image(location=self._location, key=key) if refresh_enabled else None

        # 列配置
        size_to_cols_map = {
            "mini": {"cols": 12, "md": 4},
            "small": {"cols": 12, "md": 6},
            "medium": {"cols": 12, "md": 8},
            "large": {"cols": 12, "md": 12}
        }
        cols = size_to_cols_map.get(self._component_size, size_to_cols_map.get("mini"))

        # 全局配置
        attrs = {
            "border": not image
        }

        # 拼装页面元素
        if not image:
            elements = [
                {
                    'component': 'VCardItem',
                    'content': [
                        {
                            'component': 'div',
                            'text': '暂无数据' if refresh_enabled else '已关闭天气刷新',
                            'props': {
                                'class': 'text-center'
                            }
                        }
                    ]
                }
            ]
        else:
            elements = self.__get_total_elements(image=image, key=key)
        return cols, attrs, elements

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {

                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'border',
                                            'label': '显示边框',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear_cache',
                                            'label': '清理缓存',
                                        },
                                    }
                                ],
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'auto_theme_enabled',
                                            'label': '自动主题',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'auto_height',
                                            'label': '自动高度',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'weather_notify',
                                            'label': '开启天气通知',
                                        },
                                    }
                                ],
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'weather_notify_cron',
                                            'label': '天气通知周期',
                                            'placeholder': '5位cron表达式',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'weather_notify_type',
                                            'label': '消息类型',
                                            'items': [{"title": item.value, "value": item.name}
                                                      for item in NotificationType]
                                        }
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'travel_advice',
                                            'label': '出行建议',
                                            'hint': '在天气概述后追加一句出行提示',
                                            'persistent-hint': True,
                                        }
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'weather_api_key',
                                            'label': '和风天气 API Key',
                                            'type': 'password',
                                            'clearable': True,
                                            'hint': '未填写时读取 QWEATHER_API_KEY 环境变量',
                                            'persistent-hint': True,
                                        }
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'weather_api_host',
                                            'label': '和风天气 API Host',
                                            'clearable': True,
                                            'hint': '新版控制台的专属域名，形如 xxxx.qweatherapi.com；留空使用公共 Host',
                                            'persistent-hint': True,
                                        }
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'component_size',
                                            'label': '组件规格',
                                            'items': [
                                                {"title": "迷你", "value": "mini"},
                                                {"title": "小型", "value": "small"},
                                                {"title": "中型", "value": "medium"},
                                                {"title": "大型", "value": "large"}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'refresh_interval',
                                            'label': '天气刷新周期',
                                            'items': [
                                                {"title": "不刷新（仅通知）", "value": REFRESH_DISABLED},
                                                *[
                                                    {"title": f"每 {hours} 小时", "value": hours}
                                                    for hours in REFRESH_INTERVAL_HOURS
                                                ]
                                            ],
                                            'hint': '每次刷新都会启动浏览器截图；不刷新时仅推送通知，仪表盘不显示天气',
                                            'persistent-hint': True,
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'location',
                                            'label': '城市',
                                            'placeholder': '城市地点',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'location_url',
                                            'label': '城市链接',
                                            'placeholder': '和风天气的城市天气链接',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'adapt_mode',
                                            'label': '适配方案',
                                            'items': [
                                                {'title': '兼容模式', 'value': 'compatibility'},
                                                {'title': '画质模式', 'value': 'quality'}
                                            ]
                                        }
                                    }
                                ],
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '注意：通过在和风天气官网获取对应链接精确定位城市，如「秦淮区」的链接为'
                                                    'https://www.qweather.com/weather/qinhuai-101190109.html，'
                                                    '则在城市链接填写 qinhuai-101190109'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '注意：数据异常时，可通过填写城市链接精确定位城市'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '天气数据来源于 '
                                        },
                                        'content': [
                                            {
                                                'component': 'a',
                                                'props': {
                                                    'href': 'https://www.qweather.com',
                                                    'target': '_blank'
                                                },
                                                'content': [
                                                    {
                                                        'component': 'u',
                                                        'text': '和风天气'
                                                    }
                                                ]
                                            },
                                            {
                                                'component': 'span',
                                                'text': '，再次感谢和风天气（https://www.qweather.com）提供的服务'
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "border": False,
            "auto_theme_enabled": True,
            "adapt_mode": "compatibility",
            "weather_notify": True,
            "weather_notify_cron": "0 8 * * *",
            "auto_height": False,
            "component_size": "mini",
            "refresh_interval": DEFAULT_REFRESH_INTERVAL
        }

    def get_page(self) -> Optional[List[dict]]:
        """不提供详情页，函数体保持占位使框架判定 has_page=False，点击卡片直接进入配置页。"""
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """

        services = []

        if self.__is_refresh_enabled():
            services.append({
                "id": "RefreshWeather",
                "name": "定时获取天气信息",
                "trigger": "interval",
                "func": self.__take_screenshots,
                "kwargs": {"hours": self._refresh_interval}
            })

        if self._weather_notify and self._weather_notify_cron:
            services.append({
                "id": "NotifyWeather",
                "name": "定时推送天气通知",
                "trigger": CronTrigger.from_crontab(self._weather_notify_cron),
                "func": self.notify_weather,
                "kwargs": {}
            })

        return services

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown(wait=False)
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            logger.info(str(e))

    def __update_config(self):
        """保存插件配置。

        密钥与 Host 属于敏感且易丢失的配置：本方法是全量覆盖更新，
        若本次未携带某个字段，它会从配置中永久消失。因此这两个字段
        在写入时先从现有配置回读补齐，保证任何一次更新都不会弄丢。
        """
        # 先取回当前已保存的配置，用于补齐本次未携带的敏感字段
        saved_config = self.get_config() or {}
        config = {
            "enabled": self._enabled,
            "border": self._border,
            "location": self._location,
            "location_url": self._location_url,
            "weather_background": self._weather_background,
            "weather_current_time": self._weather_current_time,
            "weather_air_tag": self._weather_air_tag,
            "weather_air_tag_background": self._weather_air_tag_background,
            "auto_theme_enabled": self._auto_theme_enabled,
            "auto_height": self._auto_height,
            'adapt_mode': self._adapt_mode,
            "component_size": self._component_size,
            "refresh_interval": self._refresh_interval,
            "weather_notify": self._weather_notify,
            "weather_notify_cron": self._weather_notify_cron,
            "weather_notify_type": self._weather_notify_type,
            "travel_advice": self._travel_advice,
        }
        # 显式配置过写新值；否则从已保存配置中保留，避免字段被整存更新抹掉。
        if self._weather_api_host_configured:
            config["weather_api_host"] = self._weather_api_host
        elif self._weather_api_host_original is not None:
            config["weather_api_host"] = self._weather_api_host_original
        elif saved_config.get("weather_api_host"):
            config["weather_api_host"] = saved_config.get("weather_api_host")
        if self._weather_api_key_configured:
            config["weather_api_key"] = self._weather_api_key
        elif self._weather_api_key_original is not None:
            config["weather_api_key"] = self._weather_api_key_original
        elif saved_config.get("weather_api_key"):
            config["weather_api_key"] = saved_config.get("weather_api_key")
        self.update_config(config)

    def invoke_service(self, request: Request, location: str, apikey: str) -> Any:
        """invokeService"""
        return PluginManager().run_plugin_method(self.__class__.__name__, "get_weather_image", **{
            "request": request,
            "location": location,
            "apikey": apikey
        })

    def get_weather_image(self, request: Request, location: str = None, apikey: str = None) -> Any:
        """读取天气截图。

        该接口供仪表盘使用，需携带系统 API_TOKEN；未携带或不一致时拒绝访问。
        ``location`` 缺省时使用插件配置的城市。
        """
        if apikey != settings.API_TOKEN:
            return None

        if not self.__is_refresh_enabled():
            return None
        if not location:
            location = self._location
        if not location:
            logger.error("没有地址信息，获取天气图片失败")
            return None
        # 截图过期或主题需要切换时才补一次截图，其余请求直接复用已有图片
        self.__add_screenshot_task_if_stale()
        # 获取UA
        user_agent = request.headers.get('user-agent', 'Unknown User-Agent')
        key = self.__detect_device_type(user_agent=user_agent).lower()
        # 这里实际上返回的是上一次的图片信息
        image = self.__get_latest_image(key=key)
        if not image:
            return None
        return Response(content=image.read_bytes(), media_type="image/jpeg")

    def __get_weather_base64_image(self, location: str, key: str = "mobile") -> Optional[str]:
        """获取base64图片"""
        if not location:
            logger.error("没有地址信息，获取天气图片失败")
            return None
        # 截图过期或主题需要切换时才补一次截图，其余请求直接复用已有图片
        self.__add_screenshot_task_if_stale()
        # 这里实际上返回的是上一次的图片信息
        image = self.__get_latest_image(key=key)
        if not image:
            return None
        # 读取图片文件并编码为 base64
        with open(image, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        return f"data:image/{image.suffix.replace('.', '')};base64,{encoded_string}"

    @staticmethod
    def __normalize_refresh_interval(value: Any) -> int:
        """把配置中的刷新周期规范为可选值之一，旧配置或非法值回落到默认周期。"""
        try:
            hours = int(value)
        except (TypeError, ValueError):
            return DEFAULT_REFRESH_INTERVAL
        if hours == REFRESH_DISABLED or hours in REFRESH_INTERVAL_HOURS:
            return hours
        return DEFAULT_REFRESH_INTERVAL

    def __is_refresh_enabled(self) -> bool:
        """是否需要维护天气截图；选择不刷新时插件只负责天气通知。"""
        return self._enabled and self._refresh_interval != REFRESH_DISABLED

    def __add_screenshot_task_if_stale(self):
        """
        仪表盘或图片接口读取截图时调用，只在需要时补一次截图。

        没有截图、最新截图已超过刷新周期，或按缓存的日出日落时间判断明暗主题
        已经变化时才添加任务，避免仪表盘每次加载都启动浏览器。
        """
        if not self.__is_refresh_enabled():
            return
        images = list(self.__get_images_path().glob(f"{self.__get_screenshot_image_pre_path()}_*.png"))
        if not images:
            self.__add_screenshot_task()
            return
        latest_mtime = max(image.stat().st_mtime for image in images)
        age_seconds = datetime.now().timestamp() - latest_mtime
        if age_seconds >= self._refresh_interval * 3600:
            self.__add_screenshot_task()
            return
        if self._auto_theme_enabled and self._sunrise_time and self._sunset_time:
            expected_dark = self.__is_dark_at(
                sunrise_time=self._sunrise_time,
                sunset_time=self._sunset_time,
                current_time=datetime.now(tz=pytz.timezone(settings.TZ)).time(),
            )
            if expected_dark != bool(self._use_dark_mode):
                logger.info("明暗主题已变化，重新截图")
                self.__add_screenshot_task()

    def __add_screenshot_task(self):
        """添加截图任务"""
        if not self.__is_refresh_enabled():
            return

        if not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if len(self._scheduler.get_jobs()):
            logger.info("已经存在待执行的截图任务，清空任务并继续添加")
            self._scheduler.remove_all_jobs()

        self._scheduler.add_job(
            func=self.__take_screenshots,
            trigger="date",
            run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
            name="获取一次天气信息",
        )
        logger.info("已添加截图任务，等待执行")
        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
        if not self._scheduler.running:
            self._scheduler.start()

    def __update_with_log_screenshot_time(self, current_time: Optional[datetime]):
        """更新截图时间"""
        self._last_screenshot_time = current_time
        if current_time:
            logger.info(
                f"截图记录更新，最后一次截图时间重置为 {self._last_screenshot_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logger.info(f"截图记录更新，发生错误，最后一次截图时间重置为None")

    def __take_screenshots(self):
        """管理多设备截图任务"""
        if not self.__is_refresh_enabled():
            return
        self.__get_images_path().mkdir(parents=True, exist_ok=True)
        current_time = datetime.now(tz=pytz.timezone(settings.TZ))
        if self._last_screenshot_time and self.__check_image():
            time_since_last = (current_time - self._last_screenshot_time).total_seconds()
            time_to_wait = self._min_screenshot_span - time_since_last
            if time_since_last < self._min_screenshot_span:
                logger.info(f"截图过快，最小截图间隔为 {self._min_screenshot_span} 秒。请在 {time_to_wait:.2f} 秒后重试。")
                return

        self.__update_with_log_screenshot_time(current_time=current_time)

        if not self._weather_url:
            logger.error("无法获取天气请求地址，请检查配置")
            self.__update_with_log_screenshot_time(current_time=None)
            return

        try:
            start_time = datetime.now()
            self.__update_with_log_screenshot_time(current_time=current_time)
            screenshot_devices = self.__get_screenshot_device()
            if not screenshot_devices:
                logger.error("获取截图设备失败，请检查")
                self.__update_with_log_screenshot_time(current_time=None)
                return
            self._use_dark_mode = self.__should_use_dark_mode()
            color_scheme = "dark" if self._use_dark_mode else "light"
            self.__take_screenshots_by_browser(screenshot_devices=screenshot_devices,
                                               color_scheme=color_scheme,
                                               start_time=start_time)
        except Exception as e:
            logger.error(f"take_screenshots failed: {str(e)}")

    def __take_screenshots_by_browser(self,
                                      screenshot_devices: dict,
                                      color_scheme: str,
                                      start_time: datetime) -> None:
        """
        使用宿主浏览器 SDK 为不同设备生成天气截图。

        一轮截图只启动一个浏览器：第一个设备通过 SDK 启动浏览器上下文，其余设备在同一
        浏览器上各开一个上下文，设备参数（UA、视口、缩放）都按上下文隔离。浏览器进程会
        把约 300MB 的程序文件读进页缓存，并计入 MoviePilot 容器的内存占用，因此关闭后
        主动释放这些文件的缓存。
        """
        logger.info("正在准备截图服务，宿主浏览器内核启动中")
        known_pids = self.__get_child_pids()
        primary_context = None
        # 额外上下文需要先于主上下文关闭：主上下文关闭时会连带关闭整个浏览器
        extra_contexts = []
        browser_procs = []
        mapped_files = set()
        try:
            for key, device in screenshot_devices.items():
                try:
                    logger.info(f'{key} 正在启动浏览器截图 ...')
                    context_options = self.__get_browser_context_options(device=device, color_scheme=color_scheme)
                    if primary_context is None:
                        primary_context = self.__launch_browser_context(device=device, color_scheme=color_scheme)
                        context = primary_context
                    elif primary_context.browser:
                        context = primary_context.browser.new_context(**context_options)
                        extra_contexts.append(context)
                    else:
                        # 持久化上下文没有独立的 Browser 对象，只能为该设备另起一个浏览器
                        context = self.__launch_browser_context(device=device, color_scheme=color_scheme)
                        extra_contexts.append(context)
                    self.__screenshot_element_by_browser(key=key, device=device, context=context)
                    elapsed_time = datetime.now() - start_time
                    logger.info(f'运行完毕，用时 {elapsed_time.total_seconds()} 秒')
                except Exception as e:
                    logger.error(f"{key} 浏览器截图失败: {str(e)}")
            # 必须在浏览器仍在运行时收集进程和映射文件：渲染进程只在打开页面后才出现，
            # 而进程退出后就无法再读取它的内存映射
            browser_procs = self.__get_new_child_procs(known_pids=known_pids)
            mapped_files = self.__collect_mapped_files(browser_procs=browser_procs)
        finally:
            for context in reversed(extra_contexts):
                try:
                    context.close()
                except Exception as e:
                    logger.warning(f"关闭浏览器上下文失败: {e}")
            if primary_context:
                try:
                    primary_context.close()
                except Exception as e:
                    logger.warning(f"关闭浏览器失败: {e}")
            self.__release_browser_file_cache(browser_procs=browser_procs, mapped_files=mapped_files)

    @staticmethod
    def __get_child_pids() -> set:
        """返回当前进程所有后代进程的 PID，用于识别本轮截图新启动的浏览器进程。"""
        try:
            return {proc.pid for proc in psutil.Process().children(recursive=True)}
        except psutil.Error:
            return set()

    @staticmethod
    def __get_new_child_procs(known_pids: set) -> list:
        """返回截图开始后新出现的后代进程，包括 Playwright 驱动和浏览器的各个子进程。"""
        try:
            return [proc for proc in psutil.Process().children(recursive=True) if proc.pid not in known_pids]
        except psutil.Error:
            return []

    @staticmethod
    def __collect_mapped_files(browser_procs: list) -> set:
        """收集浏览器进程映射的真实文件，包括浏览器主程序、资源包、动态库和字体。"""
        mapped_files = set()
        for proc in browser_procs:
            try:
                for mapping in proc.memory_maps(grouped=True):
                    if mapping.path.startswith("/") and not mapping.path.endswith("(deleted)"):
                        mapped_files.add(mapping.path)
            except psutil.Error:
                continue
        return mapped_files

    @staticmethod
    def __release_browser_file_cache(browser_procs: list, mapped_files: set) -> None:
        """
        浏览器退出后释放它映射过的文件的页缓存。

        等待浏览器进程退出后，对其映射过的文件逐个调用 posix_fadvise(DONTNEED)。内核
        不会释放仍被其他进程映射的页，所以同时运行的其他浏览器或共享的系统库不受影响。
        缓存本身可回收，释放失败只影响内存显示，不影响截图结果，因此所有异常都只记录不抛出。
        """
        if not mapped_files:
            return
        # posix_fadvise 是平台可选接口，macOS、Windows 上不存在，此时跳过释放。
        advise = getattr(os, "posix_fadvise", None)
        dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)
        if advise is None or dontneed is None:
            return
        psutil.wait_procs(browser_procs, timeout=BROWSER_EXIT_TIMEOUT)
        released = 0
        for path in mapped_files:
            try:
                fd = os.open(path, os.O_RDONLY)
            except OSError:
                continue
            try:
                advise(fd, 0, 0, dontneed)
                released += 1
            except OSError:
                pass
            finally:
                os.close(fd)
        logger.info(f"已释放浏览器文件缓存，共 {released} 个文件")

    @staticmethod
    def __get_browser_context_options(device: dict, color_scheme: str) -> dict:
        """生成宿主浏览器上下文参数。"""
        context_options = SCREENSHOT_DEVICE_CONTEXTS.get(device.get("device"), {}).copy()
        viewport = device.get("size") or context_options.get("viewport")
        if viewport:
            context_options["viewport"] = viewport
        context_options["color_scheme"] = color_scheme
        return context_options

    @staticmethod
    def __launch_browser_context(device: dict, color_scheme: str):
        """启动用于天气截图的宿主浏览器上下文。"""
        return launch_browser_context(
            headless=True,
            proxy=settings.PROXY_SERVER,
            humanize=settings.CLOAKBROWSER_HUMANIZE,
            human_preset=settings.CLOAKBROWSER_HUMAN_PRESET,
            **WeatherWidget.__get_browser_context_options(
                device=device,
                color_scheme=color_scheme,
            ),
        )

    def __screenshot_element_by_browser(self, key: str, device: dict, context: Any) -> bool:
        """在调用方提供的浏览器上下文中执行单个截图任务，上下文的关闭由调用方负责。"""
        current_time = datetime.now(tz=pytz.timezone(settings.TZ))
        timestamp = current_time.strftime("%Y%m%d%H%M%S")
        selector = ".c-city-weather-current"
        image_path = self.__get_images_path() / f"{self.__get_screenshot_image_pre_path(key=key)}_{timestamp}.png"

        logger.info(f"开始加载 {key} 页面: {self._weather_url}")
        self.__update_with_log_screenshot_time(current_time=current_time)
        page = None
        try:
            page = context.new_page()
            size = device.get("size")
            if size:
                page.set_viewport_size(device.get("size"))
            page.goto(self._weather_url)
            page.wait_for_selector(selector, timeout=self._screenshot_timeout * 1000)
            self.__reset_weather_style(page=page)
            self.__reset_page_style(page=page, key=key)
            logger.info(f"{key} 页面加载成功，标题: {page.title()}")
            self.__update_with_log_screenshot_time(current_time=datetime.now(tz=pytz.timezone(settings.TZ)))
            element = page.query_selector(selector)
            if element:
                box = element.bounding_box()
                if box:
                    # 裁剪区域向内收缩 2px，避免外层阴影或圆角边缘进入仪表盘图片。
                    clip = {
                        "x": box["x"] + 2,
                        "y": box["y"] + 2,
                        "width": box["width"] - 4,
                        "height": box["height"] - 4
                    }
                    page.screenshot(path=image_path, clip=clip)
                    logger.info(f"{key} 截图成功，截图路径: {image_path}")
                else:
                    element.screenshot(path=image_path)
                    logger.info(f"{key} 截图成功，截图路径: {image_path}")
                self.__manage_images(key=key)
                self.__update_with_log_screenshot_time(current_time=datetime.now(tz=pytz.timezone(settings.TZ)))
                return True
            else:
                logger.warning(f"{key} 未找到指定的选择器: {selector}")
                self.__update_with_log_screenshot_time(current_time=None)
                return False
        except Exception as e:
            logger.error(f"{key} 截图失败，URL: {self._weather_url}, 错误：{e}")
            self.__update_with_log_screenshot_time(current_time=None)
            return False
        finally:
            if page:
                page.close()

    def __reset_page_style(self, page: Any, key: str = 'mobile'):
        """重置页面样式"""

        # 无边框模式时，调整右上角的样式显示效果
        if not self._border:
            # 重置时间为空
            page.evaluate("""() => {
                        const element = document.querySelector('.current-time');
                        if (element) {
                            element.style.display = 'block';
                            element.style.height = '16px';
                            element.textContent  = '';
                        }
                    }""")

            # 移除空气质量
            page.evaluate("""() => {
                    const element = document.querySelector('.current-live .current-live__item > .air-tag');
                    if (element) {
                        element.remove();
                    }
                }""")

        # 修改天气边框圆角为直角
        page.evaluate("""() => {
                    const element = document.querySelector('.c-city-weather-current');
                    if (element) {
                        element.style.borderRadius = '0';
                    }
                }""")

        # 显示边框时，不需要重置其他样式
        if self._border:
            return

        # # 修改天气背景Padding、Position
        # page.evaluate("""() => {
        #         const element = document.querySelector('.current-weather__bg');
        #         if (element) {
        #             element.style.paddingTop = '40px';
        #             element.style.position = 'relative';
        #         }
        #     }""")

        # 修改时间位置
        # page.evaluate("""() => {
        #         const element = document.querySelector('.current-time');
        #         if (element) {
        #             element.style.position = 'absolute';
        #             element.style.right = '28px';
        #             element.style.top = '10px';
        #         }
        #     }""")

        # 修改空气质量位置

        # page.evaluate("""(key) => {
        #         const element = document.querySelector('.current-live .current-live__item > .air-tag');
        #         if (element) {
        #             element.style.top = key === 'mobile' ? '-36px' : '-41px';
        #             //element.style.right = '28px';
        #         }
        #     }""", key)

    def __reset_weather_style(self, page: Any):
        """重置天气样式"""
        # 获取指定元素
        weather_element = page.query_selector('.c-city-weather-current')

        # 获取元素的 CSS 样式
        # css_background = weather_element.evaluate("el => getComputedStyle(el).background")
        css_background_image = weather_element.evaluate("el => getComputedStyle(el).backgroundImage")

        # if css_background:
        #     self._weather_background = css_background
        if css_background_image:
            self._weather_background = css_background_image
        else:
            self._weather_background = "linear-gradient(225deg, #fee5ca, #e9f0ff 55%, #dce3fb)"

        # 尝试获取页面中的时间，如果不存在，则使用当前时间
        current_time_from_page = page.evaluate(
            "() => document.querySelector('.current-time') ? document.querySelector('.current-time').textContent : ''")

        if current_time_from_page:
            self._weather_current_time = current_time_from_page
        else:
            # 获取当前时间并格式化为 'YYYY-MM-DD HH:MM' 格式
            self._weather_current_time = datetime.now(tz=pytz.timezone(settings.TZ)).strftime('%Y-%m-%d %H:%M')

        # 尝试获取页面中的空气质量标签
        air_tag_from_page = page.evaluate("""() => {
                                        const element = document.querySelector('.current-live .current-live__item > .air-tag');
                                        if (element) {
                                            return element ? element.textContent : '';
                                        }
                                    }""")

        # 尝试获取页面中的空气质量标签和背景色
        air_tag_details = page.evaluate("""
                () => {
                    const element = document.querySelector('.current-live .current-live__item > .air-tag');
                    if (element) {
                        return {
                            text: element.textContent.trim(),
                            backgroundColor: window.getComputedStyle(element, null).getPropertyValue('background-color')
                        };
                    } else {
                        return { text: '', backgroundColor: '' };
                    }
                }
            """)

        if air_tag_details:
            self._weather_air_tag = f" {air_tag_details.get('text', 'AQI 优')} "
            self._weather_air_tag_background = air_tag_details.get("backgroundColor", "#95B359")
        else:
            # 如果没有找到空气质量标签或背景色，使用默认值
            self._weather_air_tag = " AQI 优 "
            self._weather_air_tag_background = "#95B359"  # 默认背景颜色

        self.__update_config()
        logger.info(f"更新天气背景样式为：{self._weather_background}")
        logger.info(f"更新当前时间为：{self._weather_current_time}")
        logger.info(f"更新空气质量为：{self._weather_air_tag}")
        logger.info(f"更新空气质量背景色为：{self._weather_air_tag_background}")

    def __manage_images(self, key: str, max_files: int = 5):
        """管理图片文件，确保每种类型最多保留 max_files 张"""
        files = sorted(self.__get_images_path().glob(f"{self.__get_screenshot_image_pre_path(key=key)}_*.png"),
                       key=lambda x: x.stat().st_mtime)
        if len(files) > max_files:
            for file in files[:-max_files]:
                file.unlink()
                logger.info(f"删除旧图片: {file}")

    def __check_image(self) -> bool:
        """判断是否存在图片"""
        files_exist = any(self.__get_images_path().glob(f"{self.__get_screenshot_image_pre_path()}_*.png"))
        if not files_exist:
            logger.error("没有找到图片信息")
        return files_exist

    def __get_latest_image(self, key: str) -> Optional[Path]:
        """获取指定key的最新图片路径"""
        # 搜索所有匹配的图片文件，并按修改时间排序
        try:
            image = max(self.__get_images_path().glob(f"{self.__get_screenshot_image_pre_path(key=key)}_*.png"),
                        key=lambda x: x.stat().st_mtime)
            if not image:
                return None
            if not image.exists():
                return None
            if not image.is_file():
                return None
            # 判断是否图片文件
            if image.suffix.lower() not in [".jpg", ".png", ".gif", ".bmp", ".jpeg", ".webp"]:
                return None
            return image
        except ValueError:
            logger.error(f"{key}: 没有找到图片信息")
            return None

    def __get_weather_api_key(self) -> str:
        """获取天气api密钥"""
        return self._weather_api_key

    @staticmethod
    def __normalize_api_host(host: str) -> str:
        """归一化 API Host，返回可安全拼接的 https 基址。

        兼容用户填写裸域名、带协议前缀或带结尾斜杠的形式；空值回退公共 Host。
        """
        value = str(host or "").strip()
        if not value:
            return DEFAULT_QWEATHER_API_HOST
        if not value.startswith(("http://", "https://")):
            value = f"https://{value}"
        return value.rstrip("/")

    def __get_weather_api_host(self) -> str:
        """获取天气 API Host 基址"""
        return self._weather_api_host or DEFAULT_QWEATHER_API_HOST

    def __get_geo_api_host(self) -> str:
        """获取城市反查（geoapi）Host 基址。

        和风新版控制台的专属 Host 同时承担 geoapi 能力，因此与天气 API 使用同一 Host。
        """
        return self.__get_weather_api_host()

    def __get_weather_url(self) -> Optional[str]:
        """获取天气Url"""
        if not self._location:
            logger.error("没有配置城市，无法获取对应的城市天气链接")
            return None

        if self._location_url:
            return f"https://www.qweather.com/weather/{self._location_url}.html"

        location_map = self.get_data("location")
        if location_map:
            weather_url = location_map.get(self._location, {}).get("fxLink")
            if weather_url:
                return weather_url
        else:
            location_map = {}

        api_key = self.__get_weather_api_key()
        if not api_key:
            logger.error("未配置和风天气 API Key，无法查询城市链接")
            return None

        # 新版控制台的专属 Host 同时提供城市反查能力，优先使用配置的 Host，
        # 未配置时退回公共 geoapi Host，以兼容旧版 Key。
        if self._weather_api_host and self._weather_api_host != DEFAULT_QWEATHER_API_HOST:
            geo_host = self.__get_geo_api_host()
        else:
            geo_host = DEFAULT_QWEATHER_GEO_HOST

        url = (f"{geo_host}/geo/v2/city/lookup?"
               f"key={api_key}&location={self._location}&lang=zh")

        response = RequestUtils().get_res(url)
        # URL 仅用于请求，不写入日志，避免第三方 API 密钥进入日志系统。
        logger.info("请求和风天气获取城市详情信息：%s", self._location)
        if not response:
            logger.error("连接和风天气失败，未获取到响应信息")
            return None
        logger.info(f"响应信息: {response.text}")

        if response.status_code != 200:
            logger.error(f"连接和风天气失败, 状态码: {response.status_code}")
            return None
        else:
            data = response.json()
            if data.get('code') == "200":
                remote_locations = data.get('location', [])
                if remote_locations:
                    first = remote_locations[0]
                    weather_url = first.get("fxLink")
                    logger.info(f"城市: {self._location} 获取到对应的城市天气链接为: {weather_url}")
                    if weather_url:
                        location_map[self._location] = first
                        self.__save_data(location_map)
                        return weather_url

        logger.error(f"连接和风天气成功, 但获取详情信息失败")
        return None

    def __save_data(self, data: dict):
        """保存插件数据"""
        self.save_data("location", data)

    def __clear_image(self):
        """清理缓存图片"""
        image_path = self.__get_images_path()
        if image_path.exists():
            # 删除目录及其所有内容
            shutil.rmtree(image_path)
            logger.info(f"目录 {image_path} 已清理")
        else:
            logger.info(f"目录 {image_path} 不存在")

    @staticmethod
    def __detect_device_type(user_agent: Optional[str]) -> str:
        """根据UA获取设备类型"""
        if not user_agent:
            logger.info(f"无法获取UA，当前访问设备类型默认为 desktop")
            return "desktop"

        logger.info(f"detect_device_type user_agent: {user_agent}")
        # 定义移动设备的关键字列表
        mobile_keywords = ['Mobile', 'Android', 'iPhone', 'iPad']

        # 检查UA中是否包含移动设备的关键字
        for keyword in mobile_keywords:
            if keyword in user_agent:
                logger.info(f"当前访问设备类型为 mobile")
                return 'mobile'

        logger.info(f"当前访问设备类型为 desktop")
        return 'desktop'

    def __get_screenshot_type(self):
        """获取截图类型"""
        return "border" if self._border else "default"

    def __get_images_path(self) -> Path:
        """返回当前插件实例的截图缓存目录，确保分身之间互不共享文件。"""
        return self.get_data_path() / "images"

    def __get_screenshot_device(self) -> Optional[dict]:
        """获取截图设备信息"""
        screenshot_type = self._screenshot_type
        screenshot_devices = SCREENSHOT_DEVICES.get(screenshot_type)
        return screenshot_devices

    def __get_screenshot_image_pre_path(self, key: str = None) -> str:
        """获取截图前置路径"""
        image_path = f"weather_{self._location}_{self._screenshot_type}"
        return f"{image_path}_{key}" if key else image_path

    def __should_use_dark_mode(self):
        """是否启用暗黑模式"""
        if not self._auto_theme_enabled:
            return False

        try:
            response = RequestUtils(timeout=10).get_res(self._weather_url)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"请求天气信息失败: {e}")
            return None

        try:
            # 正则表达式用于找到日出和日落时间
            sunrise_pattern = r'"rise":"(\d{2}:\d{2})"'
            sunset_pattern = r'"set":"(\d{2}:\d{2})"'

            sunrise_match = re.search(sunrise_pattern, response.text)
            sunset_match = re.search(sunset_pattern, response.text)

            if sunrise_match and sunset_match:
                sunrise_time_str = sunrise_match.group(1)
                sunset_time_str = sunset_match.group(1)

                # 转换时间字符串为datetime.time对象
                sunrise_time = datetime.strptime(sunrise_time_str, '%H:%M').time()
                sunset_time = datetime.strptime(sunset_time_str, '%H:%M').time()
                self._sunrise_time = sunrise_time
                self._sunset_time = sunset_time

                # 获取当前时间（只关心时间，不关心日期）
                current_time = datetime.now(tz=pytz.timezone(settings.TZ)).time()
                return self.__is_dark_at(sunrise_time=sunrise_time,
                                         sunset_time=sunset_time,
                                         current_time=current_time)

            return False
        except re.error as e:
            logger.error(f"Regex error: {e}")
            return False
        except ValueError as e:
            logger.error(f"Date conversion error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    @staticmethod
    def __is_dark_at(sunrise_time, sunset_time, current_time) -> bool:
        """按日出日落时间判断给定时刻是否处于夜间。"""
        if sunrise_time < sunset_time:
            # 日出和日落在同一天
            return not (sunrise_time <= current_time <= sunset_time)
        # 跨天情况：日落发生在日出前（例如在极地地区）
        return not (sunset_time <= current_time <= sunrise_time)

    @eventmanager.register(EventType.PluginAction)
    def notify_weather(self, event: Event = None):
        """推送天气通知"""
        if not self._weather_notify:
            return

        if event:
            logger.info(f"event： {event}")
            event_data = event.event_data
            if not event_data or event_data.get("action") != "weather_notify":
                return

        weather_report = self.__resolve_weather()
        # 带上天气截图时，企业微信渠道会改用 news 图文卡片呈现
        # （上方截图、下方正文）；生成失败时仍按普通文本推送。
        cover = self.__resolve_notification_cover()
        self.post_message(
            mtype=NotificationType[self._weather_notify_type],
            title=self.__resolve_notification_title(),
            text=weather_report,
            image=cover,
        )
        return

    def __resolve_notification_title(self) -> str:
        """
        生成通知标题，形如「【实时天气】- 咸阳市」。

        配置的城市常带上级行政区前缀（如「陕西省咸阳市」），
        这里尽量只取末级地名作为标题城市。

        :return: 通知标题；未配置城市时退回通用标题
        """
        location = str(self._location or "").strip()
        if location:
            # 按常见行政区划后缀切分，取最后一段作为城市名
            city = re.split(r"省|自治区|市|地区|自治州|县|区", location)
            city = next((part for part in reversed(city) if part), "")
            if city:
                return f"【实时天气】- {city}市"
            return f"【实时天气】- {location}"
        return "【实时天气】"

    def __resolve_notification_cover(self) -> Optional[str]:
        """
        生成通知卡片使用的天气截图地址。

        先把最新截图发布到前端静态目录，再返回其公网文件 URL，
        供企业微信等外部服务免鉴权拉取。

        :return: 可公开访问的封面 URL；不可用时返回 None
        """
        return publish_weather_cover()



    def __resolve_weather(self) -> Optional[str]:
        """解析当前天气。

        优先使用官方 API（可配置专属 Host），失败时回退到网页抓取，
        保证任一数据源可用时都能推送出天气正文。
        """
        api_report = self.__resolve_weather_by_api()
        if api_report:
            return api_report

        logger.info("官方 API 未获取到天气，回退到网页抓取")
        return self.__resolve_weather_by_web()

    def __extract_location_id(self) -> Optional[str]:
        """从配置的城市链接或已缓存数据中解析和风城市 ID。

        城市链接形如 xianyang-101110200，其中末尾数字即城市 ID。
        """
        for source in (self._location_url,):
            if not source:
                continue
            match = re.search(r"(\d{6,})", str(source))
            if match:
                return match.group(1)
        location_map = self.get_data("location") or {}
        cached = location_map.get(self._location) or {}
        city_id = str(cached.get("id") or "").strip()
        return city_id or None

    def __request_qweather(self, path: str, params: dict, optional: bool = False) -> Optional[dict]:
        """请求和风天气官方 API 并校验业务状态码。

        :param path: 以 / 开头的接口路径，例如 /v7/weather/now
        :param params: 除 key 之外的查询参数
        :param optional: 是否为可选字段；为 True 时失败只记录调试日志，不影响主流程
        :return: code 为 200 时返回完整响应字典，否则返回 None
        """
        api_key = self.__get_weather_api_key()
        if not api_key:
            return None
        host = self.__get_weather_api_host()
        query = dict(params or {})
        query["key"] = api_key
        try:
            response = RequestUtils(timeout=10).get_res(f"{host}{path}", params=query)
            if not response or response.status_code != 200:
                # 可选字段（空气质量、紫外线等）未开通权限时不应打断主流程
                self.__log_qweather_failure(
                    optional,
                    f"和风天气接口请求失败：HTTP {getattr(response, 'status_code', '无响应')} {path}"
                )
                return None
            data = response.json()
        except Exception as err:
            self.__log_qweather_failure(optional, f"和风天气接口请求异常：{err} {path}")
            return None

        if str(data.get("code")) != "200":
            self.__log_qweather_failure(
                optional,
                f"和风天气接口返回业务错误：code={data.get('code')} {path}"
            )
            return None
        return data

    @staticmethod
    def __log_qweather_failure(optional: bool, message: str):
        """按接口是否可选选择日志级别，避免未开通权限的可选接口被误报为错误。"""
        if optional:
            logger.info(f"{message}（可选接口，已跳过该字段）")
            return
        logger.error(message)

    def __resolve_weather_by_api(self) -> Optional[str]:
        """通过官方 API 组装天气正文，格式与网页抓取保持一致。"""
        city_id = self.__extract_location_id()
        if not city_id:
            logger.info("未能解析出城市 ID，跳过官方 API 取天气")
            return None

        now_data = self.__request_qweather("/v7/weather/now", {"location": city_id})
        if not now_data:
            return None

        now = now_data.get("now") or {}
        report_parts = [
            f"{TIME_ICON}时间: {self.__format_api_time(now_data.get('updateTime'))}",
        ]
        if now.get("temp"):
            report_parts.append(f"{TEMP_ICON}温度: {now.get('temp')}°")
        if now.get("text"):
            report_parts.append(f"{WEATHER_ICON}天气状况: {now.get('text')}")

        air_text = self.__resolve_air_quality_by_api(city_id)
        if air_text:
            report_parts.append(f"{AIR_ICON}空气质量: {air_text}")

        if now.get("windDir") and now.get("windScale"):
            report_parts.append(f"{WIND_ICON}{now.get('windDir')}: {now.get('windScale')}级")
        if now.get("humidity"):
            report_parts.append(f"{HUMIDITY_ICON}相对湿度: {now.get('humidity')}%")
        if now.get("feelsLike"):
            report_parts.append(f"{FEELS_LIKE_ICON}体感温度: {now.get('feelsLike')}°")
        if now.get("vis"):
            report_parts.append(f"{VISIBILITY_ICON}能见度: {now.get('vis')}km")
        if now.get("precip") is not None:
            report_parts.append(f"{PRECIP_ICON}降水量: {now.get('precip')}mm")
        if now.get("pressure"):
            report_parts.append(f"{PRESSURE_ICON}大气压: {now.get('pressure')}hPa")

        # 紫外线不在实时接口中，单独取生活指数补全
        uv_text = self.__resolve_uv_by_api(city_id)
        if uv_text:
            report_parts.append(f"{UV_ICON}紫外线: {uv_text}")

        abstract = self.__resolve_forecast_abstract_by_api(city_id, now)
        if abstract:
            # 出行建议基于同一批实况/预报数据生成，关闭后不再追加；
            # 建议接在概述同一行，用分号连接，保持整体是一段话。
            advice = self.__resolve_travel_advice(now=now, daily=self._forecast_daily)
            if advice:
                report_parts.append(f"\n{ABSTRACT_INDENT}{abstract.rstrip('。')}；{advice}")
            else:
                report_parts.append(f"\n{ABSTRACT_INDENT}{abstract}")

        logger.info("已通过官方 API 获取到天气信息")
        return "\n".join(report_parts)

    @staticmethod
    def __format_api_time(update_time: Optional[str]) -> str:
        """把和风的 ISO 更新时间格式化为本地展示时间。"""
        raw = str(update_time or "").strip()
        if not raw:
            return datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M")
        try:
            # 形如 2026-10-24T09:37+08:00
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=pytz.timezone(settings.TZ))
            return parsed.astimezone(pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return raw.replace("T", " ")[:16]

    def __resolve_air_quality_by_api(self, city_id: str) -> Optional[str]:
        """通过官方空气质量接口获取描述，形如「AQI 轻度污染」。

        新版接口 /airquality/v1/current/{lat}/{lon} 按经纬度查询，
        需要先由城市 ID 反查坐标；旧接口 /v7/air/now 已被上游废弃。
        """
        coords = self.__resolve_city_coordinates(city_id)
        if not coords:
            return None
        lat, lon = coords
        data = self.__request_air_quality(f"/airquality/v1/current/{lat}/{lon}", optional=True)
        if not data:
            return None

        indexes = data.get("indexes") or []
        if not indexes:
            return None
        # 优先使用中国标准 cn-mee，缺失时退回首个指数
        chosen = next((item for item in indexes if str(item.get("code")) == "cn-mee"), indexes[0])
        category = str(chosen.get("category") or "").strip()
        display = str(chosen.get("aqiDisplay") or "").strip()
        if category and display:
            return f"AQI {display} {category}"
        return category or None

    def __resolve_city_coordinates(self, city_id: str) -> Optional[tuple]:
        """由城市 ID 反查经纬度，结果缓存在插件数据中以减少请求。

        :param city_id: 和风城市 ID，例如 101110200
        :return: (纬度, 经度) 字符串元组；失败返回 None
        """
        cache = self.get_data("city_coords") or {}
        cached = cache.get(city_id) if isinstance(cache, dict) else None
        if isinstance(cached, dict) and cached.get("lat") and cached.get("lon"):
            return str(cached["lat"]), str(cached["lon"])

        api_key = self.__get_weather_api_key()
        if not api_key:
            return None
        host = self.__get_geo_api_host()
        try:
            response = RequestUtils(timeout=10).get_res(
                f"{host}/geo/v2/city/lookup",
                params={"location": city_id, "key": api_key},
            )
            if not response or response.status_code != 200:
                logger.info("城市坐标反查失败，跳过空气质量字段")
                return None
            payload = response.json()
        except Exception as err:
            logger.info(f"城市坐标反查异常，跳过空气质量字段：{err}")
            return None

        locations = payload.get("location") or []
        if not locations:
            return None
        first = locations[0]
        lat, lon = first.get("lat"), first.get("lon")
        if not lat or not lon:
            return None
        # 新版接口要求小数不超过两位
        lat, lon = f"{float(lat):.2f}", f"{float(lon):.2f}"
        if not isinstance(cache, dict):
            cache = {}
        cache[city_id] = {"lat": lat, "lon": lon}
        self.save_data("city_coords", cache)
        return lat, lon

    def __request_air_quality(self, path: str, optional: bool = False) -> Optional[dict]:
        """请求新版空气质量接口并返回解析后的 JSON。

        :param path: 以 / 开头的接口路径
        :param optional: 失败时是否只记录提示日志
        :return: HTTP 200 时返回响应字典，否则返回 None
        """
        api_key = self.__get_weather_api_key()
        if not api_key:
            return None
        host = self.__get_weather_api_host()
        try:
            response = RequestUtils(timeout=10).get_res(f"{host}{path}", params={"key": api_key})
            if not response or response.status_code != 200:
                self.__log_qweather_failure(
                    optional,
                    f"空气质量接口请求失败：HTTP {getattr(response, 'status_code', '无响应')} {path}"
                )
                return None
            return response.json()
        except Exception as err:
            self.__log_qweather_failure(optional, f"空气质量接口请求异常：{err} {path}")
            return None

    def __resolve_uv_by_api(self, city_id: str) -> Optional[str]:
        """通过官方生活指数接口获取紫外线等级。"""
        data = self.__request_qweather("/v7/indices/1d", {"location": city_id, "type": "5"}, optional=True)
        if not data:
            return None
        for item in data.get("daily") or []:
            if str(item.get("type")) == "5":
                return str(item.get("category") or "").strip() or None
        return None

    def __resolve_forecast_abstract_by_api(self, city_id: str, now: dict) -> Optional[str]:
        """通过官方三日预报接口生成一段天气概述，并把当日预报缓存下来供出行建议复用。"""
        data = self.__request_qweather("/v7/weather/3d", {"location": city_id}, optional=True)
        if not data:
            return None
        daily = data.get("daily") or []
        if not daily:
            return None
        today = daily[0]
        # 缓存当日预报，避免出行建议再次请求同一接口
        self._forecast_daily = today
        text_day = str(today.get("textDay") or "").strip()
        text_night = str(today.get("textNight") or "").strip()
        temp_max = str(today.get("tempMax") or "").strip()
        temp_min = str(today.get("tempMin") or "").strip()
        if not (text_day or text_night):
            return None
        parts = [f"今天白天{text_day}"]
        if text_night:
            parts.append(f"夜晚{text_night}")
        if temp_min and temp_max:
            parts.append(f"气温{temp_min}~{temp_max}°")
        current_temp = str(now.get("temp") or "").strip()
        if current_temp:
            parts.append(f"现在{current_temp}°")
        return "，".join(parts) + "。"
    def __resolve_travel_advice(self, now: dict, daily: Optional[dict] = None) -> Optional[str]:
        """
        依据实况与当日预报生成一句出行建议。

        按优先级只给最关键的一条，避免出现一长串注意事项：
        降水 → 空气/能见度 → 大风 → 温差 → 紫外线 → 通用提示。

        :param now: 官方实况接口返回的 now 字段
        :param daily: 当日预报字段，可为空
        :return: 出行建议文本；关闭开关或无数据时返回 None
        """
        if not self._travel_advice:
            return None

        tips: List[str] = []
        text = str(now.get("text") or "").strip()
        precip = str(now.get("precip") or "").strip()
        wind_scale = str(now.get("windScale") or "").strip()
        humidity = str(now.get("humidity") or "").strip()
        vis = str(now.get("vis") or "").strip()

        # 降水：实况降水或天气描述含雨/雪
        has_precip = self.__to_float(precip) > 0 or any(
            word in text for word in ("雨", "雪", "雷", "冰雹")
        )
        if has_precip:
            tips.append("出门记得带伞，路面湿滑注意慢行。")

        # 空气质量与能见度：霾/雾/浮尘等影响呼吸与视线
        if any(word in text for word in ("霾", "浮尘", "沙尘")):
            tips.append("空气较差，敏感人群外出建议佩戴口罩。")
        elif "雾" in text or (vis and self.__to_float(vis) <= 5):
            tips.append("能见度偏低，驾车请减速并保持车距。")

        # 大风
        if wind_scale and self.__to_float(wind_scale) >= 6:
            tips.append("风力较大，注意远离广告牌和高空坠物。")

        # 昼夜温差
        temp_min = self.__to_float(str((daily or {}).get("tempMin") or "").strip())
        temp_max = self.__to_float(str((daily or {}).get("tempMax") or "").strip())
        if temp_min and temp_max and (temp_max - temp_min) >= 8:
            tips.append(f"昼夜温差约{temp_max - temp_min:.0f}°，早晚记得添衣。")

        # 湿度偏高时的体感提示
        if humidity and self.__to_float(humidity) >= 85 and not tips:
            tips.append("湿度较高，体感偏闷，注意适时补水。")

        if not tips:
            tips.append("天气总体适宜出行，注意适时增减衣物。")
        # 只保留最关键的一条，避免正文过长
        return tips[0]

    @staticmethod
    def __to_float(value: str) -> float:
        """把可能为数值字符串的内容安全转换为浮点数。

        :param value: 待转换的文本，例如「3」「0.0」
        :return: 转换结果；无法解析时返回 0.0
        """
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def __icon_for_label(label: str, content: str = "") -> str:
        """为网页抓取得到的天气字段标题匹配展示图标。

        风向标签随实际风向变化（如「东北风」「北风」），无法穷举，
        因此按「标题含风、内容以级结尾」判断并复用风力图标。

        :param label: 字段标题，例如「相对湿度」
        :param content: 字段内容，例如「3级」
        :return: 对应图标；未收录的字段返回空字符串
        """
        name = str(label or "").strip()
        value = str(content or "").strip()
        if name in BASIC_LABEL_ICONS:
            return BASIC_LABEL_ICONS[name]
        if name.endswith("风") and value.endswith("级"):
            return WIND_ICON
        return ""

    def __resolve_weather_by_web(self) -> Optional[str]:
        """通过抓取和风天气网页解析当前天气，作为官方 API 不可用时的回退。"""
        if not self._weather_url:
            # API 优先模式下可能没有提前构建网页地址，这里按需补齐。
            self._weather_url = self.__get_weather_url()
        if not self._weather_url:
            logger.error("没有可用的城市天气链接，无法抓取网页天气")
            return None
        try:
            response = RequestUtils(timeout=10).get_res(self._weather_url)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"请求天气信息失败: {e}")
            return None

        try:
            # 使用 BeautifulSoup 解析 HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            weather_report_parts = []

            # 提取当前时间
            current_time = soup.find("p", class_="current-time")
            if current_time:
                weather_report_parts.append(f"{TIME_ICON}时间: {current_time.text.strip()}")

            # 提取温度
            temperature = soup.find("div", class_="current-live__item")
            if temperature:
                temperature = temperature.find_next_sibling().p
                if temperature:
                    weather_report_parts.append(f"{TEMP_ICON}温度: {temperature.text.strip()}")

            # 提取天气状况
            weather_condition = soup.find("div", class_="current-live__item")
            if weather_condition:
                weather_condition = weather_condition.find_next_sibling().p.find_next_sibling()
                if weather_condition:
                    weather_report_parts.append(f"{WEATHER_ICON}天气状况: {weather_condition.text.strip()}")

            # 提取空气质量
            air_quality = soup.find("a", class_="air-tag")
            if air_quality:
                weather_report_parts.append(f"{AIR_ICON}空气质量: {air_quality.text.strip()}")

            # 提取基本天气数据
            basic_items = soup.find_all("div", class_="current-basic___item")
            # 收集已解析字段，供出行建议复用
            web_now: Dict[str, str] = {}
            for item in basic_items:
                # 提取每个项目的内容(p0)和标题(p1)
                p0 = item.find("p")
                p1 = p0.find_next_sibling("p") if p0 else None

                if p0 and p1:
                    content = p0.text.strip()
                    label = p1.text.strip()
                    weather_report_parts.append(
                        f"{self.__icon_for_label(label, content)}{label}: {content}"
                    )
                    # 风向标签随实际风向变化，统一按 windScale 记录风力等级
                    if label.endswith("风") and content.endswith("级"):
                        web_now["windScale"] = content.replace("级", "")
                    elif label == "相对湿度":
                        web_now["humidity"] = content.replace("%", "")
                    elif label == "能见度":
                        web_now["vis"] = content.replace("km", "")
                    elif label == "降水量":
                        web_now["precip"] = content.replace("mm", "")
            # 天气文字（雾/霾/雨等）用于判断出行影响
            if weather_condition:
                web_now["text"] = weather_condition.text.strip()

            # 提取天气汇总
            weather_abstract = soup.find("div", class_="current-abstract")
            if weather_abstract:
                abstract_text = weather_abstract.text.strip()
                # 出行建议复用上面已解析出的基本数据，关闭后不再追加；
                # 建议接在概述同一行，用分号连接，保持整体是一段话。
                advice = self.__resolve_travel_advice(now=web_now, daily=None)
                if advice:
                    weather_report_parts.append(
                        f"\n{ABSTRACT_INDENT}{abstract_text.rstrip('。')}；{advice}"
                    )
                else:
                    weather_report_parts.append(f"\n{ABSTRACT_INDENT}{abstract_text}")

            # 构建和返回最终的天气报告
            if not weather_report_parts:
                return "未能获取天气信息"
            return "\n".join(weather_report_parts)
        except Exception as e:
            logger.error(f"解析天气信息时出错: {e}")
            return None
