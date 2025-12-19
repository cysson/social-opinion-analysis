# # -*- coding: utf-8 -*-
# # Copyright (c) 2025 relakkes@gmail.com
# #
# # This file is part of MediaCrawler project.
# # Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/config/base_config.py
# # GitHub: https://github.com/NanmiCoder
# # Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
# #

# # 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# # 1. 不得用于任何商业用途。
# # 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# # 3. 不得进行大规模爬取或对平台造成运营干扰。
# # 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# # 5. 不得用于任何非法或不当的用途。
# #
# # 详细许可条款请参阅项目根目录下的LICENSE文件。
# # 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

# # 基础配置
# PLATFORM = "xhs"  # 平台，xhs | dy | ks | bili | wb | tieba | zhihu
# KEYWORDS = "编程副业,编程兼职"  # 关键词搜索配置，以英文逗号分隔
# LOGIN_TYPE = "qrcode"  # qrcode or phone or cookie
# COOKIES = ""
# CRAWLER_TYPE = (
#     "search"  # 爬取类型，search(关键词搜索) | detail(帖子详情)| creator(创作者主页数据)
# )
# # 是否开启 IP 代理
# ENABLE_IP_PROXY = False

# # 代理IP池数量
# IP_PROXY_POOL_COUNT = 2

# # 代理IP提供商名称
# IP_PROXY_PROVIDER_NAME = "kuaidaili"  # kuaidaili | wandouhttp

# # 设置为True不会打开浏览器（无头浏览器）
# # 设置False会打开一个浏览器
# # 小红书如果一直扫码登录不通过，打开浏览器手动过一下滑动验证码
# # 抖音如果一直提示失败，打开浏览器看下是否扫码登录之后出现了手机号验证，如果出现了手动过一下再试。
# HEADLESS = False

# # 是否保存登录状态
# SAVE_LOGIN_STATE = True

# # ==================== CDP (Chrome DevTools Protocol) 配置 ====================
# # 是否启用CDP模式 - 使用用户现有的Chrome/Edge浏览器进行爬取，提供更好的反检测能力
# # 启用后将自动检测并启动用户的Chrome/Edge浏览器，通过CDP协议进行控制
# # 这种方式使用真实的浏览器环境，包括用户的扩展、Cookie和设置，大大降低被检测的风险
# ENABLE_CDP_MODE = True

# # CDP调试端口，用于与浏览器通信
# # 如果端口被占用，系统会自动尝试下一个可用端口
# CDP_DEBUG_PORT = 9222

# # 自定义浏览器路径（可选）
# # 如果为空，系统会自动检测Chrome/Edge的安装路径
# # Windows示例: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
# # macOS示例: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
# CUSTOM_BROWSER_PATH = ""

# # CDP模式下是否启用无头模式
# # 注意：即使设置为True，某些反检测功能在无头模式下可能效果不佳
# CDP_HEADLESS = False

# # 浏览器启动超时时间（秒）
# BROWSER_LAUNCH_TIMEOUT = 60

# # 是否在程序结束时自动关闭浏览器
# # 设置为False可以保持浏览器运行，便于调试
# AUTO_CLOSE_BROWSER = True

# # 数据保存类型选项配置,支持五种类型：csv、db、json、sqlite、excel, 最好保存到DB，有排重的功能。
# SAVE_DATA_OPTION = "json"  # csv or db or json or sqlite or excel

# # 用户浏览器缓存的浏览器文件配置
# USER_DATA_DIR = "%s_user_data_dir"  # %s will be replaced by platform name

# # 爬取开始页数 默认从第一页开始
# START_PAGE = 1

# # 爬取视频/帖子的数量控制
# CRAWLER_MAX_NOTES_COUNT = 15

# # 并发爬虫数量控制
# MAX_CONCURRENCY_NUM = 1

# # 是否开启爬媒体模式（包含图片或视频资源），默认不开启爬媒体
# ENABLE_GET_MEIDAS = False

# # 是否开启爬评论模式, 默认开启爬评论
# ENABLE_GET_COMMENTS = True

# # 爬取一级评论的数量控制(单视频/帖子)
# CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = 10

# # 是否开启爬二级评论模式, 默认不开启爬二级评论
# # 老版本项目使用了 db, 则需参考 schema/tables.sql line 287 增加表字段
# ENABLE_GET_SUB_COMMENTS = False

# # 词云相关
# # 是否开启生成评论词云图
# ENABLE_GET_WORDCLOUD = False
# # 自定义词语及其分组
# # 添加规则：xx:yy 其中xx为自定义添加的词组，yy为将xx该词组分到的组名。
# CUSTOM_WORDS = {
#     "零几": "年份",  # 将“零几”识别为一个整体
#     "高频词": "专业术语",  # 示例自定义词
# }

# # 停用(禁用)词文件路径
# STOP_WORDS_FILE = "./docs/hit_stopwords.txt"

# # 中文字体文件路径
# FONT_PATH = "./docs/STZHONGS.TTF"

# # 爬取间隔时间
# CRAWLER_MAX_SLEEP_SEC = 2

# from .bilibili_config import *
# from .xhs_config import *
# from .dy_config import *
# from .ks_config import *
# from .weibo_config import *
# from .tieba_config import *
# from .zhihu_config import *
# -*- coding: utf-8 -*-
# Copyright (c) 2025
#
# MediaCrawler - Base Config (Course Assignment Version)
# For NON-COMMERCIAL LEARNING PURPOSE ONLY
#

# ==================== 基础配置 ====================

# 平台选择
# xhs（小红书）评论密集、情绪明显，最适合舆情分析
PLATFORM = "xhs"  # xhs | dy | ks | bili | wb | tieba | zhihu

# ==================== 舆情关键词配置 ====================
# 聚焦【AI + 就业焦虑 + 大学生舆情】，适合写分析与论证
KEYWORDS = (
    "AI裁员,"
    "人工智能取代人类,"
    "大学生就业焦虑,"
    "计算机专业就业,"
    "应届生失业,"
    # 正面关键词
    "AI就业机会,"
    "人工智能创业,"
    "技能提升,"
    "高薪职位,"
    "职业发展规划,"
    "AI行业前景,"
)

# 登录方式
LOGIN_TYPE = "qrcode"  # qrcode | phone | cookie
COOKIES = ""

# 爬取类型
CRAWLER_TYPE = "search"  # search | detail | creator

# ==================== 代理配置（作业可关闭） ====================
ENABLE_IP_PROXY = False
IP_PROXY_POOL_COUNT = 2
IP_PROXY_PROVIDER_NAME = "kuaidaili"  # kuaidaili | wandouhttp

# ==================== 浏览器配置 ====================

# Playwright 浏览器模式
HEADLESS = False  # 作业环境建议 False，便于人工处理验证码
SAVE_LOGIN_STATE = True

# ==================== CDP（强烈推荐开启） ====================
ENABLE_CDP_MODE = True
CDP_DEBUG_PORT = 9222

# 自定义浏览器路径（macOS / Windows 可留空自动检测）
CUSTOM_BROWSER_PATH = ""

# CDP 模式下是否无头
CDP_HEADLESS = False

# 浏览器启动超时时间（秒）
BROWSER_LAUNCH_TIMEOUT = 60

# 程序结束是否自动关闭浏览器
AUTO_CLOSE_BROWSER = True

# ==================== 数据保存配置 ====================
# 作业建议：
# 第一次：db（去重 + 清洗）
# 第二次：json
# 第三次：csv
SAVE_DATA_OPTION = "db"  # csv | db | json | sqlite | excel

# 浏览器用户数据目录
USER_DATA_DIR = "%s_user_data_dir"  # %s 会被平台名替换

# ==================== 爬取规模控制（关键） ====================

# 起始页
START_PAGE = 1

# 爬取帖子数量（舆情主体）
CRAWLER_MAX_NOTES_COUNT = 3000

# 并发数量（稳妥为主）
MAX_CONCURRENCY_NUM = 1

# 是否爬取媒体文件（作业不需要）
ENABLE_GET_MEIDAS = False

# ==================== 评论配置（舆情核心） ====================

# 是否开启评论爬取
ENABLE_GET_COMMENTS = True

# 单条帖子爬取的一级评论数量
CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = 20

# 是否开启二级评论（作业非必须）
ENABLE_GET_SUB_COMMENTS = False

# ==================== 词云与文本分析 ====================

# 是否生成词云图（建议后处理阶段再做）
ENABLE_GET_WORDCLOUD = False

# 自定义词组
CUSTOM_WORDS = {
    "人工智能": "技术",
    "AI裁员": "社会事件",
    "就业焦虑": "情绪",
    "互联网寒冬": "社会事件",
    "大厂裁员": "社会事件",
}

# 停用词表
STOP_WORDS_FILE = "./docs/hit_stopwords.txt"

# 中文字体（词云/可视化用）
FONT_PATH = "./docs/STZHONGS.TTF"

# ==================== 爬取节奏控制 ====================

# 单次请求最大休眠时间（秒）
CRAWLER_MAX_SLEEP_SEC = 3

# ==================== 平台子配置 ====================
from .bilibili_config import *
from .xhs_config import *
from .dy_config import *
from .ks_config import *
from .weibo_config import *
from .tieba_config import *
from .zhihu_config import *
