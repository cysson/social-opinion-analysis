import requests
import time
import random

HEADERS = {"User-Agent": "Mozilla/5.0"}

# def fetch(url, params=None):
#     """发起 GET 请求，返回页面 HTML"""
#     time.sleep(random.uniform(1, 2))  # 模拟访问延时
#     resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
#     resp.raise_for_status()
#     resp.encoding = "utf-8"
#     return resp.text
def fetch(url, params=None, retries=3):
    import requests
    import time, random
    HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://tieba.baidu.com/",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive"
    }

    for i in range(retries):
        try:
            time.sleep(random.uniform(3, 5))
            resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except requests.exceptions.HTTPError as e:
            print(f"HTTP错误: {e}, 重试 {i+1}/{retries}")
        except requests.exceptions.RequestException as e:
            print(f"请求异常: {e}, 重试 {i+1}/{retries}")
    return ""

