from bs4 import BeautifulSoup

def parse_thread_list(html):
    """解析帖子列表页，返回帖子标题和链接"""
    soup = BeautifulSoup(html, "lxml")
    threads = []
    for item in soup.select(".threadlist_lz"):
        title_tag = item.select_one(".threadlist_title a")
        if title_tag:
            threads.append({
                "title": title_tag.text.strip(),
                "link": "https://tieba.baidu.com" + title_tag["href"]
            })
    return threads

def parse_post(html):
    """解析帖子详情页，返回帖子内容列表"""
    soup = BeautifulSoup(html, "lxml")
    contents = []
    for div in soup.select(".d_post_content"):
        contents.append(div.text.strip())
    return contents
