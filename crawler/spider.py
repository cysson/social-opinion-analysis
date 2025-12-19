from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import csv

# -----------------------
# 配置浏览器
# -----------------------
chrome_options = Options()
chrome_options.add_argument("--headless")  # 无头模式
chrome_options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=chrome_options)

keyword = "人工智能"
pages = 5
output_csv = "tieba_posts_selenium.csv"

all_threads = []

for page in range(pages):
    pn = page * 50
    url = f"https://tieba.baidu.com/f?kw={keyword}&pn={pn}"
    print(f"正在爬取第 {page+1} 页: {url}")
    driver.get(url)
    time.sleep(2)  # 等待网页加载
    
    lis = driver.find_elements("css selector", "li.j_thread_list")
    for li in lis:
        try:
            title_ele = li.find_element("css selector", "a.j_th_tit")
            title = title_ele.text
            link = title_ele.get_attribute("href")
            all_threads.append((title, link))
        except:
            continue

driver.quit()

# -----------------------
# 保存 CSV
# -----------------------
with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["标题", "链接"])
    writer.writerows(all_threads)

print(f"总共抓取 {len(all_threads)} 条帖子")
print(f"数据已保存到 {output_csv}")
