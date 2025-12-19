# 社交舆情大数据分析系统

## 1. 项目简介
本项目通过网络爬虫采集社交媒体舆情数据，
利用 Hadoop / Spark 对多源异构数据进行分析，
实现舆情统计、情感分析和趋势挖掘。

## 2. 技术路线


## 3. 数据来源


## 4. 核心算法


## 5. 项目结构说明
```text
social-opinion-analysis/
│
├── README.md                  
├── requirements.txt           
├── .gitignore
│
├── docs/                       # 文档与报告
│
├── data/
│   ├── raw/                    # 原始爬取数据
│   ├── processed/              # 清洗后的数据
│   └── sample/                 # 小样本数据
│
├── crawler/                    # 爬虫
│
├── preprocess/                 # 数据清洗与预处理
│
├── analysis/                   # 大数据分析
│
├── visualization/              # 结果可视化

```



## 6. 运行方式
crawlwer: macos上conda环境运行，python版本3.11，爬取小红书关键词：

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

一键启动爬虫：
```bash
python main.py \
  --platform xhs \
  --type search \
  --lt qrcode \
  --save_data_option json
```
共爬取数据30000+，数据结构实例：

```json
    {
        "comment_id": "693198be000000001d025812",
        "create_time": 1764858046000,
        "ip_location": "河南",
        "note_id": "69316656000000000d03a013",
        "content": "十几年的屎山代码，别说AI，啥新功能都不建议往上加。",
        "user_id": "62b57b70000000001902b224",
        "nickname": "momo",
        "avatar": "https://sns-avatar-qc.xhscdn.com/avatar/1040g2jo31cjj3373g86g5ollfdo6dch45rf40co?imageView2/2/w/120/format/jpg",
        "sub_comment_count": "0",
        "pictures": "",
        "parent_comment_id": 0,
        "last_modify_ts": 1765911165184,
        "like_count": "11"
    },
```