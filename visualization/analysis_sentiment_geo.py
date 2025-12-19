# 文件名: visualization/analysis_sentiment_geo.py
import findspark
findspark.init()

from pyspark.sql.functions import col, to_date, count, avg, round, length # <--- 加上 length

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date, count, avg, round
from pyspark.sql.types import FloatType
import pandas as pd
import os

# ================= 1. 定义情感词典 (简易版) =================
# 你可以根据需要扩充这个列表，越全越准
POSITIVE_WORDS = set([
    '喜欢', '不错', '支持', '加油', '希望', '成功', '上岸', '机会', '发展', '利好', 
    '优秀', '进步', '好', '棒', '赞', '开心', '快乐', '推荐', '收获', '丰富', 
    '稳定', '高薪', '牛', '厉害', '期待', '拥有', '解决', '提升', '明确', '肯定'
])

NEGATIVE_WORDS = set([
    '焦虑', '担心', '害怕', '失业', '裁员', '迷茫', '垃圾', '恶心', '讨厌', '差', 
    '难', '累', '卷', '痛苦', '失望', '后悔', '完蛋', '离谱', '无语', '压力', 
    '甚至', '怀疑', '不行', '放弃', '淘汰', '危机', '没用', '骗子', '忽悠', '惨'
])

def calculate_sentiment(content):
    if not content:
        return 0.0
    score = 0
    # 简单匹配：出现一个词加减一分
    for word in POSITIVE_WORDS:
        if word in content:
            score += 1
    for word in NEGATIVE_WORDS:
        if word in content:
            score -= 1
    return float(score)

# ================= 2. Spark 初始化 =================
spark = SparkSession.builder \
    .appName("Sentiment_Geo_Analysis") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> [1/3] 读取数据...")
# 确保路径正确
df = spark.read.csv("data/cleaned/final_all_data.csv", header=True, inferSchema=True)

# 注册情感分析 UDF
sentiment_udf = udf(calculate_sentiment, FloatType())

print(">>> [2/3] 计算情感得分 (Sentiment Scoring)...")
# 新增一列 sentiment_score
df_scored = df.withColumn("sentiment_score", sentiment_udf(col("content")))

# 缓存一下，因为后面要做两个聚合
df_scored.cache()

# ================= 3. 核心计算：两个维度的聚合 =================

# --- 任务 A: 地域分布 (Map Data) ---
print(">>> 正在统计：各省份热度与情感均值...")

df_geo = df_scored.filter(col("location_province").isNotNull()) \
    .filter(col("location_province") != "未知") \
    .filter(col("location_province") != "") \
    .groupBy("location_province") \
    .agg(
        count("*").alias("count"), 
        round(avg("sentiment_score"), 2).alias("avg_sentiment")
    ) \
    .orderBy(col("count").desc())

# --- 任务 B: 时间趋势 (Trend Data) ---
print(">>> 正在统计：每日热度与情感趋势...")

# 【修复核心】：先过滤掉脏数据！
# 逻辑：有效的时间字符串长度至少要大于8 (例如 2023-1-1)，且包含 '-'
# 或者更严谨一点，只保留以 20 开头的行 (假设数据都是2000年以后的)
df_clean_trend = df_scored.filter(col("create_time").contains("-")) \
                          .filter(length(col("create_time")) > 8)

# 现在的转换就安全了
df_trend = df_clean_trend \
    .withColumn("date", to_date(col("create_time"))) \
    .filter(col("date").isNotNull()) \
    .groupBy("date") \
    .agg(
        count("*").alias("daily_count"),
        round(avg("sentiment_score"), 2).alias("daily_sentiment")
    ) \
    .orderBy("date")

# ================= 4. 导出结果 =================
print(">>> 正在导出数据...")

# 导出地域数据 -> result_province.csv
# 只需要前 30 个省份，避免太多杂乱数据
geo_pd = df_geo.limit(34).toPandas()
geo_pd.to_csv("visualization/result_province.csv", index=False)

# 导出趋势数据 -> result_trend.csv
trend_pd = df_trend.toPandas()
trend_pd.to_csv("visualization/result_trend.csv", index=False)

print("✅ 计算完成！结果已保存：")
print("1. visualization/result_province.csv")
print("2. visualization/result_trend.csv")

spark.stop()