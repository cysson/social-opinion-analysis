# analysis_kmeans.py
import os
import sys

# ================= 配置 Spark 环境 =================
# 自动寻找本地的 Spark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StringType, ArrayType

# 引入 MLlib 机器学习库
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer
from pyspark.ml.clustering import KMeans
import jieba
import pandas as pd

# 解决中文编码问题
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 1. 启动 Spark
spark = SparkSession.builder \
    .appName("Opinion_Cluster_KMeans") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # 减少刷屏日志

print(">>> Spark 启动成功！开始读取数据...")

# 2. 读取清洗后的数据
# 请确认你的路径是否正确
csv_path = "data/cleaned/final_all_data.csv" 
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# 过滤掉空内容
df = df.filter(df.content.isNotNull())

# ================= 核心步骤：中文分词 =================
print(">>> 正在进行中文分词 (Jieba)...")

# 定义一个 UDF (User Defined Function) 让 Spark 可以调用 jieba
def jieba_cut(text):
    if not text:
        return []
    # 使用精确模式分词，并过滤掉长度小于2的词（去掉 '的', '了' 这种噪音）
    words = [w for w in jieba.cut(text) if len(w) > 1]
    return words

# 注册 UDF
jieba_udf = udf(jieba_cut, ArrayType(StringType()))

# 应用分词：新增一列 'words'，里面是分好词的数组
# 例如: "我爱AI" -> ["我", "爱", "AI"]
df_words = df.withColumn("words", jieba_udf(col("content")))

# 缓存一下，因为后面要反复用
df_words.cache()

# ================= 核心步骤：TF-IDF 向量化 =================
print(">>> 正在进行 TF-IDF 向量化...")

# 1. 词频统计 (CountVectorizer)
# vocabSize=1000 表示只取出现频率最高的前1000个词，减少计算量
# minDF=5.0 表示如果一个词在少于5条评论里出现，就忽略它
cv = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=1000, minDF=5.0)
cv_model = cv.fit(df_words)
featurizedData = cv_model.transform(df_words)

# 2. 逆文档频率 (IDF) - 降低常见词（如“但是”、“因为”）的权重
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# ================= 核心步骤：K-Means 聚类 =================
print(">>> 正在训练 K-Means 模型...")

# 设置聚类数量 K=4 (对应你的论点：焦虑、薪资、技术、其他)
# setSeed(1) 保证每次跑出来的结果一样
kmeans = KMeans().setK(4).setSeed(1).setFeaturesCol("features").setPredictionCol("cluster_id")
model = kmeans.fit(rescaledData)

# 进行预测
predictions = model.transform(rescaledData)

print(">>> 聚类完成！查看部分结果：")
predictions.select("cluster_id", "content").show(10, truncate=30)

# ================= 结果分析：每个类都在说什么？ =================
print(">>> 正在分析每个聚类的主题关键词...")

# 获取词表 (索引 -> 单词)
vocab = cv_model.vocabulary

# 获取聚类中心点
centers = model.clusterCenters()

# 这是一个极其有用的函数：找出每个聚类中心权重最高的词
cluster_keywords = {}
for i, center in enumerate(centers):
    # 获取该中心点数值最大的前10个索引
    top_indices = center.argsort()[-10:][::-1]
    # 映射回单词
    top_words = [vocab[idx] for idx in top_indices]
    cluster_keywords[i] = top_words
    print(f"Cluster {i} (主题 {i}): {', '.join(top_words)}")

# ================= 导出数据用于画图 =================
print(">>> 正在导出数据用于可视化...")

# 1. 导出“聚类结果分布” (用于画散点图或饼图)
# 只要 cluster_id
plot_data = predictions.select("cluster_id").toPandas()
plot_data.to_csv("visualization/result_kmeans_cluster.csv", index=False)

# 2. 导出“每个类的关键词” (用于写报告分析)
# 把字典转成 DataFrame 保存
keywords_df = pd.DataFrame.from_dict(cluster_keywords, orient='index')
keywords_df.to_csv("visualization/result_kmeans_keywords.csv", header=False)

print("✅ 所有工作完成！")
print("1. 聚类结果已保存至: visualization/result_kmeans_cluster.csv")
print("2. 聚类关键词已保存至: visualization/result_kmeans_keywords.csv")

spark.stop()