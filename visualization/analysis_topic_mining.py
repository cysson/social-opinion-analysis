# 文件名: visualization/analysis_topic_mining.py
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, size
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import CountVectorizer, IDF, PCA # <--- 引入 PCA
from pyspark.ml.clustering import KMeans
import jieba
import pandas as pd
import os

# 1. 启动 Spark
spark = SparkSession.builder \
    .appName("Topic_Mining_PCA") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> [1/5] 读取数据...")
# 注意：这里假设你在 bigdata 根目录运行
df = spark.read.csv("data/cleaned/final_all_data.csv", header=True, inferSchema=True)
df = df.dropna(subset=["content"])

# 2. 中文分词 & 去停用词
print(">>> [2/5] 中文分词处理...")
# 简单的停用词表
stopwords = set(['的', '了', '是', '在', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去', '你', '会', '着', '没有', '看', '好', '自己', '这'])

def jieba_cut_filter(text):
    if not text: return []
    words = jieba.lcut(text)
    # 过滤掉 1. 停用词 2. 长度小于2的词 3. 纯数字
    return [w for w in words if w not in stopwords and len(w) > 1 and not w.isdigit()]

jieba_udf = udf(jieba_cut_filter, ArrayType(StringType()))
df_words = df.withColumn("words", jieba_udf(col("content")))
# 过滤掉分词后为空的行
df_words = df_words.filter(size(col("words")) > 0)

# 3. TF-IDF 向量化
print(">>> [3/5] TF-IDF 向量化...")
# 提取前 2000 个高频词
cv = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=2000, minDF=2.0)
cv_model = cv.fit(df_words)
featurizedData = cv_model.transform(df_words)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# 4. K-Means 聚类
print(">>> [4/5] K-Means 聚类 (K=4)...")
kmeans = KMeans().setK(4).setSeed(1).setFeaturesCol("features").setPredictionCol("cluster_id")
model = kmeans.fit(rescaledData)
predictions = model.transform(rescaledData)

# 5. PCA 降维 (这是画散点图的关键!)
print(">>> [5/5] PCA 降维 (将高维向量压缩到 2 维)...")
pca = PCA(k=2, inputCol="features", outputCol="pca_features")
pca_model = pca.fit(predictions)
result = pca_model.transform(predictions)

# 提取 PCA 结果 (x, y) 坐标
def extract_x(vec): return float(vec[0])
def extract_y(vec): return float(vec[1])

extract_x_udf = udf(extract_x, "float")
extract_y_udf = udf(extract_y, "float")

final_df = result.withColumn("x", extract_x_udf(col("pca_features"))) \
                 .withColumn("y", extract_y_udf(col("pca_features")))

# 导出数据用于画图
print(">>> 正在导出结果到 visualization/result_topics.csv ...")
# 只需要: cluster_id, x, y, content
output_data = final_df.select("cluster_id", "x", "y", "content").toPandas()
output_data.to_csv("visualization/result_topics.csv", index=False)

# 顺便把高频词也导出来画词云
vocab = cv_model.vocabulary
with open("visualization/result_vocab.txt", "w", encoding="utf-8") as f:
    f.write(" ".join(vocab))

print("✅ 计算完成！")
spark.stop()