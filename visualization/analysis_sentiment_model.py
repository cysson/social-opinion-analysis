# 文件名: visualization/analysis_sentiment_model.py
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, length, when
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import jieba
import pandas as pd
import os

# ================= 1. 准备工作 =================
# 定义情感词典 (用于生成训练标签)
POSITIVE_WORDS = set(['喜欢', '不错', '支持', '加油', '希望', '成功', '上岸', '机会', '发展', '优秀', '好', '棒', '赞', '开心', '快乐'])
NEGATIVE_WORDS = set(['焦虑', '担心', '害怕', '失业', '裁员', '迷茫', '垃圾', '恶心', '讨厌', '差', '难', '累', '卷', '痛苦', '失望'])

# 计算情感得分 (用于分析)
def get_score(text):
    if not text: return 0.0
    score = 0
    for w in POSITIVE_WORDS:
        if w in text: score += 1
    for w in NEGATIVE_WORDS:
        if w in text: score -= 1
    return float(score)

# 分词函数 (用于机器学习)
def jieba_cut(text):
    if not text: return []
    return [w for w in jieba.cut(text) if len(w) > 1]

# ================= 2. 启动 Spark =================
spark = SparkSession.builder \
    .appName("Sentiment_NaiveBayes") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> [1/4] 读取数据...")
df = spark.read.csv("data/cleaned/final_all_data.csv", header=True, inferSchema=True)
df = df.dropna(subset=["content"])

# 注册 UDF
score_udf = udf(get_score, FloatType())
jieba_udf = udf(jieba_cut, ArrayType(StringType()))

# ================= 3. 特征工程 (Feature Engineering) =================
print(">>> [2/4] 数据预处理与特征构建...")

# 1. 计算长度 & 情感得分
df_processed = df.withColumn("content_len", length(col("content"))) \
                 .withColumn("score", score_udf(col("content"))) \
                 .withColumn("words", jieba_udf(col("content")))

# 2. 生成标签 (Label) 用于训练贝叶斯
# 规则：分 > 0 标为 1 (正向)，分 < 0 标为 0 (负向)，0分的不参与训练
df_labeled = df_processed.filter(col("score") != 0) \
    .withColumn("label", when(col("score") > 0, 1.0).otherwise(0.0))

# 3. TF-IDF 向量化 (机器学习的输入必须是数字向量)
# HashingTF 将文本转为频率向量
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2000)
featurizedData = hashingTF.transform(df_labeled)

# IDF 调整权重
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# ================= 4. 训练朴素贝叶斯模型 (Naive Bayes) =================
print(">>> [3/4] 训练 Naive Bayes 分类器...")

# 拆分训练集和测试集
(trainingData, testData) = rescaledData.randomSplit([0.8, 0.2], seed=1234)

# 训练模型
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
model = nb.fit(trainingData)

# 预测
predictions = model.transform(testData)

# 评估准确率
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"\n==========================================")
print(f"🤖 模型评估结果 (Model Evaluation):")
print(f"   算法: Naive Bayes (朴素贝叶斯)")
print(f"   准确率 (Accuracy): {accuracy:.2%}")
print(f"==========================================\n")

# ================= 5. 导出数据用于箱线图分析 =================
print(">>> [4/4] 导出数据用于可视化 (长度 vs 情感)...")

# 【修改处】计算归一化得分： (原始得分 / 评论长度) * 100
# 含义：每 100 个字的情感强度
export_df = df_processed.filter(col("content_len") > 0) \
    .withColumn("norm_score", (col("score") / col("content_len")) * 100) \
    .select("content_len", "norm_score") \
    .sample(fraction=0.5, seed=42) 

pdf_export = export_df.toPandas()

# 保存
pdf_export.to_csv("visualization/result_length_sentiment.csv", index=False)
print("✅ 结果已保存: visualization/result_length_sentiment.csv")

spark.stop()