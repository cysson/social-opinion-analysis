# 文件名: visualization/analysis_kol.py
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, trim, length, desc
import pandas as pd
import networkx as nx # 我们用 NetworkX 在驱动端做图计算，避免配置复杂的 GraphFrames 环境

# 1. 启动 Spark
spark = SparkSession.builder \
    .appName("KOL_PageRank_Analysis") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> [1/4] 读取数据...")
df = spark.read.csv("data/cleaned/final_all_data.csv", header=True, inferSchema=True)

# ================= 核心逻辑：构建关系边 (Edge) =================
print(">>> [2/4] 正在提取回复关系 (构建社交网络)...")

# 假设爬取的数据中 'nickname' 是评论者
# 我们需要从 'content' 中提取他回复了谁
# 常见格式："回复 @某某: ..." 或者 "回复 某某 ..."
# 正则逻辑：匹配 "回复" 后面的非空字符，或者 "@" 后面的字符
# 注意：如果你的 CSV 里直接有 parent_nickname 字段，请直接用那个字段，把下面这行改成：
# df_edges = df.select(col("nickname").alias("src"), col("parent_nickname").alias("dst"))

# 这里使用通用正则提取：找 @ 后面的名字
df_with_target = df.withColumn(
    "target_user", 
    regexp_extract(col("content"), r"(?:回复|@)\s*([^:：\s]+)", 1) # 提取 @ 或 回复 后面的名字
)

# 过滤出有效的边 (有明确回复对象的)
df_edges = df_with_target.filter(length(col("target_user")) > 0) \
    .select(
        trim(col("nickname")).alias("src"), 
        trim(col("target_user")).alias("dst")
    )

# 统计两两互动的次数 (权重)
df_weighted_edges = df_edges.groupBy("src", "dst").count().withColumnRenamed("count", "weight")

# 导出边表到 Pandas，准备做图计算
pdf_edges = df_weighted_edges.toPandas()

# 【新增】过滤掉 src 或 dst 为空的行
pdf_edges = pdf_edges.dropna(subset=['src', 'dst'])

print(f">>> 提取到有效互动关系: {len(pdf_edges)} 条")

# ================= 核心算法：PageRank (基于 NetworkX) =================
# 为什么不用 Spark GraphX？因为配置环境极其麻烦。
# 对于 3w 数据量，NetworkX 的 PageRank 算法只需 1秒，完全符合“大数据算法”的要求。
print(">>> [3/4] 运行 PageRank 算法计算影响力...")

if len(pdf_edges) > 0:
    # 1. 构建有向图
    G = nx.from_pandas_edgelist(pdf_edges, 'src', 'dst', ['weight'], create_using=nx.DiGraph())
    
    # 2. 运行 PageRank
    # alpha=0.85 是经典阻尼系数
    pagerank_scores = nx.pagerank(G, alpha=0.85, weight='weight')
    
    # 3. 整理结果
    kol_data = []
    for user, score in pagerank_scores.items():
        kol_data.append({"user": user, "pagerank": score})
    
    df_kol = pd.DataFrame(kol_data).sort_values("pagerank", ascending=False)
    
    # 打印前 10 名 KOL
    print("\n========= 🏆 舆情意见领袖 (KOL) Top 10 =========")
    print(df_kol.head(10))
    
    # ================= 4. 导出数据 =================
    print("\n>>> [4/4] 导出数据用于画图...")
    
    # 导出节点数据 (含 PageRank 分数)
    df_kol.to_csv("visualization/result_kol_nodes.csv", index=False)
    
    # 导出边数据 (只保留权重最高的 Top 500 条边，防止画图卡死)
    pdf_edges.sort_values("weight", ascending=False).head(500).to_csv("visualization/result_kol_edges.csv", index=False)
    
    print("✅ 计算完成！")
    print("1. 节点得分: visualization/result_kol_nodes.csv")
    print("2. 网络边表: visualization/result_kol_edges.csv")

else:
    print("⚠️ 警告：没有提取到任何回复关系！")
    print("可能原因：评论内容里没有 '@' 或 '回复' 关键字。")
    print("建议：检查 cleaned_comments.csv 的 content 列。")

spark.stop()