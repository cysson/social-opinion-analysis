# 文件名: preprocess/merge_safe.py
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# 1. 启动 Spark
# 增加一些配置，允许读取比较脏的 CSV
spark = SparkSession.builder \
    .appName("Safe_Merge_Data") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> [1/4] 启动安全模式读取 (强制所有列为 String)...")

# 定义读取函数：强制不推断类型 (inferSchema=False)
# 这样 '123' 和 'https://...' 都会被当做字符串，绝对不会报错
def read_csv_safe(path):
    try:
        # quote="\"" 和 escape="\"" 处理评论里带逗号或引号的情况
        # multiLine=True 允许一条评论换行
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "false") \
                       .option("multiLine", "true") \
                       .option("quote", "\"") \
                       .option("escape", "\"") \
                       .csv(path)
        print(f"✅ 成功读取: {path} | 数据量: {df.count()}")
        return df
    except Exception as e:
        print(f"⚠️ 读取失败: {path}")
        # print(e) # 调试时可以打开
        return None

# 2. 读取文件
df_old = read_csv_safe("data/cleaned/old_data.csv")
df_new = read_csv_safe("data/cleaned/cleaned_comments.csv")

# 3. 合并
print(">>> [2/4] 正在合并...")
if df_old is not None and df_new is not None:
    # 自动对齐列名
    df_merged = df_old.unionByName(df_new, allowMissingColumns=True)
elif df_old is not None:
    df_merged = df_old
elif df_new is not None:
    df_merged = df_new
else:
    print("❌ 错误：没有找到任何 CSV 文件！请检查 data/cleaned/ 目录下是否有 old_data.csv 和 cleaned_comments.csv")
    exit()

print(f"   合并后原始数量: {df_merged.count()}")

# 4. 去重
print(">>> [3/4] 正在去重...")
if "content" in df_merged.columns:
    df_final = df_merged.dropDuplicates(["content"])
else:
    df_final = df_merged.dropDuplicates()

print(f"✅ 去重后最终数量: {df_final.count()}")

# 5. 保存
# 保存时也保留 header，且不做任何压缩，保证兼容性
output_path = "data/cleaned/final_all_data.csv"
print(f">>> [4/4] 正在保存到 {output_path} ...")

df_final.coalesce(1).write \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .mode("overwrite") \
    .csv(output_path)

print("\n🎉 合并成功！")
print("注意：因为我们强制使用了 String 类型读取，后续运行 analysis 脚本时，")
print("Spark 会自动尝试将 String 转回数字/日期。如果 analysis 报错，")
print("请检查 final_all_data.csv 里是否真的混入了奇怪的 URL。")

spark.stop()