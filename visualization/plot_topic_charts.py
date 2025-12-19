# 文件名: visualization/plot_topic_charts.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import os
import jieba
from matplotlib import font_manager # <--- 引入字体管理器
import glob

# ================= 配置中文显示 (WSL 核心修改) =================
# 1. 确认字体文件路径
font_path_local = './simhei.ttf' 

# 2. 强制 Matplotlib 加载这个字体文件
if os.path.exists(font_path_local):
    # 将当前目录的 simhei.ttf 添加到 matplotlib 的字体库中
    font_manager.fontManager.addfont(font_path_local)
    # 设置全局默认字体为 SimHei (黑体)
    plt.rcParams['font.sans-serif'] = ['SimHei']
    print(f"✅ 已成功加载字体文件: {font_path_local}")
else:
    print(f"⚠️ 警告: 未找到 {font_path_local}，中文可能会乱码！")

# 3. 解决负号显示为方块的问题
plt.rcParams['axes.unicode_minus'] = False 

# 配置输出目录
output_dir = 'output_images'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 定义统一的图例名称 (散点图和饼图共用)
# 请确保这里的描述和你想要表达的一致
CLUSTER_NAMES = {
    0: 'Topic A: 网友调侃/泛泛而谈', 
    1: 'Topic B: AI与人类未来/哲学', 
    2: 'Topic C: 就业焦虑/现状吐槽', 
    3: 'Topic D: 学生升学/职业规划' 
}

# ================= 1. 画图 4-2：聚类散点图 =================
print("\n正在绘制图 4-2：聚类散点图...")
data_path = 'result_topics.csv'

if os.path.exists(data_path):
    df = pd.read_csv(data_path)
    
    # 映射中文标签
    df['label'] = df['cluster_id'].map(CLUSTER_NAMES)

    plt.figure(figsize=(12, 10))
    
    # 使用 seaborn 画散点图
    sns.scatterplot(
        data=df, 
        x='x', 
        y='y', 
        hue='label', 
        palette='viridis', 
        alpha=0.6,
        s=40
    )
    
    plt.title('舆情观点聚类可视化 (PCA降维)', fontsize=16) # 中文标题
    plt.xlabel('特征维度 1 (PCA)', fontsize=12)           # 中文坐标
    plt.ylabel('特征维度 2 (PCA)', fontsize=12)
    plt.legend(title='讨论主题类别', loc='upper right')    # 中文图例
    
    save_path = f'{output_dir}/analysis_cluster_scatter.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"✅ 散点图已保存: {save_path}")
    plt.close()
else:
    print("❌ 找不到 result_topics.csv，请先运行 calculation 脚本")

# ================= 2. 画图 4-1：高频词云图 =================
print("\n正在绘制图 4-1：高频词云图...")
vocab_path = 'result_vocab.txt'

if os.path.exists(vocab_path):
    with open(vocab_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    # 词云配置
    wc = WordCloud(
        background_color='white',
        width=1000, 
        height=800,
        max_words=150,
        font_path=font_path_local if os.path.exists(font_path_local) else None, # 这里的字体也要传
        colormap='tab20'
    ).generate(text)
    
    plt.figure(figsize=(10, 8))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    plt.title('“AI职业规划”话题高频词云', fontsize=16)
    
    save_path = f'{output_dir}/analysis_wordcloud.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"✅ 词云图已保存: {save_path}")
    plt.close()
else:
    print("❌ 找不到 result_vocab.txt")

# ================= 3. 画图 4-3：聚类占比图 =================
print("\n正在绘制图 4-3：主题聚类占比饼图...")

# 优化：直接使用上面已经读取的 df (result_topics.csv)，不用再去读 result_kmeans_cluster.csv
# 这样你只需要运行 analysis_topic_mining.py 一个脚本就能画出所有图
if 'cluster_id' in df.columns:
    # 统计数量
    cluster_counts = df['cluster_id'].value_counts()
    
    # 生成带中文的标签
    labels = [CLUSTER_NAMES.get(i, f'Cluster {i}') for i in cluster_counts.index]
    
    plt.figure(figsize=(10, 8)) 
    plt.pie(
        cluster_counts, 
        labels=labels, 
        autopct='%1.1f%%', 
        startangle=140, 
        colors=sns.color_palette("pastel"),
        textprops={'fontsize': 12, 'fontproperties': font_manager.FontProperties(fname=font_path_local)}
    )
    plt.title('舆情讨论主题分布占比', fontsize=16)
    
    plt.savefig(f'{output_dir}/analysis_cluster_pie.png', dpi=300, bbox_inches='tight')
    print(f"✅ 饼图已保存: {output_dir}/analysis_cluster_pie.png")
    plt.close()
else:
    print("❌ 数据中缺少 cluster_id 字段，无法绘制饼图")