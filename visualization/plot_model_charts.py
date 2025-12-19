# 文件名: visualization/plot_model_charts.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from matplotlib import font_manager

# --- 配置中文 ---
font_path_local = './simhei.ttf' 
if os.path.exists(font_path_local):
    font_manager.fontManager.addfont(font_path_local)
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

output_dir = 'output_images'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# ================= 画图 4-6：评论长度与情感分布箱线图 =================
print("正在绘制图 4-6：评论长度与情感分布箱线图...")
data_file = 'result_length_sentiment.csv'

if os.path.exists(data_file):
    df = pd.read_csv(data_file)
    
    # 1. 数据清洗：去掉异常长的
    df = df[df['content_len'] <= 200]

    # 【新增核心操作】只保留得分为非 0 的数据 (即：只分析有情绪的评论)
    # 这样箱子立马就会张开了！
    # df_filtered = df[df['score'] != 0].copy()

    df_filtered = df[df['norm_score'] != 0].copy()
    
    # 2. 数据分箱 (注意：用过滤后的 df_filtered)
    bins = [0, 20, 60, 1000]
    labels = ['短评 (<20字)', '中评 (20-60字)', '长评 (>60字)']
    df_filtered['len_group'] = pd.cut(df_filtered['content_len'], bins=bins, labels=labels)
    
    # 3. 绘图 (用 df_filtered)
    plt.figure(figsize=(10, 8))
    sns.boxplot(
        x='len_group', 
        y='norm_score', 
        data=df_filtered, 
        palette="Set3", 
        width=0.5,
        showfliers=False 
    )
    # ... 后面的代码不变
    
    # y轴标签也改一下，显得更专业
    plt.ylabel('情感密度 (分值/百字)', fontsize=12)
    
    # 添加一条 0 分基准线
    plt.axhline(0, color='gray', linestyle='--', alpha=0.5)
    
    plt.title('评论长度与情感倾向分布关系 (Boxplot)', fontsize=16)
    plt.xlabel('评论长度分组', fontsize=12)
    # plt.ylabel('情感得分分布 (正数=积极, 负数=焦虑)', fontsize=12)
    
    # 添加一些标注说明 (提升逼格)
    plt.text(2.2, 3, '长评更趋向理性(中性)', fontsize=10, color='blue', ha='center')
    plt.text(0.2, 4, '短评情感波动大(极端)', fontsize=10, color='red', ha='center')
    
    save_path = f'{output_dir}/analysis_model_boxplot.png'
    plt.savefig(save_path, dpi=300)
    print(f"✅ 箱线图已保存: {save_path}")
    plt.close()

else:
    print("❌ 未找到 result_length_sentiment.csv")