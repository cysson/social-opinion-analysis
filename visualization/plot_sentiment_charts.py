# 文件名: visualization/plot_sentiment_charts.py
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import seaborn as sns
import os
from matplotlib import font_manager

# --- 配置中文 ---
font_path_local = './simhei.ttf' 
if os.path.exists(font_path_local):
    font_manager.fontManager.addfont(font_path_local)
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
else:
    print("⚠️ 警告：无中文字体，请检查 simhei.ttf")

output_dir = 'output_images'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# ================= 1. 画图 4-4：双轴演化趋势图 =================
print("正在绘制图 4-4：话题热度与情感双轴趋势图...")
trend_file = 'result_trend.csv'

if os.path.exists(trend_file):
    df_trend = pd.read_csv(trend_file)
    df_trend['date'] = pd.to_datetime(df_trend['date'])
    
    # 设置画布
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # --- 左轴：画柱状图 (评论量/热度) ---
    color = 'tab:blue'
    ax1.set_xlabel('日期', fontsize=12)
    ax1.set_ylabel('评论热度 (条/天)', color=color, fontsize=12)
    ax1.bar(df_trend['date'], df_trend['daily_count'], color=color, alpha=0.3, label='评论热度')
    ax1.tick_params(axis='y', labelcolor=color)

    # --- 右轴：画折线图 (情感得分) ---
    ax2 = ax1.twinx()  # 共享X轴
    color = 'tab:red'
    ax2.set_ylabel('平均情感得分 (Avg Score)', color=color, fontsize=12)
    # 画线，加点
    ax2.plot(df_trend['date'], df_trend['daily_sentiment'], color=color, marker='o', linewidth=2, label='情感倾向')
    ax2.tick_params(axis='y', labelcolor=color)
    
    # 添加一条 0 分基准线 (0以上偏正向，0以下偏负向)
    ax2.axhline(0, color='gray', linestyle='--', alpha=0.5)

    plt.title('“AI职业规划”话题热度与情感演变趋势 (双轴)', fontsize=16)
    fig.tight_layout()
    
    save_path = f'{output_dir}/analysis_trend_dual_axis.png'
    plt.savefig(save_path, dpi=300)
    print(f"✅ 趋势图已保存: {save_path}")
    plt.close()
else:
    print("❌ 未找到 result_trend.csv")

# ================= 2. 画图 4-3：地域热度与情感分布图 =================
# (这里用带有颜色映射的横向柱状图来代替地图，能清晰展示热度Top的省份和它们的情感)
print("正在绘制图 4-3：地域热度与情感分布图...")
geo_file = 'result_province.csv'

if os.path.exists(geo_file):
    df_geo = pd.read_csv(geo_file)
    
    # 取前 15 个热度最高的省份
    df_top = df_geo.head(15).sort_values(by='count', ascending=True) # 为了画图从上到下排，先升序

    # 创建颜色映射：情感分越高越红，越低越绿
    # 归一化情感得分到 0-1 之间
    norm = mcolors.Normalize(vmin=df_top['avg_sentiment'].min(), vmax=df_top['avg_sentiment'].max())
    cmap = cm.get_cmap('RdYlGn') # 红黄绿配色 (Red-Yellow-Green)
    
    plt.figure(figsize=(10, 8))
    
    # 画横向柱状图
    bars = plt.barh(df_top['location_province'], df_top['count'], color=cmap(norm(df_top['avg_sentiment'])))
    
    plt.xlabel('讨论热度 (评论数量)', fontsize=12)
    plt.title('Top 15 地区舆情热度与情感倾向 (颜色代表情感)', fontsize=16)
    
    # 添加颜色条 (Colorbar) 说明
    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    # cbar = plt.colorbar(sm)

    # ax=plt.gca() 的意思是：把颜色条画在“当前这张图 (Get Current Axes)”的旁边
    cbar = plt.colorbar(sm, ax=plt.gca())

    cbar.set_label('平均情感得分 (绿=负向, 红=正向)', rotation=270, labelpad=15)

    # 在柱子旁边标上具体数值
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 5, bar.get_y() + bar.get_height()/2, f'{int(width)}', va='center')

    save_path = f'{output_dir}/analysis_geo_heatmap.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"✅ 地域图已保存: {save_path}")
    plt.close()
else:
    print("❌ 未找到 result_province.csv")