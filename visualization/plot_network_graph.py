# 文件名: visualization/plot_network_graph.py
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import os
import re  # <--- 引入正则库用于过滤乱码
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

# ================= 画图 4-5：KOL 传播网络图 =================
print("正在绘制图 4-5：KOL传播节点关系网络图...")
nodes_file = 'result_kol_nodes.csv'
edges_file = 'result_kol_edges.csv'

# 定义一个函数：判断 ID 是否合法（必须包含汉字、字母或数字）
def is_valid_name(name):
    if not isinstance(name, str):
        return False
    name = name.strip()
    if len(name) < 1: # 去掉空字符串
        return False
    # 正则：匹配 中文、大小写字母、数字
    # 如果一个名字里连一个这就字符都没有（比如全是 Emoji），就丢弃
    if not re.search(r'[\u4e00-\u9fa5a-zA-Z0-9]', name):
        return False
    return True

if os.path.exists(nodes_file) and os.path.exists(edges_file):
    # 1. 读取数据
    df_nodes = pd.read_csv(nodes_file)
    df_edges = pd.read_csv(edges_file)
    
    # ========= 【新增清洗逻辑】 开始 =========
    print(f"清洗前节点数: {len(df_nodes)}, 边数: {len(df_edges)}")
    
    # 1. 去掉空值 (NaN)
    df_nodes = df_nodes.dropna(subset=['user'])
    df_edges = df_edges.dropna(subset=['src', 'dst'])
    
    # 2. 应用正则过滤 (去掉纯表情/乱码 ID)
    df_nodes = df_nodes[df_nodes['user'].apply(is_valid_name)]
    df_edges = df_edges[df_edges['src'].apply(is_valid_name) & df_edges['dst'].apply(is_valid_name)]
    
    print(f"清洗后节点数: {len(df_nodes)}, 边数: {len(df_edges)}")
    # ========= 【新增清洗逻辑】 结束 =========

    # --- 筛选核心网络 ---
    # 只画 PageRank 前 50 名的用户
    top_k = 50
    top_nodes = df_nodes.head(top_k)['user'].tolist()
    
    # 筛选出 与 Top 50 有关的边
    filtered_edges = df_edges[
        (df_edges['src'].isin(top_nodes)) | 
        (df_edges['dst'].isin(top_nodes))
    ].head(100) 
    
    # 重新构建图对象
    G = nx.from_pandas_edgelist(filtered_edges, 'src', 'dst', create_using=nx.DiGraph())
    
    # 准备节点大小
    node_sizes = []
    node_colors = []
    pr_dict = dict(zip(df_nodes['user'], df_nodes['pagerank']))
    
    # 遍历图中的节点（注意：图中可能包含不在 top_nodes 里的边缘节点）
    valid_nodes_in_graph = []
    
    for node in G.nodes():
        # 二次检查：防止清洗漏网之鱼进入图结构
        if not is_valid_name(node): 
            continue
            
        valid_nodes_in_graph.append(node)
        
        score = pr_dict.get(node, 0.001) 
        node_sizes.append(score * 80000) 
        
        if node in top_nodes[:5]:
            node_colors.append('#FF6B6B') 
        else:
            node_colors.append('#4ECDC4') 
            
    # 只画合法的子图 (SubGraph)
    G = G.subgraph(valid_nodes_in_graph)

    # 2. 开始画图
    plt.figure(figsize=(12, 12))
    
    # 布局算法
    pos = nx.spring_layout(G, k=0.6, iterations=50, seed=42) # k值调大一点，拉开距离
    
    # 画边
    nx.draw_networkx_edges(G, pos, alpha=0.3, edge_color='gray', arrows=True, arrowsize=10)
    
    # 画点
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors, alpha=0.9)
    
    # 画标签 (只给 Top 10 打标签)
    labels = {node: node for node in top_nodes[:10] if node in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels, font_size=10, font_family='SimHei', font_color='black')
    
    plt.title(f'KOL 舆情传播关键节点网络 (Top {top_k})', fontsize=16)
    plt.axis('off')
    
    save_path = f'{output_dir}/analysis_kol_network.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"✅ 网络图已保存: {save_path}")
    plt.close()
    
else:
    print("❌ 未找到 KOL 数据文件，请先运行 analysis_kol.py")