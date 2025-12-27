import sys
import os

# 添加项目根目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from datetime import datetime
import warnings
from mysql.connect2DB import ConnectToDB
warnings.filterwarnings('ignore')


class DragonListAnalyzer:
    def __init__(self):
        """
        初始化分析器
        db_config: 数据库连接配置字典
        """
        self.df = None
        self.connection = None
        

    def getData(self):
        """
        执行SQL查询并返回DataFrame
        """
        sql = f'''SELECT * FROM stock.dragon;'''
        data,columns = dbConnection.Query(sql)
        self.df =pd.DataFrame(list(data),columns=columns)
        print(self.df.head())
        return self.df
    
    
    def preprocess_data(self):
        """数据预处理"""
        if self.df is None:
            print("请先加载数据")
            return False
            
        # 数据类型转换
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['buy'] = pd.to_numeric(self.df['buy'], errors='coerce')
        self.df['sell'] = pd.to_numeric(self.df['sell'], errors='coerce')
        self.df['NET'] = pd.to_numeric(self.df['NET'], errors='coerce')
        
        # 处理缺失值
        self.df = self.df.dropna(subset=['operator_Name', 'stockID'])

        # 过滤掉特定的 operator_ID（使用 isin + 取反），原写法在 Series 上使用 `not in`/`.str not in` 是错误的
        self.df = self.df[~self.df["operator_Name"].isin(["东方财富证券股份有限公司拉萨团结路第二证券营业部", "深股通专用", "沪股通专用",
                                                          "东方财富证券股份有限公司拉萨团结路第一证券营业部","东方财富证券股份有限公司拉萨金融城南环路证券营业部",
                                                          "机构专用","东方财富证券股份有限公司拉萨东环路第二证券营业部","东方财富证券股份有限公司拉萨东环路第一证券营业部",
                                                          "中国国际金融股份有限公司上海分公司","华鑫证券有限责任公司上海分公司","华泰证券股份有限公司总部",
                                                          "西藏东方财富证券股份有限公司拉萨团结路第二证券营业部","西藏东方财富证券股份有限公司拉萨东环路第二证券营业部",
                                                          "机构投资者","其他自然人","自然人","中小投资者"
                                                          ]) ]
        
        print("数据预处理完成")
        return True
    
    def find_cooccurrence_pairs(self, min_cooccurrence=3):
        """
        找出经常一起出现的营业部对
        """
        print("正在分析营业部共现关系...")
        
        # 创建营业部对的组合
        pairs_data = []
        
        # 按日期和股票分组
        grouped = self.df.groupby(['date', 'stockID'])
        
        for (date, stock), group in grouped:
            if len(group) < 2:
                continue
                
            operators = group['operator_Name'].unique()
            
            # 生成所有营业部对组合
            for i in range(len(operators)):
                for j in range(i + 1, len(operators)):
                    op1 = operators[i]
                    op2 = operators[j]
                    
                    # 获取两个营业部的记录
                    record1 = group[group['operator_Name'] == op1].iloc[0]
                    record2 = group[group['operator_Name'] == op2].iloc[0]
                    
                    pairs_data.append({
                        'date': date,
                        'stockID': stock,
                        'stockName': record1['stockName'],
                        'operator_A': op1,
                        'operator_B': op2,
                        'flag_A': record1['flag'],
                        'flag_B': record2['flag'],
                        'NET_A': record1['NET'],
                        'NET_B': record2['NET'],
                        'direction_same': 1 if record1['flag'] == record2['flag'] else 0
                    })
        
        pairs_df = pd.DataFrame(pairs_data)
        
        if len(pairs_df) == 0:
            print("未找到共现的营业部对")
            return pd.DataFrame()
        
        # 聚合统计
        cooccurrence_stats = pairs_df.groupby(['operator_A', 'operator_B']).agg({
            'date': ['count', 'min', 'max'],
            'stockID': 'nunique',
            'direction_same': 'mean',
            'NET_A': 'mean',
            'NET_B': 'mean'
        }).reset_index()
        
        # 扁平化列名
        cooccurrence_stats.columns = [
            'operator_A', 'operator_B', 'cooccurrence_count', 
            'first_cooccurrence', 'last_cooccurrence', 'unique_stocks',
            'direction_consistency', 'avg_NET_A', 'avg_NET_B'
        ]
        
        # 计算合作月数
        cooccurrence_stats['first_cooccurrence'] = pd.to_datetime(cooccurrence_stats['first_cooccurrence'])
        cooccurrence_stats['last_cooccurrence'] = pd.to_datetime(cooccurrence_stats['last_cooccurrence'])
        cooccurrence_stats['collaboration_months'] = (
            (cooccurrence_stats['last_cooccurrence'] - cooccurrence_stats['first_cooccurrence']).dt.days / 30
        ).round(1)
        
        # 计算关联强度得分
        cooccurrence_stats['association_score'] = (
            cooccurrence_stats['cooccurrence_count'] * 0.4 +
            cooccurrence_stats['direction_consistency'] * 0.3 +
            (cooccurrence_stats['collaboration_months'] / 12) * 0.2 +
            (cooccurrence_stats['unique_stocks'] / 10) * 0.1
        ).round(3)
        
        # 筛选最小共现次数
        result = cooccurrence_stats[cooccurrence_stats['cooccurrence_count'] >= min_cooccurrence].copy()
        result = result.sort_values('association_score', ascending=False)
        
        print(f"找到 {len(result)} 对高关联营业部")
        return result
    
    def analyze_specific_operator(self, operator_name, min_cooccurrence=2):
        """
        分析特定营业部的关联网络
        """
        print(f"正在分析 {operator_name} 的关联网络...")
        
        # 找出所有与该营业部共同出现的记录
        operator_records = self.df[self.df['operator_Name'] == operator_name]
        
        if len(operator_records) == 0:
            print(f"未找到营业部: {operator_name}")
            return pd.DataFrame()
        
        related_data = []
        
        for _, record in operator_records.iterrows():
            same_day_stock = self.df[
                (self.df['date'] == record['date']) & 
                (self.df['stockID'] == record['stockID']) &
                (self.df['operator_Name'] != operator_name)
            ]
            
            for _, related_record in same_day_stock.iterrows():
                related_data.append({
                    'date': record['date'],
                    'stockID': record['stockID'],
                    'stockName': record['stockName'],
                    'target_operator': operator_name,
                    'related_operator': related_record['operator_Name'],
                    'target_flag': record['flag'],
                    'related_flag': related_record['flag'],
                    'target_NET': record['NET'],
                    'related_NET': related_record['NET'],
                    'direction_same': 1 if record['flag'] == related_record['flag'] else 0
                })
        
        if len(related_data) == 0:
            print(f"{operator_name} 没有找到关联营业部")
            return pd.DataFrame()
        
        related_df = pd.DataFrame(related_data)
        
        # 聚合统计
        association_stats = related_df.groupby('related_operator').agg({
            'date': ['count', 'min', 'max'],
            'stockID': 'nunique',
            'direction_same': 'mean',
            'target_NET': 'mean',
            'related_NET': 'mean'
        }).reset_index()
        
        association_stats.columns = [
            'related_operator', 'cooccurrence_count', 'first_cooccurrence', 
            'last_cooccurrence', 'unique_stocks', 'direction_consistency',
            'avg_target_NET', 'avg_related_NET'
        ]
        
        # 计算时间跨度
        association_stats['first_cooccurrence'] = pd.to_datetime(association_stats['first_cooccurrence'])
        association_stats['last_cooccurrence'] = pd.to_datetime(association_stats['last_cooccurrence'])
        association_stats['collaboration_months'] = (
            (association_stats['last_cooccurrence'] - association_stats['first_cooccurrence']).dt.days / 30
        ).round(1)
        
        # 计算关联强度
        association_stats['association_score'] = (
            association_stats['cooccurrence_count'] * 0.5 +
            association_stats['direction_consistency'] * 0.5
        ).round(3)
        
        # 获取最近共同操作的股票
        recent_stocks = related_df.sort_values('date', ascending=False).groupby('related_operator').head(15)
        recent_stocks_grouped = recent_stocks.groupby('related_operator')['stockName'].apply(
            lambda x: ', '.join(x.head(15))
        ).reset_index()
        recent_stocks_grouped.columns = ['related_operator', 'recent_stocks']
        
        # 合并结果
        result = pd.merge(association_stats, recent_stocks_grouped, on='related_operator', how='left')
        result = result[result['cooccurrence_count'] >= min_cooccurrence]
        result = result.sort_values('association_score', ascending=False)
        
        print(f"找到 {len(result)} 个与 {operator_name} 关联的营业部")
        return result
    
    def build_network_graph(self, cooccurrence_pairs, min_association_score=0.5):
        """
        构建关联网络图
        """
        print("正在构建关联网络图...")
        
        G = nx.Graph()
        
        # 添加节点和边
        for _, row in cooccurrence_pairs.iterrows():
            if row['association_score'] >= min_association_score:
                G.add_edge(
                    row['operator_A'], 
                    row['operator_B'],
                    weight=row['association_score'],
                    cooccurrence=row['cooccurrence_count'],
                    consistency=row['direction_consistency']
                )
        
        print(f"网络包含 {G.number_of_nodes()} 个节点和 {G.number_of_edges()} 条边")
        return G
    
    def detect_communities(self, G):
        """
        使用Louvain算法检测社区
        """
        try:
            import community as community_louvain
            
            partition = community_louvain.best_partition(G)
            
            # 将社区信息添加到节点
            for node, community_id in partition.items():
                G.nodes[node]['community'] = community_id
            
            # 统计社区信息
            communities = {}
            for node, community_id in partition.items():
                if community_id not in communities:
                    communities[community_id] = []
                communities[community_id].append(node)
            
            print(f"检测到 {len(communities)} 个社区")
            return G, communities
        except ImportError:
            print("请安装python-louvain包: pip install python-louvain")
            return G, {}
    
    def calculate_network_metrics(self, G):
        """
        计算网络指标
        """
        metrics = {}
        
        # 度中心性
        metrics['degree_centrality'] = nx.degree_centrality(G)
        
        # 介数中心性
        metrics['betweenness_centrality'] = nx.betweenness_centrality(G)
        
        # 紧密中心性
        metrics['closeness_centrality'] = nx.closeness_centrality(G)
        
        return metrics
    
    def visualize_network(self, G, communities=None, top_n=50):
        """
        可视化关联网络
        """
        plt.figure(figsize=(15, 12))
        
        # 选择度中心性最高的节点进行展示（避免图太拥挤）
        if G.number_of_nodes() > top_n:
            degrees = dict(G.degree())
            top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:top_n]
            G = G.subgraph([node for node, _ in top_nodes])
        
        pos = nx.spring_layout(G, k=1, iterations=50)
        
        # 节点颜色（按社区或度中心性）
        if communities and len(communities) > 0:
            node_colors = [G.nodes[node].get('community', 0) for node in G.nodes()]
            cmap = plt.cm.tab20
        else:
            node_colors = [G.degree(node) for node in G.nodes()]
            cmap = plt.cm.Blues
        
        # 节点大小（按度中心性）
        node_sizes = [G.degree(node) * 100 for node in G.nodes()]
        
        # 绘制网络
        nx.draw_networkx_nodes(G, pos, node_size=node_sizes, 
                              node_color=node_colors, cmap=cmap, alpha=0.8)
        
        # 边的宽度（按关联强度）
        edge_widths = [G[u][v]['weight'] * 3 for u, v in G.edges()]
        nx.draw_networkx_edges(G, pos, width=edge_widths, alpha=0.5, edge_color='gray')
        
        # 节点标签（只显示高度中心性的节点）
        labels = {}
        for node in G.nodes():
            if G.degree(node) >= np.percentile([G.degree(n) for n in G.nodes()], 70):
                # 简化营业部名称显示
                short_name = node.split('公司')[-1][:10] + '...' if len(node) > 13 else node
                labels[node] = short_name
        
        nx.draw_networkx_labels(G, pos, labels, font_size=8, font_family='Arial Unicode MS')
        
        plt.title('营业部关联网络图', fontsize=16, fontfamily='Arial Unicode MS')
        plt.axis('off')
        plt.tight_layout()
        plt.show()
    
    def generate_report(self, operator_name=None):
        """
        生成分析报告
        """
        print("=" * 50)
        print("营业部关联性分析报告")
        print("=" * 50)
        
        if self.df is None:
            print("请先加载数据")
            return
        
        # 基础统计
        print(f"\n1. 数据概览:")
        print(f"   - 总记录数: {len(self.df):,}")
        print(f"   - 营业部数量: {self.df['operator_Name'].nunique():,}")
        print(f"   - 股票数量: {self.df['stockID'].nunique():,}")
        print(f"   - 时间范围: {self.df['date'].min().strftime('%Y-%m-%d')} 到 {self.df['date'].max().strftime('%Y-%m-%d')}")
        
        # 最活跃的营业部
        top_operators = self.df['operator_Name'].value_counts().head(50)
        print(f"\n2. 最活跃的50个营业部:")
        for i, (operator, count) in enumerate(top_operators.items(), 1):
            print(f"   {i:2d}. {operator}: {count}次")
        
        # 关联分析
        cooccurrence_pairs = self.find_cooccurrence_pairs(min_cooccurrence=3)
        
        if len(cooccurrence_pairs) > 0:
            print(f"\n3. 关联性最强的50对营业部:")
            top_pairs = cooccurrence_pairs.head(50)
            for i, (_, pair) in enumerate(top_pairs.iterrows(), 1):
                print(f"   {i:2d}. {pair['operator_A']} & {pair['operator_B']}")
                print(f"       共现次数: {pair['cooccurrence_count']}, 方向一致率: {pair['direction_consistency']:.1%}, 关联得分: {pair['association_score']:.3f}")
        
        # 特定营业部分析
        if operator_name:
            association_results = self.analyze_specific_operator(operator_name)
            if len(association_results) > 0:
                print(f"\n4. {operator_name} 的主要关联方:")
                top_associates = association_results.head(5)
                for i, (_, associate) in enumerate(top_associates.iterrows(), 1):
                    print(f"   {i:2d}. {associate['related_operator']}")
                    print(f"       共现次数: {associate['cooccurrence_count']}, 方向一致率: {associate['direction_consistency']:.1%}")
                    print(f"       近期共同操作: {associate['recent_stocks']}")

# 使用示例
if __name__ == "__main__":
    # 数据库配置 - 请根据你的实际情况修改

    # 创建分析器实例
    analyzer = DragonListAnalyzer()
    dbConnection = ConnectToDB()
    
    analyzer.getData()
    analyzer.preprocess_data()
    # 生成分析报告
    analyzer.generate_report(operator_name='国泰海通证券股份有限公司总部')

    # 详细关联分析
    cooccurrence_pairs = analyzer.find_cooccurrence_pairs(min_cooccurrence=3)

    if len(cooccurrence_pairs) > 0:
        # 构建网络图
        G = analyzer.build_network_graph(cooccurrence_pairs)

        # 检测社区
        G, communities = analyzer.detect_communities(G)

        # 计算网络指标
        metrics = analyzer.calculate_network_metrics(G)

        # 可视化网络
        analyzer.visualize_network(G, communities)

        # 保存结果到文件
        cooccurrence_pairs.to_csv('./营业部关联分析结果.csv', index=False, encoding='utf-8-sig')
        print("结果已保存到 '营业部关联分析结果.csv'")