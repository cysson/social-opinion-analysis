# src/data_cleaner.py
"""
社交舆情大数据清洗脚本
功能：读取原始JSON数据，进行数据清洗，输出3种格式的清洗后数据
作者：数据清洗小组
"""

import pandas as pd
import numpy as np
import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Dict, Any


# 自定义JSON编码器，处理numpy类型
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, pd.Series):
            return obj.to_list()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')
        elif isinstance(obj, pd.Index):
            return obj.tolist()
        else:
            return super().default(obj)


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_cleaning.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SocialMediaDataCleaner:
    """社交舆情数据清洗器"""

    def __init__(self, raw_data_dir: str = None, output_dir: str = None):
        """
        初始化清洗器

        Args:
            raw_data_dir: 原始数据目录路径
            output_dir: 输出目录路径
        """
        self.project_root = Path(__file__).parent.parent

        # 设置目录路径
        self.raw_data_dir = Path(raw_data_dir) if raw_data_dir else self.project_root / "data" / "raw" / "json"
        self.output_dir = Path(output_dir) if output_dir else self.project_root / "data" / "cleaned"

        # 创建输出目录
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 保存原始数据引用
        self.df_raw = None

        logger.info(f"原始数据目录: {self.raw_data_dir}")
        logger.info(f"输出目录: {self.output_dir}")

    def load_json_files(self) -> List[Dict[str, Any]]:
        """
        加载所有JSON文件数据

        Returns:
            合并后的数据列表
        """
        all_data = []
        json_files = list(self.raw_data_dir.glob("*.json"))

        if not json_files:
            logger.error(f"在 {self.raw_data_dir} 中没有找到JSON文件")
            return all_data

        logger.info(f"找到 {len(json_files)} 个JSON文件:")

        for file_path in json_files:
            logger.info(f"  正在读取: {file_path.name}")

            try:
                # 读取JSON文件
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 处理不同的JSON结构
                if isinstance(data, list):
                    all_data.extend(data)
                    logger.info(f"    成功加载 {len(data)} 条记录")
                elif isinstance(data, dict):
                    # 尝试从常见键中提取数据
                    if 'data' in data:
                        if isinstance(data['data'], list):
                            all_data.extend(data['data'])
                            logger.info(f"    从'data'键中加载 {len(data['data'])} 条记录")
                    elif 'comments' in data:
                        if isinstance(data['comments'], list):
                            all_data.extend(data['comments'])
                            logger.info(f"    从'comments'键中加载 {len(data['comments'])} 条记录")
                    elif 'items' in data:
                        if isinstance(data['items'], list):
                            all_data.extend(data['items'])
                            logger.info(f"    从'items'键中加载 {len(data['items'])} 条记录")
                    else:
                        # 如果只有一条记录，作为列表添加
                        all_data.append(data)
                        logger.info(f"    加载单条记录")

            except json.JSONDecodeError as e:
                logger.error(f"    JSON解析错误: {e}")
            except Exception as e:
                logger.error(f"    读取文件时出错: {e}")

        logger.info(f"总共加载 {len(all_data)} 条原始记录")
        return all_data

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        执行完整的数据清洗流程
        
        清洗目标：提高数据质量，为后续分析（情感分析、聚类、关联规则挖掘、可视化）做准备
        
        清洗步骤：
        1. 删除完全空白的行
        2. 数据类型标准化（确保时间、数值、字符串类型正确）
        3. 处理缺失值（填充或删除关键字段缺失的记录）
        4. 文本内容清洗（保留有用信息，移除噪音）
        5. 去重处理（基于内容和用户行为）
        6. 特征工程（添加分析所需特征）
        7. 过滤噪音数据（移除无效记录）

        Args:
            df: 原始数据DataFrame

        Returns:
            清洗后的DataFrame
        """
        logger.info("=" * 60)
        logger.info("开始数据清洗流程...")
        logger.info("清洗目标：提高数据质量，为后续分析做准备")
        logger.info("=" * 60)
        initial_count = len(df)

        # Step 1: 删除完全空白的行
        logger.info("\n【步骤1】删除完全空白的行")
        logger.info("原因：空白行无分析价值，占用存储空间")
        df = df.dropna(how='all')
        logger.info(f"结果：删除空白行后剩余 {len(df)} 条记录")

        # Step 2: 数据类型标准化
        logger.info("\n【步骤2】数据类型标准化")
        logger.info("原因：确保时间、数值、字符串类型正确，便于后续分析")
        df = self.standardize_data_types(df)

        # Step 3: 处理缺失值
        logger.info("\n【步骤3】处理缺失值")
        logger.info("原因：关键字段（时间、内容、用户ID）缺失会影响分析准确性")
        df = self.handle_missing_values(df)

        # Step 4: 文本内容清洗
        logger.info("\n【步骤4】文本内容清洗")
        logger.info("原因：移除URL、表情符号等噪音，保留有用文本用于情感分析和聚类")
        if 'content' in df.columns:
            df['content_clean'] = df['content'].apply(self.clean_text)
            # 删除清洗后内容为空的记录
            if len(df) > 0:
                before_text_clean = len(df)
                df = df[df['content_clean'].str.len() > 0]
                logger.info(f"结果：移除 {before_text_clean - len(df)} 条空内容记录，剩余 {len(df)} 条")

        # Step 5: 去重处理
        logger.info("\n【步骤5】去重处理")
        logger.info("原因：重复数据会影响统计分析和算法准确性")
        df = self.remove_duplicates(df)

        # Step 6: 特征工程
        logger.info("\n【步骤6】特征工程")
        logger.info("原因：添加时间特征、地理位置特征、用户活跃度等，支撑后续可视化分析")
        df = self.feature_engineering(df)

        # Step 7: 过滤噪音数据
        logger.info("\n【步骤7】过滤噪音数据")
        logger.info("原因：移除过短、无意义的评论和疑似机器人账号，提高数据质量")
        df = self.filter_noise(df)

        logger.info("\n" + "=" * 60)
        logger.info(f"清洗完成！原始: {initial_count} 条 → 清洗后: {len(df)} 条")
        if initial_count > 0:
            retention_rate = len(df) / initial_count * 100
            logger.info(f"数据保留率: {retention_rate:.1f}%")
            logger.info(f"移除记录数: {initial_count - len(df)} 条")
        logger.info("=" * 60)

        return df

    def standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化数据类型"""
        logger.info("正在标准化数据类型...")

        # 数值字段转换
        numeric_columns = ['sub_comment_count', 'like_count', 'parent_comment_id']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.info(f"  转换 {col} 为数值类型")

        # 时间戳转换
        time_columns = ['create_time', 'last_modify_ts']
        for col in time_columns:
            if col in df.columns:
                try:
                    # 尝试毫秒级时间戳转换
                    df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')
                    logger.info(f"  转换 {col} 为日期时间（毫秒级）")
                except:
                    # 如果失败，尝试其他格式
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        # 字符串类型转换
        string_columns = ['comment_id', 'note_id', 'user_id', 'nickname', 'ip_location', 'content']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理缺失值
        
        策略：
        1. 关键字段（content, user_id, create_time）缺失则删除记录
        2. 分析关键字段（ip_location, parent_comment_id）缺失则填充默认值
        3. 数值字段缺失填充为0
        """
        logger.info("正在处理缺失值...")

        # 关键字段不能为空（用于后续分析）
        critical_columns = ['content', 'user_id', 'create_time']
        initial_count = len(df)

        for col in critical_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    logger.info(f"  {col} 有 {missing_count} 个缺失值")

        # 删除关键字段为空的行（这些字段对后续分析至关重要）
        for col in ['content', 'user_id']:
            if col in df.columns:
                before_drop = len(df)
                df = df.dropna(subset=[col])
                if before_drop > len(df):
                    logger.info(f"  删除 {col} 缺失的记录: {before_drop - len(df)} 条")

        # 对于时间字段，如果缺失但其他字段完整，尝试从其他时间字段推断
        if 'create_time' in df.columns:
            missing_time = df['create_time'].isna().sum()
            if missing_time > 0:
                # 尝试从last_modify_ts推断
                if 'last_modify_ts' in df.columns:
                    df.loc[df['create_time'].isna(), 'create_time'] = df.loc[df['create_time'].isna(), 'last_modify_ts']
                    logger.info(f"  从last_modify_ts推断create_time: {df['create_time'].isna().sum()} 个仍缺失")
                # 如果仍有缺失，删除（时间对趋势分析至关重要）
                before_drop = len(df)
                df = df.dropna(subset=['create_time'])
                if before_drop > len(df):
                    logger.info(f"  删除create_time仍缺失的记录: {before_drop - len(df)} 条")

        # 填充其他字段的缺失值（这些字段用于分析但不是必须的）
        fill_values = {
            'ip_location': '未知',  # 用于地域分布分析，缺失填充为"未知"
            'like_count': 0,  # 用于用户活跃度分析
            'sub_comment_count': 0,  # 用于互动关系分析
            'parent_comment_id': 0  # 用于网络图分析，0表示顶级评论
        }

        for col, default_value in fill_values.items():
            if col in df.columns:
                missing_before = df[col].isna().sum()
                df[col] = df[col].fillna(default_value)
                if missing_before > 0:
                    logger.info(f"  填充 {col} 的缺失值 ({missing_before} 个) 为: {default_value}")

        logger.info(f"处理缺失值后剩余: {len(df)} 条记录（移除了 {initial_count - len(df)} 条）")
        return df

    def clean_text(self, text: str) -> str:
        """
        清洗文本内容
        
        策略：移除噪音但保留有用信息，确保文本可用于：
        1. 情感分析（保留情感词汇）
        2. 聚类分析（保留主题关键词）
        3. 词云生成（保留高频词）

        Args:
            text: 原始文本

        Returns:
            清洗后的文本
        """
        if not isinstance(text, str) or text == 'nan':
            return ""

        # 1. 移除URL（URL对文本分析无意义）
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

        # 2. 移除表情符号占位符（如[哇h]），但保留中文字符
        text = re.sub(r'\[.*?\]', '', text)

        # 3. 移除HTML标签
        text = re.sub(r'<.*?>', '', text)

        # 4. 移除特殊格式但保留文本内容
        # 移除话题标签和@用户（这些对聚类分析可能有用，但先移除以简化）
        # 注意：如果后续需要分析话题，可以保留#标签
        text = re.sub(r'@\S+', '', text)  # 移除@用户
        # 保留#标签，因为可能包含话题信息
        
        # 5. 规范化空格（多个空格合并为一个）
        text = re.sub(r'\s+', ' ', text).strip()
        
        # 6. 移除控制字符和特殊Unicode字符（保留中文、英文、数字、常用标点）
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)  # 移除控制字符

        return text

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """去重处理"""
        logger.info("正在执行去重处理...")

        initial_count = len(df)

        # 1. 基于内容去重（完全相同的评论）
        if 'content_clean' in df.columns:
            df = df.drop_duplicates(subset=['content_clean'], keep='first')
            logger.info(f"  基于内容去重: 移除 {initial_count - len(df)} 条重复记录")

        # 2. 基于用户-时间窗口去重（防止刷评）
        if all(col in df.columns for col in ['user_id', 'create_time', 'content_clean']):
            df = df.sort_values(['user_id', 'create_time'])
            df['time_diff'] = df.groupby('user_id')['create_time'].diff().dt.total_seconds()

            # 标记可能的刷评（10秒内发布的相似短评）
            df['is_spam'] = (df['time_diff'] < 10) & (df['content_clean'].str.len() < 20)

            spam_count = df['is_spam'].sum()
            if spam_count > 0:
                logger.info(f"  检测到 {spam_count} 条疑似刷评记录")
                df = df[~df['is_spam']]

            df = df.drop(columns=['time_diff', 'is_spam'])

        logger.info(f"去重后剩余: {len(df)} 条记录")
        return df

    def feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        特征工程
        
        添加以下特征以支持后续分析：
        1. 文本特征：长度、关键词（用于聚类和词云）
        2. 时间特征：年、季度、月、周、日、小时（用于时间趋势图）
        3. 地理位置特征：标准化省份/城市（用于地域分布图）
        4. 用户特征：活跃度、是否为KOL（用于网络图分析）
        5. 互动特征：是否为回复、互动数（用于关联规则挖掘）
        6. 情感特征：情感得分（用于情感分析）
        """
        logger.info("正在进行特征工程...")

        # 1. 评论长度特征（用于过滤和统计）
        if 'content_clean' in df.columns:
            df['content_length'] = df['content_clean'].str.len()
            logger.info(f"  ✓ 添加 content_length 特征")

        # 2. 是否包含图片
        if 'pictures' in df.columns:
            df['has_picture'] = df['pictures'].apply(
                lambda x: isinstance(x, str) and len(x) > 10 and not str(x).lower() in ['null', 'none', '']
            )
            logger.info(f"  ✓ 添加 has_picture 特征")

        # 3. 是否为回复（用于网络图分析）
        if 'parent_comment_id' in df.columns:
            df['is_reply'] = df['parent_comment_id'].apply(
                lambda x: x != 0 and x != '0' and pd.notna(x) and float(x) != 0.0
            )
            logger.info(f"  ✓ 添加 is_reply 特征（用于互动关系网络图）")

        # 4. 扩展时间特征（用于时间趋势分析）
        if 'create_time' in df.columns:
            df['year'] = df['create_time'].dt.year
            df['quarter'] = df['create_time'].dt.quarter
            df['month'] = df['create_time'].dt.month
            df['day'] = df['create_time'].dt.day
            df['hour'] = df['create_time'].dt.hour
            df['day_of_week'] = df['create_time'].dt.dayofweek
            df['day_name'] = df['create_time'].dt.day_name()
            df['date'] = df['create_time'].dt.date
            df['datetime_str'] = df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"  ✓ 添加时间特征: year, quarter, month, day, hour, day_of_week, date（用于时间趋势图）")

        # 5. 地理位置标准化（用于地域分布图）
        if 'ip_location' in df.columns:
            df['location_standard'] = df['ip_location'].apply(self.standardize_location)
            df['location_province'] = df['ip_location'].apply(self.extract_province)
            df['location_city'] = df['ip_location'].apply(self.extract_city)
            logger.info(f"  ✓ 添加地理位置特征: location_standard, location_province, location_city（用于地域分布图）")

        # 6. 用户活跃度特征（用于KOL识别）
        if 'user_id' in df.columns:
            user_stats = df.groupby('user_id').agg({
                'comment_id': 'count',  # 评论数
                'like_count': 'sum',    # 总点赞数
                'sub_comment_count': 'sum'  # 总回复数
            }).reset_index()
            user_stats.columns = ['user_id', 'user_comment_count', 'user_total_likes', 'user_total_replies']
            df = df.merge(user_stats, on='user_id', how='left')
            df['user_activity_score'] = (
                df['user_comment_count'] * 1 + 
                df['user_total_likes'] * 0.1 + 
                df['user_total_replies'] * 0.5
            )
            # 标记KOL（活跃度前10%的用户）
            if len(df) > 0:
                threshold = df['user_activity_score'].quantile(0.9)
                df['is_kol'] = df['user_activity_score'] >= threshold
            logger.info(f"  ✓ 添加用户活跃度特征: user_comment_count, user_activity_score, is_kol（用于KOL识别）")

        # 7. 情感倾向特征（用于情感分析）
        if 'content_clean' in df.columns:
            df['sentiment_score'] = df['content_clean'].apply(self.calculate_simple_sentiment)
            # 情感分类：正面(>0.1)、中性(-0.1到0.1)、负面(<-0.1)
            df['sentiment_label'] = df['sentiment_score'].apply(
                lambda x: '正面' if x > 0.1 else ('负面' if x < -0.1 else '中性')
            )
            logger.info(f"  ✓ 添加情感特征: sentiment_score, sentiment_label（用于情感分析和情感分布图）")

        # 8. 互动数特征（用于关联规则挖掘）
        if 'like_count' in df.columns and 'sub_comment_count' in df.columns:
            df['interaction_count'] = df['like_count'] + df['sub_comment_count']
            df['has_interaction'] = df['interaction_count'] > 0
            logger.info(f"  ✓ 添加互动特征: interaction_count, has_interaction（用于关联规则挖掘）")

        logger.info(f"特征工程完成，共添加 {len([c for c in df.columns if c not in ['comment_id', 'create_time', 'ip_location', 'note_id', 'content', 'user_id', 'nickname', 'avatar', 'sub_comment_count', 'pictures', 'parent_comment_id', 'last_modify_ts', 'like_count']])} 个新特征")
        return df

    def standardize_location(self, location: str) -> str:
        """
        标准化地理位置（国家/地区级别）
        用于地域分布图的宏观分析
        """
        if not isinstance(location, str) or location == 'nan' or location == '未知':
            return '未知'

        location_lower = location.lower()

        # 中国识别
        if '中国' in location or 'china' in location_lower or 'cn' in location_lower:
            return '中国'
        elif '美国' in location or 'usa' in location_lower or 'america' in location_lower or 'us' in location_lower:
            return '美国'
        elif '日本' in location or 'japan' in location_lower or 'jp' in location_lower:
            return '日本'
        elif '韩国' in location or 'korea' in location_lower or 'kr' in location_lower:
            return '韩国'
        elif '欧洲' in location or 'europe' in location_lower or 'eu' in location_lower:
            return '欧洲'
        elif '英国' in location or 'uk' in location_lower or 'united kingdom' in location_lower:
            return '英国'
        elif '加拿大' in location or 'canada' in location_lower or 'ca' in location_lower:
            return '加拿大'
        elif '澳大利亚' in location or 'australia' in location_lower or 'au' in location_lower:
            return '澳大利亚'
        else:
            # 如果包含中国省份，默认为中国
            china_provinces = ['北京', '上海', '广东', '深圳', '广州', '江苏', '浙江', '四川',
                             '湖北', '湖南', '山东', '河南', '河北', '陕西', '福建', '安徽',
                             '重庆', '天津', '辽宁', '吉林', '黑龙江', '内蒙古', '新疆', '西藏',
                             '云南', '贵州', '广西', '海南', '江西', '山西', '甘肃', '青海', '宁夏']
            for province in china_provinces:
                if province in location:
                    return '中国'
            return '其他地区'

    def extract_province(self, location: str) -> str:
        """
        提取省份信息（用于更细粒度的地域分析）
        """
        if not isinstance(location, str) or location == 'nan' or location == '未知':
            return '未知'

        # 中国省份列表
        provinces = {
            '北京': ['北京', 'beijing'],
            '上海': ['上海', 'shanghai'],
            '天津': ['天津', 'tianjin'],
            '重庆': ['重庆', 'chongqing'],
            '广东': ['广东', 'guangdong', '广州', 'guangzhou', '深圳', 'shenzhen', '珠海', 'zhuhai', '佛山', 'foshan'],
            '江苏': ['江苏', 'jiangsu', '南京', 'nanjing', '苏州', 'suzhou', '无锡', 'wuxi'],
            '浙江': ['浙江', 'zhejiang', '杭州', 'hangzhou', '宁波', 'ningbo', '温州', 'wenzhou'],
            '四川': ['四川', 'sichuan', '成都', 'chengdu'],
            '湖北': ['湖北', 'hubei', '武汉', 'wuhan'],
            '湖南': ['湖南', 'hunan', '长沙', 'changsha'],
            '山东': ['山东', 'shandong', '济南', 'jinan', '青岛', 'qingdao'],
            '河南': ['河南', 'henan', '郑州', 'zhengzhou'],
            '河北': ['河北', 'hebei', '石家庄', 'shijiazhuang'],
            '陕西': ['陕西', 'shaanxi', '西安', 'xian'],
            '福建': ['福建', 'fujian', '厦门', 'xiamen', '福州', 'fuzhou'],
            '安徽': ['安徽', 'anhui', '合肥', 'hefei'],
            '辽宁': ['辽宁', 'liaoning', '沈阳', 'shenyang', '大连', 'dalian'],
            '吉林': ['吉林', 'jilin', '长春', 'changchun'],
            '黑龙江': ['黑龙江', 'heilongjiang', '哈尔滨', 'harbin'],
            '云南': ['云南', 'yunnan', '昆明', 'kunming'],
            '贵州': ['贵州', 'guizhou', '贵阳', 'guiyang'],
            '广西': ['广西', 'guangxi', '南宁', 'nanning'],
            '海南': ['海南', 'hainan', '海口', 'haikou'],
            '江西': ['江西', 'jiangxi', '南昌', 'nanchang'],
            '山西': ['山西', 'shanxi', '太原', 'taiyuan'],
            '甘肃': ['甘肃', 'gansu', '兰州', 'lanzhou'],
            '青海': ['青海', 'qinghai', '西宁', 'xining'],
            '新疆': ['新疆', 'xinjiang', '乌鲁木齐', 'urumqi'],
            '西藏': ['西藏', 'tibet', '拉萨', 'lhasa'],
            '内蒙古': ['内蒙古', 'inner mongolia', '呼和浩特', 'hohhot'],
            '宁夏': ['宁夏', 'ningxia', '银川', 'yinchuan']
        }

        location_lower = location.lower()
        for province, keywords in provinces.items():
            for keyword in keywords:
                if keyword in location or keyword.lower() in location_lower:
                    return province

        return '未知'

    def extract_city(self, location: str) -> str:
        """
        提取城市信息（如果可识别）
        """
        if not isinstance(location, str) or location == 'nan' or location == '未知':
            return '未知'

        # 主要城市列表
        major_cities = {
            '北京': ['北京', 'beijing'],
            '上海': ['上海', 'shanghai'],
            '广州': ['广州', 'guangzhou'],
            '深圳': ['深圳', 'shenzhen'],
            '杭州': ['杭州', 'hangzhou'],
            '成都': ['成都', 'chengdu'],
            '武汉': ['武汉', 'wuhan'],
            '西安': ['西安', 'xian'],
            '南京': ['南京', 'nanjing'],
            '苏州': ['苏州', 'suzhou']
        }

        location_lower = location.lower()
        for city, keywords in major_cities.items():
            for keyword in keywords:
                if keyword in location or keyword.lower() in location_lower:
                    return city

        return '未知'

    def calculate_simple_sentiment(self, text: str) -> float:
        """
        简单情感分析（基于关键词）
        
        注意：这是基础版本，后续可以使用BERT等深度学习模型进行更准确的情感分析
        返回值范围：-1（极端负面）到 1（极端正面），0表示中性
        """
        if not isinstance(text, str) or len(text) == 0:
            return 0.0

        # 扩展正面词汇（用于情感分析）
        positive_words = [
            '好', '喜欢', '赞', '棒', '优秀', '支持', '感谢', '厉害', '牛', '开心',
            '不错', '很好', '太好了', '完美', '满意', '推荐', '值得', '有用', '有帮助',
            '专业', '清晰', '易懂', '实用', '有效', '成功', '进步', '提升', '改善',
            '鼓励', '加油', '努力', '坚持', '希望', '期待', '相信', '肯定', '认可'
        ]
        
        # 扩展负面词汇
        negative_words = [
            '差', '讨厌', '垃圾', '不好', '失望', '反对', '恶心', '糟糕', '烦', '生气',
            '不行', '没用', '错误', '失败', '问题', '困难', '麻烦', '复杂', '困惑',
            '担心', '焦虑', '压力', '累', '难', '慢', '贵', '贵', '贵', '贵',
            '批评', '质疑', '怀疑', '不满', '抱怨', '遗憾', '可惜', '无奈', '无语'
        ]

        positive_count = sum(1 for word in positive_words if word in text)
        negative_count = sum(1 for word in negative_words if word in text)

        total = positive_count + negative_count
        if total == 0:
            return 0.0

        # 计算情感得分：正面词越多得分越高，负面词越多得分越低
        sentiment = (positive_count - negative_count) / total
        
        # 归一化到[-1, 1]范围
        return max(-1.0, min(1.0, sentiment))

    def filter_noise(self, df: pd.DataFrame) -> pd.DataFrame:
        """过滤噪音数据"""
        logger.info("正在过滤噪音数据...")

        initial_count = len(df)

        # 1. 过滤过短评论（少于3个字符）
        if 'content_clean' in df.columns:
            df = df[df['content_clean'].str.len() >= 3]

        # 2. 过滤无意义评论
        meaningless_patterns = ['转发微博', '转发', '转', '...', '。。', '???', '!!!', '？？？', '！！！']
        if 'content_clean' in df.columns:
            mask = df['content_clean'].apply(
                lambda x: all(pattern not in x for pattern in meaningless_patterns)
            )
            df = df[mask]

        # 3. 过滤疑似机器人账号（无头像）
        if 'avatar' in df.columns:
            df = df[~df['avatar'].str.contains('default|placeholder|null', na=False)]

        logger.info(f"噪音过滤后剩余: {len(df)} 条记录")
        logger.info(f"过滤掉 {initial_count - len(df)} 条噪音记录")

        return df

    def convert_to_serializable(self, obj):
        """将对象转换为可JSON序列化的类型"""
        if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
            return float(obj)
        elif isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')
        elif isinstance(obj, pd.Index):
            return obj.tolist()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return obj

    def generate_cleaning_report(self, df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> Dict[str, Any]:
        """
        生成详细的清洗报告
        
        报告包含：
        1. 清洗前后数据量对比
        2. 清洗步骤说明
        3. 数据质量指标
        4. 新增特征说明
        """
        logger.info("正在生成清洗报告...")

        # 转换数据为可序列化格式
        if not df_raw.empty:
            raw_missing_values = df_raw.isna().sum()
            raw_missing_dict = {col: self.convert_to_serializable(raw_missing_values[col]) for col in
                                raw_missing_values.index}
        else:
            raw_missing_dict = {}

        if not df_clean.empty:
            # 计算数据质量指标
            missing_content = self.convert_to_serializable(
                df_clean['content'].isna().sum() if 'content' in df_clean.columns else 0)
            avg_content_length = self.convert_to_serializable(
                float(df_clean['content_length'].mean()) if 'content_length' in df_clean.columns else 0.0)
            has_picture_ratio = self.convert_to_serializable(
                float(df_clean['has_picture'].mean()) if 'has_picture' in df_clean.columns else 0.0)
            reply_ratio = self.convert_to_serializable(
                float(df_clean['is_reply'].mean()) if 'is_reply' in df_clean.columns else 0.0)
            avg_sentiment_score = self.convert_to_serializable(
                float(df_clean['sentiment_score'].mean()) if 'sentiment_score' in df_clean.columns else 0.0)
            
            # 计算时间覆盖范围
            if 'create_time' in df_clean.columns:
                time_range = {
                    "start": self.convert_to_serializable(df_clean['create_time'].min().strftime('%Y-%m-%d %H:%M:%S')) if pd.notna(df_clean['create_time'].min()) else None,
                    "end": self.convert_to_serializable(df_clean['create_time'].max().strftime('%Y-%m-%d %H:%M:%S')) if pd.notna(df_clean['create_time'].max()) else None
                }
            else:
                time_range = {}
            
            # 计算地域分布统计
            if 'location_standard' in df_clean.columns:
                location_dist = df_clean['location_standard'].value_counts().to_dict()
                location_dist = {k: self.convert_to_serializable(v) for k, v in location_dist.items()}
            else:
                location_dist = {}
            
            # 计算情感分布
            if 'sentiment_label' in df_clean.columns:
                sentiment_dist = df_clean['sentiment_label'].value_counts().to_dict()
                sentiment_dist = {k: self.convert_to_serializable(v) for k, v in sentiment_dist.items()}
            else:
                sentiment_dist = {}
            
            # KOL统计
            if 'is_kol' in df_clean.columns:
                kol_count = self.convert_to_serializable(df_clean['is_kol'].sum())
            else:
                kol_count = 0
        else:
            missing_content = avg_content_length = has_picture_ratio = reply_ratio = avg_sentiment_score = 0.0
            time_range = {}
            location_dist = {}
            sentiment_dist = {}
            kol_count = 0

        # 清洗步骤说明
        cleaning_steps = [
            {
                "step": 1,
                "name": "删除完全空白的行",
                "reason": "空白行无分析价值，占用存储空间"
            },
            {
                "step": 2,
                "name": "数据类型标准化",
                "reason": "确保时间、数值、字符串类型正确，便于后续分析"
            },
            {
                "step": 3,
                "name": "处理缺失值",
                "reason": "关键字段（时间、内容、用户ID）缺失会影响分析准确性"
            },
            {
                "step": 4,
                "name": "文本内容清洗",
                "reason": "移除URL、表情符号等噪音，保留有用文本用于情感分析和聚类"
            },
            {
                "step": 5,
                "name": "去重处理",
                "reason": "重复数据会影响统计分析和算法准确性"
            },
            {
                "step": 6,
                "name": "特征工程",
                "reason": "添加时间特征、地理位置特征、用户活跃度等，支撑后续可视化分析"
            },
            {
                "step": 7,
                "name": "过滤噪音数据",
                "reason": "移除过短、无意义的评论和疑似机器人账号，提高数据质量"
            }
        ]

        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cleaning_objective": "提高数据质量，为后续分析（情感分析、聚类、关联规则挖掘、可视化）做准备",
            "cleaning_steps": cleaning_steps,
            "raw_data": {
                "total_records": self.convert_to_serializable(len(df_raw)),
                "columns": self.convert_to_serializable(list(df_raw.columns)) if not df_raw.empty else [],
                "data_types": str(df_raw.dtypes.astype(str).to_dict()) if not df_raw.empty else {},
                "missing_values": raw_missing_dict
            },
            "cleaned_data": {
                "total_records": self.convert_to_serializable(len(df_clean)),
                "columns": self.convert_to_serializable(list(df_clean.columns)) if not df_clean.empty else [],
                "retention_rate": f"{len(df_clean) / len(df_raw) * 100:.2f}%" if len(df_raw) > 0 else "N/A",
                "records_removed": self.convert_to_serializable(len(df_raw) - len(df_clean))
            },
            "cleaning_stats": {
                "records_removed": self.convert_to_serializable(len(df_raw) - len(df_clean)),
                "new_features_added": self.convert_to_serializable(list(
                    set(df_clean.columns) - set(df_raw.columns))) if not df_clean.empty and not df_raw.empty else []
            },
            "data_quality_metrics": {
                "missing_content": missing_content,
                "avg_content_length": avg_content_length,
                "has_picture_ratio": has_picture_ratio,
                "reply_ratio": reply_ratio,
                "avg_sentiment_score": avg_sentiment_score,
                "time_range": time_range,
                "location_distribution": location_dist,
                "sentiment_distribution": sentiment_dist,
                "kol_count": kol_count
            },
            "technical_tools": "Python Pandas（数据清洗和特征工程）",
            "output_formats": ["CSV", "Parquet（适合Hadoop/Spark）", "JSON Lines"]
        }

        return report

    def save_data(self, df: pd.DataFrame):
        """保存清洗后的数据为3种格式"""
        logger.info("正在保存清洗后的数据...")

        # 1. CSV格式（通用格式）
        csv_path = self.output_dir / "cleaned_comments.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"✓ 保存为CSV: {csv_path} ({df.shape[0]}行, {df.shape[1]}列)")

        # 2. Parquet格式（适合Hadoop/Spark）
        parquet_path = self.output_dir / "cleaned_comments.parquet"
        try:
            df.to_parquet(parquet_path, index=False)
            logger.info(f"✓ 保存为Parquet: {parquet_path}")
        except Exception as e:
            logger.error(f"Parquet保存失败: {e}")
            # 如果失败，创建占位文件
            with open(parquet_path, 'w') as f:
                f.write("Parquet格式保存失败，请检查pyarrow安装")

        # 3. JSON Lines格式
        jsonl_path = self.output_dir / "cleaned_comments.jsonl"
        df.to_json(jsonl_path, orient='records', lines=True, force_ascii=False)
        logger.info(f"✓ 保存为JSON Lines: {jsonl_path}")

        # 4. 保存清洗报告
        if self.df_raw is not None:
            report_path = self.output_dir / "cleaning_report.json"
            try:
                report_data = self.generate_cleaning_report(self.df_raw, df)
                with open(report_path, 'w', encoding='utf-8') as f:
                    json.dump(report_data, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
                logger.info(f"✓ 保存清洗报告: {report_path}")
            except Exception as e:
                logger.error(f"保存清洗报告失败: {e}")
                # 保存简化版报告
                simple_report = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "raw_records": int(len(self.df_raw)),
                    "cleaned_records": int(len(df)),
                    "retention_rate": f"{len(df) / len(self.df_raw) * 100:.2f}%" if len(self.df_raw) > 0 else "N/A"
                }
                with open(report_path, 'w', encoding='utf-8') as f:
                    json.dump(simple_report, f, indent=2, ensure_ascii=False)

    def run(self):
        """运行完整的清洗流程"""
        logger.info("=" * 60)
        logger.info("开始社交舆情数据清洗流程")
        logger.info("=" * 60)

        try:
            # 1. 加载数据
            raw_data = self.load_json_files()
            if not raw_data:
                logger.error("没有加载到任何数据，请检查原始数据文件")
                return False

            # 2. 转换为DataFrame
            df_raw = pd.DataFrame(raw_data)
            self.df_raw = df_raw  # 保存原始数据引用
            logger.info(f"创建DataFrame: {df_raw.shape[0]}行, {df_raw.shape[1]}列")

            # 3. 执行清洗
            df_clean = self.clean_data(df_raw)

            # 4. 保存清洗后的数据
            self.save_data(df_clean)

            # 5. 生成总结报告
            logger.info("=" * 60)
            logger.info("数据清洗完成！")
            logger.info(f"原始数据: {len(df_raw)} 条")
            logger.info(f"清洗后数据: {len(df_clean)} 条")
            if len(df_raw) > 0:
                logger.info(f"数据保留率: {len(df_clean) / len(df_raw) * 100:.1f}%")
            logger.info(f"输出位置: {self.output_dir}")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"清洗过程发生错误: {e}", exc_info=True)
            return False


def main():
    """主函数"""
    # 创建清洗器实例
    cleaner = SocialMediaDataCleaner()

    # 运行清洗流程
    success = cleaner.run()

    if success:
        print("\n✅ 数据清洗成功完成！")
        print(f"请在 {cleaner.output_dir} 目录查看清洗结果")
        print("包含以下文件:")
        print("  - cleaned_comments.csv      # CSV格式")
        print("  - cleaned_comments.parquet  # Parquet格式（适合Spark）")
        print("  - cleaned_comments.jsonl    # JSON Lines格式")
        print("  - cleaning_report.json      # 清洗报告")
        print("  - data_cleaning.log         # 运行日志")
    else:
        print("\n❌ 数据清洗失败，请查看日志文件 data_cleaning.log")


if __name__ == "__main__":
    main()