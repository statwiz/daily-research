import pywencai
import re
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
from log_setup import get_logger
from trading_calendar import TradingCalendar
from notification import DingDingRobot
from wencai import WencaiUtils
from utils import execute_with_retry, check_file_exists_after_time
import warnings
warnings.filterwarnings("ignore")


class StockPoolConfig:
    """股票池配置类"""
    # 重试配置
    MAX_RETRY_COUNT = 3
    RETRY_BASE_DELAY = 2  # 基础等待时间，实际等待时间为 (attempt + 1) * BASE_DELAY
    
    # 区间配置 [(天数, 排名)]
    INTERVAL_CONFIGS = [
        (2, 10),   # 最近2日前10名
        (3, 10),   # 最近3日前10名  
        (5, 10),   # 最近5日前10名
        (10, 10),   # 最近10日前10名
        (15, 5),   # 最近15日前5名
    ]
    
    # 重要度计算参数
    IMPORTANCE_ALPHA = 0.99  # 排名权重，越大越强调排名差距
    IMPORTANCE_BETA = 0.01   # 区间长度权重，越大越强调短区间
    
    # 数据保存配置
    DATA_SAVE_DIR = 'data'
    CSV_SUBDIR = 'csv'
    TXT_SUBDIR = 'txt'
    DATA_FILE_PREFIX = 'stock_pool'

    # 查询等待时间
    QUERY_SLEEP_SECONDS = 3
    MAIN_RETRY_SLEEP_SECONDS = 10
    
    # 股票池配置：(筛选条件, 描述)
    CORE_STOCK_CONFIGS = [
        (None, "全量股票池数据"),
        ('30', "筛选后股票池数据: 自由流通市值>30亿"),  
        ('60', "筛选后股票池数据: 自由流通市值>60亿"),
        ('100', "筛选后股票池数据: 自由流通市值>100亿"),
        ('200', "筛选后股票池数据: 自由流通市值>200亿"),
    ]


# 全局对象
trading_calendar = TradingCalendar()
dingding_robot = DingDingRobot()
logger = get_logger("stock_pool", "logs", "daily_research.log")


class StockPool:
    """股票池类 - 统一管理股票池数据的获取、计算、保存和分析"""
    
    def __init__(self):
        """初始化股票池对象"""


    @staticmethod
    def _validate_dataframe(df: pd.DataFrame, operation_name: str) -> bool:
        """验证DataFrame是否有效"""
        if df is None or df.empty:
            logger.warning(f"{operation_name}返回空数据")
            return False
        return True
    
    @staticmethod
    def _create_directories(date_str: str):
        """创建必要的目录结构"""
        os.makedirs(f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/{date_str}", exist_ok=True)
        os.makedirs(f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.TXT_SUBDIR}/stock_pool/{date_str}", exist_ok=True)
        
    @staticmethod
    def _save_file_pair(df: pd.DataFrame, file_prefix: str, date_str: str) -> dict:
        """保存CSV和TXT文件对"""
        paths = {}
        
        # 保存CSV文件
        csv_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/{date_str}/{file_prefix}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        paths['csv'] = csv_path
        logger.info(f"CSV保存完成: {csv_path}")
        
        # 保存TXT文件
        stock_codes = df['code'].astype(str).str.zfill(6).tolist()
        txt_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.TXT_SUBDIR}/stock_pool/{date_str}/{file_prefix}.txt"
        with open(txt_path, 'w', encoding='utf-8') as f:
            for code in stock_codes:
                f.write(f"{code}\n")
        paths['txt'] = txt_path
        logger.info(f"TXT保存完成: {txt_path}")
        
        return paths
        
    def read_stock_pool_data(self, date_str: str = None, prefix: str = 'core_stocks') -> pd.DataFrame:
        if date_str is None:
            date_str = trading_calendar.get_default_trade_date()
        csv_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/{date_str}/{prefix}.csv"
        return pd.read_csv(csv_path, dtype={'code': str, 'market_code': str})
        
    def save_stock_pool_data(self, df: pd.DataFrame, date_str: str = None, prefix: str = 'core_stocks', 
                            allow_zj: bool = True, market_value_threshold: float = 100) -> dict:
        """
        保存股票池数据到CSV和TXT文件
        
        Args:
            df: 要保存的DataFrame
            date_str: 日期字符串，如果为None则使用当前日期
            prefix: 文件名前缀
            allow_zj: 是否保存中军版
            market_value_threshold: 市值筛选阈值（亿元）
        
        Returns:
            dict: 保存的文件路径信息
        """
        if date_str is None:
            date_str = trading_calendar.get_default_trade_date()
        
        logger.info(f"开始保存股票池数据, 数据量: {len(df)}")
        
        # 验证数据
        if not StockPool._validate_dataframe(df, "保存股票池数据"):
            return {}
        
        # 检查必要的列
        if 'code' not in df.columns:
            raise ValueError("DataFrame中缺少'code'列")
        
        # 创建目录
        StockPool._create_directories(date_str)
        
        saved_paths = {}
        
        # 保存全量数据
        main_paths = StockPool._save_file_pair(df, prefix, date_str)
        saved_paths.update({
            'main_csv': main_paths['csv'],
            'main_txt': main_paths['txt']
        })
        
        # 保存中军版（如果需要）
        if allow_zj and '市值Z' in df.columns:
            threshold_value = market_value_threshold * 1e8
            filtered_df = df[df['市值Z'] > threshold_value]
            logger.info(f"筛选出大于{market_value_threshold}亿自由流通市值的股票数量: {len(filtered_df)}")
            
            if len(filtered_df) > 0:
                filtered_paths = StockPool._save_file_pair(filtered_df, f"{prefix}_zj", date_str)
                saved_paths.update({
                    'filtered_csv': filtered_paths['csv'],
                    'filtered_txt': filtered_paths['txt']
                })
            else:
                logger.warning(f"没有找到大于{market_value_threshold}亿自由流通市值的股票，跳过中军版数据保存")
        
        return saved_paths

    @staticmethod
    def calc_importance(df: pd.DataFrame, 
                       alpha: float = None, 
                       beta: float = None) -> pd.DataFrame:
        """
        基于原始数据聚合并计算股票重要度
        
        Args:
            df: 包含股票区间数据的DataFrame
            alpha: 排名权重参数，越大越强调排名差距
            beta: 区间长度权重参数，越大越强调短区间
        
        Returns:
            按重要度排序的DataFrame，包含区间信息和重要度得分
            
        计算逻辑:
            1. 分组字段: [交易日期, 股票简称, market_code, code]
            2. 区间信息: '区间长度-区间排名' 用 | 连接
            3. 重要度: 100 * sum(exp(-alpha * 排名) * exp(-beta * 区间长度))
            4. 排序: 按重要度降序
        """
        # 使用配置默认值
        if alpha is None:
            alpha = StockPoolConfig.IMPORTANCE_ALPHA
        if beta is None:
            beta = StockPoolConfig.IMPORTANCE_BETA
        
        logger.info(f"开始计算股票重要度, 数据量: {len(df)}, alpha: {alpha}, beta: {beta}")
        start_time = time.time()
        
        def aggregate_importance(sub_df):
            """聚合函数：计算单只股票的重要度"""
            interval_info = "|".join(
                f"{length}-{rank}" for length, rank in zip(sub_df["区间长度"], sub_df["区间排名"])
            )
            # 重要度计算：使用对数函数
            importance_score = float(
                100 * (1 / (np.log1p(sub_df["区间排名"]) * np.log1p(sub_df["区间长度"]))).sum()
            )

            return pd.Series({
                "区间信息": interval_info,
                "重要度": importance_score
            })

        # 按股票分组并计算重要度
        grouped_df = (
            df.groupby(["交易日期", "股票简称", "市值Z", "market_code", "code"], as_index=False)
            .apply(aggregate_importance)
        )

        # 按重要度降序排序
        grouped_df.sort_values(by="重要度", ascending=False, inplace=True)
        grouped_df.reset_index(drop=True, inplace=True)
        
        processing_time = time.time() - start_time
        logger.info(f"重要度计算完成, 耗时: {processing_time:.2f}秒, 结果数量: {len(grouped_df)}")
        
        return grouped_df[["交易日期", "股票简称", "市值Z", "market_code", "code", "区间信息", "重要度"]]

    def get_muti_top_stocks(self, selected=None):
        """
        获取股票池数据，通过多个时间区间组合计算重要度
        
        Args:
            selected: 是否使用筛选条件（非ST、非退市等）
        
        Returns:
            按重要度排序的股票池DataFrame
        """
        try:
            all_df = []
            # 使用配置类中的区间设置
            for days, rank in StockPoolConfig.INTERVAL_CONFIGS:
                logger.info(f'========== selected: {selected}  {days}-{rank} ===========')
                df = execute_with_retry(WencaiUtils.get_top_stocks, 
                                       max_retry_count=StockPoolConfig.MAX_RETRY_COUNT,
                                       retry_base_delay=StockPoolConfig.RETRY_BASE_DELAY,
                                       days=days, rank=rank, use_filters=selected)
                if not StockPool._validate_dataframe(df, f'{(days,rank)}获取数据'):
                    raise Exception(f'{(days,rank)}获取数据失败')
                all_df.append(df)
                time.sleep(StockPoolConfig.QUERY_SLEEP_SECONDS)
            
            df_all = pd.concat(all_df)
            df = StockPool.calc_importance(df_all, 
                               alpha=StockPoolConfig.IMPORTANCE_ALPHA, 
                               beta=StockPoolConfig.IMPORTANCE_BETA)
            return df
        except Exception as e:
            logger.error(f"获取股票池失败: {e}")
            return None


    def get_core_stocks_data(self) -> pd.DataFrame:
        """
        获取所有核心股票池数据
        
        Returns:
            包含所有股票池DataFrame的列表
        """
        try:
            data_list = []
            
            for i, (selected, description) in enumerate(StockPoolConfig.CORE_STOCK_CONFIGS, 1):
                step_name = "步骤1" if selected is None else f"步骤2.{i-1}"
                logger.info(f" ------------ {step_name}: 获取{description} ------------")
                
                data = self.get_muti_top_stocks(selected=selected)
                if not StockPool._validate_dataframe(data, description):
                    return None  # 任何一个获取失败就返回None
                
                logger.info(f'{description}获取成功, 数据量: {data.shape}')
                data_list.append(data)
                
            if len(data_list) == 0:
                return None
                
            combined_df = pd.concat(data_list, ignore_index=True)
            
            # 按股票分组，保留重要度最高的记录
            groupby_keys = ['交易日期', '股票简称', '市值Z', 'market_code', 'code']
            max_importance_idx = combined_df.groupby(groupby_keys)['重要度'].idxmax()
            df = (combined_df.loc[max_importance_idx]
                    .sort_values(by='重要度', ascending=False)
                    .reset_index(drop=True))
            df['重要度'] = df['重要度'].round(2)
            return df
        except Exception as e:
            logger.error(f"获取核心股票池数据失败: {e}")
            return None

    def get_first_breakout_stocks(self):
        """
        获取首板股票池数据
        """
        try:
            logger.info("开始获取所有的首板股票池数据")
            df = execute_with_retry(WencaiUtils.get_first_breakout_stocks, 
                                   k=11,
                                   use_filters=False)
            if not StockPool._validate_dataframe(df, "获取所有首板股票池数据"):
                raise Exception("获取所有首板股票池数据失败")
            logger.info(f"所有首板股票池数据量: {len(df)}")
            
            logger.info("开始获取自由流通市值大于100亿的首板股票池数据")
            df_zj = execute_with_retry(WencaiUtils.get_first_breakout_stocks, 
                                   k=11,
                                   use_filters=True)
            if not StockPool._validate_dataframe(df_zj, "获取自由流通市值大于100亿的首板股票池数据"):
                raise Exception("获取自由流通市值大于100亿的首板股票池数据失败")
            logger.info(f"自由流通市值大于100亿的首板股票池数据量: {len(df_zj)}")
            
            df = pd.concat([df, df_zj])
            # 去重
            df = df.drop_duplicates(subset=['股票简称'])
            output_columns = ['交易日期', '股票简称', '市值Z', 'market_code', 'code']
            return df[output_columns]
        except Exception as e:
            logger.error(f"获取首板股票池数据失败: {e}")
            return None

    def update_stock_pool_data(self):
        try:
            logger.info("开始执行股票池数据获取任务...")
            trade_date = trading_calendar.get_default_trade_date()
            
            # 检查目标文件是否已存在且在16点后生成
            base_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/{trade_date}"
            required_files = ["core_stocks.csv", "first_stocks_zj.csv"]
            
            # 检查每个文件是否存在且在16点后生成
            all_files_exist = True
            for filename in required_files:
                file_path = os.path.join(base_path, filename)
                if not check_file_exists_after_time(file_path, cutoff_hour=16):
                    all_files_exist = False
                    break
            
            if all_files_exist:
                logger.info(f"目标文件已存在且在16点后生成，跳过数据获取任务: {required_files}")
                return
            
            # 步骤1: 获取核心股票池数据
            core_df = self.get_core_stocks_data()
            if not StockPool._validate_dataframe(core_df, "核心股票池数据收集"):
                raise Exception("核心股票池数据收集失败")
            logger.info(f"核心股票池数据获取成功, 数据量: {core_df.shape}")

            # 步骤2: 获取并保存首板股票池数据
            logger.info("------------ 步骤2: 获取首板股票池数据------------")
            first_stocks_df = self.get_first_breakout_stocks()
            if not StockPool._validate_dataframe(first_stocks_df, "首板股票池数据收集"):
                raise Exception("首板股票池数据收集失败")
            logger.info(f"首板股票池数据获取成功, 数据量: {first_stocks_df.shape}")

            # 步骤3: 保存核心股票池数据和首板股票池数据
            logger.info("------------ 步骤3: 保存核心股票池数据和首板股票池数据------------")
            self.save_stock_pool_data(core_df, trade_date, prefix='core_stocks')
            self.save_stock_pool_data(first_stocks_df, trade_date, prefix='first_stocks')
            logger.info("核心股票池数据和首板股票池数据保存成功")

        except Exception as e:
            logger.error(f"股票池数据获取任务失败: {e}")
            raise



if __name__ == "__main__":
    stock_pool = StockPool()
    stock_pool.update_stock_pool_data()