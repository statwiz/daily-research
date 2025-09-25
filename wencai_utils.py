"""
问财API工具类
用于统一管理所有与问财接口交互的函数
"""

import pywencai
import pandas as pd
import numpy as np
import re
import time
from typing import Optional
import logging
from log_setup import get_logger
# 使用标准的logger命名方式，会自动继承主脚本的日志配置
# logger = logging.getLogger(__name__)
logger = get_logger("wencai_utils", "logs", "daily_research.log")

class WencaiUtils:
    """问财API工具类"""
    
    @staticmethod
    def extract_trade_date(df: pd.DataFrame) -> Optional[str]:
        """
        从 DataFrame 的列名中提取第一个形如 [YYYYMMDD] 的日期字符串。
        如果未找到则返回 None。
        """
        for col in df.columns:
            single_date_match = re.search(r'\[(\d{8})\]', col)
            if single_date_match:
                return single_date_match.group(1)
            date_range_match = re.search(r'\[(\d{8}-\d{8})\]', col)
            if date_range_match:
                return date_range_match.group(1).split('-')[1]
        return None

    @staticmethod
    def remove_date_suffix(df: pd.DataFrame) -> pd.DataFrame:
        """
        删除 DataFrame 列名中形如 [YYYYMMDD]和[YYYYMMDD-YYYYMMDD] 的日期后缀。
        返回修改后的 DataFrame（不会修改原 df）。
        """
        column_mapping = {}
        for col in df.columns:
            if re.search(r'\[\d{8}\]', col):
                new_name = re.sub(r'\[\d{8}\]', '', col)
                column_mapping[col] = new_name
            elif re.search(r'\[\d{8}-\d{8}\]', col):
                new_name = re.sub(r'\[\d{8}-\d{8}\]', '', col)
                column_mapping[col] = new_name
        return df.rename(columns=column_mapping)

    @staticmethod
    def clean_dataframe(df: pd.DataFrame, columns: list[str] = None) -> pd.DataFrame:
        """
        仅对指定列剔除正无穷/负无穷，并删除这些列中含 NaN 的行，
        然后重置索引并返回新 DataFrame。

        参数
        ----
        df : pd.DataFrame
            需要清洗的数据
        columns : list[str] or None
            需要清洗的列名列表；为 None 时对全部列处理

        返回
        ----
        pd.DataFrame
            清洗后的新 DataFrame
        """
        cleaned = df.copy()
        cols = columns if columns is not None else cleaned.columns
        # 只处理指定列
        cleaned[cols] = cleaned[cols].replace([np.inf, -np.inf], np.nan)
        # 只要指定列中出现 NaN 就删整行
        cleaned.dropna(subset=cols, inplace=True)
        cleaned.reset_index(drop=True, inplace=True)
        remove_num = df.shape[0] - cleaned.shape[0]
        if remove_num > 0:
            logger.info(f"数据清洗完成, 剔除异常行数: {remove_num}")
        return cleaned
 
    @staticmethod
    def get_top_stocks(days: int = 5, rank: int = 5, use_filters: bool = False) -> pd.DataFrame:
        """
        获取指定天数内涨幅排名前N的股票数据
        
        Args:
            days: 统计天数
            rank: 取前N名
            use_filters: 是否使用筛选条件（非ST、非退市、上市时间>30天、流通市值>100亿）
        
        Returns:
            处理后的股票数据DataFrame
        """
        logger.info(f"开始获取{days}日内前{rank}名股票数据, 使用筛选: {use_filters}")
        start_time = time.time()
        
        # 构建查询语句
        if use_filters:
            query_text = (f"非新股,非ST,股票简称不包含退,上市天数大于30,流通市值大于100亿,"
                         f"最近{days}个交易日的区间涨跌幅从大到小排序前{rank}")
        else:
            query_text = f"非新股,最近{days}个交易日的区间涨跌幅从大到小排序前{rank}"
        
        logger.info(f"查询语句: {query_text}")
        
        try:
            # 调用问财API获取数据
            raw_df = pywencai.get(query=query_text, query_type='stock')
            logger.info(f"原始数据获取成功, 数据量: {len(raw_df)}")
            
            # 数据处理
            df = raw_df.copy()
            df['交易日期'] = WencaiUtils.extract_trade_date(df)
            df = WencaiUtils.remove_date_suffix(df)
            
            # 重命名列
            column_mapping = {
                '区间涨跌幅:前复权': '区间涨幅', 
                '区间涨跌幅:前复权排名': '区间排名'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            # 添加区间长度
            df['区间长度'] = days
            
            # 数据类型转换
            # 处理排名列（格式：1/4532 -> 1）
            df['区间排名'] = (df['区间排名']
                            .astype(str)
                            .str.split('/')
                            .str[0]
                            .astype(int))
            
            # 处理数值列
            if '区间涨幅' in df.columns:
                df['区间涨幅'] = df['区间涨幅'].astype(float).round(2)
            
            # 处理字符串列
            for col in ['market_code', 'code']:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # 选择需要的列
            required_columns = ['交易日期', '股票简称', '区间长度', '区间涨幅', '区间排名', 'market_code', 'code']
            result = df[required_columns]        
            return result
            
        except Exception as e:
            logger.error(f"获取股票数据失败: {e}")
            raise

    @staticmethod
    def get_first_breakout_stocks() -> pd.DataFrame:
        """
        获取首次突破股票数据
        """
        logger.info(f"开始获取首次突破股票数据")
        query_text = f"最新涨跌幅大于9.5%,前10个交易日至昨日的涨幅超过9.5%的次数等于0"
        logger.info(f"查询语句: {query_text}")
        try:
            df = pywencai.get(query=query_text, query_type='stock', loop = True)
            logger.info(f"原始数据获取成功, 数据量: {len(df)}")
            df['交易日期'] = WencaiUtils.extract_trade_date(df)
            df = WencaiUtils.remove_date_suffix(df)
            df.rename(columns={'涨跌幅:前复权': '涨跌幅'}, inplace=True)
            for col in ['涨跌幅']:
                if col in df.columns:
                    df[col] = df[col].astype(float).round(2)
            for col in ['market_code', 'code']:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            format_name = ['交易日期','股票简称','涨跌幅','market_code','code']
            df = df[format_name]
            return df
        except Exception as e:
            logger.error(f"获取首次突破股票数据失败: {e}")
            raise
    
    @staticmethod
    def get_market_overview_data(loop: bool = False, is_bj_exchange: bool = False) -> pd.DataFrame:
        """
        获取A股市场全景数据，包含基础行情、资金流向、热度等信息
        
        Returns:
            包含市场全景数据的DataFrame，包括：
            - 基础行情：开盘价、最高价、最低价、收盘价
            - 前复权数据：开盘价_前、最高价_前、最低价_前、收盘价_前
            - 成交数据：成交额、成交量、换手率
            - 资金流向：大单净额
            - 市场热度：热度排名
            - 竞价数据：竞价涨幅、竞价金额、竞价量、竞换手率
            - 基础信息：上市板块、上市天数
        """
        logger.info("开始获取市场全景数据")
        start_time = time.time()
        if is_bj_exchange:
            logger.info("开始获取北交所市场全景数据")
            query_text = 'A股,北交所,上市板块,上市天数,开盘价,最高价,最低价,收盘价,前复权:开盘价,前复权:最高价,前复权:最低价,前复权:收盘价,成交额,成交量,竞价涨幅,竞价金额,竞价量,实际换手率,自由流通市值,自由流通股,个股热度排名'

        else:
            logger.info("开始获取A股市场全景数据(非北交所)")
            query_text = 'A股,非北交所,上市板块,上市天数,开盘价,最高价,最低价,收盘价,前复权:开盘价,前复权:最高价,前复权:最低价,前复权:收盘价,成交额,成交量,竞价涨幅,竞价金额,竞价量,dde大单净额,实际换手率,自由流通市值,自由流通股,个股热度排名'
        
        logger.info(f"查询语句: {query_text}")
        
        try:
            # 调用问财API获取数据
            raw_df = pywencai.get(query=query_text, query_type='stock', loop = loop)
            logger.info(f"原始数据获取成功, 数据量: {len(raw_df)}")
            
            # 数据处理
            df = raw_df.copy()
            trade_date = WencaiUtils.extract_trade_date(df)
            df = WencaiUtils.remove_date_suffix(df)
            
            # 列名映射
            if is_bj_exchange:
                df['dde大单净额'] = 0
            column_mapping = {
                '开盘价:前复权': '开盘价_前',
                '收盘价:前复权': '收盘价_前',
                '最高价:前复权': '最高价_前',
                '最低价:前复权': '最低价_前',
                '开盘价:不复权': '开盘价',
                '收盘价:不复权': '收盘价',
                '最高价:不复权': '最高价',
                '最低价:不复权': '最低价',
                '最新涨跌幅': '涨跌幅',
                '自由流通市值': '市值Z',
                '实际换手率': '换手Z',
                'dde大单净额': '大单净额',
                '个股热度排名': '热度排名'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            # 定义数据类型处理列
            int_columns = ['市值Z', '上市天数', '大单净额', '热度排名', '自由流通股', 
                          '成交量', '成交额', '竞价量', '竞价金额']
            float_columns = ['涨跌幅', '竞价涨幅', '换手Z', 
                           '开盘价', '最高价', '最低价', '收盘价',
                           '开盘价_前', '最高价_前', '最低价_前', '收盘价_前']
            str_columns = ['market_code', 'code']
            
            # 数据清洗
            df = WencaiUtils.clean_dataframe(df, int_columns + float_columns + str_columns)
            
            # 数据类型转换
            for col in int_columns:
                if col in df.columns:
                    df[col] = df[col].astype(float).astype(int)
                    
            for col in float_columns:
                if col in df.columns:
                    df[col] = df[col].astype(float).round(2)
                    
            for col in str_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # 添加交易日期
            df['交易日期'] = trade_date
            
            # 计算衍生指标
            # 竞价换手率 = 竞价量 / 自由流通股 * 100
            df['竞换手Z'] = df.apply(
                lambda x: round(x['竞价量'] / x['自由流通股'] * 100, 2) 
                if x['自由流通股'] > 0 else -1000, 
                axis=1
            )
            
            # 实体涨幅 = (收盘价 / 开盘价 - 1) * 100
            df['实体涨幅'] = df.apply(
                lambda x: round((x['收盘价'] / x['开盘价'] - 1) * 100, 2) 
                if x['开盘价'] > 0 else -1000, 
                axis=1
            )
            
            # 选择输出列
            output_columns = [
                '交易日期', '股票简称', '涨跌幅', '实体涨幅', '大单净额', '热度排名',
                '开盘价', '最高价', '最低价', '收盘价',
                '成交额', '成交量', '市值Z', '换手Z',
                '竞价涨幅', '竞价金额', '竞价量', '竞换手Z',
                '开盘价_前', '最高价_前', '最低价_前', '收盘价_前',
                '上市板块', '上市天数',
                'market_code', 'code'
            ]
            
            result = df[output_columns]
            elapsed_time = time.time() - start_time
            logger.info(f"市场全景数据获取完成, 最终数据量: {len(result)}, 耗时: {elapsed_time:.2f}秒")
            
            return result
            
        except Exception as e:
            logger.error(f"获取市场全景数据失败: {e}")
            raise

    @staticmethod
    def get_zt_stocks() -> pd.DataFrame:
        """
        获取今日涨停股票数据，包含涨停相关的详细信息
        
        Args:
            loop: 是否启用问财的循环查询模式，默认False
            
        Returns:
            包含涨停股票数据的DataFrame，包括：
            - 基础信息：交易日期、股票简称、上市板块
            - 市值信息：自由流通市值
            - 涨停信息：涨跌幅、连续涨停次数、几天几板
            - 时间信息：首次涨停时间、最终涨停时间
            - 涨停特征：涨停类型、涨停原因类别
            - 资金信息：涨停封单额、开板次数
            - 成交特征：封单量占成交量比、封单量占流通股比
        """
        logger.info("开始获取今日涨停股票数据")
        start_time = time.time()
        
        query_text = ("今日涨停,涨跌幅,连续涨停次数,几天几板,涨停时间,涨停类型,"
                     "涨停原因类别,封单金额,自由流通市值,上市板块")
        
        logger.info(f"查询语句: {query_text}")
        
        try:
            # 调用问财API获取数据
            raw_df = pywencai.get(query=query_text, query_type='stock', loop=True)
            logger.info(f"原始数据获取成功, 数据量: {len(raw_df)}")
            
            # 数据处理
            df = raw_df.copy()
            trade_date = WencaiUtils.extract_trade_date(df)
            df = WencaiUtils.remove_date_suffix(df)
            
            # 列名映射
            column_mapping = {
                '涨跌幅:前复权': '涨跌幅',
                '自由流通市值': '市值Z',
                '连续涨停天数': '连板',
                '涨停开板次数': '开板次数',
                '涨停封单量占成交量比': '封成量比',
                '涨停封单量占流通a股比': '封流量比'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            # 定义数据类型处理列
            int_columns = ['市值Z', '涨停封单额', '连板', '开板次数']
            float_columns = ['涨跌幅', '封成量比', '封流量比']
            str_columns = ['market_code', 'code']
            
            # 数据清洗
            df = WencaiUtils.clean_dataframe(df, int_columns + float_columns + str_columns)
            
            # 数据类型转换
            for col in int_columns:
                if col in df.columns:
                    df[col] = df[col].astype(float).astype(int)
                    
            for col in float_columns:
                if col in df.columns:
                    df[col] = df[col].astype(float).round(2)
                    
            for col in str_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # 添加交易日期
            df['交易日期'] = trade_date
            
            # 选择输出列
            output_columns = [
                '交易日期', '股票简称', '上市板块', '市值Z', '涨跌幅', '连板', '几天几板',
                '首次涨停时间', '涨停类型', '涨停原因类别', '涨停封单额', '最终涨停时间',
                '开板次数', '封成量比', '封流量比', 'market_code', 'code'
            ]
            
            result = df[output_columns]
            elapsed_time = time.time() - start_time
            logger.info(f"涨停股票数据获取完成, 最终数据量: {len(result)}, 耗时: {elapsed_time:.2f}秒")
            
            return result
            
        except Exception as e:
            logger.error(f"获取涨停股票数据失败: {e}")
            raise





if __name__ == "__main__":
    df = WencaiUtils.get_zt_stocks()
    print(df)