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
from utils import execute_with_retry
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


# 全局对象
trading_calendar = TradingCalendar()
dingding_robot = DingDingRobot()
logger = get_logger("stock_pool", "logs", "daily_research.log")


class StockPool:
    """股票池类 - 统一管理股票池数据的获取、计算、保存和分析"""
    
    def __init__(self):
        """初始化股票池对象"""
        self.trading_calendar = trading_calendar
        self.dingding_robot = dingding_robot
        self.logger = logger


    def read_stock_pool_data(self, date_str: str = None) -> pd.DataFrame:
        """读取股票池数据"""
        if date_str is None:
            date_str = trading_calendar.get_default_trade_date()
        csv_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/core_stocks_{date_str}.csv"
        return pd.read_csv(csv_path, dtype={'code': str, 'market_code': str})
        
    def save_stock_pool_data(self, df: pd.DataFrame, date_str: str = None,prefix: str = 'core_stocks',
                            market_value_threshold: float = 100) -> dict:
        """
        保存股票池数据到CSV和TXT文件
        
        Args:
            df: 要保存的DataFrame
            date_str: 日期字符串，如果为None则使用当前日期
            market_value_threshold: 市值筛选阈值（亿元）
        
        Returns:
            dict: 保存的文件路径信息
        """
        if date_str is None:
            date_str = trading_calendar.get_default_trade_date()
        
        self.logger.info(f"开始保存股票池数据, 数据量: {len(df)}")
        saved_paths = {}
        
        # 创建所需的目录结构
        os.makedirs(f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool", exist_ok=True)
        os.makedirs(f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.TXT_SUBDIR}/stock_pool", exist_ok=True)
        
        if df.empty:
            self.logger.warning("没有找到数据，跳过保存")
            return saved_paths
        
        # 检查必要的列
        if 'code' not in df.columns:
            raise ValueError("DataFrame中缺少'code'列")
        
        # 保存全量数据到CSV
        csv_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/{prefix}_{date_str}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        saved_paths['main_csv'] = csv_path
        self.logger.info(f"股票池全量CSV保存完成, 路径: {csv_path}")
        
        # 保存全量代码到TXT
        stock_codes = df['code'].astype(str).str.zfill(6).tolist()
        txt_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.TXT_SUBDIR}/stock_pool/{prefix}_{date_str}.txt"
        with open(txt_path, 'w', encoding='utf-8') as f:
            for code in stock_codes:
                f.write(f"{code}\n")
        saved_paths['main_txt'] = txt_path
        self.logger.info(f"股票池全量版代码保存完成, 路径: {txt_path}")
        
        # 如果有市值列，保存中军版
        if '市值Z' in df.columns:
            threshold_value = market_value_threshold * 1e8
            filtered_df = df[df['市值Z'] > threshold_value]
            self.logger.info(f"筛选出大于{market_value_threshold}亿自由流通市值的股票数量: {len(filtered_df)}")
            
            if len(filtered_df) > 0:
                # 保存中军版CSV
                filtered_csv_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/{prefix}_zj_{date_str}.csv"
                filtered_df.to_csv(filtered_csv_path, index=False, encoding='utf-8-sig')
                saved_paths['filtered_csv'] = filtered_csv_path
                self.logger.info(f"股票池中军版CSV保存完成, 路径: {filtered_csv_path}")
                
                # 保存中军版代码到TXT
                filtered_stock_codes = filtered_df['code'].astype(str).str.zfill(6).tolist()
                filtered_txt_path = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.TXT_SUBDIR}/stock_pool/{prefix}_zj_{date_str}.txt"
                with open(filtered_txt_path, 'w', encoding='utf-8') as f:
                    for code in filtered_stock_codes:
                        f.write(f"{code}\n")
                saved_paths['filtered_txt'] = filtered_txt_path
                self.logger.info(f"股票池中军版代码保存完成, 路径: {filtered_txt_path}")
            else:
                self.logger.warning(f"没有找到大于{market_value_threshold}亿自由流通市值的股票，跳过中军版数据保存")
        
        return saved_paths

    def compare_with_previous_trading_day(self, current_df: pd.DataFrame, date_str: str = None) -> str:
        """与前一个交易日的数据进行对比，返回对比消息，并保存新增和减少的股票数据"""
        if date_str is None:
            date_str = trading_calendar.get_default_trade_date()
        
        # 获取前一交易日数据
        previous_date = self.trading_calendar.get_previous_trading_day(date_str)
        if not previous_date:
            return f"📊 今日股票池更新完成\n数据量: {len(current_df)}只股票\n⚠️ 未找到前一交易日数据"
        
        # 查找前一交易日的CSV文件
        previous_date_str = previous_date.strftime('%Y%m%d')
        previous_file = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool/core_stocks_{previous_date_str}.csv"
        
        if not os.path.exists(previous_file):
            return f"📊 今日股票池更新完成\n数据量: {len(current_df)}只股票\n⚠️ 未找到前一交易日数据文件"
        
        try:
            # 读取CSV文件，指定股票代码列为字符串类型以保持前导零
            previous_df = pd.read_csv(previous_file, dtype={'code': str, 'market_code': str})
        except Exception as e:
            self.logger.warning(f"读取前一交易日数据失败: {e}")
            return f"📊 今日股票池更新完成\n数据量: {len(current_df)}只股票\n⚠️ 读取前一交易日数据失败"
        
        # 基于股票代码计算变动（确保唯一性）
        current_codes = set(current_df['code'].astype(str))
        previous_codes = set(previous_df['code'].astype(str))
        
        # 获取新增和移除的股票代码
        new_codes = current_codes - previous_codes
        removed_codes = previous_codes - current_codes
        
        # 保存新增和减少的股票数据到CSV文件
        save_dir = f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}/stock_pool"
        os.makedirs(save_dir, exist_ok=True)
        
        # 保存新增股票数据
        if len(new_codes) > 0:
            new_stocks_df = current_df[current_df['code'].astype(str).isin(new_codes)].copy()
            new_stocks_df = new_stocks_df.sort_values('重要度', ascending=False)
            add_file_path = f"{save_dir}/add_{date_str}.csv"
            new_stocks_df.to_csv(add_file_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"新增股票数据保存完成, 路径: {add_file_path}, 数量: {len(new_stocks_df)}")
        
        # 保存减少股票数据
        if len(removed_codes) > 0:
            removed_stocks_df = previous_df[previous_df['code'].astype(str).isin(removed_codes)].copy()
            removed_stocks_df = removed_stocks_df.sort_values('重要度', ascending=False)
            remove_file_path = f"{save_dir}/remove_{date_str}.csv"
            removed_stocks_df.to_csv(remove_file_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"减少股票数据保存完成, 路径: {remove_file_path}, 数量: {len(removed_stocks_df)}")
        
        # 构建基础消息
        date_fmt = f"{previous_date.strftime('%Y-%m-%d')}"
        message = [
            "📊 每日复盘:股票池",
            f"📅 对比基准: {date_fmt}",
            f"📈 今日: {len(current_codes)}只 | 昨日: {len(previous_codes)}只"
        ]
        
        if len(new_codes) == 0 and len(removed_codes) == 0:
            message.append("✨ 股票池无变动")
        else:
            # 新增股票详情
            if len(new_codes) > 0:
                message.append(f"🆕 新增: {len(new_codes)}只")
                
                # 获取新增股票的详细信息
                new_stocks_info = current_df[current_df['code'].astype(str).isin(new_codes)].copy()
                new_stocks_info = new_stocks_info.sort_values('重要度', ascending=False)
                
                for _, stock in new_stocks_info.iterrows():
                    code = stock['code']
                    name = stock['股票简称']
                    interval_info = stock.get('区间信息', '无')
                    message.append(f"  • {name}({code}) 区间:{interval_info}")
            
            # 移除股票统计
            if len(removed_codes) > 0:
                message.append(f"🔻 移除: {len(removed_codes)}只")
                
                # 获取移除股票的详细信息（从前一天的数据中）
                removed_stocks_info = previous_df[previous_df['code'].astype(str).isin(removed_codes)].copy()
                removed_stocks_info = removed_stocks_info.sort_values('重要度', ascending=False)
                
                for _, stock in removed_stocks_info.iterrows():
                    code = stock['code']
                    name = stock['股票简称']
                    interval_info = stock.get('区间信息', '无')
                    message.append(f"  • {name}({code}) 区间:{interval_info}")
        
        msg = "\n".join(message)
        self.logger.info(msg)
        return msg

    def calc_importance(self, df: pd.DataFrame, 
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
        
        self.logger.info(f"开始计算股票重要度, 数据量: {len(df)}, alpha: {alpha}, beta: {beta}")
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
        self.logger.info(f"重要度计算完成, 耗时: {processing_time:.2f}秒, 结果数量: {len(grouped_df)}")
        
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
                self.logger.info(f'========== selected: {selected}  {days}-{rank} ===========')
                df = execute_with_retry(WencaiUtils.get_top_stocks, 
                                       max_retry_count=StockPoolConfig.MAX_RETRY_COUNT,
                                       retry_base_delay=StockPoolConfig.RETRY_BASE_DELAY,
                                       days=days, rank=rank, use_filters=selected)
                if df is None or df.empty:
                    raise Exception(f'{(days,rank)}获取数据失败')
                all_df.append(df)
                time.sleep(StockPoolConfig.QUERY_SLEEP_SECONDS)
            
            df_all = pd.concat(all_df)
            df = self.calc_importance(df_all, 
                               alpha=StockPoolConfig.IMPORTANCE_ALPHA, 
                               beta=StockPoolConfig.IMPORTANCE_BETA)
            return df
        except Exception as e:
            self.logger.error(f"获取股票池失败: {e}")
            return None


    def get_core_stocks_data(self) -> pd.DataFrame:
        """
        获取所有核心股票池数据
        
        Returns:
            包含所有股票池DataFrame的列表
        """
        try:
            data_list = []
            
            # 股票池配置：(筛选条件, 描述)
            pool_configs = [
                (None, "全量股票池数据"),
                ('30', "筛选后股票池数据: 自由流通市值>30亿"),  
                ('60', "筛选后股票池数据: 自由流通市值>60亿"),
                ('100', "筛选后股票池数据: 自由流通市值>100亿"),
                ('200', "筛选后股票池数据: 自由流通市值>200亿"),
            ]
            
            for i, (selected, description) in enumerate(pool_configs, 1):
                step_name = "步骤1" if selected is None else f"步骤2.{i-1}"
                self.logger.info(f" ------------ {step_name}: 获取{description} ------------")
                
                data = self.get_muti_top_stocks(selected=selected)
                if data is None or data.empty:
                    self.logger.warning(f"{description}获取失败，跳过本次尝试")
                    return None  # 任何一个获取失败就返回None
                
                self.logger.info(f'{description}获取成功, 数据量: {data.shape}')
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
            self.logger.error(f"获取核心股票池数据失败: {e}")
            return None

    def get_first_breakout_stocks(self):
        """
        获取首板股票池数据
        """
        try:
            self.logger.info("开始获取所有的首板股票池数据")
            df = execute_with_retry(WencaiUtils.get_first_breakout_stocks, 
                                   use_filters=False)
            if df is None or df.empty:
                raise Exception("获取所有首板股票池数据失败")
            self.logger.info(f"所有首板股票池数据量: {len(df)}")
            # 保存所有首板股票池数据
            # self.save_stock_pool_codes(df, trade_date)
            self.logger.info("开始获取自由流通市值大于100亿的首板股票池数据")
            df_zj = execute_with_retry(WencaiUtils.get_first_breakout_stocks, 
                                   use_filters=True)
            if df_zj is None or df_zj.empty:
                raise Exception("获取自由流通市值大于100亿的首板股票池数据失败")
            self.logger.info(f"自由流通市值大于100亿的首板股票池数据量: {len(df_zj)}")
            df = pd.concat([df, df_zj])
            # 去重
            df = df.drop_duplicates(subset=['股票简称'])
            return df
        except Exception as e:
            self.logger.error(f"获取首板股票池数据失败: {e}")
            return None

    def run(self):

        self.logger.info("开始执行股票池数据获取任务...")
        
        # 检查是否为交易日
        # today = datetime.now().strftime('%Y%m%d')
        # if not self.trading_calendar.is_trading_day(today):
        #     self.logger.info(f"今日({today})非交易日，跳过执行")
        #     return
        
        for i in range(StockPoolConfig.MAX_RETRY_COUNT):
            try:
                if i > 0:
                    self.logger.info(f"------------ 第{i}次重试 ------------")
                # 步骤1: 获取核心股票池数据
                core_df = self.get_core_stocks_data()
                if core_df is None or core_df.empty:
                    self.logger.warning("核心股票池数据收集失败，跳过本次尝试")
                    continue            
                # 获取交易日期
                trade_date = core_df['交易日期'].iloc[0]
                # 步骤2: 保存首板股票池数据
                self.logger.info("------------ 步骤2: 保存首板股票池数据------------")
                first_stocks_df = self.get_first_breakout_stocks()
                if first_stocks_df is None or first_stocks_df.empty:
                    self.logger.warning("首板股票池数据收集失败，跳过本次尝试")
                    continue
                self.save_stock_pool_data(first_stocks_df, trade_date, prefix='first_stocks')
                
                # 步骤3: 保存核心股票池数据
                self.logger.info("------------ 步骤2: 保存核心股票池数据------------")
                self.save_stock_pool_data(core_df, trade_date, prefix='core_stocks')
                
                # 步骤4: 数据对比分析
                self.logger.info("------------ 步骤3: 与前一交易日数据进行对比分析------------")
                comparison_msg = self.compare_with_previous_trading_day(core_df, trade_date)
                
                # 步骤4: 发送数据对比结果到钉钉
                self.logger.info("------------ 步骤4: 发送数据对比结果到钉钉------------")
                self.logger.info(f"任务完成! 最终数据量: {core_df.shape}")
                self.dingding_robot.send_message(comparison_msg, 'robot3')
                break                        
            except Exception as e:
                if i == StockPoolConfig.MAX_RETRY_COUNT - 1:
                    # 最后一次尝试失败
                    error_msg = f"今日股票池更新失败: {e}"
                    self.logger.error(f"重试{StockPoolConfig.MAX_RETRY_COUNT}次后仍失败: {e}")
                    self.dingding_robot.send_message(error_msg, 'robot3')
                else:
                    # 还有重试机会
                    self.logger.warning(f"第{i+1}次尝试失败，准备重试: {e}")
                
                if i < StockPoolConfig.MAX_RETRY_COUNT - 1:
                    time.sleep(StockPoolConfig.MAIN_RETRY_SLEEP_SECONDS)


def main():
    """主函数：使用StockPool类执行任务"""
    stock_pool = StockPool()
    stock_pool.run()


if __name__ == "__main__":
    main()