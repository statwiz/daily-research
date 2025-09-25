import pywencai
import re
import pandas as pd
from typing import Optional
import numpy as np
import time
import os
from datetime import datetime, timedelta
from log_setup import get_logger
from utils import send_dingding_msg, trading_calendar
from wencai_utils import WencaiUtils
import akshare as ak
import configparser

import warnings
warnings.filterwarnings("ignore")

# 配置类 - 统一管理所有参数
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
        (10, 5),   # 最近10日前5名
        (15, 3),   # 最近15日前3名
        (20, 2)    # 最近20日前2名
    ]
    
    # 重要度计算参数
    IMPORTANCE_ALPHA = 0.999  # 排名权重，越大越强调排名差距
    IMPORTANCE_BETA = 0.001   # 区间长度权重，越大越强调短区间
    
    # 数据保存配置
    DATA_SAVE_DIR = 'data'
    CSV_SUBDIR = 'csv'
    TXT_SUBDIR = 'txt'
    DATA_FILE_PREFIX = 'stock_pool'
    
    @classmethod
    def get_csv_save_path(cls, date_str: str = None) -> str:
        """生成CSV文件保存路径"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
        return f"{cls.DATA_SAVE_DIR}/{cls.CSV_SUBDIR}/{cls.DATA_FILE_PREFIX}_{date_str}.csv"
    
    @classmethod
    def get_txt_save_path(cls, date_str: str = None) -> str:
        """生成TXT文件保存路径"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
        return f"{cls.DATA_SAVE_DIR}/{cls.TXT_SUBDIR}/{cls.DATA_FILE_PREFIX}_{date_str}.txt"
    
    @classmethod 
    def get_data_save_path(cls, date_str: str = None) -> str:
        """获取主要数据保存路径"""
        return cls.get_csv_save_path(date_str)
    
    # 查询等待时间
    QUERY_SLEEP_SECONDS = 3
    MAIN_RETRY_SLEEP_SECONDS = 10

# 日志配置
logger = get_logger("stock_pool", "logs", "stock_pool.log")




def compare_with_previous_trading_day(current_df: pd.DataFrame, date_str: str = None) -> str:
    """与前一个交易日的数据进行对比，返回对比消息"""
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    
    # 获取前一交易日数据
    previous_date = trading_calendar.get_previous_trading_day(date_str)
    if not previous_date:
        return f"📊 今日股票池更新完成\n数据量: {len(current_df)}只股票\n⚠️ 未找到前一交易日数据"
    
    # 查找前一交易日的CSV文件
    previous_date_str = previous_date.strftime('%Y%m%d')
    previous_file = StockPoolConfig.get_csv_save_path(previous_date_str)
    
    if not os.path.exists(previous_file):
        return f"📊 今日股票池更新完成\n数据量: {len(current_df)}只股票\n⚠️ 未找到前一交易日数据文件"
    
    try:
        # 读取CSV文件，指定股票代码列为字符串类型以保持前导零
        previous_df = pd.read_csv(previous_file, dtype={'code': str, 'market_code': str})
    except Exception as e:
        logger.warning(f"读取前一交易日数据失败: {e}")
        return f"📊 今日股票池更新完成\n数据量: {len(current_df)}只股票\n⚠️ 读取前一交易日数据失败"
    
    # 基于股票代码计算变动（确保唯一性）
    current_codes = set(current_df['code'].astype(str))
    previous_codes = set(previous_df['code'].astype(str))
    
    # 获取新增和移除的股票代码
    new_codes = current_codes - previous_codes
    removed_codes = previous_codes - current_codes
    
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
    logger.info(msg)
    return msg



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
        
        # 重要度计算：使用指数衰减
        importance_score = float(100 * (
            np.exp(-alpha * sub_df["区间排名"]) * 
            np.exp(-beta * sub_df["区间长度"])
        ).sum())
        
        return pd.Series({
            "区间信息": interval_info,
            "重要度": importance_score
        })

    # 按股票分组并计算重要度
    grouped_df = (
        df.groupby(["交易日期", "股票简称", "market_code", "code"], as_index=False)
        .apply(aggregate_importance)
    )

    # 按重要度降序排序
    grouped_df.sort_values(by="重要度", ascending=False, inplace=True)
    grouped_df.reset_index(drop=True, inplace=True)
    
    processing_time = time.time() - start_time
    logger.info(f"重要度计算完成, 耗时: {processing_time:.2f}秒, 结果数量: {len(grouped_df)}")
    
    return grouped_df[["交易日期", "股票简称", "market_code", "code", "区间信息", "重要度"]]



def execute_with_retry(func, *args, **kwargs):
    """
    带重试机制的函数包装器
    
    Args:
        func: 要执行的函数
        *args, **kwargs: 函数参数
    
    Returns:
        函数执行结果
        
    Raises:
        最后一次失败的异常
    """
    last_exception = None
    retry_count = StockPoolConfig.MAX_RETRY_COUNT
    func_name = getattr(func, '__name__', str(func))
    
    logger.info(f"开始执行函数 {func_name}, 最大重试次数: {retry_count}")
    
    for i in range(retry_count):
        try:
            logger.debug(f"第{i + 1}次尝试执行 {func_name}")
            result = func(*args, **kwargs)
            
            if i > 0:
                logger.info(f"函数 {func_name} 在第{i + 1}次尝试后成功执行")
            
            return result
            
        except Exception as e:
            last_exception = e
            if i < retry_count - 1:  # 不是最后一次尝试
                wait_time = (i + 1) * StockPoolConfig.RETRY_BASE_DELAY
                logger.warning(f"函数 {func_name} 第{i + 1}次尝试失败，{wait_time}秒后重试: {e}")
                time.sleep(wait_time)
            else:
                logger.error(f"函数 {func_name} 重试{retry_count}次后仍失败: {e}")
    
    # 所有重试都失败了，抛出最后的异常
    raise last_exception
    

def get_stock_pool(selected=False):
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
            df = execute_with_retry(WencaiUtils.get_top_stocks, days=days, rank=rank, use_filters=selected)
            if df is None or df.empty:
                raise Exception(f'{(days,rank)}获取数据失败')
            all_df.append(df)
            time.sleep(StockPoolConfig.QUERY_SLEEP_SECONDS)
        
        df_all = pd.concat(all_df)
        df = calc_importance(df_all, 
                           alpha=StockPoolConfig.IMPORTANCE_ALPHA, 
                           beta=StockPoolConfig.IMPORTANCE_BETA)
        return df
    except Exception as e:
        logger.error(f"获取股票池失败: {e}")
        return None

def save_stock_pool_codes(df: pd.DataFrame, date_str: str = None) -> str:
    """
    将股票池的代码保存为txt文件
    
    Args:
        df: 包含股票数据的DataFrame
        date_str: 日期字符串，如果为None则使用当前日期
    
    Returns:
        保存的txt文件路径
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"开始保存股票池代码到txt文件, 股票数量: {len(df)}")
    start_time = time.time()
    
    # 检查必要的列
    if 'code' not in df.columns:
        raise ValueError("DataFrame中缺少'code'列")
    
    # 获取股票代码列表，并确保格式正确（6位数字）
    stock_codes = df['code'].astype(str).str.zfill(6).tolist()
    
    # 确保保存目录存在
    txt_path = StockPoolConfig.get_txt_save_path(date_str)
    txt_dir = os.path.dirname(txt_path)
    os.makedirs(txt_dir, exist_ok=True)
    
    # 保存代码到txt文件，每行一个代码
    with open(txt_path, 'w', encoding='utf-8') as f:
        for code in stock_codes:
            f.write(f"{code}\n")
    
    processing_time = time.time() - start_time
    logger.info(f"股票池代码保存完成, 耗时: {processing_time:.2f}秒, 保存路径: {txt_path}")
    
    return txt_path

def main():
    """
    主函数：获取股票池数据并保存
    
    执行流程：
    1. 检查是否为交易日
    2. 获取全量股票池数据（无筛选）
    3. 获取筛选后的股票池数据（有筛选条件）  
    4. 合并数据并去重（同一股票保留重要度最高的记录）
    5. 保存结果并发送通知
    """
    logger.info("开始执行股票池数据获取任务...")
    
    # 检查是否为交易日
    today = datetime.now().strftime('%Y%m%d')
    if not trading_calendar.is_trading_day(today):
        logger.info(f"今日({today})非交易日，跳过执行")
        return
    
    for i in range(StockPoolConfig.MAX_RETRY_COUNT):
        try:
            # 获取全量数据（无筛选条件）
            logger.info("步骤1: 获取全量股票池数据")
            full_data = get_stock_pool(selected=False)
            if full_data is None or full_data.empty:
                logger.warning("全量数据获取失败，跳过本次尝试")
                continue
            logger.info(f'全量数据获取成功, 数据量: {full_data.shape}')
            
            # 获取筛选数据（有筛选条件）
            logger.info("步骤2: 获取筛选后股票池数据")
            filtered_data = get_stock_pool(selected=True)
            if filtered_data is None or filtered_data.empty:
                logger.warning("筛选数据获取失败，跳过本次尝试")
                continue
            logger.info(f'筛选数据获取成功, 数据量: {filtered_data.shape}')
            
            # 合并并去重处理
            logger.info("步骤3: 合并数据并去重")
            combined_df = pd.concat([full_data, filtered_data], ignore_index=True)
            
            # 按股票分组，保留重要度最高的记录
            groupby_keys = ['交易日期', '股票简称', 'market_code', 'code']
            max_importance_idx = combined_df.groupby(groupby_keys)['重要度'].idxmax()
            final_df = (combined_df.loc[max_importance_idx]
                       .sort_values(by='重要度', ascending=False)
                       .reset_index(drop=True))
            
            # 步骤4: 数据对比分析
            logger.info("步骤4: 与前一交易日数据进行对比分析")
            comparison_msg = compare_with_previous_trading_day(final_df)
            
            # 步骤5: 保存数据
            logger.info("步骤5: 保存数据")
            save_path = StockPoolConfig.get_csv_save_path()
            # 创建所需的目录结构
            os.makedirs(f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.CSV_SUBDIR}", exist_ok=True)
            os.makedirs(f"{StockPoolConfig.DATA_SAVE_DIR}/{StockPoolConfig.TXT_SUBDIR}", exist_ok=True)
            
            # 保存为CSV格式，能更好地保持数据类型
            final_df.to_csv(save_path, index=False, encoding='utf-8-sig')

            # 步骤6: 保存股票池代码到txt文件
            logger.info("步骤6: 保存股票池代码到txt文件")
            save_stock_pool_codes(final_df)
            
            # 步骤7: 发送对比结果通知
            logger.info("步骤7: 发送数据对比结果到钉钉")
            logger.info(f"任务完成! 最终数据量: {final_df.shape}, 保存路径: {save_path}")
            send_dingding_msg(comparison_msg)
            return  # 成功完成，退出函数
            
        except Exception as e:
            if i == StockPoolConfig.MAX_RETRY_COUNT - 1:
                # 最后一次尝试失败
                error_msg = f"每日复盘 \n 今日股票池更新失败: {e}"
                logger.error(f"重试{StockPoolConfig.MAX_RETRY_COUNT}次后仍失败: {e}")
                send_dingding_msg(error_msg)
            else:
                # 还有重试机会
                logger.warning(f"第{i+1}次尝试失败，准备重试: {e}")
            
            if i < StockPoolConfig.MAX_RETRY_COUNT - 1:
                time.sleep(StockPoolConfig.MAIN_RETRY_SLEEP_SECONDS)

if __name__ == "__main__":
    main()