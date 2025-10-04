"""
合并股票池、同花顺行情数据、涨停数据和异动数据
"""
import os
import sys
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from typing import Tuple, Optional
from stock_pool import StockPool
from wencai import WencaiUtils
from jygs import JygsUtils
from log_setup import get_logger
from trading_calendar import TradingCalendar
from notification import DingDingRobot

# 配置常量
OUTPUT_DIR = './data/csv'
OUTPUT_FILE_PREFIX = 'merge_'

# 配置日志
logger = get_logger("merge", "logs", "daily_research.log")

# 全局对象
trading_calendar = TradingCalendar()
dingding_robot = DingDingRobot()


def compare_previous(current_merged_df: pd.DataFrame, date_str: str = None) -> str:
    """
    与前一个交易日的合并后数据进行对比，返回对比消息，并保存新增和减少的股票宽表数据
    
    Args:
        current_merged_df: 当前合并后的宽表数据
        date_str: 日期字符串，默认为None时使用当前交易日
        
    Returns:
        str: 对比消息
    """
    if date_str is None:
        date_str = trading_calendar.get_default_trade_date()
    
    # 获取前一交易日数据
    previous_date = trading_calendar.get_previous_trading_day(date_str)
    if not previous_date:
        return f"📊 今日股票池更新完成\n数据量: {len(current_merged_df)}只股票\n⚠️ 未找到前一交易日数据"
    
    # 查找前一交易日的合并后CSV文件
    previous_date_str = previous_date.strftime('%Y%m%d')
    previous_file = f"data/csv/merge_core_stocks_{previous_date_str}.csv"
    
    if not os.path.exists(previous_file):
        logger.warning("未找到前一交易日的合并数据文件，使用原始股票池数据进行对比")
        # 如果没有合并数据，尝试使用原始股票池数据
        previous_raw_file = f"data/csv/stock_pool/core_stocks_{previous_date_str}.csv"
        if not os.path.exists(previous_raw_file):
            return f"📊 今日股票池更新完成\n数据量: {len(current_merged_df)}只股票\n⚠️ 未找到前一交易日数据文件"
        previous_file = previous_raw_file
    
    try:
        previous_df = pd.read_csv(previous_file, dtype={'code': str, 'market_code': str})
        if 'code' in previous_df.columns:
            previous_df['code'] = previous_df['code'].astype(str).str.replace("'", "")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        logger.warning(f"前一交易日数据文件不存在或为空: {previous_file}")
        return f"📊 今日股票池更新完成\n数据量: {len(current_merged_df)}只股票\n⚠️ 无历史数据对比"
    except Exception as e:
        logger.warning(f"读取前一交易日数据失败: {e}")
        return f"📊 今日股票池更新完成\n数据量: {len(current_merged_df)}只股票\n⚠️ 读取历史数据失败"
    
    # 清理当前数据的code列
    current_df_clean = current_merged_df.copy()
    if 'code' in current_df_clean.columns:
        current_df_clean['code'] = current_df_clean['code'].astype(str).str.replace("'", "")
    
    # 基于股票代码计算变动（确保唯一性）
    current_codes = set(current_df_clean['code'].astype(str))
    previous_codes = set(previous_df['code'].astype(str))
    
    # 获取新增和移除的股票代码
    new_codes = current_codes - previous_codes
    removed_codes = previous_codes - current_codes
    
    # 保存新增和减少的股票宽表数据到CSV文件
    save_dir = "data/csv"
    os.makedirs(save_dir, exist_ok=True)
    
    # 保存新增股票宽表数据
    if len(new_codes) > 0:
        new_stocks_df = current_df_clean[current_df_clean['code'].astype(str).isin(new_codes)].copy()
        if '重要度' in new_stocks_df.columns:
            new_stocks_df = new_stocks_df.sort_values('重要度', ascending=False)
        add_file_path = f"{save_dir}/merge_add_{date_str}.csv"
        new_stocks_df.to_csv(add_file_path, index=False, encoding='utf-8-sig')
        logger.info(f"新增股票宽表数据保存完成, 路径: {add_file_path}, 数量: {len(new_stocks_df)}")
    
    # 保存减少股票宽表数据
    if len(removed_codes) > 0:
        removed_stocks_df = previous_df[previous_df['code'].astype(str).isin(removed_codes)].copy()
        if '重要度' in removed_stocks_df.columns:
            removed_stocks_df = removed_stocks_df.sort_values('重要度', ascending=False)
        remove_file_path = f"{save_dir}/merge_remove_{date_str}.csv"
        removed_stocks_df.to_csv(remove_file_path, index=False, encoding='utf-8-sig')
        logger.info(f"减少股票宽表数据保存完成, 路径: {remove_file_path}, 数量: {len(removed_stocks_df)}")
    
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
            new_stocks_info = current_df_clean[current_df_clean['code'].astype(str).isin(new_codes)].copy()
            if '重要度' in new_stocks_info.columns:
                new_stocks_info = new_stocks_info.sort_values('重要度', ascending=False)
            
            for _, stock in new_stocks_info.iterrows():
                code = stock['code']
                name = stock.get('股票简称', '未知')
                interval_info = stock.get('区间信息', '无')
                hotspot = stock.get('热点', '其他')
                message.append(f"  • {name}({code}) 热点:{hotspot} 区间:{interval_info}")
        
        # 移除股票统计
        if len(removed_codes) > 0:
            message.append(f"🔻 移除: {len(removed_codes)}只")
            
            # 获取移除股票的详细信息（从前一天的数据中）
            removed_stocks_info = previous_df[previous_df['code'].astype(str).isin(removed_codes)].copy()
            if '重要度' in removed_stocks_info.columns:
                removed_stocks_info = removed_stocks_info.sort_values('重要度', ascending=False)
            
            for _, stock in removed_stocks_info.iterrows():
                code = stock['code']
                name = stock.get('股票简称', '未知')
                interval_info = stock.get('区间信息', '无')
                hotspot = stock.get('热点', '其他')
                message.append(f"  • {name}({code}) 热点:{hotspot} 区间:{interval_info}")
    
    msg = "\n".join(message)
    logger.info(msg)
    return msg

def load_all_data() -> Optional[Tuple]:
    """
    加载所有市场数据，包括股票池、行情数据、涨停数据和异动数据
    
    Returns:
        Optional[Tuple]: 包含所有数据DataFrame的元组，如果加载失败则返回None
            - core_stocks_data: 核心股票池数据
            - added_stocks_data: 新增股票池数据  
            - removed_stocks_data: 减少股票池数据
            - first_board_stocks_data: 首板股票池数据
            - market_overview_data: 同花顺行情概览数据
            - zt_stocks_data: 涨停股票数据
            - jygs_data: 异动股票数据
    """
    try:
        logger.info("开始加载市场数据...")
        
        # 创建股票池实例，避免重复创建
        stock_pool_manager = StockPool()
        
        logger.info("1.正在读取核心股票池数据...")
        core_stocks_data = stock_pool_manager.read_stock_pool_data(prefix='core_stocks')
        if core_stocks_data is None or core_stocks_data.empty:
            logger.error("核心股票池数据为空")
            raise Exception("核心股票池数据为空")
        logger.info(f"核心股票池数据量: {len(core_stocks_data)}")

        logger.info("2.正在读取首板股票池数据...")
        first_board_stocks_data = stock_pool_manager.read_stock_pool_data(prefix='first_stocks')
        if first_board_stocks_data is None or first_board_stocks_data.empty:
            logger.error("首板股票池数据为空")
            raise Exception("首板股票池数据为空")
        logger.info(f"首板股票池数据量: {len(first_board_stocks_data)}")
        
        logger.info("3. 正在读取市场行情数据...")
        market_overview_data = WencaiUtils.read_market_overview_data()
        if market_overview_data is None or market_overview_data.empty:
            logger.error("市场行情数据为空")
            raise Exception("市场行情数据为空")
        logger.info(f"市场行情数据量: {len(market_overview_data)}")
        
        logger.info("4.正在读取更新后的涨停数据...")
        zt_stocks_data = WencaiUtils.read_latest_zt_stocks()
        if zt_stocks_data is None or zt_stocks_data.empty:
            logger.error("涨停股票数据为空")
            raise Exception("涨停股票数据为空")
        logger.info(f"涨停股票数据量: {len(zt_stocks_data)}")
        zt_stocks_data.rename(columns={'交易日期': '涨停日期'}, inplace=True)
        
        logger.info("5.正在读取更新后的异动数据...")
        jygs_data = JygsUtils.read_stocks_data()
        if jygs_data is None or jygs_data.empty:
            logger.error("异动股票数据为空")
            raise Exception("异动股票数据为空")
        logger.info(f"异动股票数据量: {len(jygs_data)}")
        jygs_data.rename(columns={'交易日期': '异动日期'}, inplace=True)
        
        logger.info("所有必要的数据加载完成")
        return (core_stocks_data, 
                first_board_stocks_data, market_overview_data, 
                zt_stocks_data, jygs_data)
                
    except Exception as e:
        logger.error(f"加载市场数据失败: {e}")
        raise

def clean_data(merged_data: pd.DataFrame) -> pd.DataFrame:
    """
    清洗和格式化财务数据，并处理所有类型的NaN值
    
    Args:
        merged_data: 合并后的数据DataFrame
        
    Returns:
        pd.DataFrame: 清洗和格式化后的数据
    """
    try:
        # 定义不同类型列的NaN填充规则
        # 数值型列：填充为0
        numeric_columns = ['市值Z', '成交额', '竞价金额', '大单净额', '涨停封单额', 
                          '热度排名', '竞换手Z', '连板', '开板次数', '封成量比', '封流量比']
        
        # 字符串型列：填充为空字符串或默认值
        string_columns = ['热点', '热点导火索', '异动原因', '解析']
        
        # 处理数值型列
        for column in numeric_columns:
            if column in merged_data.columns:
                # 替换无穷值为NaN
                merged_data[column] = merged_data[column].replace([np.inf, -np.inf], np.nan)
                # 填充NaN为0
                merged_data[column].fillna(0, inplace=True)
        
        # 处理字符串型列
        for column in string_columns:
            if column in merged_data.columns:
                # 填充NaN为空字符串
                merged_data[column].fillna('其他', inplace=True)
        
        # 将金额单位转换为亿元，添加异常处理
        conversion_config = {
            '市值Z': (1e8, 0),      # 转换为亿元，保留整数
            '成交额': (1e8, 1),     # 转换为亿元，保留1位小数
            '大单净额': (1e8, 2),   # 转换为亿元，保留2位小数
            '涨停封单额': (1e8, 2), # 转换为亿元，保留2位小数
            '竞价金额': (1e8, 2)    # 转换为亿元，保留2位小数
        }
        
        for column, (divisor, decimal_places) in conversion_config.items():
            if column in merged_data.columns:
                try:
                    merged_data[column] = merged_data[column].apply(
                        lambda x: round(float(x) / divisor, decimal_places) if pd.notnull(x) else 0
                    )
                    # logger.info(f"已转换{column}列的单位为亿元")
                except Exception as e:
                    logger.error(f"转换{column}列时发生错误: {e}")
                    # 如果转换失败，保持原值
                    continue
        
        return merged_data
        
    except Exception as e:
        logger.error(f"清洗和格式化财务数据时发生错误: {e}")
        return merged_data


class MarketData:
    """市场数据封装类"""
    def __init__(self, market_overview: pd.DataFrame, zt_stocks: pd.DataFrame, jygs: pd.DataFrame):
        self.market_overview = market_overview
        self.zt_stocks = zt_stocks
        self.jygs = jygs

def merge_data(stock_pool_data: pd.DataFrame, 
               market_data: MarketData,
               output_prefix: str = 'core_stocks',
               date_str: str = None) -> Optional[pd.DataFrame]:
    """
    将股票池数据与市场数据进行合并
    
    Args:
        stock_pool_data: 股票池数据
        market_data: 市场数据封装对象
        output_prefix: 输出文件前缀
        date_str: 日期字符串，默认为None时使用当前交易日
        
    Returns:
        Optional[pd.DataFrame]: 合并后的数据，如果合并失败则返回None
    """
    try:
        # 获取日期字符串
        if date_str is None:
            date_str = trading_calendar.get_default_trade_date()
        
        logger.info(f"开始合并{output_prefix}数据...")
        
        # 验证输入数据    
        if not all([
            stock_pool_data is not None and not stock_pool_data.empty,
            market_data.market_overview is not None and not market_data.market_overview.empty,
            market_data.zt_stocks is not None and not market_data.zt_stocks.empty,
            market_data.jygs is not None and not market_data.jygs.empty
        ]):
            logger.error("输入数据验证失败，无法进行合并")
            return None
        
        # 选择需要的市场概览列，检查列是否存在
        market_columns = ['code', 'market_code', '热度排名', '竞换手Z', '竞价金额', 
                         '竞价涨幅', '涨跌幅', '实体涨幅', '换手Z', '成交额', '大单净额', '上市板块','几天几板']
        available_market_columns = [col for col in market_columns if col in market_data.market_overview.columns]
        
        # 选择需要的涨停数据列
        zt_columns = ['code', 'market_code', '涨停日期', '连板', '涨停封单额', 
                           '涨停原因类别', '封成量比', '封流量比']
        available_zt_columns = [col for col in zt_columns if col in market_data.zt_stocks.columns]
        
        # 选择需要的异动数据列
        jygs_columns = ['code', '异动日期', '热点', '热点导火索', '异动原因', '解析']
        available_jygs_columns = [col for col in jygs_columns if col in market_data.jygs.columns]
        
        # 处理空股票池的情况
        if stock_pool_data.empty:
            logger.info(f"{output_prefix}股票池为空，创建空的合并结果")
            # 创建空的DataFrame，包含必要的列结构
            merged_data = pd.DataFrame(columns=['code'])
        else:
            # 逐步合并数据
            logger.info("正在合并市场概览数据...")
            merged_data = pd.merge(stock_pool_data, 
                                  market_data.market_overview[available_market_columns], 
                                  on=['code', 'market_code'], 
                                  how='left')
            
            logger.info("正在合并涨停数据...")
            merged_data = pd.merge(merged_data, 
                                  market_data.zt_stocks[available_zt_columns], 
                                  on=['code', 'market_code'], 
                                  how='left')
            logger.info("正在合并异动数据...")
            merged_data = pd.merge(merged_data, 
                                  market_data.jygs[available_jygs_columns], 
                                  on=['code'], 
                                  how='left')
        
        # 格式化代码和区间信息（添加单引号前缀）
        if 'code' in merged_data.columns and not merged_data.empty:
            merged_data['code'] = merged_data['code'].apply(lambda x: f"'{x}")
        
        if '区间信息' in merged_data.columns and not merged_data.empty:
            merged_data['区间信息'] = merged_data['区间信息'].apply(lambda x: f"'{x}")
        
        # 删除不需要的列
        if 'market_code' in merged_data.columns:
            merged_data.drop(columns=['market_code'], inplace=True)
        
        # 清洗和格式化财务数据
        merged_data = clean_data(merged_data)
        
        # 确保输出目录存在
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 保存合并后的数据（加上日期）
        output_path = f'{OUTPUT_DIR}/{OUTPUT_FILE_PREFIX}{output_prefix}_{date_str}.csv'
        merged_data.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"{output_prefix}数据合并完成，已保存到: {output_path}")
        
        return merged_data
        
    except Exception as e:
        logger.error(f"合并{output_prefix}数据失败: {e}")
        return None



def identify_emerging_hotspots(date_str: str = None, lookback_days: int = 10) -> list:
    """
    识别今日新出现的热点
    
    Args:
        lookback_days: 回看的交易日天数，默认10天
        
    Returns:
        list: 今日新出现的热点列表
    """
    def is_hotspot_similar(hotspot_a: str, hotspot_b: str) -> bool:
        """
        判断两个热点是否相似(通过字符串包含关系)
        
        Args:
            hotspot_a: 热点A
            hotspot_b: 热点B
            
        Returns:
            bool: 如果存在包含关系则返回True
        """
        return (hotspot_a in hotspot_b) or (hotspot_b in hotspot_a)
    
    try:
        # 读取板块历史数据
        bk_historical_data = JygsUtils.read_bk_data()
        if bk_historical_data.empty:
            logger.error("板块历史数据为空")
            raise Exception("板块历史数据为空")
            
        trading_dates = bk_historical_data['交易日期'].unique()
        if len(trading_dates) == 0 or date_str != trading_dates[0]:
            logger.error("没有找到交易日期的板块数据")
            raise Exception("没有找到交易日期的板块数据")
        
        # 获取过去N个交易日的日期范围
        past_trading_dates = trading_dates[1:lookback_days+1] if len(trading_dates) > lookback_days else trading_dates[1:]
        
        # 提取过去N天的所有热点
        past_hotspots = bk_historical_data[bk_historical_data['交易日期'].isin(past_trading_dates)]['热点'].unique()
        # 获取今日热点数据
        latest_trading_date = trading_dates[0]
        today_data = bk_historical_data[bk_historical_data['交易日期'] == latest_trading_date]
        today_hotspots = today_data['热点'].unique()
        # 筛选新兴热点
        emerging_hotspots = []
        for today_hotspot in today_hotspots:
            is_existing = False
            for past_hotspot in past_hotspots:
                if is_hotspot_similar(today_hotspot, past_hotspot):
                    is_existing = True
                    break
            
            if not is_existing and today_hotspot != '其他':
                emerging_hotspots.append(today_hotspot)
        
        logger.info(f"识别到 {len(emerging_hotspots)} 个新兴热点: {emerging_hotspots}")
        return emerging_hotspots
        
    except Exception as e:
        logger.error(f"识别新兴热点时发生错误: {e}")
        raise



def generate_report(merged_first_board_data: pd.DataFrame, 
                                     emerging_hotspots: list,
                                     date_str: str = None) -> str:
    """
    生成新兴热点报告，保存宽表数据并生成格式化消息
    
    Args:
        merged_first_board_data: 合并后的首板股票宽表数据
        emerging_hotspots: 新兴热点列表
        date_str: 日期字符串，默认为None时使用当前交易日
        
    Returns:
        str: 格式化的热点报告消息
    """
    if date_str is None:
        date_str = trading_calendar.get_default_trade_date()
    
    if not emerging_hotspots or len(emerging_hotspots) == 0:
        return "📊 今日新兴热点分析\n✨ 暂无新出现的热点"
    
    try:
        # 筛选出新兴热点相关的股票
        emerging_hotspots_df = merged_first_board_data[
            merged_first_board_data['热点'].isin(emerging_hotspots)
        ].copy()
        
        if emerging_hotspots_df.empty:
            return f"📊 今日新兴热点分析\n🔍 发现{len(emerging_hotspots)}个新热点，但无相关首板股票数据"
        
        # 保存新兴热点宽表数据到csv文件
        save_dir = "data/csv"
        os.makedirs(save_dir, exist_ok=True)
        
        emerging_hotspots_file = f"{save_dir}/merge_emerging_hotspots_{date_str}.csv"
        emerging_hotspots_df.to_csv(emerging_hotspots_file, index=False, encoding='utf-8-sig')
        logger.info(f"新兴热点宽表数据保存完成, 路径: {emerging_hotspots_file}, 数量: {len(emerging_hotspots_df)}")
        
        # 构建格式化消息
        message = [
            "📊 今日新兴热点分析",
            f"🔥 发现{len(emerging_hotspots)}个新热点，涉及{len(emerging_hotspots_df)}只首板股票",
            ""
        ]
        
        # 按热点分组展示股票信息
        for hotspot in emerging_hotspots:
            hotspot_stocks = emerging_hotspots_df[emerging_hotspots_df['热点'] == hotspot]
            if not hotspot_stocks.empty:
                message.append(f"🎯 热点: {hotspot}")
                
                # 按重要度排序（如果有的话）
                if '重要度' in hotspot_stocks.columns:
                    hotspot_stocks = hotspot_stocks.sort_values('重要度', ascending=False)
                
                for _, stock in hotspot_stocks.iterrows():
                    # 清理code列的单引号前缀
                    code = str(stock['code']).replace("'", "")
                    name = stock.get('股票简称', '未知')
                    jygs_reason = stock.get('异动原因', '其他')
                    hotspot_trigger = stock.get('热点导火索', '无')
                    
                    # 格式化股票信息
                    stock_info = f"  • {name}({code})"
                    if jygs_reason and jygs_reason != '其他':
                        stock_info += f" | 异动原因: {jygs_reason}"
                    if hotspot_trigger and hotspot_trigger != '无':
                        stock_info += f" | 导火索: {hotspot_trigger}"
                    
                    message.append(stock_info)
                
                message.append("")  # 添加空行分隔不同热点
        
        msg = "\n".join(message)
        logger.info(msg)
        return msg
        
    except Exception as e:
        logger.error(f"生成新兴热点报告时发生错误: {e}")
        return f"📊 今日新兴热点分析\n❌ 生成报告时发生错误: {e}"


def merge():
    """
    合并股票池数据、市场数据、涨停数据和异动数据
    """
    try:
        logger.info("开始执行合并数据主流程")
        
        # 获取当前交易日期
        current_date = trading_calendar.get_default_trade_date()
        logger.info(f"当前交易日期: {current_date}")
        
        logger.info("------------ 步骤1: 加载股票池数据------------")
        loaded_data = load_all_data()
        (core_stocks_data, 
         first_board_stocks_data, 
         market_overview_data, 
         zt_stocks_data, 
         jygs_data) = loaded_data
        # 封装市场数据
        market_data = MarketData(market_overview_data, zt_stocks_data, jygs_data)

        logger.info("------------ 步骤2: 合并核心股票池数据------------")
        merged_core_data = merge_data(core_stocks_data, market_data, 'core_stocks', current_date)
        
        if merged_core_data is None:
            raise Exception("核心股票池数据合并失败")
        logger.info("核心股票池数据合并完成")

        logger.info("------------ 步骤3: 核心股票池与上一个交易日对比分析------------")
        comparison_msg = compare_previous(merged_core_data, current_date)
        dingding_robot.send_message(comparison_msg, 'robot3')
        logger.info("核心股票池对比分析完成")

        logger.info("------------ 步骤4: 合并首板股票池数据------------")

        merged_first_board_data = merge_data(first_board_stocks_data, market_data, 'first_stocks', current_date)
        
        if merged_first_board_data is None:
            raise Exception("首板股票池数据合并失败")
        logger.info("首板股票池数据合并完成")

        logger.info("------------ 步骤5: 识别今日新出现的热点------------")
        emerging_hotspots = identify_emerging_hotspots(date_str=current_date)
        
        # 生成新兴热点报告并发送消息
        hotspots_report_msg = generate_report(
            merged_first_board_data, 
            emerging_hotspots, 
            current_date
        )
        dingding_robot.send_message(hotspots_report_msg, 'robot3')
        logger.info("新兴热点分析完成")

        logger.info("合并数据主流程执行完成")
    
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        dingding_robot.send_message(f"每日数据处理失败: {e}", 'robot3')
        raise


if __name__ == "__main__":
    merge()
