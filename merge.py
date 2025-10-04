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
OUTPUT_BASE_DIR = './output'

# 数据列配置
MARKET_COLUMNS = ['code', 'market_code', '热度排名', '竞换手Z', '竞价金额', 
                 '竞价涨幅', '涨跌幅', '实体涨幅', '换手Z', '成交额', '大单净额', '上市板块', '几天几板']

ZT_COLUMNS = ['code', 'market_code', '涨停日期', '连板', '涨停封单额', 
             '涨停原因类别', '封成量比', '封流量比']

JYGS_COLUMNS = ['code', '异动日期', '热点', '热点导火索', '异动原因', '解析']

# 数值型列（需要填充为0）
NUMERIC_COLUMNS = ['市值Z', '成交额', '竞价金额', '大单净额', '涨停封单额', 
                  '热度排名', '竞换手Z', '连板', '开板次数', '封成量比', '封流量比']

# 字符串型列（需要填充为默认值）
STRING_COLUMNS = ['热点', '热点导火索', '异动原因', '解析']

# 金额转换配置（单位：元转亿元）
AMOUNT_CONVERSION = {
    '市值Z': (1e8, 0),      # 转换为亿元，保留整数
    '成交额': (1e8, 1),     # 转换为亿元，保留1位小数
    '大单净额': (1e8, 2),   # 转换为亿元，保留2位小数
    '涨停封单额': (1e8, 2), # 转换为亿元，保留2位小数
    '竞价金额': (1e8, 2)    # 转换为亿元，保留2位小数
}

# 配置日志
logger = get_logger("merge", "logs", "daily_research.log")

# 全局对象
trading_calendar = TradingCalendar()
dingding_robot = DingDingRobot()


def _load_previous_data(date_str: str) -> Optional[pd.DataFrame]:
    """加载前一交易日数据"""
    previous_date = trading_calendar.get_previous_trading_day(date_str)
    if not previous_date:
        return None
    
    previous_date_str = previous_date.strftime('%Y%m%d')
    previous_file = f"{OUTPUT_BASE_DIR}/{previous_date_str}/core_stocks.csv"
    
    if not os.path.exists(previous_file):
        logger.warning("未找到前一交易日的合并数据文件，使用原始股票池数据进行对比")
        return None
    
    try:
        df = pd.read_csv(previous_file, dtype={'code': str, 'market_code': str})
        if 'code' in df.columns:
            df['code'] = df['code'].astype(str).str.replace("'", "")
        return df
    except Exception as e:
        logger.warning(f"读取前一交易日数据失败: {e}")
        return None

def _save_stock_changes(current_df: pd.DataFrame, previous_df: pd.DataFrame, 
                       new_codes: set, removed_codes: set, date_str: str):
    """保存新增和移除的股票数据"""
    save_dir = f"output/{date_str}"
    os.makedirs(save_dir, exist_ok=True)
    
    # 保存新增股票
    if new_codes:
        new_stocks_df = current_df[current_df['code'].astype(str).isin(new_codes)].copy()
        if '重要度' in new_stocks_df.columns:
            new_stocks_df = new_stocks_df.sort_values('重要度', ascending=False)
        add_file_path = f"{save_dir}/add.csv"
        new_stocks_df.to_csv(add_file_path, index=False, encoding='utf-8-sig')
        logger.info(f"新增股票数据保存: {add_file_path}, 数量: {len(new_stocks_df)}")
    
    # 保存移除股票
    if removed_codes:
        removed_stocks_df = previous_df[previous_df['code'].astype(str).isin(removed_codes)].copy()
        if '重要度' in removed_stocks_df.columns:
            removed_stocks_df = removed_stocks_df.sort_values('重要度', ascending=False)
        remove_file_path = f"{save_dir}/remove.csv"
        removed_stocks_df.to_csv(remove_file_path, index=False, encoding='utf-8-sig')
        logger.info(f"移除股票数据保存: {remove_file_path}, 数量: {len(removed_stocks_df)}")

def _build_comparison_message(current_df: pd.DataFrame, previous_df: pd.DataFrame,
                            new_codes: set, removed_codes: set, previous_date) -> str:
    """构建对比消息"""
    current_codes = set(current_df['code'].astype(str))
    previous_codes = set(previous_df['code'].astype(str))
    
    message = [
        "📊 每日复盘:股票池",
        f"📅 对比基准: {previous_date.strftime('%Y-%m-%d')}",
        f"📈 今日: {len(current_codes)}只 | 昨日: {len(previous_codes)}只"
    ]
    
    if not new_codes and not removed_codes:
        message.append("✨ 股票池无变动")
        return "\n".join(message)
    
    # 新增股票详情
    if new_codes:
        message.append(f"🆕 新增: {len(new_codes)}只")
        new_stocks_info = current_df[current_df['code'].astype(str).isin(new_codes)].copy()
        if '重要度' in new_stocks_info.columns:
            new_stocks_info = new_stocks_info.sort_values('重要度', ascending=False)
        
        for _, stock in new_stocks_info.iterrows():
            code = stock['code']
            name = stock.get('股票简称', '未知')
            interval_info = stock.get('区间信息', '无')
            hotspot = stock.get('热点', '其他')
            message.append(f"  • {name}({code}) 热点:{hotspot} 区间:{interval_info}")
    
    # 移除股票详情
    if removed_codes:
        message.append(f"🔻 移除: {len(removed_codes)}只")
        removed_stocks_info = previous_df[previous_df['code'].astype(str).isin(removed_codes)].copy()
        if '重要度' in removed_stocks_info.columns:
            removed_stocks_info = removed_stocks_info.sort_values('重要度', ascending=False)
        
        for _, stock in removed_stocks_info.iterrows():
            code = stock['code']
            name = stock.get('股票简称', '未知')
            interval_info = stock.get('区间信息', '无')
            hotspot = stock.get('热点', '其他')
            message.append(f"  • {name}({code}) 热点:{hotspot} 区间:{interval_info}")
    
    return "\n".join(message)

def compare_previous(current_merged_df: pd.DataFrame, date_str: str = None) -> str:
    """与前一个交易日的合并后数据进行对比"""
    if date_str is None:
        date_str = trading_calendar.get_default_trade_date()
    
    # 加载前一交易日数据
    previous_df = _load_previous_data(date_str)
    if previous_df is None:
        return f"📊 今日股票池更新完成\n数据量: {len(current_merged_df)}只股票\n⚠️ 未找到前一交易日数据"
    
    # 清理当前数据的code列
    current_df_clean = current_merged_df.copy()
    if 'code' in current_df_clean.columns:
        current_df_clean['code'] = current_df_clean['code'].astype(str).str.replace("'", "")
    
    # 计算股票池变动
    current_codes = set(current_df_clean['code'].astype(str))
    previous_codes = set(previous_df['code'].astype(str))
    new_codes = current_codes - previous_codes
    removed_codes = previous_codes - current_codes
    
    # 保存变动数据
    _save_stock_changes(current_df_clean, previous_df, new_codes, removed_codes, date_str)
    
    # 构建并返回对比消息
    previous_date = trading_calendar.get_previous_trading_day(date_str)
    msg = _build_comparison_message(current_df_clean, previous_df, new_codes, removed_codes, previous_date)
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
    """清洗和格式化财务数据"""
    try:
        # 处理数值型列
        for column in NUMERIC_COLUMNS:
            if column in merged_data.columns:
                merged_data[column] = merged_data[column].replace([np.inf, -np.inf], np.nan)
                merged_data[column].fillna(0, inplace=True)
        
        # 处理字符串型列
        for column in STRING_COLUMNS:
            if column in merged_data.columns:
                merged_data[column].fillna('其他', inplace=True)
        
        # 金额单位转换（元转亿元）
        for column, (divisor, decimal_places) in AMOUNT_CONVERSION.items():
            if column in merged_data.columns:
                try:
                    merged_data[column] = merged_data[column].apply(
                        lambda x: round(float(x) / divisor, decimal_places) if pd.notnull(x) else 0
                    )
                except Exception as e:
                    logger.error(f"转换{column}列时发生错误: {e}")
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

def _get_available_columns(columns: list, df: pd.DataFrame) -> list:
    """获取DataFrame中实际存在的列"""
    return [col for col in columns if col in df.columns]

def _merge_market_data(stock_pool_data: pd.DataFrame, market_data: MarketData) -> pd.DataFrame:
    """合并市场数据"""
    # 逐步合并数据
    merged_data = pd.merge(
        stock_pool_data, 
        market_data.market_overview[_get_available_columns(MARKET_COLUMNS, market_data.market_overview)], 
        on=['code', 'market_code'], 
        how='left'
    )
    
    merged_data = pd.merge(
        merged_data, 
        market_data.zt_stocks[_get_available_columns(ZT_COLUMNS, market_data.zt_stocks)], 
        on=['code', 'market_code'], 
        how='left'
    )
    
    merged_data = pd.merge(
        merged_data, 
        market_data.jygs[_get_available_columns(JYGS_COLUMNS, market_data.jygs)], 
        on=['code'], 
        how='left'
    )
    
    return merged_data

def _format_data_columns(merged_data: pd.DataFrame) -> pd.DataFrame:
    """格式化数据列"""
    # 格式化代码和区间信息（添加单引号前缀）
    if 'code' in merged_data.columns and not merged_data.empty:
        merged_data['code'] = merged_data['code'].apply(lambda x: f"'{x}")
    
    if '区间信息' in merged_data.columns and not merged_data.empty:
        merged_data['区间信息'] = merged_data['区间信息'].apply(lambda x: f"'{x}")
    
    # 删除不需要的列
    if 'market_code' in merged_data.columns:
        merged_data.drop(columns=['market_code'], inplace=True)
    
    return merged_data

def merge_data(stock_pool_data: pd.DataFrame, 
               market_data: MarketData,
               output_prefix: str = 'core_stocks',
               date_str: str = None) -> Optional[pd.DataFrame]:
    """将股票池数据与市场数据进行合并"""
    try:
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
        
        # 处理空股票池的情况
        if stock_pool_data.empty:
            logger.info(f"{output_prefix}股票池为空，创建空的合并结果")
            merged_data = pd.DataFrame(columns=['code'])
        else:
            merged_data = _merge_market_data(stock_pool_data, market_data)
        
        # 格式化数据列
        merged_data = _format_data_columns(merged_data)
        
        # 清洗和格式化财务数据
        merged_data = clean_data(merged_data)
        
        # 保存合并后的数据
        output_dir = f"{OUTPUT_BASE_DIR}/{date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f'{output_dir}/{output_prefix}.csv'
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



def _format_stock_info(stock: pd.Series) -> str:
    """格式化股票信息"""
    code = str(stock['code']).replace("'", "")
    name = stock.get('股票简称', '未知')
    jygs_reason = stock.get('异动原因', '其他')
    hotspot_trigger = stock.get('热点导火索', '无')
    
    stock_info = f"  • {name}({code})"
    if jygs_reason and jygs_reason != '其他':
        stock_info += f" | 异动原因: {jygs_reason}"
    if hotspot_trigger and hotspot_trigger != '无':
        stock_info += f" | 导火索: {hotspot_trigger}"
    
    return stock_info

def generate_report(merged_first_board_data: pd.DataFrame, 
                   emerging_hotspots: list,
                   date_str: str = None) -> str:
    """生成新兴热点报告"""
    if date_str is None:
        date_str = trading_calendar.get_default_trade_date()
    
    if not emerging_hotspots:
        return "📊 今日新兴热点分析\n✨ 暂无新出现的热点"
    
    try:
        # 筛选新兴热点相关的股票
        emerging_hotspots_df = merged_first_board_data[
            merged_first_board_data['热点'].isin(emerging_hotspots)
        ].copy()
        
        if emerging_hotspots_df.empty:
            return f"📊 今日新兴热点分析\n🔍 发现{len(emerging_hotspots)}个新热点，但无相关首板股票数据"
        
        # 保存新兴热点数据
        save_dir = f"output/{date_str}"
        os.makedirs(save_dir, exist_ok=True)
        emerging_hotspots_file = f"{save_dir}/emerging_hotspots.csv"
        emerging_hotspots_df.to_csv(emerging_hotspots_file, index=False, encoding='utf-8-sig')
        logger.info(f"新兴热点数据保存: {emerging_hotspots_file}, 数量: {len(emerging_hotspots_df)}")
        
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
                
                # 按重要度排序
                if '重要度' in hotspot_stocks.columns:
                    hotspot_stocks = hotspot_stocks.sort_values('重要度', ascending=False)
                
                for _, stock in hotspot_stocks.iterrows():
                    message.append(_format_stock_info(stock))
                
                message.append("")  # 添加空行分隔
        
        msg = "\n".join(message)
        logger.info(msg)
        return msg
        
    except Exception as e:
        logger.error(f"生成新兴热点报告时发生错误: {e}")
        return f"📊 今日新兴热点分析\n❌ 生成报告时发生错误: {e}"


def merge():
    """合并股票池数据、市场数据、涨停数据和异动数据"""
    try:
        logger.info("开始执行合并数据主流程")
        
        # 获取当前交易日期
        current_date = trading_calendar.get_default_trade_date()
        logger.info(f"当前交易日期: {current_date}")
        
        # 步骤1: 加载所有数据
        logger.info("加载股票池和市场数据...")
        loaded_data = load_all_data()
        (core_stocks_data, first_board_stocks_data, 
         market_overview_data, zt_stocks_data, jygs_data) = loaded_data
        market_data = MarketData(market_overview_data, zt_stocks_data, jygs_data)

        # 步骤2: 合并核心股票池数据
        logger.info("合并核心股票池数据...")
        merged_core_data = merge_data(core_stocks_data, market_data, 'core_stocks', current_date)
        if merged_core_data is None:
            raise Exception("核心股票池数据合并失败")

        # 步骤3: 核心股票池对比分析
        logger.info("核心股票池对比分析...")
        comparison_msg = compare_previous(merged_core_data, current_date)
        dingding_robot.send_message(comparison_msg, 'robot3')

        # 步骤4: 合并首板股票池数据
        logger.info("合并首板股票池数据...")
        merged_first_board_data = merge_data(first_board_stocks_data, market_data, 'first_stocks', current_date)
        if merged_first_board_data is None:
            raise Exception("首板股票池数据合并失败")

        # 步骤5: 识别新兴热点并生成报告
        logger.info("识别新兴热点...")
        emerging_hotspots = identify_emerging_hotspots(date_str=current_date)
        hotspots_report_msg = generate_report(merged_first_board_data, emerging_hotspots, current_date)
        dingding_robot.send_message(hotspots_report_msg, 'robot3')

        logger.info("合并数据主流程执行完成")
    
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        dingding_robot.send_message(f"每日数据处理失败: {e}", 'robot3')
        raise


if __name__ == "__main__":
    merge()
