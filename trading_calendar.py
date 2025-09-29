# -*- coding: utf-8 -*-
"""
交易日历模块
管理交易日相关的功能
"""

from datetime import datetime, timedelta
import pandas as pd
import logging
import akshare as ak

# 使用标准的logger命名方式，会自动继承主脚本的日志配置
logger = logging.getLogger(__name__)


class TradingCalendar:
    """
    交易日历工具类
    """
    
    def __init__(self):
        self._trading_days = None
        self._last_update = None
    
    def _load_trading_calendar(self):
        """加载交易日历数据"""
        try:
            logger.info("正在获取交易日历...")
            trade_df = ak.tool_trade_date_hist_sina()
            # 转换为pandas.Timestamp格式并存储
            trade_df['trade_date'] = pd.to_datetime(trade_df['trade_date'])
            self._trading_days = set(trade_df['trade_date'])
            self._last_update = datetime.now()
            logger.info(f"交易日历加载成功，共{len(self._trading_days)}个交易日")
        except Exception as e:
            logger.error(f"获取交易日历失败: {e}")
            self._trading_days = set()
    
    def _ensure_calendar_loaded(self):
        """确保交易日历已加载"""
        if self._trading_days is None:
            self._load_trading_calendar()
    
    def is_trading_day(self, date):
        """
        判断指定日期是否为交易日
        
        参数:
            date: 日期，支持字符串(YYYYMMDD)、datetime对象或pandas.Timestamp
        
        返回:
            bool: True表示是交易日，False表示非交易日
        """
        self._ensure_calendar_loaded()
        
        # 转换为pandas.Timestamp
        if isinstance(date, str):
            # 假设输入格式为YYYYMMDD
            if len(date) == 8:
                date_obj = pd.Timestamp(f"{date[:4]}-{date[4:6]}-{date[6:8]}")
            else:
                date_obj = pd.Timestamp(date)
        elif isinstance(date, datetime):
            date_obj = pd.Timestamp(date)
        else:
            date_obj = pd.Timestamp(date)
        
        return date_obj in self._trading_days
    
    def get_default_trade_date(self):
        """获取默认交易日期，延迟初始化"""
        date_lst = self.get_recent_trading_days(k=1)
        if date_lst:
            date_str = date_lst[0]
        else:
            date_str = datetime.now().strftime('%Y%m%d')
        logger.info(f"默认交易日期: {date_str}")
        return date_str
    
    def get_previous_trading_day(self, date, max_days_back=10):
        """
        获取指定日期的前一个交易日
        
        参数:
            date: 日期，支持字符串(YYYYMMDD)、datetime对象或pandas.Timestamp
            max_days_back: 最多向前查找的天数
        
        返回:
            pandas.Timestamp: 前一个交易日，如果未找到则返回None
        """
        self._ensure_calendar_loaded()
        
        # 转换为pandas.Timestamp
        if isinstance(date, str):
            if len(date) == 8:
                current_date = pd.Timestamp(f"{date[:4]}-{date[4:6]}-{date[6:8]}")
            else:
                current_date = pd.Timestamp(date)
        elif isinstance(date, datetime):
            current_date = pd.Timestamp(date)
        else:
            current_date = pd.Timestamp(date)
        
        # 向前查找交易日
        for i in range(1, max_days_back + 1):
            check_date = current_date - pd.Timedelta(days=i)
            if check_date in self._trading_days:
                return check_date
        
        logger.warning(f"在{max_days_back}天内未找到前一交易日")
        return None
    
    def get_recent_trading_days(self, k: int = 30) -> list:
        """
        获取最近的交易日列表
        
        参数:
            k: 需要获取的交易日天数
            
        返回:
            交易日字符串列表，格式为YYYYMMDD，按日期升序排列
        """
        self._ensure_calendar_loaded()
        
        # 获取所有交易日并排序
        sorted_trading_days = sorted(list(self._trading_days), reverse=True)
        
        # 过滤掉未来日期，只保留今天及以前的交易日
        today = pd.Timestamp.now().normalize()
        valid_trading_days = [day for day in sorted_trading_days if day <= today]
        
        # 取前k天
        recent_days = valid_trading_days[:k]
        
        # 转换为YYYYMMDD格式的字符串列表，并反转为升序
        result = [day.strftime('%Y%m%d') for day in reversed(recent_days)]
        
        logger.info(f"获取最近{k}个交易日: {len(result)}天")
        return result
    


if __name__ == "__main__":
    # 测试交易日历功能
    trading_calendar = TradingCalendar()
    # print("测试交易日判断:", trading_calendar.is_trading_day('20250927'))
    # print("测试前一交易日:", trading_calendar.get_previous_trading_day('20250927'))
    print("测试最近5个交易日:", trading_calendar.get_recent_trading_days(1))
