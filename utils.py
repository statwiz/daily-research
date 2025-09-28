# -*- coding: utf-8 -*-
"""
工具函数模块
只包含纯工具函数，不包含类定义
"""

from datetime import datetime, timedelta
import pandas as pd
import logging
import time

# 使用标准的logger命名方式，会自动继承主脚本的日志配置
logger = logging.getLogger(__name__)


def execute_with_retry(func, max_retry_count=3, retry_base_delay=3, *args, **kwargs):
    """
    带重试机制的函数包装器
    
    Args:
        func: 要执行的函数
        max_retry_count: 最大重试次数，默认3次
        retry_base_delay: 基础等待时间，实际等待时间为 (attempt + 1) * retry_base_delay，默认2秒
        *args, **kwargs: 函数参数
    
    Returns:
        函数执行结果
        
    Raises:
        最后一次失败的异常
    """
    last_exception = None
    func_name = getattr(func, '__name__', str(func))
    
    logger.info(f"开始执行函数 {func_name}, 最大重试次数: {max_retry_count}")
    
    for i in range(max_retry_count):
        try:
            logger.debug(f"第{i + 1}次尝试执行 {func_name}")
            result = func(*args, **kwargs)
            
            if i > 0:
                logger.info(f"函数 {func_name} 在第{i + 1}次尝试后成功执行")
            
            return result
            
        except Exception as e:
            last_exception = e
            if i < max_retry_count - 1:  # 不是最后一次尝试
                wait_time = (i + 1) * retry_base_delay
                logger.warning(f"函数 {func_name} 第{i + 1}次尝试失败，{wait_time}秒后重试: {e}")
                time.sleep(wait_time)
            else:
                logger.error(f"函数 {func_name} 重试{max_retry_count}次后仍失败: {e}")
    
    # 所有重试都失败了，抛出最后的异常
    raise last_exception


# 这里可以添加其他纯工具函数，例如：
# def format_date(date_str):
#     """格式化日期字符串"""
#     pass
# 
# def calculate_percentage(value1, value2):
#     """计算百分比"""
#     pass


if __name__ == "__main__":
    # 测试工具函数
    pass