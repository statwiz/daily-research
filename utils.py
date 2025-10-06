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


def check_file_exists_after_time(file_path: str, cutoff_hour: int = 16) -> bool:
    """
    检查文件是否存在且在指定时间点后生成
    
    Args:
        file_path: 文件路径
        cutoff_hour: 截止时间点(小时)，默认16点
    
    Returns:
        bool: 如果文件存在且在指定时间后生成返回True，否则返回False
    """
    import os
    from datetime import datetime
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        return False
    
    try:
        # 获取文件修改时间
        file_mtime = os.path.getmtime(file_path)
        file_datetime = datetime.fromtimestamp(file_mtime)
        
        # 获取文件生成当天指定时间点的时间戳
        file_date = file_datetime.date()
        cutoff_time = datetime.combine(file_date, datetime.min.time().replace(hour=cutoff_hour))
        
        # 比较文件修改时间和截止时间
        return file_datetime >= cutoff_time
        
    except OSError as e:
        logger.warning(f"获取文件时间失败: {file_path}, 错误: {e}")
        return False



if __name__ == "__main__":
    # 测试工具函数
    pass