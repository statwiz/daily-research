"""
合并股票池、同花顺行情数据、涨停数据和异动数据
"""
import os
import time
from datetime import datetime
from stock_pool import StockPool
from wencai import WencaiUtils
from jygs import JygsUtils
from log_setup import get_logger
from trading_calendar import TradingCalendar
from notification import DingDingRobot
from merge import merge


# 配置日志
logger = get_logger("main", "logs", "daily_research.log")

# 全局对象
trading_calendar = TradingCalendar()
dingding_robot = DingDingRobot()
stock_pool = StockPool()

# 重试配置
MAX_RETRIES = 3
RETRY_INTERVAL = 0.015 * 60  # 15分钟，单位：秒



def check_result_files_exist(trading_date: str) -> bool:
    """
    检查结果文件是否已存在且在15点后生成
    
    参数:
        trading_date: 交易日期，格式YYYYMMDD
    
    返回:
        bool: 如果核心文件都已存在且在15点后生成返回True，否则返回False
    """
    base_path = "./data/csv"
    
    # 需要检查的关键文件
    required_files = [
        f"merge_first_stocks_{trading_date}.csv",
        f"merge_core_stocks_{trading_date}.csv"
    ]
    
    # 获取今天15点的时间戳
    today = datetime.now().date()
    cutoff_time = datetime.combine(today, datetime.min.time().replace(hour=15))
    cutoff_timestamp = cutoff_time.timestamp()
    
    for filename in required_files:
        file_path = os.path.join(base_path, filename)
        if not os.path.exists(file_path):
            logger.info(f"文件不存在: {file_path}")
            return False
            
        # 检查文件修改时间
        file_mtime = os.path.getmtime(file_path)
        if file_mtime < cutoff_timestamp:
            logger.info(f"文件存在但在15点前生成，视为无效: {file_path}")
            return False
            
        logger.info(f"文件已存在且在15点后生成: {file_path}")    
    return True


def main():
    """
    主函数，包含重试机制
    """
    start_time = datetime.now()
    
    for retry_count in range(MAX_RETRIES):
        try:
            if retry_count > 0:
                logger.info(f"开始执行主流程 (第{retry_count}次重试)")
            
            # 获取交易日期
            trading_date = trading_calendar.get_default_trade_date()
            # today = datetime.now().strftime('%Y%m%d')
            # if trading_date != today:
            #     logger.warning(f"今天 {today} 不是交易日，跳过执行")
            #     return

            # if check_result_files_exist(trading_date):
            #     logger.warning(f"交易日 {trading_date} 的关键结果文件已存在，跳过执行")
            #     return

            logger.info("=" * 50)
            logger.info(f"开始执行主流程，当前交易日期: {trading_date}")
            logger.info("=" * 50)

            # 1. 更新韭研公社每日数据
            logger.info("开始更新韭研公社每日数据")
            JygsUtils.update_daily_data(trading_date=trading_date)
            logger.info("韭研公社每日数据更新完成")

            # 2. 更新同花顺每日数据
            logger.info("开始更新同花顺行情数据")
            WencaiUtils.update_daily_market_overview_data()
            logger.info("同花顺行情数据更新完成")
            
            # 3. 更新涨停数据  
            logger.info("开始更新同花顺涨停数据")
            WencaiUtils.update_daily_zt_data()
            logger.info("同花顺涨停数据更新完成")

            # 4. 更新股票池数据
            logger.info("开始更新股票池数据")
            stock_pool.update_stock_pool_data()
            logger.info("股票池数据更新完成")

            # 5. 合并数据
            logger.info("开始合并数据")
            merge()
            logger.info("数据合并完成")
            
            # 执行完成，发送成功通知
            end_time = datetime.now()
            duration = end_time - start_time
            success_msg = f"主流程执行完成！耗时: {duration}"
            logger.info(success_msg)
            dingding_robot.send_message(success_msg, 'robot3')
            return
            
        except Exception as e:
            logger.error(f"主流程执行失败: {e}")
            
            if retry_count == MAX_RETRIES - 1:
                # 最后一次重试失败
                end_time = datetime.now()
                duration = end_time - start_time
                final_error_msg = f"主流程在{MAX_RETRIES}次重试后仍然失败: {e}，总耗时: {duration}"
                logger.error(final_error_msg)
                dingding_robot.send_message(final_error_msg, 'robot3')
                raise
        time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    main()