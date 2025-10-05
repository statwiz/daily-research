"""
合并股票池、同花顺行情数据、涨停数据和异动数据
"""
import os
import time
from datetime import datetime
from wencai import WencaiUtils
from log_setup import get_logger
from notification import DingDingRobot


# 配置日志
logger = get_logger("overseas", "logs", "daily_research.log")

# 全局对象
dingding_robot = DingDingRobot()

# 重试配置
MAX_RETRIES = 3
RETRY_INTERVAL = 15 * 60  # 15分钟，单位：秒
OUTPUT_BASE_DIR = "./output"


def main():
    """
    主函数，包含重试机制
    """
    start_time = datetime.now()
    
    for retry_count in range(MAX_RETRIES):
        try:
            if retry_count > 0:
                logger.info(f"开始执行海外股票池 (第{retry_count}次重试)")

            logger.info("=" * 50)
            logger.info(f"开始执行海外股票池")
            logger.info("=" * 50)

            # 1. 更新美股数据
            try:
                logger.info("开始更新美股数据")
                us_stocks = WencaiUtils.get_us_top_stocks()
                us_stocks.to_csv(os.path.join(OUTPUT_BASE_DIR, "us_stocks.csv"), index=False)
                logger.info("美股数据更新完成")
            except Exception as e:
                logger.error(f"更新美股数据失败: {e}")
                dingding_robot.send_message(f"更新美股数据失败: {e}", 'robot3')
                raise

            # 2. 更新港股数据
            try:
                logger.info("开始更新港股数据")
                hk_stocks = WencaiUtils.get_hk_top_stocks()
                hk_stocks.to_csv(os.path.join(OUTPUT_BASE_DIR, "hk_stocks.csv"), index=False)
                logger.info("港股数据更新完成")
            except Exception as e:
                logger.error(f"更新港股数据失败: {e}")
                dingding_robot.send_message(f"更新港股数据失败: {e}", 'robot3')
                raise
            # 执行完成，发送成功通知
            end_time = datetime.now()
            duration = end_time - start_time
            success_msg = f"海外股票池执行完成！耗时: {duration}"
            logger.info(success_msg)
            dingding_robot.send_message(success_msg, 'robot3')
            return
            
        except Exception as e:
            logger.error(f"海外股票池执行失败: {e}")
            
            if retry_count == MAX_RETRIES - 1:
                # 最后一次重试失败
                end_time = datetime.now()
                duration = end_time - start_time
                final_error_msg = f"海外股票池在{MAX_RETRIES}次重试后仍然失败: {e}，总耗时: {duration}"
                logger.error(final_error_msg)
                dingding_robot.send_message(final_error_msg, 'robot3')
                raise
        time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    main()