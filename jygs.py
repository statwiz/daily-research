"""
韭研公社API工具类
用于获取股票异动解析数据
"""

import requests
import os
import pandas as pd
from trading_calendar import TradingCalendar
import time
from random import randint
import execjs
from typing import Optional
from log_setup import get_logger
from utils import execute_with_retry
# 设置日志记录器
logger = get_logger("jygs", "logs", "daily_research.log")
trading_calendar = TradingCalendar()

class JygsUtils:
    """韭研公社API工具类"""
    
    # API配置
    BASE_URL = "https://app.jiuyangongshe.com/jystock-app/api/v1"
    REFERER_URL = "https://www.jiuyangongshe.com/"
    JS_FILE_PATH = "./js/jygs.js"
    
    # 登录配置
    DEFAULT_PHONE = "18019378260"
    DEFAULT_PASSWORD = "jygs123456"
    # DEFAULT_PHONE = "18210581245"
    # DEFAULT_PASSWORD = "jygs888666"
    # 数据路径配置
    DATA_DIR = "./data/csv/jygs"
    
    @staticmethod
    def _generate_token_and_timestamp() -> tuple[str, str]:
        """生成时间戳和token"""
        try:
            timestamp = str(int(time.time() * 1000))
            js_code = open(JygsUtils.JS_FILE_PATH, 'r', encoding='utf-8').read()
            token = execjs.compile(js_code).call('jiemi', timestamp)
            return timestamp, token
        except Exception as e:
            logger.error(f"生成token失败: {e}")
            raise
    
    @staticmethod
    def _get_headers(timestamp: str, token: str) -> dict:
        """获取请求头"""
        return {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7',
            'content-type': 'application/json',
            'origin': 'https://www.jiuyangongshe.com',
            'platform': '3',
            'referer': JygsUtils.REFERER_URL,
            'timestamp': timestamp,
            'token': token,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
    
    @staticmethod
    def _get_session(phone: str = None, password: str = None) -> requests.Session:
        """获取韭研公社登录会话"""
        logger.info("开始获取登录会话")
        
        phone = phone or JygsUtils.DEFAULT_PHONE
        password = password or JygsUtils.DEFAULT_PASSWORD
        
        try:
            timestamp, token = JygsUtils._generate_token_and_timestamp()
            headers = JygsUtils._get_headers(timestamp, token)

            json_data = {'phone': phone, 'password': password}
            
            session = requests.Session()
            response = session.post(
                f'{JygsUtils.BASE_URL}/user/login',
                headers=headers,
                json=json_data,
            )
            result = response.json()
            if result.get('errCode') == '0':
                logger.info(f"登录成功: {result['data']['nickname']}")
                return session
            else:
                raise Exception(f"登录失败: {result.get('msg', '未知错误')}")
                
        except Exception as e:
            logger.error(f"获取登录会话失败: {e}")
            raise
    
    @staticmethod
    def _get_single_date_data(session: requests.Session, trading_date: str = None) -> pd.DataFrame:
        """获取指定交易日的异动解析数据"""
        if trading_date is None:
            recent_days = execute_with_retry(trading_calendar.get_recent_trading_days, k=1)
            trading_date = recent_days[0]
            
        logger.info(f"获取{trading_date}异动解析数据")
        
        try:
            timestamp, token = JygsUtils._generate_token_and_timestamp()
            headers = JygsUtils._get_headers(timestamp, token)
            
            # 格式化日期
            formatted_date = f"{trading_date[:4]}-{trading_date[4:6]}-{trading_date[6:]}"
            
            json_data = {'date': formatted_date, 'pc': 1}
            
            response = session.post(
                f'{JygsUtils.BASE_URL}/action/field',
                headers=headers,
                json=json_data,
            )
            
            result = response.json()
            data = result.get('data')
            
            if not data:
                logger.warning(f"{formatted_date}无异动数据")
                return pd.DataFrame()
            
            # 处理股票数据
            stocks_data = []
            for item in data[1:]:  # 跳过第一个元素
                hotspot_name = item['name']
                hotspot_reason = item.get('reason', '')
                
                for stock in item['list']:
                    action_info = stock['article']['action_info']
                    expound_parts = action_info['expound'].split('\n')
                    
                    stocks_data.append({
                        '股票简称': stock['name'],
                        'code': stock['code'][2:],  # 去掉前缀
                        'zt_time': action_info['time'],
                        '异动原因': expound_parts[0],
                        '解析': '\n'.join(expound_parts[1:]) if len(expound_parts) > 1 else '',
                        '热点': hotspot_name,
                        '热点导火索': hotspot_reason,
                        '日期': trading_date
                    })
            
            df = pd.DataFrame(stocks_data)
            logger.info(f"获取到{len(df)}条股票异动数据")
            return df
            
        except Exception as e:
            logger.error(f"获取{trading_date}异动数据失败: {e}")
            raise
    
    @staticmethod
    def _update_stocks_data(df: pd.DataFrame, trading_date: str, data_dir: str) -> None:
        """保存解析数据到历史文件"""
        try:
            his_file_path = os.path.join(data_dir, "jygs_his.csv")
            
            if os.path.exists(his_file_path):
                existing_df = pd.read_csv(his_file_path, dtype={'日期': str})
                
                if trading_date in existing_df['日期'].values:
                    logger.info(f"{trading_date}的解析数据已存在，跳过追加")
                    return
                
                combined_df = pd.concat([df, existing_df], ignore_index=True)
            else:
                combined_df = df
            
            # 按日期倒序排列并保存
            combined_df = combined_df.sort_values('日期', ascending=False)
            combined_df.to_csv(his_file_path, index=False, encoding='utf-8-sig')
            logger.info(f"追加{trading_date}解析数据到: {his_file_path}，共{len(df)}条新记录")
            
        except Exception as e:
            logger.error(f"保存{trading_date}解析数据失败: {e}")
            raise
    
    @staticmethod
    def _update_bk_data(df: pd.DataFrame, trading_date: str, data_dir: str) -> None:
        """保存热点板块统计信息"""
        try:
            # 生成板块统计数据
            hotspot_stats = df.groupby(['热点', '日期']).size().reset_index(name='股票数量')
            hotspot_stats['交易日期'] = trading_date
            hotspot_stats = hotspot_stats[['交易日期', '热点', '股票数量']]
            
            bk_his_file = os.path.join(data_dir, "jygs_bk_his.csv")
            
            if os.path.exists(bk_his_file):
                existing_bk_df = pd.read_csv(bk_his_file, dtype={'交易日期': str})
                
                if trading_date in existing_bk_df['交易日期'].values:
                    logger.info(f"{trading_date}的板块数据已存在，跳过追加")
                    return
                
                combined_bk_df = pd.concat([hotspot_stats, existing_bk_df], ignore_index=True)
            else:
                combined_bk_df = hotspot_stats
            
            # 按交易日期倒序排列并保存
            combined_bk_df = combined_bk_df.sort_values('交易日期', ascending=False)
            combined_bk_df.to_csv(bk_his_file, index=False, encoding='utf-8-sig')
            logger.info(f"追加{trading_date}板块统计到: {bk_his_file}，共{len(hotspot_stats)}个热点")
            
        except Exception as e:
            logger.error(f"保存{trading_date}板块统计失败: {e}")
            raise
    
    @staticmethod
    def _update_latest_stocks_data(new_data: pd.DataFrame, data_dir: str) -> None:
        """更新最新异动数据文件"""
        try:
            latest_file = os.path.join(data_dir, "jygs.csv")
            
            if os.path.exists(latest_file):
                # 读取现有数据
                existing_df = pd.read_csv(latest_file, dtype={'日期': str})
                
                # 检查新数据中的日期是否已存在，避免重复添加
                new_dates = set(new_data['日期'].unique())
                existing_dates = set(existing_df['日期'].unique())
                duplicate_dates = new_dates & existing_dates
                
                if duplicate_dates:
                    logger.info(f"日期 {duplicate_dates} 的数据已存在，跳过重复添加")
                    return
                
                # 合并数据
                combined_df = pd.concat([existing_df, new_data], ignore_index=True)
                
                # 对每只股票只保留最新日期的记录
                latest_df = combined_df.sort_values('日期', ascending=False).groupby('股票简称').first().reset_index()
            else:
                latest_df = new_data
            
            # 按日期倒序排列（最新日期在最上方）
            latest_df = latest_df.sort_values('日期', ascending=False)
            
            # 保存更新后的数据
            latest_df.to_csv(latest_file, index=False, encoding='utf-8-sig')
            logger.info(f"更新最新数据到: {latest_file}，共{len(latest_df)}条记录")
            
        except Exception as e:
            logger.error(f"更新最新异动数据失败: {e}")
            raise

    @staticmethod
    def update_daily_data(trading_date: str, data_dir: str = None, session: requests.Session = None) -> None:
        """保存每日异动数据的主入口函数"""
        try:
            session = session or execute_with_retry(JygsUtils._get_session)
            df = execute_with_retry(JygsUtils._get_single_date_data, session, trading_date)
            
            if df.empty:
                logger.error(f"{trading_date}无异动数据，跳过保存")
                raise Exception(f"{trading_date}无异动数据，跳过保存")
            
            # 创建数据目录
            data_dir = data_dir or JygsUtils.DATA_DIR
            os.makedirs(data_dir, exist_ok=True)
            
            logger.info(f"开始保存{trading_date}的异动数据，共{len(df)}条记录")
            
            # 1. 保存异动个股数据到历史文件
            JygsUtils._update_stocks_data(df, trading_date, data_dir)
            
            # 2. 保存异动热点板块统计信息
            JygsUtils._update_bk_data(df, trading_date, data_dir)
            
            # 3. 更新最新异动个股数据文件
            JygsUtils._update_latest_stocks_data(df, data_dir)
            
            logger.info(f"完成{trading_date}异动数据保存")
            
        except Exception as e:
            logger.error(f"保存{trading_date}异动数据失败: {e}")
            raise
        finally:
            if session:
                session.close()


if __name__ == "__main__":
    # 测试代码
    try:
        ds_list = trading_calendar.get_recent_trading_days(k=20)
        for i,dt in enumerate(ds_list[8:]):
            logger.info(f"开始保存{dt}异动数据，第{i+1}天")
            try:
                JygsUtils.update_daily_data(trading_date=dt, data_dir=JygsUtils.DATA_DIR)
            except Exception as e:
                logger.error(f"保存{dt}异动数据失败: {e}")
                break
            time.sleep(5 + randint(1, 5))
    except Exception as e:
        logger.error(f"测试失败: {e}")