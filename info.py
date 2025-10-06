import requests
import json
import re
import datetime
import time
import pandas as pd
from trading_calendar import TradingCalendar
from log_setup import get_logger
from notification import DingDingRobot


trading_calendar = TradingCalendar()
dingding_robot = DingDingRobot()
logger = get_logger("info", "logs", "daily_research.log")

class StockInfo:
    """
    股票信息获取类
    整合新闻、公告、研报三种信息获取功能
    """
    
    def __init__(self):
        """初始化"""
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        }
    
    def clean_html_tags(self, text):
        """清理HTML标签"""
        if not text:
            return ""
        clean = re.sub(r'<[^>]+>', '', text)
        return clean.strip()
    
    def get_news(self, stock_name, date_range='2', size='5'):
        """获取股票新闻信息"""
        data = {
            'query': stock_name,
            'query_source': 'filter',
            'range': 'title',
            'sort': '',
            'size': str(size),
            'offset': '0',
            'cat_id': '',
            'date_range': str(date_range),
        }

        def parse_news_response(response_data, query_keyword):
            """解析新闻响应数据"""
            result = []
            
            if response_data.get('status_code') != 0:
                return result
            
            ignore_keywords = {
                "融资买入", "融资净买", "资金净流入", "成交额", "龙虎榜",
                '两融余额', '热榜', '新高', '股东户数', '主力资金','涨停板','连板','开盘涨幅',
            }
            
            results = response_data.get('data', {}).get('results', [])
            
            for item in results:
                title = self.clean_html_tags(item.get('title', ''))
                publish_source = item.get('publish_source', '')
                
                if query_keyword not in title:
                    continue

                ignore_flag = False
                for keyword in ignore_keywords:
                    if keyword in title:
                        ignore_flag = True
                        break
                if ignore_flag:
                    continue
                
                news_item = {
                    'title': title,
                    'summary': self.clean_html_tags(item.get('summary', '')),
                    'publish_source': publish_source,
                    'score': item.get('score', 0),
                    'publish_time': item.get('publish_time', '')
                }
                result.append(news_item)
            
            return result

        try:
            response = requests.post(
                'https://www.iwencai.com/unifiedwap/unified-wap/v1/information/news',
                cookies=None,
                headers=self.headers,
                data=data,
            )

            response_data = response.json()
            query_keyword = data['query']
            parsed_news = parse_news_response(response_data, query_keyword)
            parsed_news.sort(key=lambda x: x.get('publish_time', ''), reverse=True)

            result = {
                '股票简称': stock_name,
                '消息': {}
            }
            
            for i, news in enumerate(parsed_news):
                result['消息'][f'news_{i+1}'] = news
                
            return result
            
        except Exception as e:
            return {
                '股票简称': stock_name,
                '消息': {},
                'error': f'获取新闻失败: {str(e)}'
            }

    def get_announcements(self, stock_name, date_range='2', size='15'):
        """获取股票公告信息"""
        data = {
            'query': stock_name,
            'query_source': 'user',
            'range': '',
            'sort': 'time',
            'size': str(size),
            'offset': '0',
            'cat_id': '',
            'dl': '120',
            'tl': '41',
            'date_range': str(date_range),
        }

        try:
            response = requests.post(
                'https://www.iwencai.com/unifiedwap/unified-wap/v1/information/notice',
                cookies=None,
                headers=self.headers,
                data=data,
            )

            response_data = response.json()
            
            if response_data.get('status_code') != 0:
                return {
                    '股票简称': stock_name,
                    '公告': {},
                    'error': f'API返回错误: {response_data.get("status_msg", "未知错误")}'
                }

            results = response_data.get('data', {}).get('results', [])
            
            result = {
                '股票简称': stock_name,
                '公告': {}
            }
            
            for i, item in enumerate(results):
                announcement_item = {
                    'title': self.clean_html_tags(item.get('title', '')),
                    'summary': self.clean_html_tags(item.get('summary', '')),
                    'publish_time': item.get('publish_time', '')
                }
                result['公告'][f'announcement_{i+1}'] = announcement_item
                
            return result
            
        except Exception as e:
            return {
                '股票简称': stock_name,
                '公告': {},
                'error': f'获取公告失败: {str(e)}'
            }

    def get_research_reports(self, stock_name, date_range='3', perpage='20'):
        """获取股票研报信息"""
        data = {
            'w': stock_name,
            'source': 'Ths_iwencai_Xuangu',
            'scene_val': '1',
            'business': '1',
            'company': '1',
            'business_cat': '8',
            'perpage': perpage,
            'page': '0',
            'tag': '11513',
            'uuid': '11513',
            'sort': 'time',
            'date_range': date_range,
        }

        def extract_report_info(json_data):
            """提取研报观点信息"""
            extracted_reports = []
            
            try:
                components = json_data.get('data', {}).get('answer', {}).get('components', [])
                
                for component in components:
                    if component.get('show_type') == 'strip1':
                        datas = component.get('data', {}).get('datas', [])
                        
                        for item in datas:
                            publish_time = item.get('publish_time', 0)
                            publish_date = ''
                            if publish_time:
                                publish_date = datetime.datetime.fromtimestamp(publish_time).strftime('%Y-%m-%d')
                            
                            view_args = item.get('view_arguments', [])
                            view_str = ''
                            if view_args:
                                view_parts = []
                                for i, va in enumerate(view_args, 1):
                                    view = va.get('view', '').strip()
                                    argument = va.get('argument', '').strip()
                                    if view:
                                        if argument:
                                            view_parts.append(f"{i}.{view}({argument})")
                                        else:
                                            view_parts.append(f"{i}.{view}")
                                view_str = ';'.join(view_parts)
                            
                            report_info = {
                                'author_name': item.get('author_name', ''),
                                'author_org': item.get('author_org', ''),
                                'title': item.get('title', ''),
                                'publish_date': publish_date,
                                'view_arguments': view_str,
                                'source_url': item.get('source_url', '')
                            }
                            extracted_reports.append(report_info)
                        break
                        
            except Exception as e:
                print(f"提取数据时发生错误: {e}")
                return []
            
            return extracted_reports

        try:
            response = requests.post(
                'https://www.iwencai.com/unifiedwap/unified-wap/result/get-urp-data',
                cookies=None,
                headers=self.headers,
                data=data,
            )
            
            json_data = response.json()
            reports = extract_report_info(json_data)
            
            result = {
                '股票简称': stock_name,
                '研报': {}
            }
            
            for i, report in enumerate(reports):
                result['研报'][f'report_{i+1}'] = report
                
            return result
            
        except json.JSONDecodeError:
            import codecs
            decoded_text = codecs.decode(response.text, 'unicode_escape')
            return {
                'error': '解析JSON失败',
                'raw_response': decoded_text
            }
        except Exception as e:
            return {
                'error': f'请求失败: {str(e)}'
            }

    def _convert_news_to_df(self, news_data, stock_name):
        """将新闻数据转换为DataFrame格式"""
        df_rows = []
        
        if 'error' in news_data:
            return df_rows
            
        news_items = news_data.get('消息', {})
        
        for key, item in news_items.items():
            # 检查是否有有效的标题和内容，避免空行
            title = item.get('title', '').strip()
            summary = item.get('summary', '').strip()
            publish_time = item.get('publish_time', '').strip()
            
            # 只有当标题不为空时才添加记录
            if title:
                row = {
                    'stock_name': stock_name,
                    'info_type': 'news',
                    'publish_time': publish_time,
                    'source': item.get('publish_source', ''),
                    'title': title,
                    'summary': summary,
                    'author': '',
                    'url': '',
                    'extra_info': json.dumps({
                        'score': item.get('score', 0)
                    }, ensure_ascii=False)
                }
                df_rows.append(row)
            
        return df_rows

    def _convert_announcements_to_df(self, announcement_data, stock_name):
        """将公告数据转换为DataFrame格式"""
        df_rows = []
        
        if 'error' in announcement_data:
            return df_rows
            
        announcement_items = announcement_data.get('公告', {})
        
        for key, item in announcement_items.items():
            # 检查是否有有效的标题，避免空行
            title = item.get('title', '').strip()
            summary = item.get('summary', '').strip()
            publish_time = item.get('publish_time', '').strip()
            
            # 只有当标题不为空时才添加记录
            if title:
                row = {
                    'stock_name': stock_name,
                    'info_type': 'announcement',
                    'publish_time': publish_time,
                    'source': '',
                    'title': title,
                    'summary': summary,
                    'author': '',
                    'url': '',
                    'extra_info': '{}'
                }
                df_rows.append(row)
            
        return df_rows

    def _convert_research_to_df(self, research_data, stock_name):
        """将研报数据转换为DataFrame格式"""
        df_rows = []
        
        if 'error' in research_data:
            return df_rows
            
        research_items = research_data.get('研报', {})
        
        for key, item in research_items.items():
            # 检查是否有有效的标题，避免空行
            title = item.get('title', '').strip()
            summary = item.get('view_arguments', '').strip()
            publish_time = item.get('publish_date', '').strip()
            
            # 只有当标题不为空时才添加记录
            if title:
                row = {
                    'stock_name': stock_name,
                    'info_type': 'research_report',
                    'publish_time': publish_time,
                    'source': item.get('author_org', ''),
                    'title': title,
                    'summary': summary,
                    'author': item.get('author_name', ''),
                    'url': item.get('source_url', ''),
                    'extra_info': '{}'
                }
                df_rows.append(row)
            
        return df_rows

    def get_all_info(self, stock_name, news_params=None, announcement_params=None, research_params=None):
        """
        获取股票的所有信息并返回DataFrame
        
        Args:
            stock_name (str): 股票名称
            news_params (dict): 新闻参数，如 {'date_range': '2', 'size': '5'}
            announcement_params (dict): 公告参数，如 {'date_range': '2', 'size': '15'}
            research_params (dict): 研报参数，如 {'date_range': '3', 'perpage': '20'}
        
        Returns:
            pd.DataFrame: 包含新闻、公告、研报的统一DataFrame
        """
        # 设置默认参数
        news_params = news_params or {'date_range': '2', 'size': '15'}
        announcement_params = announcement_params or {'date_range': '2', 'size': '5'}
        research_params = research_params or {'date_range': '3', 'perpage': '5'}
        
        # 获取三种信息
        print(f"正在获取 {stock_name} 的信息...")
        
        news_data = self.get_news(stock_name, **news_params)
        print(f"  - 新闻获取完成")
        time.sleep(1)
        
        announcement_data = self.get_announcements(stock_name, **announcement_params)
        print(f"  - 公告获取完成")
        time.sleep(1)
        research_data = self.get_research_reports(stock_name, **research_params)
        print(f"  - 研报获取完成")
        time.sleep(1)
        # 转换为DataFrame格式
        df_rows = []
        df_rows.extend(self._convert_news_to_df(news_data, stock_name))
        df_rows.extend(self._convert_announcements_to_df(announcement_data, stock_name))
        df_rows.extend(self._convert_research_to_df(research_data, stock_name))
        
        # 创建DataFrame
        if df_rows:
            result_df = pd.DataFrame(df_rows)
            
            # 添加信息类型优先级列（消息=1, 公告=2, 研报=3）
            type_priority = {'news': 1, 'announcement': 2, 'research_report': 3}
            result_df['type_priority'] = result_df['info_type'].map(type_priority)
            
            # 转换时间格式用于排序
            def convert_time_for_sort(time_str):
                if not time_str:
                    return '0000-00-00'
                # 如果是YYYY-MM-DD格式，直接返回
                if len(time_str) == 10 and '-' in time_str:
                    return time_str
                # 如果是MM月DD日格式，转换为2025-MM-DD格式
                if '月' in time_str and '日' in time_str:
                    try:
                        month = time_str.split('月')[0].zfill(2)
                        day = time_str.split('月')[1].replace('日', '').zfill(2)
                        return f'2025-{month}-{day}'
                    except:
                        return '0000-00-00'
                return '0000-00-00'
            
            result_df['sort_time'] = result_df['publish_time'].apply(convert_time_for_sort)
            
            # 先按时间排序，再按信息类型优先级排序
            result_df = result_df.sort_values(['sort_time', 'type_priority'], 
                                            ascending=[False, True], na_position='last')
            
            # 删除辅助列并重置索引
            result_df = result_df.drop(['type_priority', 'sort_time'], axis=1)
            result_df = result_df.reset_index(drop=True)
        else:
            # 如果没有数据，返回空DataFrame但包含所有列
            columns = ['stock_name', 'info_type', 'publish_time', 'source', 'title', 
                      'summary', 'author', 'url', 'extra_info']
            result_df = pd.DataFrame(columns=columns)
        
        print(f"  - 数据整合完成，共 {len(result_df)} 条记录")
        return result_df


# 重试配置
MAX_RETRIES = 3
RETRY_INTERVAL = 15 * 60  # 15分钟，单位：秒
OUTPUT_BASE_DIR = "./output"


def process_stock_pool(stock_pool, output_filename, pool_name):
    """
    处理股票池数据
    
    Args:
        stock_pool (list): 股票列表
        output_filename (str): 输出文件名
        pool_name (str): 股票池名称
    
    Returns:
        pd.DataFrame: 合并后的数据
    """
    stock_info = StockInfo()
    all_data = []
    failed_stocks = []
    
    logger.info(f"开始处理{pool_name}，共 {len(stock_pool)} 只股票")
    
    for i, stock in enumerate(stock_pool, 1):
        try:
            logger.info(f"正在处理 {stock} ({i}/{len(stock_pool)})")
            df = stock_info.get_all_info(stock)
            
            if len(df) > 0:
                all_data.append(df)
                logger.info(f'{stock} 获取完成，总共获取到 {len(df)} 条记录')
            else:
                logger.warning(f'{stock} 没有获取到任何数据')
                failed_stocks.append(stock)
                
            time.sleep(2)  # 避免请求过于频繁
            
        except Exception as e:
            logger.error(f"处理股票 {stock} 时出错: {e}")
            failed_stocks.append(stock)
            # 只有连续失败超过3只股票才发送钉钉通知，避免过多通知
            if len(failed_stocks) >= 3:
                dingding_robot.send_message(f"处理{pool_name}时连续失败: {failed_stocks[-3:]}", 'robot3')
            continue
    
    # 合并数据并保存
    if all_data:
        try:
            merged_df = pd.concat(all_data, ignore_index=True)
            
            # 确保输出目录存在
            import os
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            
            merged_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            
            success_count = len(stock_pool) - len(failed_stocks)
            logger.info(f"{pool_name}处理完成，成功 {success_count}/{len(stock_pool)} 只股票，共 {len(merged_df)} 条记录，已保存到 {output_filename}")
            
            if failed_stocks:
                logger.warning(f"{pool_name}失败的股票: {failed_stocks}")
                
            return merged_df
            
        except Exception as e:
            logger.error(f"保存{pool_name}数据时出错: {e}")
            raise
    else:
        logger.error(f"{pool_name}没有获取到任何有效数据")
        # 创建空的DataFrame并保存，保持文件结构一致
        empty_df = pd.DataFrame(columns=['stock_name', 'info_type', 'publish_time', 'source', 'title', 
                                       'summary', 'author', 'url', 'extra_info'])
        empty_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        return empty_df


def main():
    """
    主函数，包含重试机制
    """
    start_time = datetime.datetime.now()
    
    for retry_count in range(MAX_RETRIES):
        try:
            if retry_count > 0:
                logger.info(f"开始执行股票信息获取 (第{retry_count}次重试)")

            logger.info("=" * 50)
            logger.info(f"开始执行股票信息获取")
            logger.info("=" * 50)

            # 获取交易日期
            trading_days = trading_calendar.get_default_trade_date()
            logger.info(f"当前交易日期: {trading_days}")
            
            # 1. 处理核心股票池
            try:
                logger.info("开始处理核心股票池")
                core_stocks_file = f'{OUTPUT_BASE_DIR}/{trading_days}/core_stocks.csv'
                
                # 检查文件是否存在
                import os
                if not os.path.exists(core_stocks_file):
                    logger.error(f"核心股票池文件不存在: {core_stocks_file}")
                    raise FileNotFoundError(f"核心股票池文件不存在: {core_stocks_file}")
                
                core_stocks = pd.read_csv(core_stocks_file)
                if '股票简称' not in core_stocks.columns:
                    logger.error(f"核心股票池文件缺少'股票简称'列: {core_stocks.columns.tolist()}")
                    raise ValueError(f"核心股票池文件缺少'股票简称'列")
                
                core_stocks_list = core_stocks['股票简称'].unique().tolist()
                logger.info(f"核心股票池共有 {len(core_stocks_list)} 只股票")
                
                core_output = f'{OUTPUT_BASE_DIR}/core_info.csv'
                process_stock_pool(core_stocks_list, core_output, "核心股票池")
                
            except Exception as e:
                logger.error(f"处理核心股票池失败: {e}")
                dingding_robot.send_message(f"处理核心股票池失败: {e}", 'robot3')
                raise

            # 2. 处理首板股票池
            try:
                logger.info("开始处理首板股票池")
                first_stocks_file = f'{OUTPUT_BASE_DIR}/{trading_days}/first_stocks.csv'
                
                # 检查文件是否存在
                if not os.path.exists(first_stocks_file):
                    logger.error(f"首板股票池文件不存在: {first_stocks_file}")
                    raise FileNotFoundError(f"首板股票池文件不存在: {first_stocks_file}")
                
                first_stocks = pd.read_csv(first_stocks_file)
                if '股票简称' not in first_stocks.columns:
                    logger.error(f"首板股票池文件缺少'股票简称'列: {first_stocks.columns.tolist()}")
                    raise ValueError(f"首板股票池文件缺少'股票简称'列")
                
                first_stocks_list = first_stocks['股票简称'].unique().tolist()
                logger.info(f"首板股票池共有 {len(first_stocks_list)} 只股票")
                
                first_output = f'{OUTPUT_BASE_DIR}/first_info.csv'
                process_stock_pool(first_stocks_list, first_output, "首板股票池")
                
            except Exception as e:
                logger.error(f"处理首板股票池失败: {e}")
                dingding_robot.send_message(f"处理首板股票池失败: {e}", 'robot3')
                raise

            # 执行完成，发送成功通知
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            success_msg = f"股票信息获取执行完成！耗时: {duration}"
            logger.info(success_msg)
            dingding_robot.send_message(success_msg, 'robot3')
            return
            
        except Exception as e:
            logger.error(f"股票信息获取执行失败: {e}")
            
            if retry_count == MAX_RETRIES - 1:
                # 最后一次重试失败
                end_time = datetime.datetime.now()
                duration = end_time - start_time
                final_error_msg = f"股票信息获取在{MAX_RETRIES}次重试后仍然失败: {e}，总耗时: {duration}"
                logger.error(final_error_msg)
                dingding_robot.send_message(final_error_msg, 'robot3')
                raise
        time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    main()