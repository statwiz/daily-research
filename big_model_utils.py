import json
import time
import pandas as pd
from openai import OpenAI
from big_model_config import get_api_key
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 禁用第三方库的详细日志
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

class SentimentAnalyzer:
    """股票情绪分析器"""
    
    def __init__(self, model_provider='deepseek-v3', use_case='股票推手'):
        """
        初始化情绪分析器
        
        参数:
            model_provider: 模型提供商
            use_case: 使用场景
        """
        self.api_key = get_api_key(model_provider, 'deepseek-chat', use_case)
        self.client = OpenAI(api_key=self.api_key, base_url="https://api.deepseek.com")
        
    def analyze_sentiment(self, stock_name, forum_post, retry_count=3):
        """
        分析单条帖子的情绪
        
        参数:
            stock_name: 股票名称
            forum_post: 论坛帖子内容
            retry_count: 重试次数
            
        返回:
            dict: 包含情绪分数和解释的字典
        """
        # 构造消息
        messages = [
            {
                "role": "system",
                "content": (
                    "你是一个金融情绪分析助手，任务是判断一段关于股票的发言中"
                    "是否包含做多或做空该股票的意愿。\n"
                    "- 输出情绪评分，取值区间为 [-1, 1]：\n"
                    "  1 表示强烈做多，0.5~1 表示偏多，0 表示中性，-0.5~0 表示偏空，-1 表示强烈做空。\n"
                    "- 只关注发言中对该股票的态度，不考虑宏观或其他股票。\n"
                    "- 输出格式必须是有效的JSON：\n"
                    "{\n"
                    '  "股票名": "<股票名>",\n'
                    '  "情绪分数": <情绪分数>,\n'
                    '  "简要中文解释": "<简要中文解释>"\n'
                    "}"
                )
            },
            {
                "role": "user",
                "content": f"请根据以下发言，判断发言者对股票【{stock_name}】的做多/做空意愿，并给出情绪评分：\n\n{forum_post}"
            }
        ]
        
        for attempt in range(retry_count):
            try:
                # 调用大模型
                response = self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=messages,
                    stream=False,
                    temperature=0.1  # 降低随机性，提高一致性
                )
                
                # 解析响应
                content = response.choices[0].message.content.strip()
                
                # 尝试解析JSON
                try:
                    result = json.loads(content)
                    
                    # 验证结果格式
                    if all(key in result for key in ["股票名", "情绪分数", "简要中文解释"]):
                        # 确保情绪分数在有效范围内
                        sentiment_score = float(result["情绪分数"])
                        sentiment_score = max(-1, min(1, sentiment_score))  # 限制在[-1,1]范围内
                        
                        return {
                            "情绪分数": sentiment_score,
                            "简要中文解释": result["简要中文解释"]
                        }
                    else:
                        logger.warning(f"响应格式不完整，尝试第{attempt+1}次重试")
                        
                except json.JSONDecodeError:
                    logger.warning(f"JSON解析失败，尝试第{attempt+1}次重试。响应内容: {content}")
                
                # 如果JSON解析失败，尝试简单的文本解析
                if "情绪分数" in content:
                    try:
                        # 简单的正则提取（备用方案）
                        import re
                        score_match = re.search(r'"情绪分数":\s*(-?\d+\.?\d*)', content)
                        explanation_match = re.search(r'"简要中文解释":\s*"([^"]*)"', content)
                        
                        if score_match and explanation_match:
                            sentiment_score = float(score_match.group(1))
                            sentiment_score = max(-1, min(1, sentiment_score))
                            
                            return {
                                "情绪分数": sentiment_score,
                                "简要中文解释": explanation_match.group(1)
                            }
                    except Exception as e:
                        logger.warning(f"文本解析也失败: {e}")
                
                # 添加延迟避免频率限制
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"API调用失败 (尝试 {attempt+1}/{retry_count}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(2)  # 重试前等待更长时间
                
        # 如果所有重试都失败，返回中性结果
        logger.error(f"所有重试都失败，返回中性情绪分析结果")
        return {
            "情绪分数": 0.0,
            "简要中文解释": "分析失败，默认中性"
        }
    
    def batch_analyze_sentiment(self, stock_name, posts_df, text_column='text', 
                              batch_size=10, delay_between_batches=5):
        """
        批量分析帖子情绪
        
        参数:
            stock_name: 股票名称
            posts_df: 包含帖子数据的DataFrame
            text_column: 文本内容列名
            batch_size: 每批处理的数量
            delay_between_batches: 批次间延迟（秒）
            
        返回:
            DataFrame: 添加了情绪分析结果的DataFrame
        """
        logger.info(f"开始批量分析 {len(posts_df)} 条帖子的情绪")
        
        # 复制DataFrame避免修改原数据
        result_df = posts_df.copy()
        
        # 初始化情绪分析列
        result_df['情绪分数'] = 0.0
        result_df['简要中文解释'] = ""
        
        total_batches = (len(posts_df) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(posts_df))
            
            logger.info(f"处理批次 {batch_idx + 1}/{total_batches} (第{start_idx+1}-{end_idx}条)")
            
            # 处理当前批次
            for idx in range(start_idx, end_idx):
                try:
                    post_text = str(posts_df.iloc[idx][text_column])
                    
                    # 限制文本长度避免token超限
                    if len(post_text) > 2000:
                        post_text = post_text[:2000] + "..."
                    
                    # 分析情绪
                    sentiment_result = self.analyze_sentiment(stock_name, post_text)
                    
                    # 更新结果
                    result_df.loc[result_df.index[idx], '情绪分数'] = sentiment_result['情绪分数']
                    result_df.loc[result_df.index[idx], '简要中文解释'] = sentiment_result['简要中文解释']
                    
                    logger.info(f"第{idx+1}条分析完成，情绪分数: {sentiment_result['情绪分数']}")
                    
                    # 请求间添加小延迟
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"第{idx+1}条帖子分析失败: {e}")
                    result_df.loc[result_df.index[idx], '情绪分数'] = 0.0
                    result_df.loc[result_df.index[idx], '简要中文解释'] = "分析失败"
            
            # 批次间延迟
            if batch_idx < total_batches - 1:
                logger.info(f"批次完成，等待 {delay_between_batches} 秒...")
                time.sleep(delay_between_batches)
        
        logger.info("批量情绪分析完成")
        return result_df


def test_sentiment_analysis():
    """测试情绪分析功能"""
    analyzer = SentimentAnalyzer()
    
    # 测试单条分析
    stock_name = "胜宏科技"
    test_post = """
    论胜宏科技的确定性和淳中科技的值搏率：最近胜宏科技炸裂的业绩和高增长引导了极高的关注度，
    同样，淳中科技一季度业绩的严重不及预期也把公司推上了热搜。英伟达及其全球供应链...
    胜宏科技作为达链pcb的供应商，其多层和高阶技术远超同行，加上定制化，必将成为英伟达等大厂的核心供应商
    """
    
    result = analyzer.analyze_sentiment(stock_name, test_post)
    print("单条测试结果:")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    # 运行测试
    test_sentiment_analysis()