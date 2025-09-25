import json
import os

def load_big_model_config():
    """
    加载大模型配置文件
    
    返回:
        dict: 配置字典
    """
    config_file = os.path.join(os.path.dirname(__file__), 'big_model_config.json')
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"配置文件未找到: {config_file}")
    except json.JSONDecodeError as e:
        raise ValueError(f"配置文件格式错误: {e}")
    except Exception as e:
        raise Exception(f"加载配置文件失败: {e}")

def get_api_key(model_provider, model_name, use_case):
    """
    获取特定用途的API密钥
    
    参数:
        model_provider: 模型提供商 (如 'deepseek-v3')
        model_name: 模型名称 (如 'deepseek-chat')
        use_case: 使用场景 (如 '股票推手')
    
    返回:
        str: API密钥
    """
    config = load_big_model_config()
    
    try:
        api_key = config['big_model'][model_provider][use_case]['api_key']
        if not api_key:
            raise ValueError(f"API密钥为空: {model_provider}/{use_case}")
        return api_key
    except KeyError as e:
        raise KeyError(f"配置项不存在: {e}")
