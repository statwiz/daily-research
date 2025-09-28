# -*- coding: utf-8 -*-
"""
通知服务模块
管理钉钉机器人等通知功能
"""

from datetime import datetime
import requests
import json
import logging

# 使用标准的logger命名方式，会自动继承主脚本的日志配置
logger = logging.getLogger(__name__)

# 钉钉机器人配置
DINGDING_ROBOTS = {
    'robot1': {
        'token': '873bab7cc1fd40504a5e64768a5eb888bb364d648e235eee0a103f5f3d5c5573',
        'keyword': '调仓',
        'name': '调仓机器人'
    },
    'robot2': {
        'token': '0024f7e7f394490049ae20c3339452aeeecaf8610083baf4a69f9a50da21fe43',
        'keyword': '时间线',
        'name': '时间线机器人'
    },
    'robot3': {
        'token': 'b6d030967dac110e641a3c701789e6e15dd3a1cb52316782ffc2d2947f28d513',
        'keyword': '每日复盘',
        'name': '复盘机器人'
    },
    'robot4': {
        'token': 'aa7ae68b6361b67b7e30d8556f21b06ec4499f842125241d3697b0c44cc3365e',
        'keyword': '挖掘',
        'name': '低位挖掘'
    }
}

# 默认机器人设置
DEFAULT_ROBOT = 'robot3'


class DingDingRobot:
    """
    钉钉机器人管理器
    
    管理多个钉钉机器人，自动处理关键词和消息发送
    """
    
    def __init__(self):
        """初始化机器人配置"""
        self.robots = DINGDING_ROBOTS
    
    def _send_message(self, content, robot_config):
        """
        内部方法：发送消息的具体实现
        
        参数:
            content: 消息内容
            robot_config: 机器人配置字典
        """
        try:
            # 自动在内容前添加关键词（如果还没有的话）
            keyword = robot_config['keyword']
            if keyword not in content:
                final_content = f"【{keyword}】{content}"
            else:
                final_content = content
            
            # 添加时间戳
            final_content += '\n' + datetime.now().strftime('%m-%d-%H:%M:%S')
            
            msg = {
                'msgtype': 'text',
                'text': {'content': final_content}
            }
            
            headers = {'Content-Type': 'application/json; charset=utf-8'}
            url = f'https://oapi.dingtalk.com/robot/send?access_token={robot_config["token"]}'
            body = json.dumps(msg)
            
            response = requests.post(url, data=body, headers=headers)
            response.raise_for_status()
            
            logger.info(f'{robot_config["name"]}消息发送成功')
            
        except Exception as err:
            logger.error(f'{robot_config["name"]}发送失败: {err}')
            raise
    
    def send_message(self, content, robot_name=DEFAULT_ROBOT):
        """
        发送消息到指定机器人
        
        参数:
            content: 消息内容
            robot_name: 机器人名称 ('robot1', 'robot2', 'robot3', 'robot4')
        """
        if robot_name not in self.robots:
            raise ValueError(f"未知的机器人: {robot_name}, 可用的机器人: {list(self.robots.keys())}")
        
        robot_config = self.robots[robot_name]
        self._send_message(content, robot_config)


if __name__ == "__main__":
    # 测试钉钉机器人管理器
    print("=== 钉钉机器人管理器测试 ===")
    dingding_robot = DingDingRobot()
    
    # 显示机器人信息
    dingding_robot.send_message("测试挖掘消息", 'robot4')
