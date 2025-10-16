import akshare as ak
import pandas as pd
from datetime import datetime
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import execute_with_retry

# 公告分类字典
ANN_CATEGORY_DICT = {
    '重大事项': {'投资设立公司', '变更募集资金投资项目', '获得补贴（资助）',
       '股份质押、冻结', '处罚', '重大合同', '委托（受托）事项', '签订协议', '诉讼仲裁', 
       '投资理财', '增资扩股', '权益变动报告书', '借贷', 
       '整改报告', '募集资金补充流动资金', '获得认证',
       '限售股份上市流通', '对外项目投资', '违法违规', '归还募集资金',
       '重大事故损失'},

    '财务报告': {'年度报告更正公告', '年度报告全文', '分配方案实施', '分配预案', '分配方案决议公告', '分配方案调整',
       '业绩预告'},

    '融资公告': {'增发提示性公告', '增发方案修订', '增发预案', '其他增发事项公告', '增发终止', '增发获准公告',
       '网上申购及中签率公告', '增发发行结果公告', '增发上市公告书', '首发提示性公告'},

    '风险提示': {'风险提示性公告', '其它风险提示公告', '股票交易异常波动', '复牌公告', '实施退市风险警示', '停牌公告'},

    '资产重组': {'重组进展公告', '收购出售资产/股权', '回购实施公告', '回购预案',
       '回购进展情况', '要约收购报告书', '股权转让', '资产重组债权人会议', '回购方案修订',
       '要约收购报告书摘要', '股本变动', '公司注册资本变更', '股权激励行权价（数量）调整',
       '债务重组'},

    '持股变动': {'股东/实际控制人股份减持', '权益变动报告书', '股东/实际控制人股份增持', '高管人员持股变动'}
}

ANN_OUTPUT_COLUMNS = ['名称', '公告日期', '公告分类','公告类型', '公告标题', '网址']




class AkShare:
    """AkShare数据获取类"""
    
    def __init__(self):
        """初始化AkShare类"""
        self.ann_output_columns = ANN_OUTPUT_COLUMNS
        self.ann_category_dict = ANN_CATEGORY_DICT
        
    def get_announcements(self, date: str) -> pd.DataFrame:
        """
        获取股票公告数据
        
        Args:
            date (str): 日期，格式如 '20250930'
            
        Returns:
            pd.DataFrame: 公告数据框
            
        Raises:
            Exception: 获取数据失败时抛出异常
        """
        
        try:
            print(f"正在获取的公告")
            df = execute_with_retry(ak.stock_notice_report, symbol='全部', date=date)
            
            if df.empty:
                print(f"未获取到{date}的公告数据")
                raise Exception(f"未获取到{date}的公告数据")

            print(f"成功获取{len(df)}条公告")

            for category, symbols in self.ann_category_dict.items():
                for symbol in symbols:
                    mask = df['公告类型'] == symbol
                    df.loc[mask, '公告分类'] = category

            df['公告分类'] = df['公告分类'].fillna('其他')
            df.sort_values(by='名称', inplace=True)
            # 重大合同归类为重大事项
            mask = df['公告标题'].str.contains('重大合同', na=False)
            df.loc[mask, '公告分类'] = '重大事项'
            
            # 选择输出列
            df = df[self.ann_output_columns]
            
            print(f"共获取{len(df)}条公告数据")
            return df

        except Exception as e:
            error_msg = f"获取公告数据失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise Exception(error_msg)

    def _get_single_concept_data(self, name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取单个概念板块数据的辅助函数"""
        df = execute_with_retry(ak.xx, symbol=name, start_date=start_date, end_date=end_date)
        df['name'] = name
        print(f"成功获取{name}概念板块{len(df)}条数据")
        return df
    
    # 获取同花顺概念版块数据
    def get_concept_board(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取同花顺概念版块数据"""
        df_concept = execute_with_retry(ak.stock_board_concept_name_ths)
        if df_concept is None or df_concept.empty:
            print(f"概念板块名称为空")
            raise Exception(f"概念板块名称为空")  
        print(f"概念板块名称数量: {len(df_concept)}")
      
        concept_list = df_concept['name'].tolist()       
        df_all_list = []
        
        # 使用线程池并发获取数据，限制并发数为8
        with ThreadPoolExecutor(max_workers=8) as executor:
            # 提交所有任务
            future_to_name = {
                executor.submit(self._get_single_concept_data, name, start_date, end_date): name 
                for name in concept_list
            }
            
            # 获取结果
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    df = future.result()
                    df_all_list.append(df)
                except Exception as e:
                    print(f"获取{name}概念板块数据失败: {e}")
                    logging.error(f"获取{name}概念板块数据失败: {e}")
        
        if not df_all_list:
            raise Exception("所有概念板块数据获取失败")
            
        df_all = pd.concat(df_all_list)
        # 按日期倒排
        df_all = df_all.sort_values(by='日期', ascending=False)
        return df_all
    
  


# 使用示例
if __name__ == "__main__":
    akshare = AkShare()
    try:
        # df = akshare.get_announcements('20250930')
        # print(f"成功获取{len(df)}条公告数据")
        # df.to_csv('./output/announcements.csv', index=False)
        # print(f"公告数据保存完成")
        df = akshare.get_concept_board('20250910', '20251014')
        # df.to_csv('./output/concept_board.csv', index=False)
        # print(f"概念版块数据保存完成")

    except Exception as e:
        print(f"执行失败: {e}")