import pandas as pd
import akshare as ak
import os
from notification import DingDingRobot
from utils import execute_with_retry
dingding_robot = DingDingRobot()

def main():
    try:
        stock_yjyg_em_df = execute_with_retry(ak.stock_yjyg_em, date="20250930")
        cond = (stock_yjyg_em_df['预测指标']=='扣除非经常性损益后的净利润') & (stock_yjyg_em_df['预告类型']=='预增')
        stock_yjyg_em_df = stock_yjyg_em_df[cond]
        output_columns = ['股票简称', '业绩变动幅度','预测数值', '公告日期']
        if os.path.exists('stock_yjyg_em_df.xlsx'):
            stock_yjyg_em_df_pre = pd.read_excel('stock_yjyg_em_df.xlsx')
        else:
            stock_yjyg_em_df_pre = pd.DataFrame(columns=output_columns)
        # 对比两个数据框，新增股票发送到钉钉
        new_stock = stock_yjyg_em_df[~stock_yjyg_em_df['股票简称'].isin(stock_yjyg_em_df_pre['股票简称'].unique())]
        if not new_stock.empty:
            new_stock['预测数值'] = (new_stock['预测数值'].astype(float) / 1e8).round(2)
            new_stock = new_stock[new_stock['业绩变动幅度'].astype(float) > 30]

        if not new_stock.empty:            
            new_stock = new_stock[output_columns].to_dict(orient='records')
            
            # 格式化钉钉消息
            msg_lines = ["📈 新增业绩预增股票"]
            for i, stock in enumerate(new_stock, 1):
                stock_name = stock['股票简称']
                change_rate = stock['业绩变动幅度']
                forecast_value = stock['预测数值']
                announce_date = stock['公告日期']
                
                msg_lines.append(f"{i}. {stock_name}")
                msg_lines.append(f"   业绩变动幅度: {change_rate}%")
                msg_lines.append(f"   预测数值: {forecast_value}亿")
                msg_lines.append(f"   公告日期: {announce_date}")
            
            msg = "\n".join(msg_lines)
            print(msg)
            # 发送到钉钉
            dingding_robot.send_message(msg, 'robot4')
        else:
            print("没有新增业绩预增股票")
        # 保存数据
        stock_yjyg_em_df.to_excel('stock_yjyg_em_df.xlsx', index=False)
    except Exception as e:
        print(e)
        dingding_robot.send_message(f"新增业绩预增股票取数失败: {e}", 'robot4')

if __name__ == "__main__":
    main()