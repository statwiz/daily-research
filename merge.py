"""
合并股票池、同花顺行情数据、涨停数据和异动数据
"""

import pandas as pd
import numpy as np
from stock_pool import StockPool
from wencai import WencaiUtils
from jygs import JygsUtils

# 读取股票池数据
stock_pool = StockPool()
df_stock_pool = stock_pool.read_stock_pool_data()
print(df_stock_pool.columns)

# 读取同花顺行情数据
df_ths_market_overview = WencaiUtils.read_market_overview_data()
print(df_ths_market_overview.columns)

# 读取最新涨停数据
df_ths_zt = WencaiUtils.read_latest_zt_stocks()
df_ths_zt.rename(columns={'交易日期': '涨停日期'}, inplace=True)

print(df_ths_zt.columns)

# 读取异动数据
df_jygs = JygsUtils.read_latest_data()
df_jygs.rename(columns={'日期': '异动日期'}, inplace=True)
print(df_jygs.columns)

# 合并数据
df_merge = pd.merge(df_stock_pool, df_ths_market_overview[['code','market_code','热度排名','竞换手Z','竞价金额','竞价涨幅','涨跌幅','实体涨幅','换手Z','成交额','大单净额','上市板块']], on=['code', 'market_code'], how='left')
df_merge = pd.merge(df_merge, df_ths_zt[['code','market_code','涨停日期','连板','涨停封单额','涨停原因类别','封成量比','封流量比']], on=['code', 'market_code'], how='left')
df_merge = pd.merge(df_merge, df_jygs[['code','异动日期','热点','热点导火索','异动原因','解析']], on=['code'], how='left')
df_merge['code'] = df_merge['code'].apply(lambda x: f"'{x}")
df_merge['区间信息'] = df_merge['区间信息'].apply(lambda x: f"'{x}")
# 删除market_code列
df_merge.drop(columns=['market_code'], inplace=True)
col_list = ['市值Z', '成交额', '竞价金额', '大单净额', '涨停封单额']
for col in col_list:
    df_merge[col] = df_merge[col].replace([np.inf, -np.inf], np.nan)
    df_merge.fillna(0, inplace=True)

df_merge['市值Z'] = df_merge['市值Z'].apply(lambda x: round(x/1e8))
df_merge['成交额'] = df_merge['成交额'].apply(lambda x: round(x/1e8,1))
df_merge['大单净额'] = df_merge['大单净额'].apply(lambda x: round(x/1e8,2))
df_merge['涨停封单额'] = df_merge['涨停封单额'].apply(lambda x: round(x/1e8,2))
df_merge['竞价金额'] = df_merge['竞价金额'].apply(lambda x: round(x/1e8,2))

print(df_merge.head(1))
print(df_merge.columns)

#保存合并数据，文件名merge_交易日.csv
trading_date = df_merge['交易日期'].iloc[0]
df_merge.to_csv(f'./data/csv/merge_{trading_date}.csv', index=False, encoding='utf-8-sig')