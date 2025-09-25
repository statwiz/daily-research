# -*- coding: utf-8 -*-
"""
Author: thua
Date: 2024/11/30
Time: 16:21
Description: 将通用的需求包装起来
"""

from pprint import pprint
import requests,os,sys
sys.path.append('/Users/thua/PycharmProjects/fupan')
import  pandas as pd
from datetime import datetime
from utils import send_dingding_msg,get_recent_trading_days
from wencai import Wencai
import warnings
warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.colheader_justify', 'left')  # 左对齐列标题
pd.set_option('display.precision', 2)       # 设置浮点数精度


class CrawlerUtils:

    DAY_MP = {0: '今', -1: '昨', -2: '前'}
    YY = 1e8

    @staticmethod
    def fanbao(row):
       if row['涨停分类'] == '首板':
           if row['几天几板'] == '首板涨停':
               return '其他'
           else:
               return '反包首板'
       elif '天' in row['几天几板']:
           d, b = str(row['几天几板'].split('天')[0]), str(row['连续涨停天数'])
           if d == b:
               return '其他'
           else:
               if b == '2':
                   return '反包二板'
               else:
                   return '反包三板及以上'
       else:
           return '其他'

    @staticmethod
    def dde(row):
        if row['dde大单净额']  > 0.5 * CrawlerUtils.YY:
            return '大幅流入'
        elif row['dde大单净额'] < -0.5 * CrawlerUtils.YY:
            return '大幅流出'
        else:
            return '其他'

    # @staticmethod
    # def save_file_append(file_path, df):
    #     today_dt = df['日期'].max()
    #     if os.path.exists(file_path):
    #         df_old = pd.read_excel(file_path, dtype={'日期': str})
    #         a = df_old['日期'].max()
    #         if a == today_dt:
    #             df = pd.concat([df, df_old[df_old['日期'] != a]])
    #         elif a < today_dt:
    #             df = pd.concat([df, df_old])
    #         else:
    #             raise Exception(' 错误，历史数据日期比今天爬取的最新数据日期还大 ')
    #     df.to_excel(file_path, index=False)
    #     print(f"{file_path} 保存成功")

    def get_zt_by_date(self,date):
        print('========= 开始按日期抓取涨停 ===========')
        q1 = f"非ST,上市板块,{date}日涨停,涨幅,连续涨停次数,几天几板,涨停时间,涨停类型,涨停原因类别,封单金额,同花顺二级行业"
        print(f' question 1 : {q1}')
        wc1 = Wencai(q1)
        zt1 = wc1.get_root_data_all_page()
        zt_df_1 = pd.DataFrame(zt1)
        # for i in zt_df_1.columns:
        #     print(i)
        # exit()

        q2 = f"非ST,上市板块,{date}日涨停,成交金额,自由流通市值,实际换手率"
        print(f' question 2 : {q2} ')
        wc2 = Wencai(q2)
        zt2 = wc2.get_root_data_all_page()
        zt_df_2 = pd.DataFrame(zt2)

        q3 = f'非ST，上市板块，{date}日涨停，dde大单净额'
        print(f' question 3 : {q3} ')
        wc3 = Wencai(q3)
        dde = wc3.get_root_data_all_page()
        dde_df = pd.DataFrame(dde)

        zt_df_1.columns = [i.split('[')[0].split(':')[0] for i in zt_df_1.columns]
        zt_df_2.columns = [i.split('[')[0].split(':')[0] for i in zt_df_2.columns]
        dde_df.columns = [i.split('[')[0].split(':')[0] for i in dde_df.columns]

        zt_df = zt_df_1.merge(zt_df_2[['code', '成交额','自由流通市值','实际换手率']], on="code")
        zt_df = zt_df.merge(dde_df[['code','dde大单净额']], on="code", how="left")

        format_name = ['上市板块','code', '股票简称', '自由流通市值','涨跌幅', '实际换手率','成交额', '涨停封单额', '连续涨停天数', '几天几板',
                       '涨停开板次数', 'dde大单净额', '涨停类型','涨停原因类别','首次涨停时间']
        zt_df = zt_df[format_name]
        na_rows = zt_df[zt_df[['连续涨停天数', '几天几板','涨停类型','涨停原因类别','首次涨停时间']].isna().any(axis=1)]
        if not na_rows.empty:
            print("实时抓的时候出现na的股票：\n",na_rows)
        zt_df = zt_df.dropna(subset=['连续涨停天数', '几天几板','涨停类型','涨停原因类别','首次涨停时间'])
        zt_df[['自由流通市值', '成交额', '涨停封单额', 'dde大单净额','涨跌幅','实际换手率']] = zt_df[
            ['自由流通市值', '成交额', '涨停封单额', 'dde大单净额','涨跌幅','实际换手率']].astype(float).fillna(0)
        zt_df['涨停分类'] = '连板'
        zt_df.loc[zt_df['连续涨停天数'] == 1, '涨停分类'] = '首板'
        zt_df['涨停细分'] = '五板及以上'
        zt_df.loc[zt_df['连续涨停天数'] == 1, '涨停细分'] = '首板'
        zt_df.loc[zt_df['连续涨停天数'] == 2, '涨停细分'] = '二板'
        zt_df.loc[zt_df['连续涨停天数'] == 3, '涨停细分'] = '三板'
        zt_df.loc[zt_df['连续涨停天数'] == 4, '涨停细分'] = '四板'
        zt_df['反包细分'] = zt_df.apply(self.fanbao, axis=1)
        zt_df['大单细分'] = zt_df.apply(self.dde, axis=1)
        zt_df['量能分类'] = '其他'
        cond = zt_df['涨停类型'].str.contains('一字涨停')
        zt_df.loc[cond , '量能分类'] = '一字涨停'
        zt_df = zt_df.sort_values(by=['连续涨停天数','实际换手率'],ascending=[False,False]).reset_index(drop=True)
        zt_df['日期'] = date
        print(f"爬取的涨停股票：{zt_df.shape[0]}")
        return zt_df

    def get_dt_by_date(self, date):
        q1 = f'非ST，上市板块，{date}日跌停,涨跌幅,连续跌停天数,跌停类型,跌停原因类型,跌停封单额'
        print(f'========= question : {q1} ===========')
        wc = Wencai(q1)
        dt = wc.get_root_data_all_page()
        dt_df_1 = pd.DataFrame(dt)
        if dt_df_1.empty:
            colu_names = ['上市板块', 'code', '股票简称', '自由流通市值', '涨跌幅', '实际换手率', '成交额', '跌停封单额',
                   '连续跌停天数', '跌停开板次数', '跌停类型', '跌停原因类型', '跌停细分', '量能分类']
            return pd.DataFrame(columns=colu_names)
        q2 = f"非ST,上市板块,{date}日跌停,成交金额,自由流通市值,实际换手率"
        print(f' question 2 : {q2} ')
        wc2 = Wencai(q2)
        dt2 = wc2.get_root_data_all_page()
        dt_df_2 = pd.DataFrame(dt2)
        dt_df_1.columns = [i.split('[')[0].split(':')[0] for i in dt_df_1.columns]
        dt_df_2.columns = [i.split('[')[0].split(':')[0] for i in dt_df_2.columns]
        dt_df = dt_df_1.merge(dt_df_2[['code', '成交额','自由流通市值','实际换手率']], on="code")

        format_name = ['上市板块', 'code', '股票简称', '自由流通市值', '涨跌幅', '实际换手率', '成交额', '跌停封单额',
                       '连续跌停天数','跌停开板次数', '跌停类型','跌停原因类型']
        dt_df = dt_df[format_name]
        dt_df[['自由流通市值', '成交额', '跌停封单额', '涨跌幅', '实际换手率']] = dt_df[
            ['自由流通市值', '成交额', '跌停封单额', '涨跌幅', '实际换手率']].astype(float).fillna(0)

        dt_df['跌停细分'] = '三板及以上'
        dt_df.loc[dt_df['连续跌停天数'] == 1, '跌停细分'] = '首板'
        dt_df.loc[dt_df['连续跌停天数'] == 2, '跌停细分'] = '二板'

        dt_df['量能分类'] = '其他'
        dt_df.loc[(dt_df['跌停类型'].str.contains('一字跌停'))  , '量能分类'] = '一字跌停'
        print(f"爬取的跌停股票：{dt_df.shape[0]}")
        return dt_df

    def get_zb_by_date(self, date):
        q = f'非ST，上市板块，{date}日曾涨停，自由流通市值'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        zb = wc.get_root_data_all_page()
        zb_df = pd.DataFrame(zb)
        zb_df.columns = [i.split('[')[0].split(':')[0] for i in zb_df.columns]
        format_name = ['上市板块', 'code', '股票简称','最新涨跌幅','自由流通市值']
        zb_df = zb_df[format_name]
        zb_df['最新涨跌幅'] =  zb_df['最新涨跌幅'].fillna(0).astype(float).round(2)
        zb_df['自由流通市值'] =  (zb_df['自由流通市值'].fillna(0).astype(float) / 1e8).round()
        print(f"爬取的炸板股票：{zb_df.shape[0]}")
        return zb_df

    def hot50_by_date(self,date):
        q = f'非ST，{date}日的个股热度排名<100，{date}日上市天数>5'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        hot = wc.get_root_data_all_page()
        hot_df = pd.DataFrame(hot)
        hot_df.columns = [i.split('[')[0].split(':')[0] for i in hot_df.columns]
        format_name = ['code', '股票简称','个股热度排名','上市天数']
        hot_df = hot_df[format_name].sort_values(by='个股热度排名')
        print(f"爬取的热门股票：{hot_df.shape[0]}")
        return hot_df.head(50)

    def get_recent_d_days_top_rank(self, d=5, r=100):
        q = f"所属概念,非ST,上市天数大于5,实际换手率大于3%,自由流通市值大于3亿,最高价大于最低价,收盘价大于最近5个交易日的区间平均收盘价,最近{d}个交易日的区间涨跌幅从大到小排序前{r}"
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        zb = wc.get_root_data_first_page()
        df_dr = pd.DataFrame(zb)
        # for i in df_dr.columns:
        #     print(i)
        cols = []
        for i in df_dr.columns:
            a = i.split('[')[0].split(':')
            if len(a) > 1:
                cols.append(a[0] + '_' + a[1])
            else:
                cols.append(a[0])
        df_dr.columns = cols

        # for i in df_dr.columns:
        #     print(i)

        df_dr.rename(columns={'区间涨跌幅_前复权': f'{d}日涨跌幅', '区间涨跌幅_前复权排名': '个股排名','code':'个股代码'}, inplace=True)

        format_name = ['个股代码', '股票简称', f'{d}日涨跌幅', '个股排名', '最新涨跌幅','所属概念']
        df_dr = df_dr[format_name]

        return df_dr

    def block_range_rank(self, d = 5, r = 10):
        q = f'同花顺概念指数{d}日区间涨跌幅从大到小排名前{r}'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        block = wc.get_root_data_first_page(tp='zhishu')
        block_df = pd.DataFrame(block)
        block_df.columns = [i.split('[')[0] for i in block_df.columns]
        block_df.rename(columns={'指数@涨跌幅:前复权': '指数今日涨跌幅', '指数@区间涨跌幅:不复权': f'指数{d}日',
                                 '指数@区间涨跌幅:不复权排名': '指数排名','code':'概念代码'}, inplace=True)

        format_name = ['指数简称',f'指数{d}日','指数今日涨跌幅']
        block_df = block_df[format_name]
        block_df[f'指数{d}日'] = block_df[f'指数{d}日'].astype(float).round(2)
        block_df['指数今日涨跌幅'] = block_df['指数今日涨跌幅'].astype(float).round(2)
        block_df[f'指数{d}日排名'] = block_df.index + 1
        return block_df

    def block_rank_by_date(self, dt, r=5):
        q = f'同花顺概念指数{dt}日的涨跌幅从大到小排名前{r}'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        block = wc.get_root_data_first_page(tp='zhishu')
        block_df = pd.DataFrame(block)
        # for i in block_df.columns:
        #     print(i)

        # block_df.columns = [i.split('[')[0] for i in block_df.columns]
        block_df.rename(columns={f'指数@涨跌幅:前复权[{dt}]': '指数涨跌幅',
                                 'code':'概念代码'}, inplace=True)

        format_name = ['指数简称','指数涨跌幅']
        block_df = block_df[format_name]
        block_df['指数涨跌幅'] = block_df['指数涨跌幅'].astype(float).round(2)

        return block_df

    def block_rank_muti_dates(self, k=10, r=5):
        ds = get_recent_trading_days(kline_days=k)
        block_df_lst = []
        for i,dt in enumerate(ds):
            block_df = self.block_rank_by_date(dt,r)
            block_df = block_df[block_df['指数涨跌幅']  > 0 ]
            if block_df.shape[0] == 0:
                continue
            block_df['日期'] = dt
            block_df['启动第几日'] = i + 1
            block_df['排名'] = block_df.index + 1
            # block_df = block_df.reset_index(drop=True)
            block_df_lst.append(block_df)
            # print(block_df)
        return pd.concat(block_df_lst,axis=0)


    def sector_range_rank(self, d = 5, r = 10):
        # q = f'同花顺板块{d}日区间涨跌幅排名前100，非同花顺特色指数，所属同花顺行业级别'
        q = f'同花顺板块{d}日区间涨跌幅排名前{r}，非同花顺特色指数，所属同花顺行业级别是二级行业'


        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        block = wc.get_root_data_first_page(tp='zhishu')
        block_df = pd.DataFrame(block)
        # for i in block_df.columns:
        #     print(i)

        block_df.columns = [i.split('[')[0] for i in block_df.columns]
        block_df.rename(columns={'指数@涨跌幅:前复权': '指数今日涨跌幅', '指数@区间涨跌幅:不复权': f'指数{d}日',
                                 '指数@所属同花顺行业级别': '行业级别','code':'行业代码'}, inplace=True)
        # block_df = block_df[block_df['行业级别'] != '三级行业'].reset_index()
        block_df = block_df[block_df['行业级别'] == '二级行业'].reset_index()

        format_name = ['指数简称',f'指数{d}日','指数今日涨跌幅']
        block_df = block_df[format_name]
        block_df[f'指数{d}日'] = block_df[f'指数{d}日'].astype(float).round(2)
        block_df['指数今日涨跌幅'] = block_df['指数今日涨跌幅'].astype(float).round(2)
        block_df[f'指数{d}日排名'] = block_df.index + 1
        return block_df.head(r)

    def sector_rank_by_date(self, dt, r=5):
        # q = f'同花顺板块{dt}日的涨跌幅排名前100，非同花顺特色指数，所属同花顺行业级别'
        q = f'同花顺板块{dt}日的涨跌幅排名前{r}，非同花顺特色指数，所属同花顺行业级别是二级行业'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        block = wc.get_root_data_first_page(tp='zhishu')
        block_df = pd.DataFrame(block)
        # for i in block_df.columns:
        #     print(i)
        # block_df.columns = [i.split('[')[0] for i in block_df.columns]
        block_df.rename(columns={f'指数@涨跌幅:前复权[{dt}]': '指数涨跌幅','指数@所属同花顺行业级别':'行业级别',
                                 'code':'行业代码'}, inplace=True)
        # block_df = block_df[block_df['行业级别'] != '三级行业'].reset_index()
        block_df = block_df[block_df['行业级别'] == '二级行业'].reset_index()
        format_name = ['指数简称','指数涨跌幅']
        block_df = block_df[format_name]
        block_df['指数涨跌幅'] = block_df['指数涨跌幅'].astype(float).round(2)

        return block_df.head(r)



    def sector_rank_muti_dates(self, k=10, r=5):
        ds = get_recent_trading_days(kline_days=k)
        block_df_lst = []
        for i,dt in enumerate(ds):
            block_df = self.sector_rank_by_date(dt,r)
            block_df = block_df[block_df['指数涨跌幅']  > 0 ]
            if block_df.shape[0] == 0:
                continue
            block_df['日期'] = dt
            block_df['启动第几日'] = i + 1
            block_df['排名'] = block_df.index + 1
            # block_df = block_df.reset_index(drop=True)
            block_df_lst.append(block_df)
            # print(block_df)
        return pd.concat(block_df_lst,axis=0)



    def longtou_by_block(self,block,d=5,r=3):
        q = f'非ST,非新股,所属概念包含{block},{d}日区间涨跌幅从大到小排名前{r}'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        longtou = wc.get_root_data_first_page()
        longtou_df = pd.DataFrame(longtou)
        longtou_df.columns = [i.split('[')[0] for i in longtou_df.columns]
        # for i in longtou_df.columns:
        #     print(i)
        longtou_df.rename(columns={'区间涨跌幅:前复权': f'{d}日涨跌幅', '区间涨跌幅:前复权排名': '个股排名','code':'个股代码'},
                                inplace=True)
        format_name = ['个股代码', '股票简称', f'{d}日涨跌幅', '个股排名', '最新涨跌幅']
        longtou_df = longtou_df[format_name]
        longtou_df['所属概念'] = block
        # print(longtou_df)
        return longtou_df

    def longtou_by_sector(self,block,d=5,r=3):
        q = f'非ST,{block},全市场{d}日区间涨跌幅从大到小排名前{r}'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        longtou = wc.get_root_data_first_page()
        longtou_df = pd.DataFrame(longtou)
        format_name = ['个股代码', '股票简称', f'{d}日涨跌幅', '个股排名', '最新涨跌幅']
        if longtou_df.empty:
            return pd.DataFrame(columns=format_name + ['所属概念'])
        longtou_df.columns = [i.split('[')[0] for i in longtou_df.columns]
        # for i in longtou_df.columns:
        #     print(i)
        #
        # exit()
        longtou_df.rename(columns={'区间涨跌幅:前复权': f'{d}日涨跌幅', '区间涨跌幅:前复权排名': '个股排名','code':'个股代码'},
                                inplace=True)
        longtou_df = longtou_df[format_name]
        longtou_df['所属概念'] = block
        # print(longtou_df)
        return longtou_df

    def get_all_market(self):
        ts = get_recent_trading_days(kline_days=2)[0]
        all_market_data_path = f"/Users/thua/PycharmProjects/fupan/all_market_hq_data/all_market_data_{ts}.xlsx"
        q = f'非ST股,非新股,个股热度排名,上市板块,上市天数,成交额,5日涨幅,10日涨幅,自由流通市值,同花顺二级行业,所属概念,今日开盘价,今日最高价,今日最低价'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        zt = wc.get_root_data_all_page()
        # zt = wc.get_root_data_first_page()
        df = pd.DataFrame(zt)
        # for i in df.columns:
        #     print(i)
        target_cols = sorted([i for i, col in enumerate(df.columns) if "区间涨跌幅:前复权" in col])
        if len(target_cols) == 2:
            df = df.rename(columns={
                df.columns[target_cols[0]]: '10日涨幅',  # 第一个匹配的列
                df.columns[target_cols[1]]: '5日涨幅'  # 第二个匹配的列
            })
        df.columns = [i.split('[')[0].split(':')[0] for i in df.columns]
        # for i in df.columns:
        #     print(i)
        format_name = ['code', '股票简称', '上市板块', '所属同花顺二级行业','所属概念','自由流通市值',
                       '个股热度排名', '成交额', '最新价', '最新涨跌幅', '上市天数',
                       '5日涨幅','10日涨幅', '开盘价', '最高价','最低价']
        df = df[format_name]
        numeric_cols = ['自由流通市值',
                       '个股热度排名', '成交额', '最新价', '最新涨跌幅', '上市天数',
                       '5日涨幅', '10日涨幅', '开盘价','最高价','最低价']
        df[numeric_cols] = df[numeric_cols].astype(float).fillna(0)

        df['自由流通市值'] = (df['自由流通市值']/1e8).round().astype(int)
        df['昨日收盘价'] = df.apply(lambda x : round(x['最新价'] / (1 + x['最新涨跌幅']/100), 2)  if x['最新价'] > 0 else -1000, axis=1)
        df['实体涨幅'] = df.apply(lambda x : round((x['最新价'] / x['开盘价'] - 1) * 100, 2)  if x['开盘价'] > 0 else -1000, axis=1)
        df['开盘涨幅'] = df.apply(lambda x : round((x['开盘价'] / x['昨日收盘价'] - 1) * 100, 2)  if x['开盘价'] > 0 else -1000, axis=1)
        df['最大涨幅'] = df.apply(lambda x : round((x['最高价'] / x['昨日收盘价'] - 1) * 100, 2)  if x['昨日收盘价'] > 0 else -1000, axis=1)
        df['今最大回撤'] = df.apply(lambda x : round((x['最新价'] / x['最高价'] - 1) * 100, 2)  if x['最高价'] > 0 else -1000, axis=1)


        df.loc[df['上市板块'].isin(['创业板', '科创板']), '上市板块'] = '双创'
        df = df[df['上市天数'] > 1]
        df = df.sort_values(by='5日涨幅', ascending=False).reset_index(drop=True)
        df['5日排名'] = df.index + 1

        df['5日涨幅'] = df['5日涨幅'].round(2)
        df['10日涨幅'] = df['10日涨幅'].round(2)

        print(f"爬取的全市场股票：{df.shape[0]}")
        df.to_excel(all_market_data_path, index=False)
        return df

    def get_recent_k_days(self,k=9):
        q = f'非ST，上市板块，{k}日的涨停次数'
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        zb = wc.get_root_data_all_page()
        df_zt_kd = pd.DataFrame(zb)
        df_zt_kd.columns = [i.split('[')[0].split(':')[0] for i in df_zt_kd.columns]
        format_name = ['上市板块', 'code', '股票简称', '涨停次数']
        df_zt_kd = df_zt_kd[format_name]
        df_zt_kd['涨停次数'] = df_zt_kd['涨停次数'].fillna(0)
        df_zt_kd = df_zt_kd.rename(columns={'涨停次数': f'近{k}日涨停'})
        df_zt_kd = df_zt_kd.sort_values(by=f'近{k}日涨停',ascending=False).reset_index(drop=True)
        print(f"爬取的{k}日涨停股票数：{df_zt_kd.shape[0]}")
        return df_zt_kd

    def get_recent_k_days_cumulative_gain(self,bk='创业板或者科创板',k=10, n= 20):
        if k == 1:
            q = f"非ST,上市天数大于5,上市板块,自由流通市值,{bk},今日涨跌幅从大到小排名"
        else:
            q = f"非ST,上市天数大于5,上市板块,自由流通市值,{bk},{k}日涨跌幅从大到小排名"
        print(f'========= question : {q} ===========')
        wc = Wencai(q)
        zb = wc.get_root_data_first_page()
        df_zt_kd = pd.DataFrame(zb)
        cols = []
        for i in df_zt_kd.columns:
            a = i.split('[')[0].split(':')
            if len(a) > 1:
                cols.append(a[0] + '_' + a[1])
            else:
                cols.append(a[0])
        df_zt_kd.columns = cols
        if k > 1:
            format_name = ['上市板块', 'code', '股票简称', '自由流通市值', '区间涨跌幅_前复权']
            df_zt_kd = df_zt_kd[format_name]
            df_zt_kd['区间涨跌幅_前复权'] = df_zt_kd['区间涨跌幅_前复权'].fillna(0)
            df_zt_kd = df_zt_kd.rename(columns={'区间涨跌幅_前复权': f'{k}日涨幅'})
            df_zt_kd[f'{k}日涨幅'] = df_zt_kd[f'{k}日涨幅'].astype(float).round(2)
            df_zt_kd = df_zt_kd.sort_values(by=f'{k}日涨幅', ascending=False).reset_index(drop=True)

        else:
            format_name = ['上市板块', 'code', '股票简称', '自由流通市值', '涨跌幅_前复权']
            df_zt_kd = df_zt_kd[format_name]
            df_zt_kd['涨跌幅_前复权'] = df_zt_kd['涨跌幅_前复权'].fillna(0)
            df_zt_kd = df_zt_kd.rename(columns={'涨跌幅_前复权': '今日涨幅'})
            df_zt_kd['今日涨幅'] = df_zt_kd['今日涨幅'].astype(float).round(2)
            df_zt_kd = df_zt_kd.sort_values(by='今日涨幅', ascending=False).reset_index(drop=True)


        df_zt_kd['自由流通市值'] = (df_zt_kd['自由流通市值'] / 1e8).round()
        print(f"爬取的{k}日涨幅'：{df_zt_kd.shape[0]}")
        return df_zt_kd.head(n)

def save_file_append(file_path, df, topk = 60 , reverse = False):
    today_dt = df['日期'].max()
    if os.path.exists(file_path):
        df_old = pd.read_excel(file_path, dtype={'日期': str})
        a = df_old['日期'].max()
        if a == today_dt:
            if not reverse:
                df = pd.concat([df_old[df_old['日期'] != a], df])
            else:
                df = pd.concat([df, df_old[df_old['日期'] != a]])
        elif a < today_dt:
            if not reverse:
                df = pd.concat([df_old,df])
            else:
                df = pd.concat([df,df_old])
        else:
            raise Exception(' 错误，历史数据日期比今天爬取的最新数据日期还大 ')

    unique_dates = df['日期'].unique()
    sorted_dates = sorted(unique_dates, reverse=True)[:topk]  # 只留3个月
    df = df[df['日期'].isin(sorted_dates)]
    df.to_excel(file_path, index=False)
    print(f"{file_path} 保存成功")

if __name__ == '__main__':
    ds = get_recent_trading_days(kline_days=2)
    # print(ds)
    cus = CrawlerUtils()


    # block_df = cus.longtou_by_sector('NMN概念')
    # col = ['NMN概念','美容护理','跨境电商','参股银行','工业大麻','国企改革','人民币贬值受益','回购增持再贷款概念','融资融券']
    # d = block_df[block_df['指数简称'].isin(col)]
    # print(d)
    # print(block_df)

    # today_zt_df = cus.get_zt_by_date(ds[0])
    # print(today_zt_df.head())
    # exit()
    # file_path = "/Users/thua/PycharmProjects/fupan/ths/ths_zt_his.xlsx"
    # save_file_append(file_path, today_zt_df, reverse=False)


    # d = cus.get_zt_by_date('20250509')
    # print(d)

    # df = cus.block_rank_muti_dates(k=10, r=10)
    # print(df)


    # block_df = cus.sector_range_rank()

    # block_df = cus.block_rank_by_date(20250514, 15)

    # print(block_df)
    # top_block_lst = block_df['指数简称'].tolist()
    #
    # print(top_block_lst)
    # lt_lst = []
    # for block in top_block_lst:
    #     if block != 'ST板块':
    #         print(block)
    #         longtou_df = cus.longtou_by_block(block = block ,d=5,r=3)
    #         lt_lst.append(longtou_df)
    # df = pd.concat(lt_lst)
    # df['rnk'] = df['个股排名'].apply(lambda x : int(x.split('/')[0]))
    # df = df.drop_duplicates(subset=['股票简称'], keep='first')
    # df['5日涨跌幅'] = df['5日涨跌幅'].astype(float).round(2)
    # df['最新涨跌幅'] = df['最新涨跌幅'].astype(float).round(2)
    # df = df[df['rnk']<=100][['所属概念','个股代码','股票简称','5日涨跌幅','rnk','最新涨跌幅']]
    # df = pd.merge(df,block_df[['指数简称','指数5日','指数涨跌幅']],how='left',left_on='所属概念',right_on='指数简称')
    # df = df.drop(columns='所属概念')
    # pprint(df)




    # df_dr = cus.get_recent_d_days_top_rank(5, 100)
    # # print(df_dr)
    #
    # df_dr['逻辑'] = df_dr['所属概念'].apply(foo, args=(top_block_lst,))
    # cols = ['个股代码','股票简称','5日涨跌幅','逻辑','最新涨跌幅']
    #
    # df_dr = df_dr[cols]
    # print(df_dr)










    # today_dt_df = cus.get_dt_by_date(ds[0])
    # # today_zb_df = cus.get_zb_by_date(ds[0])
    # today_zt_df = cus.get_zt_by_date(ds[0])
    # print(today_dt_df)

    # cus = CrawlerUtils()
    # df_zt_9d = cus.get_recent_k_days(k=9)
    # print(df_zt_9d.sort_values(by=''))
    today_zt_df = cus.get_all_market()
    print(today_zt_df.head())

    # df_3d_10cm = cus.get_recent_k_days_cumulative_gain(bk='主板', k=3, n=20)
    # df_3d_20cm = cus.get_recent_k_days_cumulative_gain(bk='创业板或者科创板', k=3, n=20)
    # df_3d_30cm = cus.get_recent_k_days_cumulative_gain(bk='创业板或者科创板', k=1, n=20)
    # print(df_3d_30cm)



