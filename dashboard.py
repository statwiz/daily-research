from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import os
from trading_calendar import TradingCalendar

import warnings
warnings.filterwarnings("ignore")

# 初始化交易日历
trading_calendar = TradingCalendar()

# 动态获取交易日期并配置基础路径
TRADE_DATE = trading_calendar.get_default_trade_date()
BASE_PATH = f"./output/{TRADE_DATE}/"

# 日期名称映射
day_map = {
    'Monday': '一',
    'Tuesday': '二', 
    'Wednesday': '三',
    'Thursday': '四',
    'Friday': '五',
    'Saturday': '六',
    'Sunday': '日'
}

def load_data(file_path):
    """加载数据并进行预处理"""
    try:
        df = pd.read_csv(file_path,dtype={'交易日期': str})
        # 转换日期格式
        df['交易日期'] = pd.to_datetime(df['交易日期'], format='%Y%m%d')
        # 重命名列以便后续处理
        df = df.rename(columns={'交易日期': '日期', '股票数量': '数量'})
        return df
    except Exception as e:
        st.error(f"数据加载失败: {e}")
        return None

def plot_pie(df, title):
    """绘制热点分布饼状图"""
    if df is None or df.empty:
        st.warning(f"无法生成{title}：数据为空")
        return
    
    # 检查是否有热点列
    if '热点' not in df.columns:
        st.warning(f"无法生成{title}：缺少热点列")
        return
    
    # 统计热点分布
    hotspot_counts = df['热点'].value_counts().reset_index()
    hotspot_counts.columns = ['热点', '股票数量']
    
    # 过滤掉一些通用热点，只保留有意义的热点
    exclude_hotspots = ['公告', '其他', 'ST板块', '']
    hotspot_counts = hotspot_counts[
        (~hotspot_counts['热点'].isin(exclude_hotspots)) & 
        (hotspot_counts['股票数量'] > 1)  # 只显示包含2只以上股票的热点
    ]
    
    if hotspot_counts.empty:
        st.warning(f"无法生成{title}：没有符合条件的热点数据")
        return
    
    # 取前10个热点
    hotspot_counts = hotspot_counts.head(10)
    
    # 绘制饼状图
    fig = px.pie(
        hotspot_counts,
        values='股票数量',
        names='热点',
        title=title,
        color_discrete_sequence=px.colors.sequential.RdPu
    )
    
    # 配置显示格式
    fig.update_traces(
        textinfo='label+value',
        hovertemplate='<b>%{label}</b><br>股票数量: %{value}<br>占比: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        height=600,
        width=800,
        font=dict(size=14)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def is_hotspot_similar(hotspot_a: str, hotspot_b: str) -> bool:
    """判断两个热点是否相似"""
    return (hotspot_a in hotspot_b) or (hotspot_b in hotspot_a)

def unify_similar_hotspots(df):
    """统一相似热点，保留最新日期的热点名称"""
    if df is None or df.empty:
        return df
    
    df_copy = df.copy()
    
    # 确定日期列名
    date_column = None
    if '日期' in df_copy.columns:
        date_column = '日期'
    elif '交易日期' in df_copy.columns:
        date_column = '交易日期'
    elif '异动日期' in df_copy.columns:
        date_column = '异动日期'
    
    if date_column is None:
        # 如果没有日期列，直接返回原数据
        return df_copy
    
    # 获取所有唯一热点
    unique_hotspots = df_copy['热点'].unique()
    
    # 存储热点映射关系
    hotspot_mapping = {}
    processed_hotspots = set()
    
    for hotspot_a in unique_hotspots:
        if hotspot_a in processed_hotspots:
            continue
            
        # 找到与hotspot_a相似的所有热点
        similar_hotspots = [hotspot_a]
        for hotspot_b in unique_hotspots:
            if hotspot_b != hotspot_a and hotspot_b not in processed_hotspots:
                if is_hotspot_similar(hotspot_a, hotspot_b):
                    similar_hotspots.append(hotspot_b)
        
        if len(similar_hotspots) > 1:
            # 找到这些相似热点中最新日期的热点名称
            similar_data = df_copy[df_copy['热点'].isin(similar_hotspots)]
            latest_hotspot_data = similar_data.loc[similar_data[date_column].idxmax()]
            target_hotspot = latest_hotspot_data['热点']
            
            # 建立映射关系
            for hotspot in similar_hotspots:
                hotspot_mapping[hotspot] = target_hotspot
                processed_hotspots.add(hotspot)
        else:
            processed_hotspots.add(hotspot_a)
    
    # 应用映射
    if hotspot_mapping:
        df_copy['热点'] = df_copy['热点'].map(lambda x: hotspot_mapping.get(x, x))
        
        # 如果有数量列，合并相同热点的数据（按日期分组）
        if '数量' in df_copy.columns:
            df_copy = df_copy.groupby([date_column, '热点'], as_index=False)['数量'].sum()
    
    return df_copy

def filter_data(df):
    """过滤掉ST板块、其他、公告热点"""
    if df is None:
        return None
    
    exclude_hotspots = ['ST板块', '其他', '公告']
    filtered_df = df[~df['热点'].isin(exclude_hotspots)]
    return filtered_df

def plot_hotspot_rotation(df):
    """绘制热点轮动图表"""
    if df is None or df.empty:
        st.warning("没有可用的数据")
        return
    
    # 获取排序后的日期
    sorted_dates = sorted(df["日期"].unique(), reverse=True)
    
    # 设置参数选项
    options_m = [1, 2, 3, 5, 10, 15, 20, 25, 30]
    options_n = [1, 2, 3, 5, 10, 15, 20, 25, 30]
    options_k = [5, 10, 15, 20, 25, 30]

    # 创建用户界面控件
    st.subheader("热点轮动参数设置")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        m = st.selectbox("热点聚合最近天数:", options_m, index=0)
    with col2:
        n = st.selectbox("展示热点前N名:", options_n, index=8)
    with col3:
        k = st.selectbox("图表展示天数:", options_k, index=1)

    # 数据处理逻辑
    top_dates = sorted_dates[:m]
    recent_data = df[df['日期'].isin(top_dates)]
    
    # 获取每日前N热点
    top_hotspots_per_date = (
        recent_data.sort_values(["日期", "数量"], ascending=[False, False])
        .groupby("日期")
        .head(n)
    )
    
    # 获取近期高位热点
    recent_hotspots = top_hotspots_per_date['热点'].unique()
    
    # 过滤包含高位热点的所有数据
    core_hotspots_data = df[df['热点'].isin(recent_hotspots)]
    
    # 创建日期和热点的笛卡尔积，补零缺失数据
    unique_dates = core_hotspots_data["日期"].unique()
    all_combinations = pd.DataFrame(
        [(date, hotspot) for date in unique_dates for hotspot in recent_hotspots],
        columns=["日期", "热点"]
    )
    
    # 合并数据并填充缺失值
    complete_data = all_combinations.merge(
        core_hotspots_data, on=["日期", "热点"], how="left"
    ).fillna({"数量": 0})
    
    complete_data["数量"] = complete_data["数量"].astype(int)
    
    # 筛选展示天数的数据
    display_data = complete_data[complete_data['日期'].isin(sorted_dates[:k])]
    
    # 创建日期显示格式
    display_data = display_data.copy()
    display_data['日期显示'] = (
        display_data['日期'].dt.strftime('%Y-%m-%d') + 
        '(' + display_data['日期'].dt.day_name().map(day_map) + ')'
    )
    
    # 按日期排序
    display_data = display_data.sort_values(by="日期")
    
    # 创建折线图
    fig = go.Figure()
    
    # 按最新日期的股票数量排序热点
    latest_date = display_data['日期'].max()
    latest_data = display_data[display_data['日期'] == latest_date]
    hotspots_sorted = latest_data.sort_values('数量', ascending=False)['热点'].tolist()
    
    # 为每个热点创建折线（按排序后的顺序）
    for hotspot in hotspots_sorted:
        hotspot_data = display_data[display_data['热点'] == hotspot]
        fig.add_trace(go.Scatter(
            x=hotspot_data['日期显示'],
            y=hotspot_data['数量'],
            mode='lines+markers+text',
            name=hotspot,
            text=hotspot_data['数量'],
            textposition='top center',
            line=dict(width=2),
            marker=dict(size=6)
        ))
    
    # 配置图表布局
    fig.update_layout(
        title="热点周期轮动",
        xaxis_title="日期",
        yaxis_title="股票数量",
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True),
        height=800,
        legend=dict(x=0.01, y=1),
        hovermode='x unified'
    )
    
    # 设置X轴标签倾斜
    fig.update_xaxes(tickangle=-45)
    
    # 显示图表
    st.plotly_chart(fig, use_container_width=True)
    

def plot_industry_distribution(df, title="行业分布"):
    """绘制行业股票数量分布柱状图"""
    if df is None or df.empty:
        st.warning("没有可用的数据")
        return
    
    if '行业' not in df.columns:
        st.warning("数据中缺少行业列")
        return
    
    # 统计每个行业的股票数量
    industry_counts = df['行业'].value_counts()
    
    # 按数量从小到大排序
    industry_counts = industry_counts.sort_values(ascending=True)
    
    if len(industry_counts) == 0:
        st.warning("没有行业数据")
        return
    
    # 创建柱状图
    fig = go.Figure(data=[
        go.Bar(
            x=industry_counts.index,
            y=industry_counts.values,
            text=industry_counts.values,
            textposition='auto',
            marker=dict(
                color=industry_counts.values,
                colorscale='viridis',
                showscale=True,
                colorbar=dict(title="股票数量")
            )
        )
    ])
    
    fig.update_layout(
        title=title,
        xaxis_title="行业",
        yaxis_title="股票数量",
        height=500,
        showlegend=False,
        xaxis_tickangle=-45
    )
    
    # 确保x轴按照我们排序的顺序显示
    fig.update_xaxes(categoryorder='array', categoryarray=industry_counts.index.tolist())
    
    st.plotly_chart(fig, use_container_width=True)

def plot_hotspot_distribution(df, title="热点分布"):
    """绘制热点股票数量分布柱状图"""
    if df is None or df.empty:
        st.warning("没有可用的数据")
        return
    
    if '热点' not in df.columns:
        st.warning("数据中缺少热点列")
        return
    
    # 应用相似热点合并逻辑
    df_unified = unify_similar_hotspots(df)
    
    # 过滤掉不需要的热点
    filtered_df = df_unified[
        ~df_unified['热点'].isin(['ST板块', '其他', '公告'])
    ]
    
    # 统计每个热点的股票数量
    hotspot_counts = filtered_df['热点'].value_counts()
    
    # 按数量从小到大排序
    hotspot_counts = hotspot_counts.sort_values(ascending=True)
    
    if len(hotspot_counts) == 0:
        st.warning("没有热点数据")
        return
    
    # 创建柱状图
    fig = go.Figure(data=[
        go.Bar(
            x=hotspot_counts.index,
            y=hotspot_counts.values,
            text=hotspot_counts.values,
            textposition='auto',
            marker=dict(
                color=hotspot_counts.values,
                colorscale='plasma',
                showscale=True,
                colorbar=dict(title="股票数量")
            )
        )
    ])
    
    fig.update_layout(
        title=title,
        xaxis_title="热点",
        yaxis_title="股票数量",
        height=500,
        showlegend=False,
        xaxis_tickangle=-45
    )
    
    # 确保x轴按照我们排序的顺序显示
    fig.update_xaxes(categoryorder='array', categoryarray=hotspot_counts.index.tolist())
    
    st.plotly_chart(fig, use_container_width=True)

def display_csv_data(file_path, title, description="", show_industry_chart=False, show_hotspot_chart=False):
    """展示CSV文件数据"""
    if not os.path.exists(file_path):
        st.warning(f"{title}数据文件不存在")
        return
    
    try:
        df = pd.read_csv(file_path,dtype={'交易日期': str,'涨停日期': str,'异动日期': str})
        
        # 显示标题和描述
        st.subheader(f"📊 {title}")
        if description:
            st.markdown(description)
        
        # 显示基本统计信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("总记录数", len(df))
        with col2:
            if '热点' in df.columns:
                unique_hotspots = df['热点'].nunique()
                st.metric("热点数量", unique_hotspots)
            elif '行业' in df.columns:
                unique_industries = df['行业'].nunique()
                st.metric("行业数量", unique_industries)
            else:
                st.metric("列数", len(df.columns))
        with col3:
            if '涨跌幅' in df.columns:
                avg_change = df['涨跌幅'].mean()
                st.metric("平均涨跌幅", f"{avg_change:.2f}%")
            elif '最新涨跌幅' in df.columns:
                avg_change = df['最新涨跌幅'].mean()
                st.metric("平均涨跌幅", f"{avg_change:.2f}%")
            elif '股票简称' in df.columns:
                st.metric("股票数量", len(df))
        
        # 显示数据表格
        st.dataframe(
            df, 
            use_container_width=True,
            height=400
        )
        
        # 如果需要显示行业分布图
        if show_industry_chart and '行业' in df.columns and len(df) > 0:
            st.markdown("**行业分布：**")
            plot_industry_distribution(df, f"{title}行业分布")
        
        # 如果需要显示热点分布图
        if show_hotspot_chart and '热点' in df.columns and len(df) > 0:
            st.markdown("**热点分布：**")
            plot_hotspot_distribution(df, f"{title}热点分布")
        
    except Exception as e:
        st.error(f"加载{title}数据失败: {e}")

def main():
    """主函数"""
    st.set_page_config(
        page_title="股票分析仪表板",
        page_icon="📈",
        layout="wide"
    )
    
    # 创建侧边栏导航
    st.sidebar.title("📊 导航菜单")
    page = st.sidebar.selectbox(
        "选择页面",
        ["热点轮动", "股票池数据"]
    )
    
    if page == "热点轮动":
        st.title("📈 热点轮动")
        st.markdown("---")
        show_hotspot_rotation()
        
    elif page == "股票池数据":
        st.title("📊 股票池数据")
        st.markdown("---")
        show_stock_pool_data()

def show_hotspot_rotation():
    """显示热点轮动页面"""
    # 数据文件路径
    data_path = "./data/csv/jygs/jygs_bk_his.csv"
    
    # 加载数据
    with st.spinner("正在加载数据..."):
        raw_data = load_data(data_path)
    
    if raw_data is not None:
        # 过滤数据
        filtered_data = filter_data(raw_data)
        
        # 统一相似热点
        unified_data = unify_similar_hotspots(filtered_data)
        
        # 绘制热点轮动图
        plot_hotspot_rotation(unified_data)
    else:
        st.error("无法加载数据，请检查文件路径和格式")

def show_stock_pool_data():
    """显示股票池数据页面"""
    # 文件路径配置
    files_config = {
        "core_stocks.csv": {
            "title": "高位股票池",
            "description": "包含高位股票的详细信息，包括市值、涨跌幅、热点等数据"
        },
        "first_stocks.csv": {
            "title": "低位股票池", 
            "description": "包含低位股票的详细信息，适合关注的潜力股票"
        },
        "add.csv": {
            "title": "新增股票",
            "description": "最新加入股票池的股票列表"
        },
        "remove.csv": {
            "title": "移除股票",
            "description": "从股票池中移除的股票列表"
        },
        "emerging_hotspots.csv": {
            "title": "新兴热点",
            "description": "新出现的市场热点和相关股票"
        },
        "hk_stocks.csv": {
            "title": "香港股票池",
            "description": "香港市场精选股票，按行业分布展示"
        },
        "us_stocks.csv": {
            "title": "美国股票池",
            "description": "美国市场精选股票，按行业分布展示"
        }
    }
    
    
    # 创建标签页
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["高位股票池", "低位股票池", "新增股票", "移除股票", "新兴热点", "香港股票池", "美国股票池"])
    
    with tab1:
        file_path = os.path.join(BASE_PATH, "core_stocks.csv")
        display_csv_data(file_path, "高位股票池", files_config["core_stocks.csv"]["description"], show_hotspot_chart=True)
    
    with tab2:
        file_path = os.path.join(BASE_PATH, "first_stocks.csv")
        display_csv_data(file_path, "低位股票池", files_config["first_stocks.csv"]["description"], show_hotspot_chart=True)
    
    with tab3:
        file_path = os.path.join(BASE_PATH, "add.csv")
        display_csv_data(file_path, "新增股票", files_config["add.csv"]["description"], show_hotspot_chart=True)
    
    with tab4:
        file_path = os.path.join(BASE_PATH, "remove.csv") 
        display_csv_data(file_path, "移除股票", files_config["remove.csv"]["description"], show_hotspot_chart=True)
    
    with tab5:
        file_path = os.path.join(BASE_PATH, "emerging_hotspots.csv")
        display_csv_data(file_path, "新兴热点", files_config["emerging_hotspots.csv"]["description"], show_hotspot_chart=True)
    
    with tab6:
        file_path = "./output/hk_stocks.csv"
        display_csv_data(file_path, "香港股票池", files_config["hk_stocks.csv"]["description"], show_industry_chart=True)
    
    with tab7:
        file_path = "./output/us_stocks.csv"
        display_csv_data(file_path, "美国股票池", files_config["us_stocks.csv"]["description"], show_industry_chart=True)

if __name__ == "__main__":
    main()

