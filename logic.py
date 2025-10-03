import requests
import re
import json
from bs4 import BeautifulSoup

cookies = {
    'Hm_lvt_2d6d056d37910563cdaa290ee2981080': '1753201969',
    'Hm_lvt_58aa18061df7855800f2a1b32d6da7f4': '1753201969',
    'time': '1',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh,zh-CN;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.jiuyangongshe.com/search/new?k=%E8%81%9A%E5%90%88%E6%9D%90%E6%96%99',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
}

params = {
    'k': '聚合材料',
}

def parse_jygs_articles(html_content):
    """解析韭研公社文章列表"""
    articles = []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 方法1: 尝试从页面中直接解析文章内容
        article_sections = soup.find_all('section')
        
        for section in article_sections:
            title_elem = section.find('div', class_='book-title')
            if not title_elem:
                continue
                
            title = title_elem.get_text().strip()
            
            # 查找时间
            time_elem = section.find('div', class_='fs13-ash')
            create_time = time_elem.get_text().strip() if time_elem else ""
            
            # 查找内容
            content_elem = section.find('div', class_='html-text')
            if content_elem:
                # 移除高亮标签，获取纯文本
                for highlight in content_elem.find_all('span', class_='highlights-text'):
                    highlight.unwrap()
                content = content_elem.get_text().strip()
            else:
                content = ""
            
            if title and create_time:
                articles.append({
                    'title': title,
                    'content': content,
                    'create_time': create_time
                })
        
        # 方法2: 如果方法1没有获取到数据，尝试解析JavaScript中的数据
        if not articles:
            # 查找window.__NUXT__中的数据
            script_pattern = r'window\.__NUXT__=.*?;'
            script_match = re.search(script_pattern, html_content, re.DOTALL)
            
            if script_match:
                # 这里需要更复杂的解析，因为数据被压缩了
                # 可以尝试使用正则表达式提取title和时间信息
                title_pattern = r'title:"([^"]+)"'
                content_pattern = r'content:"([^"]*)"'
                time_pattern = r'create_time:"([^"]+)"'
                
                titles = re.findall(title_pattern, html_content)
                contents = re.findall(content_pattern, html_content)
                times = re.findall(time_pattern, html_content)
                
                # 组合数据
                for i in range(min(len(titles), len(times))):
                    content = contents[i] if i < len(contents) else ""
                    articles.append({
                        'title': titles[i],
                        'content': content,
                        'create_time': times[i]
                    })
        
    except Exception as e:
        print(f"解析错误: {e}")
    
    return articles

# 读取本地HTML文件
try:
    with open('tmp.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # 解析文章
    articles = parse_jygs_articles(html_content)
except FileNotFoundError:
    print("tmp.html文件不存在")
    articles = []

# 输出JSON格式
result = {
    'status': 'success',
    'total': len(articles),
    'articles': articles
}

print(json.dumps(result, ensure_ascii=False, indent=2))