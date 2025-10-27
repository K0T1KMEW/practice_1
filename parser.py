from bs4 import BeautifulSoup
import aiohttp
from datetime import datetime, timedelta
import re
import asyncio

from logger_config import setup_logger
logger = setup_logger(__name__)

class NewsParser:    
    def __init__(self, base_url='https://uralpolit.ru/news/urfo?date='):
        self.base_url = base_url
        self.timeout = aiohttp.ClientTimeout(total=30)

    @staticmethod
    def get_date_range():
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        return [now.strftime("%d.%m.%Y"), yesterday.strftime("%d.%m.%Y")]

    @staticmethod
    def is_within_24_hours(news_time_str, news_date_str):
        try:
            news_datetime = datetime.strptime(
                f"{news_date_str} {news_time_str}", 
                "%d.%m.%Y %H:%M"
            )
            return datetime.now() - news_datetime <= timedelta(hours=24)
        except Exception:
            return False

    async def fetch_page(self, session, date_str):
        try:
            async with session.get(self.base_url + date_str, timeout=self.timeout) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            logger.error(f"Error fetching page for date {date_str}: {e}")
            return None

    def parse_news_metadata(self, html_content, page_date):
        if not html_content:
            return {}
        
        soup = BeautifulSoup(html_content, "html.parser")
        news_dict = {}
        
        news_items = soup.find_all('article', class_='news-article')
        
        for news_item in news_items:
            title_element = news_item.find('a', class_='news-article__title')
            time_element = news_item.find('time')
            
            if title_element and time_element:
                href = title_element.get('href')
                news_time = time_element.get_text(strip=True)
                
                if ':' in news_time:
                    time_parts = news_time.split(':')
                    formatted_time = f"{time_parts[0]}:{time_parts[1]}" if len(time_parts) >= 2 else news_time
                else:
                    formatted_time = news_time
                
                if self.is_within_24_hours(formatted_time, page_date):
                    link = f"https://uralpolit.ru{href}" if href.startswith('/') else href
                        
                    news_dict[link] = {
                        'title': title_element.get_text(strip=True),
                        'time': formatted_time,
                        'content': ''
                    }
        
        return news_dict

    async def parse_news_content(self, session, link):
        try:
            async with session.get(link, timeout=self.timeout) as response:
                response.raise_for_status()
                html_content = await response.text()
            
            soup = BeautifulSoup(html_content, "html.parser")
            
            content_element = soup.find('div', itemprop='articleBody')
            
            if content_element:
                paragraphs = content_element.find_all('p')
                content = ' '.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                
                content = re.sub(r'\s+', ' ', content).strip()
                return content
            return ""
            
        except Exception as e:
            logger.error(f"Content parsing error for {link}: {e}")
            return ""

    async def enrich_news_with_content(self, session, news_dict):
        tasks = []
        for link, news_data in news_dict.items():
            task = self.parse_news_content(session, link)
            tasks.append((link, task))
        
        semaphore = asyncio.Semaphore(5)
        
        async def bounded_task(link, task):
            async with semaphore:
                content = await task
                return link, content
        
        bounded_tasks = [bounded_task(link, task) for link, task in tasks]
        results = await asyncio.gather(*bounded_tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in content parsing: {result}")
                continue
            link, content = result
            news_dict[link]['content'] = content
            
        return news_dict

    async def get_news(self):
        dates_to_parse = self.get_date_range()
        all_news = {}
        
        async with aiohttp.ClientSession() as session:
            for date_str in dates_to_parse:
                html_content = await self.fetch_page(session, date_str)
                if html_content:
                    news_for_date = self.parse_news_metadata(html_content, date_str)
                    all_news.update(news_for_date)
            
            if all_news:
                all_news = await self.enrich_news_with_content(session, all_news)
        
        return all_news