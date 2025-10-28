import os
from sqlalchemy import Column, Integer, String, DateTime, Text, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select
from datetime import datetime
from dotenv import load_dotenv

from logger_config import setup_logger

logger = setup_logger(__name__)

load_dotenv()

Base = declarative_base()

class News(Base):
    __tablename__ = 'news'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text, nullable=False)
    time = Column(DateTime, nullable=False)
    link = Column(String(500), unique=True, nullable=False)
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.now)

class DataBaseManager:
    def __init__(self):
        self.engine = None
        self.async_session = None
        self.session = None
        self._initialized = False

    async def initialize_database(self):
        if self._initialized:
            return True
            
        try:
            database_name = os.getenv('DB_NAME', 'news_db')
            database_url = f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{database_name}"
            
            self.engine = create_async_engine(database_url, echo=False)
            self.async_session = async_sessionmaker(
                self.engine, 
                class_=AsyncSession, 
                expire_on_commit=False
            )
            
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            logger.info("База данных и таблицы инициализированы")
            self._initialized = True
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Ошибка инициализации базы данных: {e}")
            return False

    async def create_connection(self):
        logger.info(f"Создание подключения: _initialized={self._initialized}, async_session={self.async_session is not None}")
        
        if not self._initialized:
            logger.info("База не инициализирована, запускаем инициализацию")
            if not await self.initialize_database():
                return False
        
        try:
            if not self.async_session:
                logger.info("async_session отсутствует, запускаем инициализацию")
                if not await self.initialize_database():
                    return False
            
            self.session = self.async_session()
            logger.info(f"Сессия создана успешно: {self.session is not None}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Ошибка подключения: {e}")
            return False

    async def insert_news(self, news_dict):
        if not self.session:
            logger.error("Нет активной сессии с базой данных")
            return 0
            
        try:
            inserted_count = 0
            
            for link, news_data in news_dict.items():
                existing_news = await self.session.execute(
                    select(News).where(News.link == link)
                )
                existing_news = existing_news.scalar_one_or_none()
                
                if not existing_news:
                    news = News(
                        title=news_data['title'],
                        time=news_data['time'],
                        link=link,
                        content=news_data.get('content', '')
                    )
                    
                    self.session.add(news)
                    inserted_count += 1
            
            await self.session.commit()
            logger.info(f"Успешно добавлено {inserted_count} новостей")
            return inserted_count
            
        except SQLAlchemyError as e:
            logger.error(f"Ошибка вставки данных: {e}")
            await self.session.rollback()
            return 0

    async def close_connection(self):
        if self.session:
            await self.session.close()
            logger.info("Сессия с базой данных закрыта")
        
        if self.engine:
            await self.engine.dispose()

    async def clear_database(self):
        if not self.session:
            logger.error("Нет активной сессии с базой данных")
            return False
            
        try:
            await self.session.execute(text("TRUNCATE TABLE news RESTART IDENTITY"))
            await self.session.commit()
            logger.info("База данных успешно очищена")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Ошибка очистки базы данных: {e}")
            await self.session.rollback()
            return False

    async def get_existing_links(self):
        if not self.session:
            return set()
            
        try:
            result = await self.session.execute(select(News.link))
            links = result.scalars().all()
            return {link for link in links}
        except SQLAlchemyError as e:
            logger.error(f"Ошибка получения ссылок: {e}")
            return set()

    async def get_news_by_id(self, news_id: int):
        if not self.session:
            return None
            
        try:
            result = await self.session.execute(
                select(News).where(News.id == news_id)
            )
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Ошибка получения новости по ID: {e}")
            return None