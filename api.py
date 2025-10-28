from fastapi import FastAPI, HTTPException
from db_utilities import DataBaseManager, News
from logger_config import setup_logger

from pydantic import BaseModel
from typing import Any, Optional

class DefaultResponse(BaseModel):
    error: bool
    message: str
    payload: Optional[Any] = None

logger = setup_logger(__name__)

app = FastAPI(title="News Parser API")

@app.get("/news/{news_id}")
async def get_news_by_id(news_id: int):
    logger.info(f"Запрос новости с ID: {news_id}")
    db_manager = DataBaseManager()
    
    try:
        if not await db_manager.create_connection():
            logger.error("Ошибка подключения к базе данных")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        news_item = await db_manager.get_news_by_id(news_id)
        
        if not news_item:
            logger.warning(f"Новость с ID {news_id} не найдена")
            raise HTTPException(status_code=404, detail=f"News with id {news_id} not found")
        
        response = {
            "id": news_item.id,
            "title": news_item.title,
            "time": news_item.time.isoformat() if news_item.time else None,
            "link": news_item.link,
            "content": news_item.content,
            "created_at": news_item.created_at.isoformat() if news_item.created_at else None
        }
        
        logger.info(f"Успешно возвращена новость с ID: {news_id}")
        return response
        
    except Exception as e:
        logger.error(f"Ошибка при получении новости {news_id}: {e}")
        return DefaultResponse(
            error=True
            message="Ошибка сервера"
            payload=None
        )
    finally:
        await db_manager.close_connection()
