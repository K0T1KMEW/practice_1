import sys
import asyncio
from db_utilities import DataBaseManager
from parser import NewsParser

from logger_config import setup_logger
logger = setup_logger(__name__)

async def run_parser_async():
    logger.info("Запуск парсера новостей...")
    
    parser = NewsParser()
    news_dict = await parser.get_news()
    
    if not news_dict:
        logger.warning("Не удалось получить новости")
        return
    
    logger.info(f"Найдено {len(news_dict)} новостей для обработки")
    
    db_manager = DataBaseManager()
    
    try:
        connection_result = await db_manager.create_connection()
        logger.info(f"Результат создания подключения: {connection_result}")
        
        if connection_result:
            inserted_count = await db_manager.insert_news(news_dict)
            
            if inserted_count == 0:
                logger.info("Нет новых новостей для добавления")
            else:
                logger.info(f"Добавлено {inserted_count} новых новостей")
        else:
            logger.error("Не удалось подключиться к базе данных")
    
    except Exception as e:
        logger.error(f"Ошибка при работе с базой данных: {e}")
    finally:
        await db_manager.close_connection()

async def run_api_async():
    logger.info("Запуск API сервера...")
    import uvicorn
    
    config = uvicorn.Config(
        "api:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=False,
        log_config=None
    )
    server = uvicorn.Server(config)
    await server.serve()

async def run_scheduler_async():
    logger.info("Запуск планировщика...")
    from scheduler import main as scheduler_main
    await scheduler_main()

async def clear_database_async():
    db_manager = DataBaseManager()
    try:
        if await db_manager.create_connection():
            if await db_manager.clear_database():
                logger.info("База данных очищена")
            else:
                logger.error("Не удалось очистить базу данных")
        else:
            logger.error("Не удалось подключиться к базе данных")
    finally:
        await db_manager.close_connection()

async def main_async():
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        logger.info("Запуск в режиме очистки базы данных")
        await clear_database_async()
        return
    
    await asyncio.gather(
        run_api_async(),
        run_scheduler_async(),
        return_exceptions=True
    )

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()