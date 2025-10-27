import asyncio
from datetime import datetime
from main import run_parser_async
from logger_config import setup_logger

logger = setup_logger(__name__)

class AsyncScheduler:
    def __init__(self):
        self.is_running = False
        self.task = None

    async def run_scheduled_parser(self):
        try:
            logger.info("=== Запуск парсера по расписанию ===")
            start_time = datetime.now()
            
            await run_parser_async()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Парсер завершил работу за {duration:.2f} секунд")
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении парсера: {e}", exc_info=True)

    async def start_scheduler(self):
        self.is_running = True
        logger.info("Асинхронный планировщик запущен")
        
        while self.is_running:
            await self.run_scheduled_parser()
            await asyncio.sleep(3600)

    async def stop_scheduler(self):
        self.is_running = False
        if self.task:
            self.task.cancel()
        logger.info("Планировщик остановлен")

async def main():
    scheduler = AsyncScheduler()
    
    try:
        await scheduler.run_scheduled_parser()
        await scheduler.start_scheduler()
    except KeyboardInterrupt:
        await scheduler.stop_scheduler()
    except Exception as e:
        logger.error(f"Ошибка в планировщике: {e}", exc_info=True)
        await scheduler.stop_scheduler()