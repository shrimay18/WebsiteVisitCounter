from typing import Dict, List, Any
import asyncio
from datetime import datetime, timedelta
from ..core.redis_manager import RedisManager
from collections import defaultdict

class VisitCounterService:
    cache_ttl = 50 
    buffer_flush_interval = 30

    def __init__(self, redis_manager: RedisManager):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = redis_manager 

        self.visit_count_cache: Dict[str, Dict] = {}
        self.cache_locks = defaultdict(asyncio.Lock)

        self.write_buffer = defaultdict(int)
        self.buffer_locks = defaultdict(asyncio.Lock)

        print("Initializing VisitCounterService...")
        asyncio.create_task(self.flush_buffer())

    async def flush_buffer(self) -> None:
        while True:
            print("Flushing buffer...")
            await asyncio.sleep(VisitCounterService.buffer_flush_interval)
            for page_id in list(self.write_buffer.keys()):
                await self.flush_buffer_key(page_id)
                print(f"Flushed buffer for page_id: {page_id}")

    async def flush_buffer_key(self, page_id: str) -> None:
        if page_id not in self.write_buffer:
            print(f"No buffer to flush for page_id: {page_id}")
            return 
        
        async with self.buffer_locks[page_id]:
            count = self.write_buffer[page_id]
            if count > 0:
                await self.redis_manager.increment(page_id, count)
                print(f"Flushed buffer for page_id: {page_id} with count: {count}")
            self.write_buffer.pop(page_id)
        self.buffer_locks.pop(page_id)

    def cache_validity_check(self, page_id: str) -> bool:
        is_valid = (page_id in self.visit_count_cache and 
                    (datetime.now() - self.visit_count_cache[page_id]["timestamp"]) < timedelta(seconds=VisitCounterService.cache_ttl))
        print(f"Cache validity check for page_id: {page_id}, valid: {is_valid}")
        return is_valid

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        """
        async with self.cache_locks[page_id]:
            self.write_buffer[page_id] += 1
            print(f"Incremented visit count for page_id: {page_id}, current buffer count: {self.write_buffer[page_id]}")

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        """
        visit_count = 0 
        if self.cache_validity_check(page_id):
            visit_count = self.visit_count_cache[page_id]["count"]
            print(f"Cache hit for page_id: {page_id}, count: {visit_count}")
        else:
            await self.flush_buffer_key(page_id)
            visit_count = await self.redis_manager.get(page_id)
            if visit_count is None: 
                visit_count = 0
            print(f"Fetched visit count from Redis for page_id: {page_id}, count: {visit_count}")

            async with self.cache_locks[page_id]:
                self.visit_count_cache[page_id] = {
                    "count": visit_count,
                    "timestamp": datetime.now()
                }
        async with self.buffer_locks[page_id]:
            visit_count += self.write_buffer[page_id]
            print(f"Final visit count after adding buffer for page_id: {page_id}, count: {visit_count}")
        
        return visit_count
