from typing import Dict, List, Tuple, Any
import asyncio
from datetime import datetime, timedelta
from ..core.redis_manager import RedisManager
from collections import defaultdict
 
class VisitCounterService:
    timer = 5
    flush_interval = 30
 
    def __init__(self, redis_manager: RedisManager):
        self.redis_manager = redis_manager
 
        # application layer cache for visit count
        self.visit_count_cache: Dict[str, Dict] = {}
        self.cache_locks = defaultdict(asyncio.Lock)
 
        # write buffer for visit count
        self.write_buffer = defaultdict(int)
        self.buffer_locks = defaultdict(asyncio.Lock)
 
        # start the buffer flush task
        asyncio.create_task(self.flush_buffer())
 
    async def flush_buffer(self) -> None:
        while True:
            await asyncio.sleep(VisitCounterService.flush_interval)
 
            for page_id in list(self.write_buffer.keys()):
                await self.flush_buffer_key(page_id)
 
    async def flush_buffer_key(self, page_id: str) -> None:
        if page_id not in self.write_buffer:
            return
 
        async with self.buffer_locks[page_id]:
            count = self.write_buffer[page_id]
 
            if count > 0:
                await self.redis_manager.increment(page_id, count)
            self.write_buffer.pop(page_id)
 
        self.buffer_locks.pop(page_id)
 
    def _cache_validity_check(self, page_id: str) -> bool:
        return (page_id in self.visit_count_cache and (datetime.now() - self.visit_count_cache[page_id]["timestamp"]) < timedelta(seconds=self.timer))
 
    async def increment_visit(self, page_id: str) -> None:
 
        async with self.buffer_locks[page_id]:
            self.write_buffer[page_id] += 1
 
    async def get_visit_count(self, page_id: str) -> Tuple[int, str]:
       
        visit_count = 0
        served_via = ""
 
        if self._cache_validity_check(page_id):
            # using in-memory cache
            async with self.cache_locks[page_id]:
                visit_count = self.visit_count_cache[page_id]["count"]
                served_via = "in-memory"
 
        else:
            # flushing the data to redis before fetching the data
            await self.flush_buffer_key(page_id)
 
            # using redis cache 
            visit_count = await self.redis_manager.get(page_id)
            if visit_count is None:
                visit_count = 0
 
            served_via = self.redis_manager.get_redis_node_from_key(page_id)
            served_via = f"redis_{served_via.split(':')[-1]}"
 
            # update in-memory cache
            async with self.cache_locks[page_id]:
                self.visit_count_cache[page_id] = {
                    "count": visit_count,
                    "timestamp": datetime.now()
                }
 
        async with self.buffer_locks[page_id]:
            visit_count += self.write_buffer[page_id]
 
        return visit_count, served_via
