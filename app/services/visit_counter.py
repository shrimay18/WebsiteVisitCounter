from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager
from collections import defaultdict

class VisitCounterService:
    visit_counter = defaultdict(int)
    locks = defaultdict(asyncio.Lock)

    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # TODO: Implement visit count increment
        # async with VisitCounterService.locks[page_id]:
        #     VisitCounterService.visit_counter[page_id] += 1
        await self.redis_manager.increment(page_id,1)

        pass

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # TODO: Implement getting visit count
        # async with VisitCounterService.locks[page_id]:
        #     return VisitCounterService.visit_counter[page_id]
        # return 0
        counts = await self.redis_manager.get(page_id)
        return counts
