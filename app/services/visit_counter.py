from typing import Dict, List, Any
import asyncio
from datetime import datetime,timedelta
from ..core.redis_manager import RedisManager
from collections import defaultdict

class VisitCounterService:
    # visit_counter = defaultdict(int)
    # locks = defaultdict(asyncio.Lock)
    
    visit_count_dict:Dict[str,Dict] = {}
    Locks = defaultdict(asyncio.Lock)
    tld = 50
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
        # counts = await self.redis_manager.get(page_id)
        # return counts
        if page_id in VisitCounterService.visit_count_dict:
            if datetime.now()-VisitCounterService.visit_count_dict[page_id]['time']<timedelta(seconds=VisitCounterService.tld):
                return VisitCounterService.visit_count_dict[page_id]['count']
        counts = await self.redis_manager.get(page_id)
        VisitCounterService.visit_count_dict[page_id] = {'count':counts,'time':datetime.now()}
        return counts
        pass
