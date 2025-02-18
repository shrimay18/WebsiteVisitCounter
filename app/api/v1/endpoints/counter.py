from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()

# Dependency to get VisitCounterService instance
def get_visit_counter_service():
    return VisitCounterService()

@router.post("/visit/{page_id}")
async def record_visit(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Record a visit for a website"""
    try:
        await counter_service.increment_visit(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Get visit count for a website"""
    try:
        count = await counter_service.get_visit_count(page_id)
        return VisitCount(page_id=page_id, count=count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 