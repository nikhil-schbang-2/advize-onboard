from fastapi import FastAPI
from app.insights_async import update_ad_insights
from pydantic import BaseModel
from typing import Dict, List


app = FastAPI()


class FilterRequest(BaseModel):
    filter: Dict[int, List[int]]


@app.post("/fetch-ads-and-insights")
async def test_function(filter_request: FilterRequest):
    # 1632040980684380, 841473654502812
    task_id = update_ad_insights.apply_async(
        args=[
            filter_request.filter,
        ]
    )
    return {"task_id": str(task_id.id)}
