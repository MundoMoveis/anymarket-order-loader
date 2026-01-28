import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Body
from src.jobs import backfill_recent, backfill_rolling, backfill_full_range_locked


from src.jobs import (
    backfill_full_range,
    drain_feed,
    process_feed_cycle,
    hydrate_order,
    hydrate_range,
)
from src.log import log

router = APIRouter()


@router.get("/health")
async def health():
    return {"ok": True}


@router.post("/sync/feed")
async def sync_feed():
    """
    Varrer o feed POR COMPLETO, sem time budget.
    """
    try:
        res = await process_feed_cycle(max_ms=None)
        return res
    except Exception as e:
        log.exception("sync_feed error")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/order/{order_id}")
async def sync_one(order_id: str):
    try:
        return await hydrate_order(order_id)
    except Exception as e:
        log.exception("sync_one error (order_id=%s)", order_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anymarket/backfill")
async def anymarket_backfill(
    start: datetime = Query(..., description="Data/hora inicial"),
    end: datetime = Query(..., description="Data/hora final"),
):
    if end <= start:
        raise HTTPException(status_code=400, detail="`end` deve ser maior que `start`.")

    try:
        result = await backfill_full_range_locked(start, end, job="manual")
        return result
    except Exception as e:
        log.exception("anymarket_backfill error")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anymarket/hydrate-range")
async def anymarket_hydrate_range(start: str = Body(..., embed=True)):
    """
    Reprocessa todos os pedidos já existentes na base a partir de `start`
    (ex: "2025-10-01").

    Obs.: isso dispara em background.
    """
    try:
        asyncio.create_task(hydrate_range(start))
        return {"ok": True}
    except Exception as e:
        log.exception("hydrate-range error")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anymarket/drain-feed")
async def anymarket_drain_feed():
    try:
        res = await drain_feed()
        return res
    except Exception as e:
        log.exception("drain-feed error")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/anymarket/backfill/recent")
async def anymarket_backfill_recent(days: int = Query(7, description="Quantidade de dias para trás")):
    try:
        return await backfill_recent(days)
    except Exception as e:
        log.exception("anymarket_backfill_recent error")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anymarket/backfill/rolling")
async def anymarket_backfill_rolling(days: int = Query(90, description="Quantidade de dias para trás")):
    try:
        return await backfill_rolling(days)
    except Exception as e:
        log.exception("anymarket_backfill_rolling error")
        raise HTTPException(status_code=500, detail=str(e))
