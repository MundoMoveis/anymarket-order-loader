import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Body

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
    """
    Faz backfill do intervalo [start, end] quebrando em janelas.
    Retorna métricas separadas: fetched vs persisted vs errors.
    """
    if end <= start:
        raise HTTPException(status_code=400, detail="`end` deve ser maior que `start`.")

    try:
        result = await backfill_full_range(start, end)
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
