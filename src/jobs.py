import time
from src.clients.anymarket import AnyMarketClient, FeedItem
from src.db import Session
from src.upsert import upsert_order_tree
from src.mappers import map_marketplace_id
from src.config import Cfg
from src.log import log
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, timezone

async def process_feed_cycle() -> dict:
    t0 = time.time()
    client = AnyMarketClient()
    items: list[FeedItem] = await client.list_feed(Cfg.ANY_PAGE)
    if not items:
        await client.aclose()
        return {"processed": 0, "acked": 0, "elapsedMs": 0}

    ack: list[FeedItem] = []
    async with Session() as session:  # type: AsyncSession
        async with session.begin():
            for it in items:
                try:
                    full = await client.get_order(it.resourceId)
                    try:
                        rets = await client.list_returns(it.resourceId)
                        if rets:
                            full["returns"] = rets
                    except Exception:
                        pass
                    await upsert_order_tree(session, full, map_marketplace_id(full))
                    ack.append(it)
                except Exception as e:
                    log.error("feed item error: %s", e)
                if (time.time() - t0) * 1000 > Cfg.ANY_MAX_MS:
                    break
    # commit implícito no context
    # ACK fora da transação
    try:
        await client.ack_feed(ack)
        a = len(ack)
    except Exception as e:
        log.error("ack error: %s", e)
        a = 0
    await client.aclose()
    return {"processed": len(ack), "acked": a, "elapsedMs": int((time.time() - t0) * 1000)}

async def hydrate_order(order_id: str) -> dict:
    client = AnyMarketClient()
    full = await client.get_order(order_id)
    try:
        rets = await client.list_returns(order_id)
        if rets:
            full["returns"] = rets
    except Exception:
        pass
    async with Session() as session:
        async with session.begin():
            await upsert_order_tree(session, full, map_marketplace_id(full))
    await client.aclose()
    return {"ok": True}


def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
    return dt.isoformat()

async def backfill_orders_interval(start: datetime, end: datetime) -> dict:
    client = AnyMarketClient()
    total = 0
    page = 0
    size = 100  # ajuste se quiser

    created_after = _to_iso(start)
    created_before = _to_iso(end)

    while True:
        orders, last = await client.list_orders(
            created_after=created_after,
            created_before=created_before,
            page=page,
            size=size,
        )
        if not orders:
            break

        log.info("backfill page %s: %s pedidos", page, len(orders))

        async with Session() as session:  # type: AsyncSession
            async with session.begin():
                for full in orders:
                    try:
                        # aqui full é garantidamente dict por causa de list_orders
                        await upsert_order_tree(session, full, map_marketplace_id(full))
                        total += 1
                    except Exception as e:
                        log.error("backfill order error: %s", e)

        if last:
            break
        page += 1

    await client.aclose()
    return {
        "processed": total,
        "pages": page + 1,
        "start": created_after,
        "end": created_before,
    }
