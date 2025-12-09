import time

from sqlalchemy import text
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

async def backfill_full_range(start: datetime, end: datetime) -> dict:
    """
    Faz backfill de todo o intervalo [start, end],
    quebrando em janelas de no máximo ANY_BACKFILL_MAX_DAYS.
    """
    max_days = max(1, Cfg.ANY_BACKFILL_MAX_DAYS)
    current_start = start
    total_processed = 0
    windows: list[dict] = []

    while current_start < end:
        current_end = current_start + timedelta(days=max_days)
        if current_end > end:
            current_end = end

        log.info(
            "backfill window: %s -> %s (max %s dias)",
            current_start.isoformat(), current_end.isoformat(), max_days
        )

        try:
            result = await backfill_orders_interval(current_start, current_end)
            total_processed += result.get("processed", 0)
            windows.append({
                "start": result.get("start"),
                "end": result.get("end"),
                "processed": result.get("processed", 0),
            })
        except Exception as e:
            log.error("backfill window error (%s -> %s): %s", current_start, current_end, e)

        # próxima janela
        current_start = current_end

    return {
        "processed": total_processed,
        "windows": windows,
    }

import asyncio
import httpx
from src.log import log

async def fetch_order_with_retry(client, order_id: str, max_retries: int = 5):
    """
    Faz GET /orders/{id} respeitando 429 (Too Many Requests).
    Usa Retry-After se o header existir, senão faz backoff exponencial.
    """
    delay = 1  # segundos, backoff inicial

    for attempt in range(1, max_retries + 1):
        try:
            full = await client.get_order(order_id)
            return full

        except httpx.HTTPStatusError as e:
            status = e.response.status_code

            # Trata especificamente 429
            if status == 429:
                retry_after = e.response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = delay
                else:
                    wait = delay

                log.warning(
                    "hydrate_range: 429 no pedido %s (tentativa %s/%s), "
                    "aguardando %ss",
                    order_id, attempt, max_retries, wait,
                )
                await asyncio.sleep(wait)
                delay = min(delay * 2, 60)  # limite de 60s
                continue  # tenta de novo

            # Outros HTTPStatusError: loga e propaga
            log.error(
                "hydrate_range: erro HTTP no pedido %s (status=%s): %s",
                order_id, status, e,
            )
            raise

        except httpx.HTTPError as e:
            # erro de rede/timeout etc → retry com backoff
            log.warning(
                "hydrate_range: erro HTTP genérico no pedido %s "
                "(tentativa %s/%s): %s",
                order_id, attempt, max_retries, e,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
            continue

    # Se esgotou retry:
    log.error(
        "hydrate_range: falha definitiva no pedido %s depois de %s tentativas",
        order_id, max_retries,
    )
    return None


async def hydrate_range(start: str):
    # 1) pega IDs na base
    async with Session() as session:
        rows = (await session.execute(
            text("""
                SELECT id
                FROM anymarket_orders
                WHERE created_at_marketplace >= :start
                ORDER BY created_at_marketplace
            """),
            {"start": start},
        )).scalars().all()
        ids = [str(x) for x in rows]

    total = len(ids)
    log.info("hydrate_range: iniciando reprocesso de %s pedidos a partir de %s",
             total, start)

    client = AnyMarketClient()

    for idx, oid in enumerate(ids, start=1):
        # pequeno intervalo fixo entre chamadas para respeitar QPS
        await asyncio.sleep(0.2)  # 5 requisições/segundo aprox.

        try:
            full = await fetch_order_with_retry(client, oid)
            if not full:
                continue  # já logamos o erro no helper

            try:
                rets = await client.list_returns(oid)
                if rets:
                    full["returns"] = rets
            except Exception:
                pass

            async with Session() as session:
                async with session.begin():
                    await upsert_order_tree(session, full, map_marketplace_id(full))

            if idx % 100 == 0:
                log.info(
                    "hydrate_range: %s/%s pedidos reprocessados",
                    idx, total,
                )

        except Exception as e:
            log.error("hydrate_range: erro inesperado no pedido %s: %s", oid, e)

    await client.aclose()
    log.info("hydrate_range: concluído (%s pedidos)", total)