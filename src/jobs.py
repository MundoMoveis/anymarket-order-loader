import time
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.clients.anymarket import AnyMarketClient, FeedItem
from src.config import Cfg
from src.db import Session
from src.log import log
from src.mappers import map_marketplace_id
from src.upsert import upsert_order_tree

PAGE_SIZE = 5


def _to_iso(dt: datetime) -> str:
    """
    Normaliza datetime para ISO8601 com timezone.
    Se vier naive, assume America/Sao_Paulo (-03:00).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
    return dt.isoformat()


async def process_feed_cycle(max_ms: int | None = None) -> dict:
    """
    Processa o feed até ficar vazio ou até atingir max_ms (se informado).

    Importante:
    - "fetched": quantos eventos vieram do feed e foram tentados
    - "persisted": quantos pedidos foram persistidos com sucesso
    - "errors": quantos falharam no upsert (mas ainda assim foram ACKados)
    """
    t0 = time.time()
    client = AnyMarketClient()

    cycles = 0
    fetched = 0
    persisted = 0
    errors = 0
    acked = 0

    try:
        while True:
            cycles += 1

            items: list[FeedItem] = await client.list_feed(None)  # sem limite
            if not items:
                log.info("process_feed_cycle: feed vazio após %s ciclos", cycles)
                break

            log.info("process_feed_cycle: ciclo %s, %s eventos no feed", cycles, len(items))

            ack_batch: list[FeedItem] = []

            async with Session() as session:  # type: AsyncSession
                async with session.begin():
                    for it in items:
                        # time budget
                        if max_ms is not None and (time.time() - t0) * 1000 > max_ms:
                            log.info(
                                "process_feed_cycle: time budget estourado (%sms), interrompendo",
                                max_ms,
                            )
                            break

                        fetched += 1

                        try:
                            rid = it.resourceId or it.id
                            full = await client.get_order(rid)

                            # returns (best effort)
                            try:
                                rets = await client.list_returns(rid)
                                if rets:
                                    full["returns"] = rets
                            except Exception:
                                pass

                            await upsert_order_tree(
                                session=session,
                                payload=full,
                                marketplace_id=map_marketplace_id(full),
                                source="FEED",
                            )

                            persisted += 1
                            ack_batch.append(it)

                        except Exception as e:
                            errors += 1
                            log.error("feed item error (event_id=%s resource=%s): %s", it.id, it.resourceId, e)
                            ack_batch.append(it)

            # ACK fora da transação
            if ack_batch:
                await client.ack_feed(ack_batch)
                acked += len(ack_batch)

            # se estourou o time budget, encerra
            if max_ms is not None and (time.time() - t0) * 1000 > max_ms:
                log.info("process_feed_cycle: parada por time budget (%sms)", int((time.time() - t0) * 1000))
                break

        return {
            "cycles": cycles,
            "fetched": fetched,
            "persisted": persisted,
            "errors": errors,
            "acked": acked,
            "elapsedMs": int((time.time() - t0) * 1000),
        }

    finally:
        await client.aclose()


async def hydrate_order(order_id: str) -> dict:
    client = AnyMarketClient()
    try:
        full = await client.get_order(order_id)

        # returns (best effort)
        try:
            rets = await client.list_returns(order_id)
            if rets:
                full["returns"] = rets
        except Exception:
            pass

        async with Session() as session:
            async with session.begin():
                await upsert_order_tree(
                    session=session,
                    payload=full,
                    marketplace_id=map_marketplace_id(full),
                    source="MANUAL",
                )

        return {"ok": True}
    finally:
        await client.aclose()


async def backfill_orders_interval(start: datetime, end: datetime) -> dict:
    """
    Backfill via /orders com paginação por offset.
    Retorna métricas completas e inclui start/end usados na janela.
    """
    client = AnyMarketClient()

    start_iso = _to_iso(start)
    end_iso = _to_iso(end)

    offset = 0
    page_idx = 0

    fetched = 0
    persisted = 0
    inserted = 0
    updated = 0
    skipped = 0
    errors = 0

    try:
        while True:
            page_idx += 1

            url = (
                f"/orders"
                f"?createdAfter={start_iso}"
                f"&createdBefore={end_iso}"
                f"&offset={offset}"
            )

            await asyncio.sleep(0.2)

            log.info(
                "anymarket-backfill: window %s -> %s, page %s, offset=%s",
                start_iso, end_iso, page_idx, offset,
            )

            r = await client._client.get(url)
            r.raise_for_status()
            data = r.json()

            content = data.get("content") or []
            qtd = len(content)

            log.info("anymarket-backfill: page %s: %s pedidos (offset=%s)", page_idx, qtd, offset)

            if qtd == 0:
                break

            async with Session() as session:
                async with session.begin():
                    for p in content:
                        fetched += 1
                        try:
                            full = p  # /orders já traz o pedido completo (no seu tenant)

                            res = await upsert_order_tree(
                                session=session,
                                payload=full,
                                marketplace_id=map_marketplace_id(full),
                                source="BACKFILL",
                            )

                            if res.get("skipped"):
                                skipped += 1
                            else:
                                persisted += 1
                                inserted += int(res.get("inserted", 0))
                                updated += int(res.get("updated", 0))

                        except Exception as e:
                            errors += 1
                            log.error(
                                "anymarket-backfill: erro ao processar pedido %s: %s",
                                p.get("id"),
                                e,
                            )

            offset += qtd

            # se veio menos que PAGE_SIZE, provavelmente última página
            if qtd < PAGE_SIZE:
                break

        return {
            "start": start_iso,
            "end": end_iso,
            "fetched": fetched,
            "persisted": persisted,
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }

    finally:
        await client.aclose()


async def backfill_full_range(start: datetime, end: datetime) -> dict:
    """
    Faz backfill do intervalo [start, end], quebrando em janelas de no máximo ANY_BACKFILL_MAX_DAYS.
    """
    max_days = max(1, Cfg.ANY_BACKFILL_MAX_DAYS)

    current_start = start

    totals = {
        "fetched": 0,
        "persisted": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
    }
    windows: list[dict] = []

    while current_start < end:
        current_end = current_start + timedelta(days=max_days)
        if current_end > end:
            current_end = end

        log.info("backfill window: %s -> %s (max %s dias)", current_start.isoformat(), current_end.isoformat(), max_days)

        try:
            w = await backfill_orders_interval(current_start, current_end)
            windows.append(w)

            for k in totals:
                totals[k] += int(w.get(k, 0))

        except Exception as e:
            # erro de janela inteira (HTTP etc.)
            log.error("backfill window error (%s -> %s): %s", current_start, current_end, e)
            windows.append(
                {
                    "start": _to_iso(current_start),
                    "end": _to_iso(current_end),
                    "fetched": 0,
                    "persisted": 0,
                    "inserted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errors": 1,
                    "error": str(e),
                }
            )
            totals["errors"] += 1

        current_start = current_end

    return {
        "start": _to_iso(start),
        "end": _to_iso(end),
        **totals,
        "windows": windows,
    }


async def fetch_order_with_retry(client: AnyMarketClient, order_id: str, max_retries: int = 5):
    delay = 1

    for attempt in range(1, max_retries + 1):
        try:
            return await client.get_order(order_id)

        except httpx.HTTPStatusError as e:
            status = e.response.status_code

            if status == 429:
                retry_after = e.response.headers.get("Retry-After")
                wait = delay
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = delay

                log.warning(
                    "hydrate_range: 429 no pedido %s (tentativa %s/%s), aguardando %ss",
                    order_id, attempt, max_retries, wait,
                )
                await asyncio.sleep(wait)
                delay = min(delay * 2, 60)
                continue

            log.error("hydrate_range: erro HTTP no pedido %s (status=%s): %s", order_id, status, e)
            raise

        except httpx.HTTPError as e:
            log.warning(
                "hydrate_range: erro HTTP genérico no pedido %s (tentativa %s/%s): %s",
                order_id, attempt, max_retries, e,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    log.error("hydrate_range: falha definitiva no pedido %s depois de %s tentativas", order_id, max_retries)
    return None


async def hydrate_range(start: str):
    # 1) pega IDs na base
    async with Session() as session:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT id
                    FROM anymarket_orders
                    WHERE created_at_marketplace >= :start
                    ORDER BY created_at_marketplace
                    """
                ),
                {"start": start},
            )
        ).scalars().all()

    ids = [str(x) for x in rows]
    total = len(ids)

    log.info("hydrate_range: iniciando reprocesso de %s pedidos a partir de %s", total, start)

    client = AnyMarketClient()
    try:
        for idx, oid in enumerate(ids, start=1):
            await asyncio.sleep(0.2)

            try:
                full = await fetch_order_with_retry(client, oid)
                if not full:
                    continue

                try:
                    rets = await client.list_returns(oid)
                    if rets:
                        full["returns"] = rets
                except Exception:
                    pass

                async with Session() as session:
                    async with session.begin():
                        await upsert_order_tree(
                            session=session,
                            payload=full,
                            marketplace_id=map_marketplace_id(full),
                            source="HYDRATE_RANGE",
                        )

                if idx % 100 == 0:
                    log.info("hydrate_range: %s/%s pedidos reprocessados", idx, total)

            except Exception as e:
                log.error("hydrate_range: erro inesperado no pedido %s: %s", oid, e)

        log.info("hydrate_range: concluído (%s pedidos)", total)

    finally:
        await client.aclose()


async def drain_feed(limit: int = 200) -> dict:
    client = AnyMarketClient()
    try:
        items = await client.list_feed(limit)
        log.info("drain_feed: encontrados %s eventos no feed", len(items))
        await client.ack_feed(items)
        return {"seen": len(items)}
    finally:
        await client.aclose()
