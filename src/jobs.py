import time
import asyncio
from typing import Optional
import httpx

from sqlalchemy import text
from src.clients.anymarket import AnyMarketClient, FeedItem
from src.db import Session
from src.upsert import upsert_order_tree
from src.mappers import map_marketplace_id
from src.config import Cfg
from src.log import log
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, timezone

async def process_feed_cycle(max_ms: int | None = None) -> dict:
    """
    Processa o feed até ficar vazio ou até atingir max_ms (se informado).

    max_ms=None  -> processa TUDO que tiver no feed.
    max_ms=valor -> processa até estourar esse tempo em ms.
    """
    t0 = time.time()
    client = AnyMarketClient()

    total_processed = 0
    total_acked = 0
    cycles = 0

    try:
        while True:
            cycles += 1
            items: list[FeedItem] = await client.list_feed(None)  # sem limite
            if not items:
                log.info("process_feed_cycle: feed vazio após %s ciclos", cycles)
                break

            log.info(
                "process_feed_cycle: ciclo %s, %s eventos no feed",
                cycles, len(items),
            )

            ack: list[FeedItem] = []

            async with Session() as session:  # type: AsyncSession
                async with session.begin():
                    for it in items:
                        # time budget (se configurado)
                        if max_ms is not None and (time.time() - t0) * 1000 > max_ms:
                            log.info(
                                "process_feed_cycle: time budget estourado (%sms), "
                                "interrompendo processamento",
                                max_ms,
                            )
                            # sai do loop interno; depois ainda faz ACK do que já passou
                            break

                        try:
                            # alguns tenants retornam só "id", então usa resourceId ou id
                            rid = it.resourceId or it.id
                            full = await client.get_order(rid)

                            try:
                                rets = await client.list_returns(rid)
                                if rets:
                                    full["returns"] = rets
                            except Exception:
                                pass

                            await upsert_order_tree(
                                session,
                                full,
                                map_marketplace_id(full),
                            )
                            # caminho "ok"
                            it.status = "PENDING"  # ou "SUCCESS", se preferir
                            ack.append(it)
                            total_processed += 1

                        except Exception as e:
                            log.error("feed item error (id=%s): %s", it.id, e)
                            # ainda assim, ACKa como erro, pra não ficar travado
                            it.status = "ERROR"
                            ack.append(it)
                            total_processed += 1

                # fim da transação (commit implícito)

            # ACK fora da transação
            if ack:
                await client.ack_feed(ack)
                total_acked += len(ack)

            # se estourou o time budget, para mesmo que ainda tenha eventos sobrando
            if max_ms is not None and (time.time() - t0) * 1000 > max_ms:
                log.info(
                    "process_feed_cycle: parada por time budget após %s ms",
                    int((time.time() - t0) * 1000),
                )
                break

        return {
            "cycles": cycles,
            "processed": total_processed,
            "acked": total_acked,
            "elapsedMs": int((time.time() - t0) * 1000),
        }

    finally:
        await client.aclose()

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

PAGE_SIZE = 5

async def backfill_orders_interval(start: str, end: str):
    client = AnyMarketClient()

    start_iso = _to_iso(start)
    end_iso = _to_iso(end)

    offset = 0
    total = 0
    page_idx = 0

    try:
        while True:
            page_idx += 1

            url = (
                f"/orders"
                f"?createdAfter={start_iso}"
                f"&createdBefore={end_iso}"
                f"&offset={offset}"
            )

            # pequena pausa pra não estourar QPS
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
            log.info(
                "anymarket-backfill: page %s: %s pedidos (offset=%s)",
                page_idx, qtd, offset,
            )

            # critério 1: se não veio nada, acabou
            if qtd == 0:
                log.info(
                    "anymarket-backfill: page %s vazia, encerrando janela %s -> %s",
                    page_idx, start_iso, end_iso,
                )
                break

            # processa pedidos
            async with Session() as session:
                async with session.begin():
                    for p in content:
                        try:
                            full = p  # /orders já traz o pedido completo

                            # se quiser tentar returns, mantenha:
                            """"
                            try:
                                rets = await client.list_returns(p["id"])
                                if rets:
                                    full["returns"] = rets
                            except Exception:
                                pass
                            """

                            await upsert_order_tree(
                                session,
                                full,
                                map_marketplace_id(full),
                            )
                            total += 1
                        except Exception as e:
                            log.error(
                                "anymarket-backfill: erro ao processar pedido %s: %s",
                                p.get("id"),
                                e,
                            )

            # atualiza offset
            offset += qtd

            # critério 2: se veio menos que PAGE_SIZE, era a última página
            if qtd < PAGE_SIZE:
                log.info(
                    "anymarket-backfill: última página detectada (qtd=%s < %s), "
                    "encerrando janela %s -> %s",
                    qtd, PAGE_SIZE, start_iso, end_iso,
                )
                break

        log.info(
            "anymarket-backfill: janela %s -> %s concluída, %s pedidos processados",
            start_iso, end_iso, total,
        )
        return {"processed": total}

    except Exception as e:
        log.error(
            "anymarket-backfill: window error (%s -> %s): %s",
            start_iso, end_iso, e,
        )
        raise
    finally:
        await client.aclose()

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

def extract_next_link(resp_json: dict) -> str | None:
    links = resp_json.get("links") or []
    for l in links:
        if l.get("rel") == "next":
            return l.get("href")
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

def extract_next_link(data: dict) -> Optional[str]:
    links = data.get("links") or []
    
    for l in links:
        if l.get("rel") == "next":
            return l.get("href")
    return None

async def drain_feed(limit: int = 200) -> dict:
    client = AnyMarketClient()
    try:
        items = await client.list_feed(limit)
        log.info("drain_feed: encontrados %s eventos no feed", len(items))
        await client.ack_feed(items)
        return {"seen": len(items)}
    finally:
        await client.aclose()