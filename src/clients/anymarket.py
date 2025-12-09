import httpx
from dataclasses import dataclass
from src.config import Cfg
from src.log import log
import asyncio

@dataclass
class FeedItem:
    id: str
    resourceId: str | None = None
    status: str | None = None
    token: str | None = None
    type: str | None = None

class AnyMarketClient:
    def __init__(self, base: str | None = None, token: str | None = None):
        self.base = base or Cfg.ANY_BASE
        # mantém compat com bearer se alguém passar token explicitamente
        self.token = token or Cfg.ANY_TOKEN
        self._client = httpx.AsyncClient(
            base_url=self.base,
            headers=self._make_headers(),
            timeout=httpx.Timeout(30.0)
        )

    def _make_headers(self) -> dict:
        if Cfg.ANY_AUTH_MODE == "api_key":
            if not Cfg.ANY_API_KEY:
                raise RuntimeError("ANYMARKET_API_KEY ausente. Defina no .env")
            return {
                Cfg.ANY_API_KEY_HEADER: Cfg.ANY_API_KEY,
                "Content-Type": "application/json",
            }
        # default: bearer
        if not self.token:
            raise RuntimeError("ANYMARKET_TOKEN ausente. Defina no .env ou use ANYMARKET_AUTH_MODE=api_key")
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def list_feed(self, limit: int) -> list[FeedItem]:
        r = await self._client.get("/orders/feeds", params={"limit": str(limit)})
        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict):
            raw = data.get("items") or data.get("data") or data.get("content") or []
        elif isinstance(data, list):
            raw = data
        else:
            raw = []

        items: list[FeedItem] = []
        for it in raw:
            if isinstance(it, dict):
                rid = (
                    it.get("resourceId")
                    or it.get("orderId")
                    or it.get("resource_id")
                    or it.get("id")
                )
                fid = it.get("id") or it.get("eventId") or rid

                status = it.get("status") or "PENDING"
                token = it.get("token")
                ftype = it.get("type") or it.get("eventType") or "ORDER"

                if fid is None:
                    continue

                items.append(
                    FeedItem(
                        id=str(fid),
                        resourceId=str(rid) if rid is not None else None,
                        status=status,
                        token=token,
                        type=ftype,
                    )
                )
            else:
                # item simples (string/number) – sem info pra ACK, usa defaults
                items.append(
                    FeedItem(
                        id=str(it),
                        resourceId=str(it),
                        status="PENDING",
                        token=None,
                        type="ORDER",
                    )
                )

        return items

    async def get_order(self, order_id: str) -> dict:
        r = await self._client.get(f"/orders/{order_id}")
        r.raise_for_status()
        data = r.json()
        # Alguns tenants retornam LISTA; pegamos o item que casa com id/externalId
        if isinstance(data, list):
            if not data:
                return {}
            for it in data:
                try:
                    if str(it.get("id")) == str(order_id) or str(it.get("externalId")) == str(order_id):
                        return it
                except AttributeError:
                    continue
            return data[0]
        return data if isinstance(data, dict) else {}

    async def list_returns(self, order_id: str) -> list[dict]:
        r = await self._client.get(f"/orders/returns/{order_id}")
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return (data.get("items") or []) if isinstance(data, dict) else []

    async def ack_feed(self, items: list[FeedItem]) -> None:
        """
        Envia ACK para o feed do ANYMARKET no formato esperado:
        [{ "id", "status", "token", "type" }, ...]
        """
        if not items:
            return

        payload: list[dict] = []
        for it in items:
            if not it.token:
                log.warning("ACK feed: item %s sem token, ignorando no ACK", it.id)
                continue

            # id pode ser numérico; se falhar cast, manda string mesmo
            try:
                ack_id = int(it.id)
            except ValueError:
                ack_id = it.id

            payload.append(
                {
                    "id": ack_id,
                    "status": it.status or "PENDING",
                    "token": it.token,
                    "type": it.type or "ORDER",
                }
            )

        if not payload:
            return

        # em lotes menores, pra evitar limite de payload
        MAX_BATCH = 50

        for i in range(0, len(payload), MAX_BATCH):
            batch = payload[i : i + MAX_BATCH]

            for attempt in range(3):
                try:
                    r = await self._client.put("/orders/feeds/batch", json=batch)
                    r.raise_for_status()
                    log.info(
                        "ACK feed: lote %s..%s ok (%s eventos)",
                        i,
                        i + len(batch) - 1,
                        len(batch),
                    )
                    break
                except httpx.HTTPStatusError as e:
                    status = e.response.status_code
                    body = e.response.text[:500]
                    log.error(
                        "ACK feed erro HTTP (status=%s, tent=%s): body=%s",
                        status,
                        attempt + 1,
                        body,
                    )
                    if 500 <= status < 600 and attempt < 2:
                        await asyncio.sleep(2**attempt)
                        continue
                    # se não for 5xx ou esgotou tentativas, para esse lote
                    break
                except httpx.HTTPError as e:
                    log.error(
                        "ACK feed erro HTTP genérico (tent=%s): %s",
                        attempt + 1,
                        e,
                    )
                    if attempt < 2:
                        await asyncio.sleep(2**attempt)
                        continue
                    break


    async def aclose(self):
        await self._client.aclose()

    async def list_orders(
        self,
        created_after: str | None = None,
        created_before: str | None = None,
        page: int | None = None,
        size: int | None = None,
        status: str | None = None,
    ) -> tuple[list[dict], bool]:
        """
        Lista pedidos por intervalo de datas.

        Retorna (orders, last_page), onde 'orders' é SEMPRE lista de dicts.
        """
        params: dict[str, str] = {}
        if created_after:
            params["createdAfter"] = created_after
        if created_before:
            params["createdBefore"] = created_before
        if status:
            params["status"] = status
        if page is not None:
            params["page"] = str(page)
        if size is not None:
            params["size"] = str(size)

        r = await self._client.get("/orders", params=params)
        r.raise_for_status()
        data = r.json()

        # Formato paginado Page<Order>
        if isinstance(data, dict) and "content" in data:
            raw = data.get("content") or []
            last = bool(data.get("last", True))
        # Lista simples
        elif isinstance(data, list):
            raw = data
            last = True
        else:
            from src.log import log
            log.warning("Unexpected /orders response type: %s", type(data))
            return [], True

        orders: list[dict] = []
        from src.log import log
        for it in raw:
            if isinstance(it, dict):
                orders.append(it)
            else:
                log.warning("list_orders: ignorando elemento não-dict (%s): %r", type(it), it)

        return orders, last
