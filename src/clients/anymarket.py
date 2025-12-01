import httpx
from dataclasses import dataclass
from src.config import Cfg

@dataclass
class FeedItem:
    id: str
    resourceId: str

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

        # Normaliza: pode vir dict com "items"/"data"/"content" OU lista crua
        if isinstance(data, dict):
            raw = data.get("items") or data.get("data") or data.get("content") or []
        elif isinstance(data, list):
            raw = data
        else:
            raw = []

        items: list[FeedItem] = []
        for it in raw:
            if isinstance(it, dict):
                rid = it.get("resourceId") or it.get("orderId") or it.get("resource_id") or it.get("id")
                fid = it.get("id") or it.get("eventId") or rid
                if rid:
                    items.append(FeedItem(id=str(fid), resourceId=str(rid)))
            else:
                # item simples (string/number) → trate como orderId
                items.append(FeedItem(id=str(it), resourceId=str(it)))
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
        if not items:
            return
        body = [{"id": it.id} for it in items]
        r = await self._client.put("/orders/feeds/batch", json=body)
        r.raise_for_status()

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
