import json
from datetime import datetime, timezone, timedelta
from typing import Any


def parse_dt(value: Any) -> datetime | None:
    """
    Converte string ISO (ou datetime) em datetime.
    Aceita formatos com "Z" e offsets.
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        s = str(value).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def dt(value: Any) -> datetime | None:
    """
    Alias defensivo para parse_dt.
    """
    return parse_dt(value)


def as_bool(v) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "t", "yes", "y", "sim")
    return None


def safe_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def map_marketplace_id(full) -> int:
    """
    1=Magalu (default), 2=Mercado Livre, 3=Via.
    """
    if not isinstance(full, dict):
        return 1

    ch = str(full.get("marketplace") or full.get("channel") or full.get("marketPlace") or "").upper()
    if "MERCADO" in ch or "ML" == ch:
        return 2
    if "VIA" in ch:
        return 3
    return 1


def _first_truthy(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


def _to_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return None


def _to_int(v, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def map_header(p: dict, marketplace_id: int) -> dict:
    """
    Mapeia o header do pedido para anymarket_orders.

    Ajustes importantes para evitar "processou mas não gravou":
    - id sempre como str/int vindo do payload (não inventa)
    - campos de datas aceitando múltiplos nomes
    - fulfillment pode vir em múltiplas chaves (fulfillment / isFulfillment / fulfillmentType)
    - tracking pode vir em shipping.tracking ou tracking raiz
    - substatus pode vir em marketPlaceStatus / substatus / subStatus
    """
    if not isinstance(p, dict):
        if isinstance(p, list) and p:
            p = p[0] if isinstance(p[0], dict) else {}
        else:
            p = {}

    shipping = p.get("shipping") or {}
    buyer = p.get("buyer") or {}

    # fulfillment: a AnyMarket pode variar a chave
    is_fulfillment = as_bool(
        _first_truthy(
            p.get("fulfillment"),
            p.get("isFulfillment"),
            p.get("is_fulfillment"),
        )
    )

    # tracking: pode vir em payload.tracking, shipping.tracking, shipping.trackingResult etc.
    tracking = (
        p.get("tracking")
        or shipping.get("tracking")
        or shipping.get("trackingResult")
        or {}
    )

    shipped_at = parse_dt(
        _first_truthy(
            tracking.get("shippedDate"),
            tracking.get("shippedAt"),
            tracking.get("date"),
            shipping.get("shippedDate"),
            shipping.get("shippedAt"),
        )
    )
    delivered_at = parse_dt(
        _first_truthy(
            tracking.get("deliveredDate"),
            tracking.get("deliveredAt"),
            shipping.get("deliveredDate"),
            shipping.get("deliveredAt"),
        )
    )

    # marketplace order id e channel variam bastante por tenant
    marketplace_order_id = _first_truthy(
        p.get("marketplaceOrderId"),
        p.get("marketPlaceId"),
        p.get("marketPlaceOrderId"),
        p.get("externalId"),
        p.get("idInMarketPlace"),
    )

    channel = _first_truthy(
        p.get("marketplace"),
        p.get("channel"),
        p.get("marketPlace"),
    )

    status = _first_truthy(p.get("status"), p.get("orderStatus"))
    substatus = _first_truthy(
        p.get("substatus"),
        p.get("subStatus"),
        p.get("marketPlaceStatus"),
        p.get("marketplaceStatus"),
    )

    total_amount = _to_float(_first_truthy(p.get("total"), p.get("totalAmount")))
    total_discount = _to_float(_first_truthy(p.get("discount"), p.get("totalDiscount")))
    freight_amount = _to_float(_first_truthy(p.get("freight"), p.get("freightAmount")))

    created_at_marketplace = dt(_first_truthy(p.get("createdAt"), p.get("created_at"), p.get("createdDate")))
    approved_at = dt(_first_truthy(p.get("paymentDate"), p.get("approvedAt"), p.get("approved_at")))
    cancelled_at = dt(_first_truthy(p.get("cancelDate"), p.get("cancelledAt"), p.get("canceledAt")))
    # shipped_at/delivered_at já calculados via tracking

    now = datetime.utcnow()

    return {
        "id": p.get("id"),
        "marketplace_id": marketplace_id,
        "marketplace_order_id": marketplace_order_id,
        "channel": channel,
        "fulfillment_type": "FULFILLMENT" if is_fulfillment else "OWN_LOGISTICS",
        "status": status,
        "substatus": substatus,
        "buyer_name": buyer.get("name"),
        "buyer_document": buyer.get("document"),
        "buyer_email": buyer.get("email"),
        "total_amount": total_amount,
        "total_discount": total_discount,
        "freight_amount": freight_amount,
        "created_at_marketplace": created_at_marketplace,
        "approved_at": approved_at,
        "cancelled_at": cancelled_at,
        "shipped_at": shipped_at,
        "delivered_at": delivered_at,
        "created_at": now,
        "updated_at": now,
        "extra_json": safe_json(p),
    }


def map_items(order_id: str, items: list[dict]) -> list[dict]:
    """
    Itens do pedido.

    Ajustes:
    - quantity aceita nomes variados e converte seguro
    - unit_price/total_price para float (quando possível)
    """
    out: list[dict] = []

    for it in items or []:
        if not isinstance(it, dict):
            continue

        product = it.get("product") or {}
        sku = it.get("sku") or {}
        sku_kit = it.get("skuKit") or {}

        qty_raw = _first_truthy(it.get("quantity"), it.get("qty"), it.get("amount"))
        qty = _to_int(qty_raw, default=0)

        unit_price = _to_float(_first_truthy(it.get("price"), it.get("unit"), it.get("unitPrice")))
        total_price = _to_float(_first_truthy(it.get("total"), it.get("totalPrice")))

        if total_price is None and unit_price is not None:
            try:
                total_price = float(unit_price) * float(qty)
            except Exception:
                total_price = None

        sku_id = _first_truthy(it.get("skuId"), sku.get("id"), sku_kit.get("id"))

        marketplace_item_id = _first_truthy(
            it.get("marketplaceItemId"),
            it.get("orderItemId"),
            it.get("idInMarketPlace"),
            it.get("marketPlaceItemId"),
        )

        title = _first_truthy(
            it.get("title"),
            sku.get("title"),
            product.get("title"),
            sku_kit.get("title"),
        )

        external_id = _first_truthy(
            it.get("partnerId"),
            sku.get("partnerId"),
            sku.get("externalId"),
            sku_kit.get("partnerId"),
            sku_kit.get("externalId"),
            it.get("idInMarketPlace"),
        )

        out.append(
            {
                "order_id": order_id,
                "sku_id": sku_id,
                "marketplace_item_id": marketplace_item_id,
                "title": title,
                "quantity": qty,
                "unit_price": unit_price,
                "total_price": total_price,
                "external_id": external_id,
                "extra_json": safe_json(it),
            }
        )

    return out


def map_payments(order_id: str, pays: list[dict], payload: dict) -> list[dict]:
    """
    Pagamentos.
    """
    out: list[dict] = []

    order_payment_date = parse_dt(_first_truthy(payload.get("paymentDate"), payload.get("approvedAt")))
    order_cancel_date = parse_dt(_first_truthy(payload.get("cancelDate"), payload.get("cancelledAt"), payload.get("canceledAt")))

    for p in pays or []:
        if not isinstance(p, dict):
            continue

        out.append(
            {
                "order_id": order_id,
                "method": _first_truthy(p.get("paymentDetailNormalized"), p.get("method"), p.get("paymentMethod")),
                "installments": _to_int(p.get("installments"), default=0) if p.get("installments") is not None else None,
                "amount": _to_float(_first_truthy(p.get("value"), p.get("amount"))),
                "transaction_id": _first_truthy(p.get("marketplaceId"), p.get("orderAuthorizationCardCode"), p.get("transactionId")),
                "status": p.get("status"),
                "authorized_at": order_payment_date,
                "paid_at": order_payment_date,
                "canceled_at": order_cancel_date,
                "extra_json": safe_json(p),
            }
        )

    return out


def map_shipping(order_id: str, s: dict | None, payload: dict) -> dict | None:
    """
    Shipping.
    """
    if not s or not isinstance(s, dict):
        return None

    shipping_root = payload.get("shipping") or {}
    tracking = (
        payload.get("tracking")
        or shipping_root.get("tracking")
        or shipping_root.get("trackingResult")
        or {}
    )

    # tenta pegar service/carrier a partir dos items[].shippings[] também
    items = payload.get("items") or []
    first_ship = None
    for it in items:
        if not isinstance(it, dict):
            continue
        sh_list = it.get("shippings") or []
        if sh_list and isinstance(sh_list[0], dict):
            first_ship = sh_list[0]
            break

    carrier = _first_truthy(
        tracking.get("carrier"),
        first_ship.get("shippingCarrierNormalized") if first_ship else None,
        shipping_root.get("carrier"),
    )

    service = _first_truthy(
        first_ship.get("shippingtype") if first_ship else None,
        payload.get("shippingOptionId"),
        shipping_root.get("service"),
    )

    tracking_code = _first_truthy(tracking.get("number"), tracking.get("trackingCode"), shipping_root.get("trackingCode"))

    promised_delivery = parse_dt(
        _first_truthy(
            shipping_root.get("promisedShippingTime"),
            shipping_root.get("promisedDelivery"),
            tracking.get("estimateDate"),
            tracking.get("promisedDate"),
        )
    )

    shipped_at = parse_dt(_first_truthy(tracking.get("shippedDate"), tracking.get("shippedAt")))
    delivered_at = parse_dt(_first_truthy(tracking.get("deliveredDate"), tracking.get("deliveredAt")))

    address_comp = _first_truthy(shipping_root.get("comment"), shipping_root.get("reference"))

    return {
        "order_id": order_id,
        "carrier": carrier,
        "service": service,
        "tracking_code": tracking_code,
        "promised_delivery": promised_delivery,
        "shipped_at": shipped_at,
        "delivered_at": delivered_at,
        "receiver_name": _first_truthy(s.get("receiverName"), s.get("name")),
        "address_street": _first_truthy(s.get("street"), s.get("address"), s.get("addressStreet")),
        "address_number": _first_truthy(s.get("number"), s.get("addressNumber")),
        "address_comp": address_comp,
        "address_district": _first_truthy(s.get("neighborhood"), s.get("district")),
        "address_city": _first_truthy(s.get("city"), s.get("addressCity")),
        "address_state": _first_truthy(s.get("state"), s.get("addressState")),
        "address_zip": _first_truthy(s.get("zipCode"), s.get("zip"), s.get("postalCode")),
        "extra_json": safe_json(s),
    }


def map_invoice(order_id: str, inv: dict | None) -> dict | None:
    if not inv or not isinstance(inv, dict):
        return None

    invoice_key = _first_truthy(inv.get("accessKey"), inv.get("invoiceKey"))
    status = inv.get("status")

    return {
        "order_id": order_id,
        "is_invoiced": 1 if (status == "INVOICED" or invoice_key) else 0,
        "invoice_key": invoice_key,
        "number": inv.get("number"),
        "series": _first_truthy(inv.get("serie"), inv.get("series")),
        "issued_at": dt(_first_truthy(inv.get("issueDate"), inv.get("issuedAt"))),
        "xml_url": _first_truthy(inv.get("xmlUrl"), inv.get("xml_url")),
        "pdf_url": _first_truthy(inv.get("pdfUrl"), inv.get("pdf_url")),
        "extra_json": safe_json(inv),
    }


def map_history(order_id: str, hist: list[dict]) -> list[dict]:
    out: list[dict] = []
    for h in hist or []:
        if not isinstance(h, dict):
            continue
        out.append(
            {
                "order_id": order_id,
                "status": _first_truthy(h.get("status"), h.get("code"), "UNKNOWN"),
                "substatus": _first_truthy(h.get("subStatus"), h.get("substatus")),
                "source": "FEED",
                "occurred_at": dt(h.get("changedAt")) or datetime.utcnow(),
                "payload": safe_json(h),
            }
        )
    return out


def map_returns(order_id: str, rets: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rets or []:
        if not isinstance(r, dict):
            continue
        out.append(
            {
                "order_id": order_id,
                "status": r.get("status"),
                "reason": r.get("reason"),
                "requested_at": dt(r.get("requestedAt")),
                "approved_at": dt(r.get("approvedAt")),
                "received_at": dt(r.get("receivedAt")),
                "extra_json": safe_json(r),
            }
        )
    return out
