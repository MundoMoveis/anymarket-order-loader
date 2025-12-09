import json
from datetime import datetime

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

def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def dt(val):  # parse defensivo
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None

def map_marketplace_id(full) -> int:
    # Se não vier dict (ex.: string), assume MAGALU como default
    if not isinstance(full, dict):
        return 1

    ch = str(full.get("marketplace") or full.get("channel") or "").upper()
    if "MERCADO" in ch:
        return 2
    if "VIA" in ch:
        return 3
    return 1


def map_header(p: dict, marketplace_id: int) -> dict:
    if not isinstance(p, dict):
        if isinstance(p, list) and p:
            p = p[0]
        else:
            p = {}
    ship = p.get("shipping") or {}
    is_fulfillment = as_bool(p.get("fulfillment"))
    tracking = p.get("tracking") or {}

    shipped_at = parse_dt(
        tracking.get("shippedDate")
        or tracking.get("date")  # fallback
    )
    delivered_at = parse_dt(tracking.get("deliveredDate"))

    return {
        "id": p.get("id"),
        "marketplace_id": marketplace_id,
        "marketplace_order_id": p.get("marketPlaceId"),
        "channel": p.get("marketPlace"),
        "fulfillment_type": "FULFILLMENT" if is_fulfillment else "OWN_LOGISTICS",
        "status": p.get("status"),
        "substatus": p.get("marketPlaceStatus"),
        "buyer_name": (p.get("buyer") or {}).get("name"),
        "buyer_document": (p.get("buyer") or {}).get("document"),
        "buyer_email": (p.get("buyer") or {}).get("email"),
        "total_amount": p.get("total"),
        "total_discount": p.get("discount"),
        "freight_amount": p.get("freight"),
        "created_at_marketplace": dt(p.get("createdAt")),
        "approved_at": dt(p.get("paymentDate")),
        "cancelled_at": dt(p.get("cancelDate")),
        "shipped_at": shipped_at,
        "delivered_at": delivered_at,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "extra_json": safe_json(p)
    }

def map_items(order_id: str, items: list[dict]) -> list[dict]:
    out: list[dict] = []
    for it in items or []:
        product = it.get("product") or {}
        sku = it.get("sku") or {}
        sku_kit = it.get("skuKit") or {}

        # quantidade: amount (seu caso) ou os outros nomes padrão
        qty = (
            it.get("quantity")
            or it.get("qty")
            or it.get("amount")
            or 0
        )

        # preço unitário: unit (seu caso) ou price
        unit_price = it.get("price") or it.get("unit")

        # total
        total_price = it.get("total")
        if total_price is None and unit_price is not None:
            try:
                total_price = float(unit_price) * float(qty)
            except Exception:
                total_price = None

        # sku_id: do campo skuId padrão ou do objeto sku.id
        sku_id = it.get("skuId") or sku.get("id")

        # marketplace_item_id: orderItemId / idInMarketPlace / marketplaceItemId
        marketplace_item_id = (
            it.get("marketplaceItemId")
            or it.get("orderItemId")
            or it.get("idInMarketPlace")
        )

        # título: prioridade para o próprio item, depois sku, depois product
        title = (
            it.get("title")
            or sku.get("title")
            or product.get("title")
        )

        # external_id: partnerId / sku.partnerId / sku.externalId / idInMarketPlace
        external_id = (
            it.get("partnerId")
            or sku.get("partnerId")
            or sku.get("externalId")
            or it.get("idInMarketPlace")
        )

        out.append({
            "order_id": order_id,
            "sku_id": sku_id,
            "marketplace_item_id": marketplace_item_id,
            "title": title,
            "quantity": int(qty),
            "unit_price": unit_price,
            "total_price": total_price,
            "external_id": external_id,
            "extra_json": safe_json(it),  # aqui já estamos serializando
        })
    return out


def map_payments(order_id: str, pays: list[dict], payload) -> list[dict]:
    out = []
    order_payment_date = parse_dt(payload.get("paymentDate"))
    order_cancel_date = parse_dt(payload.get("cancelDate"))

    for p in pays or []:
        out.append({
            "order_id": order_id,
            "method": p.get("paymentDetailNormalized"),
            "installments": p.get("installments"),
            "amount": p.get("value"),
            "transaction_id": p.get("marketplaceId") or p.get("orderAuthorizationCardCode"),
            "status": p.get("status"),
            "authorized_at": order_payment_date,
            "paid_at": order_payment_date,
            "canceled_at": order_cancel_date,
            "extra_json": safe_json(p)
        })
    return out

def map_shipping(order_id: str, s: dict | None, payload) -> dict | None:
    if not s: return None
    shipping = payload.get("shipping") or {}
    tracking = payload.get("tracking") or {}

    # tenta pegar service/carrier a partir dos items[].shippings[] também
    items = payload.get("items") or []
    first_ship = None
    for it in items:
        sh_list = it.get("shippings") or []
        if sh_list:
            first_ship = sh_list[0]
            break

    carrier = (
        tracking.get("carrier")
        or (first_ship.get("shippingCarrierNormalized") if first_ship else None)
    )
    service = (
        (first_ship.get("shippingtype") if first_ship else None)
        or payload.get("shippingOptionId")
    )

    tracking_code = tracking.get("number")
    promised_delivery = parse_dt(
        shipping.get("promisedShippingTime")
        or tracking.get("estimateDate")
    )
    shipped_at = parse_dt(tracking.get("shippedDate"))
    delivered_at = parse_dt(tracking.get("deliveredDate"))

    address_comp = (
        shipping.get("comment")
        or shipping.get("reference")
    )
    #addr = s.get("address") or {}
    return {
        "order_id": order_id,

        "carrier": carrier,
        "service": service,
        "tracking_code": tracking_code,
        "promised_delivery": promised_delivery,
        "shipped_at": shipped_at,   
        "delivered_at": delivered_at,

        "receiver_name": s.get("receiverName"),
        "address_street": s.get("street"),
        "address_number": s.get("number"),
        "address_comp": address_comp,
        "address_district": s.get("neighborhood"),
        "address_city": s.get("city"),
        "address_state": s.get("state"),
        "address_zip": s.get("zipCode"),
        "extra_json": safe_json(s)
    }

def map_invoice(order_id: str, inv: dict | None) -> dict | None:
    if not inv: return None
    return {
        "order_id": order_id,
        "is_invoiced": 1 if (inv.get("status") == "INVOICED" or inv.get("accessKey")) else 0,
        "invoice_key": inv.get("accessKey"),
        "number": inv.get("number"),
        "series": inv.get("serie") or inv.get("series"),
        "issued_at": dt(inv.get("issueDate")) if inv.get("issueDate") else None,
        "xml_url": inv.get("xmlUrl"),
        "pdf_url": inv.get("pdfUrl"),
        "extra_json": safe_json(inv)
    }

def map_history(order_id: str, hist: list[dict]) -> list[dict]:
    out = []
    for h in hist or []:
        out.append({
            "order_id": order_id,
            "status": h.get("status") or h.get("code") or "UNKNOWN",
            "substatus": h.get("subStatus"),
            "source": "FEED",
            "occurred_at": dt(h.get("changedAt")) or datetime.utcnow(),
            "payload": safe_json(h)
        })
    return out

def map_returns(order_id: str, rets: list[dict]) -> list[dict]:
    out = []
    for r in rets or []:
        out.append({
            "order_id": order_id,
            "status": r.get("status"),
            "reason": r.get("reason"),
            "requested_at": dt(r.get("requestedAt")) if r.get("requestedAt") else None,
            "approved_at": dt(r.get("approvedAt")) if r.get("approvedAt") else None,
            "received_at": dt(r.get("receivedAt")) if r.get("receivedAt") else None,
            "extra_json": safe_json(r)
        })
    return out
