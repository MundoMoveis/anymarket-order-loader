from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from src.mappers import map_header, map_items, map_payments, map_shipping, map_invoice, map_history, map_returns

from src.log import log

def debug_params(params):
    out = {}
    for k,v in params.items():
        out[k] = type(v).__name__
    return out

async def upsert_order_tree(session: AsyncSession, payload: dict, marketplace_id: int = 1):
    order_id =payload["id"]

    old = await session.execute(
        text("SELECT status, substatus FROM anymarket_orders WHERE id = :id"),
        {"id": payload["id"]},
    )
    row = old.fetchone()

    if row:
        old_status = row.status
        new_status = payload["status"]
        if new_status and new_status != old_status:
            await session.execute(
                text("""
                    INSERT INTO anymarket_order_status_history
                    (order_id, old_status, new_status, changed_at)
                    VALUES (:oid, :old, :new, NOW())
                """),
                {"oid": payload["id"], "old": old_status, "new": new_status},
            )

    # HEADER
    h = map_header(payload, marketplace_id)
    try:
      await session.execute(text("""
        INSERT INTO anymarket_orders
        (id, marketplace_id, marketplace_order_id, channel, fulfillment_type, status, substatus,
        buyer_name, buyer_document, buyer_email, total_amount, total_discount, freight_amount,
        created_at_marketplace, approved_at, cancelled_at, shipped_at, delivered_at, created_at, updated_at, extra_json)
        VALUES
        (:id,:marketplace_id,:marketplace_order_id,:channel,:fulfillment_type,:status,:substatus,
        :buyer_name,:buyer_document,:buyer_email,:total_amount,:total_discount,:freight_amount,
        :created_at_marketplace,:approved_at,:cancelled_at,:shipped_at,:delivered_at,:created_at,:updated_at,:extra_json)
        ON DUPLICATE KEY UPDATE
        marketplace_id=VALUES(marketplace_id),
        marketplace_order_id=VALUES(marketplace_order_id),
        channel=VALUES(channel),
        fulfillment_type=VALUES(fulfillment_type),
        status=VALUES(status),
        substatus=VALUES(substatus),
        buyer_name=VALUES(buyer_name),
        buyer_document=VALUES(buyer_document),
        buyer_email=VALUES(buyer_email),
        total_amount=VALUES(total_amount),
        total_discount=VALUES(total_discount),
        freight_amount=VALUES(freight_amount),
        created_at_marketplace=VALUES(created_at_marketplace),
        approved_at=VALUES(approved_at),
        cancelled_at=VALUES(cancelled_at),
        shipped_at=VALUES(shipped_at),
        delivered_at=VALUES(delivered_at),
        updated_at=VALUES(updated_at),
        extra_json=VALUES(extra_json)
      """), h)
    except Exception as e:
        log.error("HEADER ERROR: %s", e)
        log.error("PARAM TYPES: %s", debug_params(h))
        log.error("PARAM VALUES: %s", h)
        raise

    # ITEMS (clean + bulk)
    await session.execute(text("DELETE FROM anymarket_order_items WHERE order_id=:id"), {"id": order_id})
    items = map_items(order_id, payload.get("items") or [])
    if items:
        try:
          await session.execute(text("""
              INSERT INTO anymarket_order_items
              (order_id, sku_id, marketplace_item_id, title, quantity, unit_price, total_price, external_id, extra_json)
              VALUES
              """ + ",".join([f"(:order_id{i},:sku_id{i},:marketplace_item_id{i},:title{i},:quantity{i},:unit_price{i},:total_price{i},:external_id{i},:extra_json{i})" for i in range(len(items))])
            ), {k+str(i): v for i,row in enumerate(items) for k,v in {
                  "order_id": row["order_id"], "sku_id": row["sku_id"], "marketplace_item_id": row["marketplace_item_id"],
                  "title": row["title"], "quantity": row["quantity"], "unit_price": row["unit_price"],
                  "total_price": row["total_price"], "external_id": row["external_id"], "extra_json": row["extra_json"]
                }.items()})
        except Exception as e:
          log.error("LINES ERROR: %s", e)
          log.error("PARAM TYPES: %s", debug_params(h))
          log.error("PARAM VALUES: %s", h)
          raise

    # PAYMENTS
    await session.execute(text("DELETE FROM anymarket_order_payments WHERE order_id=:id"), {"id": order_id})
    pays = map_payments(order_id, payload.get("payments") or [], payload)
    if pays:
        try:
          await session.execute(text("""
            INSERT INTO anymarket_order_payments
            (order_id, method, installments, amount, transaction_id, status, authorized_at, paid_at, canceled_at, extra_json)
            VALUES
            """ + ",".join([f"(:order_id{i},:method{i},:installments{i},:amount{i},:transaction_id{i},:status{i},:authorized_at{i},:paid_at{i},:canceled_at{i},:extra_json{i})" for i in range(len(pays))])
          ), {k+str(i): v for i,row in enumerate(pays) for k,v in {
                "order_id": row["order_id"], "method": row["method"], "installments": row["installments"],
                "amount": row["amount"], "transaction_id": row["transaction_id"], "status": row["status"],
                "authorized_at": row["authorized_at"], "paid_at": row["paid_at"], "canceled_at": row["canceled_at"],
                "extra_json": row["extra_json"]
              }.items()})
        except Exception as e:
          log.error("PAYMENT ERROR: %s", e)
          log.error("PARAM TYPES: %s", debug_params(h))
          log.error("PARAM VALUES: %s", h)
          raise

    # SHIPPING upsert
    ship = map_shipping(order_id, payload.get("shipping"), payload)
    if ship:
        try:
          await session.execute(text("""
            INSERT INTO anymarket_order_shipping
            (order_id,carrier,service,tracking_code,promised_delivery,shipped_at,delivered_at,
            receiver_name,address_street,address_number,address_comp,address_district,address_city,address_state,address_zip,extra_json)
            VALUES
            (:order_id,:carrier,:service,:tracking_code,:promised_delivery,:shipped_at,:delivered_at,
            :receiver_name,:address_street,:address_number,:address_comp,:address_district,:address_city,:address_state,:address_zip,:extra_json)
            ON DUPLICATE KEY UPDATE
            carrier=VALUES(carrier), service=VALUES(service), tracking_code=VALUES(tracking_code),
            promised_delivery=VALUES(promised_delivery), shipped_at=VALUES(shipped_at), delivered_at=VALUES(delivered_at),
            receiver_name=VALUES(receiver_name), address_street=VALUES(address_street), address_number=VALUES(address_number),
            address_comp=VALUES(address_comp), address_district=VALUES(address_district), address_city=VALUES(address_city),
            address_state=VALUES(address_state), address_zip=VALUES(address_zip), extra_json=VALUES(extra_json)
          """), ship)
        except Exception as e:
          log.error("SHIPPING ERROR: %s", e)
          log.error("PARAM TYPES: %s", debug_params(h))
          log.error("PARAM VALUES: %s", h)
          raise

    # INVOICE upsert
    inv = map_invoice(order_id, payload.get("invoice"))
    if inv:
        try:
          await session.execute(text("""
            INSERT INTO anymarket_order_invoices
            (order_id,is_invoiced,invoice_key,number,series,issued_at,xml_url,pdf_url,extra_json)
            VALUES
            (:order_id,:is_invoiced,:invoice_key,:number,:series,:issued_at,:xml_url,:pdf_url,:extra_json)
            ON DUPLICATE KEY UPDATE
            is_invoiced=VALUES(is_invoiced), invoice_key=VALUES(invoice_key), number=VALUES(number),
            series=VALUES(series), issued_at=VALUES(issued_at), xml_url=VALUES(xml_url), pdf_url=VALUES(pdf_url), extra_json=VALUES(extra_json)
          """), inv)
        except Exception as e:
          log.error("INVOICE ERROR: %s", e)
          log.error("PARAM TYPES: %s", debug_params(h))
          log.error("PARAM VALUES: %s", h)
          raise

    # HISTORY append (best effort)
    hist = map_history(order_id, payload.get("statusHistory") or [])
    for h in hist:
        try:
            await session.execute(text("""
              INSERT INTO anymarket_order_status_history
              (order_id,status,substatus,source,occurred_at,payload)
              VALUES (:order_id,:status,:substatus,:source,:occurred_at,:payload)
            """), h)
        except Exception as e:
          log.error("HISTORY ERROR: %s", e)
          log.error("PARAM TYPES: %s", debug_params(h))
          log.error("PARAM VALUES: %s", h)
          raise

    # RETURNS clean + insert
    if isinstance(payload.get("returns"), list) and payload["returns"]:
        await session.execute(text("DELETE FROM anymarket_order_returns WHERE order_id=:id"), {"id": order_id})
        rets = map_returns(order_id, payload["returns"])
        if rets:
            await session.execute(text("""
              INSERT INTO anymarket_order_returns
              (order_id,status,reason,requested_at,approved_at,received_at,extra_json)
              VALUES
              """ + ",".join([f"(:order_id{i},:status{i},:reason{i},:requested_at{i},:approved_at{i},:received_at{i},:extra_json{i})" for i in range(len(rets))])
            ), {k+str(i): v for i,row in enumerate(rets) for k,v in {
                  "order_id": row["order_id"], "status": row["status"], "reason": row["reason"],
                  "requested_at": row["requested_at"], "approved_at": row["approved_at"],
                  "received_at": row["received_at"], "extra_json": row["extra_json"]
                }.items()})
