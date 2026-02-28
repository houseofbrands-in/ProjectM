# backend/reconciliation_routes.py
# Myntra Payment Reconciliation — Ingestion + Analytics API

from __future__ import annotations

import io
import csv
import json
import re
from datetime import datetime, date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from sqlalchemy import func, case, text, and_, cast, Float, String

from backend.db import SessionLocal, resolve_workspace_id
from backend.reconciliation_models import (
    MyntraPgForward,
    MyntraPgReverse,
    MyntraNonOrderSettlement,
    MyntraOrderFlow,
    MyntraSkuMap,
)

router = APIRouter(prefix="/db/recon", tags=["reconciliation"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v, default=None):
    if v is None or str(v).strip() in ("", "null", "None"):
        return default
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def _to_int(v, default=None):
    f = _to_float(v)
    return int(f) if f is not None else default


def _to_dt(v) -> Optional[datetime]:
    if v is None or str(v).strip() in ("", "null", "None"):
        return None
    s = str(v).strip()
    # Try common formats
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s[:26].replace("+05:30", ""), fmt.replace("%z", ""))
            return dt
        except Exception:
            continue
    # pandas fallback
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.notna(dt):
            return dt.to_pydatetime().replace(tzinfo=None)
    except Exception:
        pass
    return None


def _to_str(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().strip('"').strip("'")
    if s in ("", "null", "None"):
        return None
    return s


def _norm_col(name: str) -> str:
    """Normalize column header for matching."""
    return re.sub(r"[^a-z0-9_]", "", name.strip().lower().replace(" ", "_"))


def _read_csv(content: bytes) -> list[dict]:
    """Read CSV, return list of dicts with normalized column names."""
    text_content = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text_content))
    rows = []
    for row in reader:
        normalized = {}
        for k, v in row.items():
            if k:
                normalized[_norm_col(k)] = v
        rows.append(normalized)
    return rows


def _get(row: dict, *keys, converter=_to_str):
    """Get value from row by trying multiple key names."""
    for key in keys:
        nk = _norm_col(key)
        if nk in row:
            return converter(row[nk])
    return None


# ---------------------------------------------------------------------------
# Ingest: PG Forward (Settled / Unsettled)
# ---------------------------------------------------------------------------

@router.post("/ingest/myntra/pg-forward")
def ingest_pg_forward(
    workspace_slug: str = Query("default"),
    status: str = Query("settled", description="'settled' or 'unsettled'"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        rows_data = _read_csv(content)
        if not rows_data:
            return {"ok": True, "inserted": 0}

        # Delete existing data for this workspace + status if replace
        if replace:
            deleted = (
                db.query(MyntraPgForward)
                .filter(MyntraPgForward.workspace_id == ws_id)
                .filter(MyntraPgForward.settlement_status == status)
                .delete(synchronize_session=False)
            )
            db.commit()

        objs = []
        for r in rows_data:
            obj = MyntraPgForward(
                workspace_id=ws_id,
                settlement_status=status,
                order_release_id=_get(r, "order_release_id"),
                order_line_id=_get(r, "order_line_id"),
                sku_code=_get(r, "sku_code"),
                packet_id=_get(r, "packet_id"),
                invoice_number=_get(r, "invoice_number"),
                hsn_code=_get(r, "hsn_code"),
                product_tax_category=_get(r, "product_tax_category"),
                seller_order_id=_get(r, "seller_order_id"),
                packing_date=_get(r, "packing_date", converter=_to_dt),
                delivery_date=_get(r, "delivery_date", converter=_to_dt),
                currency=_get(r, "currency") or "INR",
                seller_product_amount=_get(r, "seller_product_amount", converter=_to_float),
                postpaid_amount=_get(r, "postpaid_amount", converter=_to_float),
                prepaid_amount=_get(r, "prepaid_amount", converter=_to_float),
                mrp=_get(r, "mrp", converter=_to_float),
                total_discount_amount=_get(r, "total_discount_amount", converter=_to_float),
                customer_paid_amt=_get(r, "customer_paid_amt", converter=_to_float),
                shipping_case=_get(r, "shipping_case"),
                total_tax_rate=_get(r, "total_tax_rate", converter=_to_float),
                igst_amount=_get(r, "igst_amount", converter=_to_float),
                cgst_amount=_get(r, "cgst_amount", converter=_to_float),
                sgst_amount=_get(r, "sgst_amount", converter=_to_float),
                tcs_amount=_get(r, "tcs_amount", converter=_to_float),
                tds_amount=_get(r, "tds_amount", converter=_to_float),
                taxable_amount=_get(r, "taxable_amount", converter=_to_float),
                igst_rate=_get(r, "igst_rate", converter=_to_float),
                cgst_rate=_get(r, "cgst_rate", converter=_to_float),
                sgst_rate=_get(r, "sgst_rate", converter=_to_float),
                cess_amount=_get(r, "cess_amount", converter=_to_float),
                cess_rate=_get(r, "cess_rate", converter=_to_float),
                tcs_igst_rate=_get(r, "tcs_igst_rate", converter=_to_float),
                tcs_sgst_rate=_get(r, "tcs_sgst_rate", converter=_to_float),
                tcs_cgst_rate=_get(r, "tcs_cgst_rate", converter=_to_float),
                tds_rate=_get(r, "tds_rate", converter=_to_float),
                commission_percentage=_get(r, "commission_percentage", converter=_to_float),
                minimum_commission=_get(r, "minimum_commission", converter=_to_float),
                platform_fees=_get(r, "platform_fees", converter=_to_float),
                total_commission=_get(r, "total_commission", converter=_to_float),
                total_commission_plus_tcs_tds_deduction=_get(r, "total_commission_plus_tcs_tds_deduction", converter=_to_float),
                commission_base_amount=_get(r, "commission_base_amount", converter=_to_float),
                commission_tax_amount=_get(r, "commission_tax_amount", converter=_to_float),
                commission_discount=_get(r, "commission_discount", converter=_to_float),
                sjit_incentive_amount=_get(r, "sjit_incentive_amount", converter=_to_float),
                total_logistics_deduction=_get(r, "total_logistics_deduction", converter=_to_float),
                shipping_fee=_get(r, "shipping_fee", converter=_to_float),
                fixed_fee=_get(r, "fixed_fee", converter=_to_float),
                pick_and_pack_fee=_get(r, "pick_and_pack_fee", converter=_to_float),
                payment_gateway_fee=_get(r, "payment_gateway_fee", converter=_to_float),
                total_tax_on_logistics=_get(r, "total_tax_on_logistics", converter=_to_float),
                article_level=_get(r, "article_level", converter=_to_int),
                shipment_zone_classification=_get(r, "shipment_zone_classification"),
                total_expected_settlement=_get(r, "total_expected_settlement", converter=_to_float),
                total_actual_settlement=_get(r, "total_actual_settlement", converter=_to_float),
                amount_pending_settlement=_get(r, "amount_pending_settlement", converter=_to_float),
                prepaid_commission_deduction=_get(r, "prepaid_commission_deduction", converter=_to_float),
                prepaid_logistics_deduction=_get(r, "prepaid_logistics_deduction", converter=_to_float),
                prepaid_payment=_get(r, "prepaid_payment", converter=_to_float),
                settlement_date_prepaid_comm_deduction=_get(r, "settlement_date_prepaid_comm_deduction", converter=_to_dt),
                settlement_date_prepaid_logistics_deduction=_get(r, "settlement_date_prepaid_logistics_deduction", converter=_to_dt),
                settlement_date_prepaid_payment=_get(r, "settlement_date_prepaid_payment", converter=_to_dt),
                bank_utr_no_prepaid_comm_deduction=_get(r, "bank_utr_no_prepaid_comm_deduction"),
                bank_utr_no_prepaid_logistics_deduction=_get(r, "bank_utr_no_prepaid_logistics_deduction"),
                bank_utr_no_prepaid_payment=_get(r, "bank_utr_no_prepaid_payment"),
                postpaid_commission_deduction=_get(r, "postpaid_commission_deduction", converter=_to_float),
                postpaid_logistics_deduction=_get(r, "postpaid_logistics_deduction", converter=_to_float),
                postpaid_payment=_get(r, "postpaid_payment", converter=_to_float),
                settlement_date_postpaid_comm_deduction=_get(r, "settlement_date_postpaid_comm_deduction", converter=_to_dt),
                settlement_date_postpaid_logistics_deduction=_get(r, "settlement_date_postpaid_logistics_deduction", converter=_to_dt),
                settlement_date_postpaid_payment=_get(r, "settlement_date_postpaid_payment", converter=_to_dt),
                bank_utr_no_postpaid_comm_deduction=_get(r, "bank_utr_no_postpaid_comm_deduction"),
                bank_utr_no_postpaid_logistics_deduction=_get(r, "bank_utr_no_postpaid_logistics_deduction"),
                bank_utr_no_postpaid_payment=_get(r, "bank_utr_no_postpaid_payment"),
                brand=_get(r, "brand"),
                gender=_get(r, "gender"),
                brand_type=_get(r, "brand_type"),
                article_type=_get(r, "article_type"),
                supply_type=_get(r, "supply_type"),
                try_and_buy_purchase=_get(r, "try_and_buy_purchase"),
                seller_tier=_get(r, "seller_tier"),
                seller_gstn=_get(r, "seller_gstn"),
                seller_name=_get(r, "seller_name"),
                myntra_gstn=_get(r, "myntra_gstn"),
                shipping_city=_get(r, "shipping_city"),
                shipping_pin_code=_get(r, "shipping_pin_code"),
                shipping_state=_get(r, "shipping_state"),
                shipping_state_code=_get(r, "shipping_state_code"),
                postpaid_amount_other=_get(r, "postpaid_amount_other", converter=_to_float),
                prepaid_amount_other=_get(r, "prepaid_amount_other", converter=_to_float),
                shipping_amount=_get(r, "shipping_amount", converter=_to_float),
                gift_amount=_get(r, "gift_amount", converter=_to_float),
                additional_amount=_get(r, "additional_amount", converter=_to_float),
                raw_json=json.dumps(r),
                ingested_at=datetime.utcnow(),
            )
            objs.append(obj)

        db.bulk_save_objects(objs)
        db.commit()
        return {"ok": True, "inserted": len(objs), "status": status, "workspace_slug": workspace_slug}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"PG Forward ingest failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Ingest: PG Reverse (Settled / Unsettled)
# ---------------------------------------------------------------------------

@router.post("/ingest/myntra/pg-reverse")
def ingest_pg_reverse(
    workspace_slug: str = Query("default"),
    status: str = Query("settled"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        rows_data = _read_csv(content)
        if not rows_data:
            return {"ok": True, "inserted": 0}

        if replace:
            db.query(MyntraPgReverse).filter(
                MyntraPgReverse.workspace_id == ws_id,
                MyntraPgReverse.settlement_status == status,
            ).delete(synchronize_session=False)
            db.commit()

        objs = []
        for r in rows_data:
            obj = MyntraPgReverse(
                workspace_id=ws_id,
                settlement_status=status,
                order_release_id=_get(r, "order_release_id"),
                order_line_id=_get(r, "order_line_id"),
                sku_code=_get(r, "sku_code"),
                packet_id=_get(r, "packet_id"),
                invoice_number=_get(r, "invoice_number"),
                hsn_code=_get(r, "hsn_code"),
                product_tax_category=_get(r, "product_tax_category"),
                seller_order_id=_get(r, "seller_order_id"),
                return_id=_get(r, "return_id"),
                return_type=_get(r, "return_type"),
                return_date=_get(r, "return_date", converter=_to_dt),
                packing_date=_get(r, "packing_date", converter=_to_dt),
                delivery_date=_get(r, "delivery_date", converter=_to_dt),
                currency=_get(r, "currency") or "INR",
                seller_product_amount=_get(r, "seller_product_amount", converter=_to_float),
                postpaid_amount=_get(r, "postpaid_amount", converter=_to_float),
                prepaid_amount=_get(r, "prepaid_amount", converter=_to_float),
                mrp=_get(r, "mrp", converter=_to_float),
                total_discount_amount=_get(r, "total_discount_amount", converter=_to_float),
                customer_paid_amt=_get(r, "customer_paid_amt", converter=_to_float),
                shipping_case=_get(r, "shipping_case"),
                total_tax_rate=_get(r, "total_tax_rate", converter=_to_float),
                igst_amount=_get(r, "igst_amount", converter=_to_float),
                cgst_amount=_get(r, "cgst_amount", converter=_to_float),
                sgst_amount=_get(r, "sgst_amount", converter=_to_float),
                tcs_amount=_get(r, "tcs_amount", converter=_to_float),
                tds_amount=_get(r, "tds_amount", converter=_to_float),
                taxable_amount=_get(r, "taxable_amount", converter=_to_float),
                commission_percentage=_get(r, "commission_percentage", converter=_to_float),
                minimum_commission=_get(r, "minimum_commission", converter=_to_float),
                platform_fees=_get(r, "platform_fees", converter=_to_float),
                total_commission=_get(r, "total_commission", converter=_to_float),
                total_commission_plus_tcs_tds_deduction=_get(r, "total_commission_plus_tcs_tds_deduction", converter=_to_float),
                commission_base_amount=_get(r, "commission_base_amount", converter=_to_float),
                commission_tax_amount=_get(r, "commission_tax_amount", converter=_to_float),
                commission_discount=_get(r, "commission_discount", converter=_to_float),
                sjit_incentive_amount=_get(r, "sjit_incentive_amount", converter=_to_float),
                total_logistics_deduction=_get(r, "total_logistics_deduction", converter=_to_float),
                shipping_fee=_get(r, "shipping_fee", converter=_to_float),
                fixed_fee=_get(r, "fixed_fee", converter=_to_float),
                pick_and_pack_fee=_get(r, "pick_and_pack_fee", converter=_to_float),
                payment_gateway_fee=_get(r, "payment_gateway_fee", converter=_to_float),
                total_tax_on_logistics=_get(r, "total_tax_on_logistics", converter=_to_float),
                article_level=_get(r, "article_level", converter=_to_int),
                shipment_zone_classification=_get(r, "shipment_zone_classification"),
                total_settlement=_get(r, "total_settlement", converter=_to_float),
                total_actual_settlement=_get(r, "total_actual_settlement", converter=_to_float),
                amount_pending_settlement=_get(r, "amount_pending_settlement", converter=_to_float),
                prepaid_commission_deduction=_get(r, "prepaid_commission_deduction", converter=_to_float),
                prepaid_logistics_deduction=_get(r, "prepaid_logistics_deduction", converter=_to_float),
                prepaid_payment=_get(r, "prepaid_payment", converter=_to_float),
                settlement_date_prepaid_comm_deduction=_get(r, "settlement_date_prepaid_comm_deduction", converter=_to_dt),
                settlement_date_prepaid_logistics_deduction=_get(r, "settlement_date_prepaid_logistics_deduction", converter=_to_dt),
                settlement_date_prepaid_payment=_get(r, "settlement_date_prepaid_payment", converter=_to_dt),
                bank_utr_no_prepaid_comm_deduction=_get(r, "bank_utr_no_prepaid_comm_deduction"),
                bank_utr_no_prepaid_logistics_deduction=_get(r, "bank_utr_no_prepaid_logistics_deduction"),
                bank_utr_no_prepaid_payment=_get(r, "bank_utr_no_prepaid_payment"),
                postpaid_commission_deduction=_get(r, "postpaid_commission_deduction", converter=_to_float),
                postpaid_logistics_deduction=_get(r, "postpaid_logistics_deduction", converter=_to_float),
                postpaid_payment=_get(r, "postpaid_payment", converter=_to_float),
                settlement_date_postpaid_comm_deduction=_get(r, "settlement_date_postpaid_comm_deduction", converter=_to_dt),
                settlement_date_postpaid_logistics_deduction=_get(r, "settlement_date_postpaid_logistics_deduction", converter=_to_dt),
                settlement_date_postpaid_payment=_get(r, "settlement_date_postpaid_payment", converter=_to_dt),
                bank_utr_no_postpaid_comm_deduction=_get(r, "bank_utr_no_postpaid_comm_deduction"),
                bank_utr_no_postpaid_logistics_deduction=_get(r, "bank_utr_no_postpaid_logistics_deduction"),
                bank_utr_no_postpaid_payment=_get(r, "bank_utr_no_postpaid_payment"),
                brand=_get(r, "brand"),
                gender=_get(r, "gender"),
                brand_type=_get(r, "brand_type"),
                article_type=_get(r, "article_type"),
                supply_type=_get(r, "supply_type"),
                try_and_buy_purchase=_get(r, "try_and_buy_purchase"),
                seller_tier=_get(r, "seller_tier"),
                seller_gstn=_get(r, "seller_gstn"),
                seller_name=_get(r, "seller_name"),
                myntra_gstn=_get(r, "myntra_gstn"),
                shipping_city=_get(r, "shipping_city"),
                shipping_pin_code=_get(r, "shipping_pin_code"),
                shipping_state=_get(r, "shipping_state"),
                shipping_state_code=_get(r, "shipping_state_code"),
                raw_json=json.dumps(r),
                ingested_at=datetime.utcnow(),
            )
            objs.append(obj)

        db.bulk_save_objects(objs)
        db.commit()
        return {"ok": True, "inserted": len(objs), "status": status, "workspace_slug": workspace_slug}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"PG Reverse ingest failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Ingest: Non-Order Settlement
# ---------------------------------------------------------------------------

@router.post("/ingest/myntra/non-order-settlement")
def ingest_non_order_settlement(
    workspace_slug: str = Query("default"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        rows_data = _read_csv(content)
        if not rows_data:
            return {"ok": True, "inserted": 0}

        if replace:
            db.query(MyntraNonOrderSettlement).filter(
                MyntraNonOrderSettlement.workspace_id == ws_id
            ).delete(synchronize_session=False)
            db.commit()

        objs = []
        for r in rows_data:
            obj = MyntraNonOrderSettlement(
                workspace_id=ws_id,
                seller_name=_get(r, "seller_name"),
                settlement_amount=_get(r, "settlement_amount", converter=_to_float),
                settlement_type=_get(r, "settlement_type"),
                utr=_get(r, "utr"),
                invoice_ref=_get(r, "invoice_ref"),
                settlement_date=_get(r, "settlement_date", converter=_to_dt),
                settlement_description=_get(r, "settlement_description"),
                raw_json=json.dumps(r),
                ingested_at=datetime.utcnow(),
            )
            objs.append(obj)

        db.bulk_save_objects(objs)
        db.commit()
        return {"ok": True, "inserted": len(objs), "workspace_slug": workspace_slug}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Non-order settlement ingest failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Ingest: Order Flow
# ---------------------------------------------------------------------------

@router.post("/ingest/myntra/order-flow")
def ingest_order_flow(
    workspace_slug: str = Query("default"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        rows_data = _read_csv(content)
        if not rows_data:
            return {"ok": True, "inserted": 0}

        if replace:
            db.query(MyntraOrderFlow).filter(
                MyntraOrderFlow.workspace_id == ws_id
            ).delete(synchronize_session=False)
            db.commit()

        objs = []
        for r in rows_data:
            obj = MyntraOrderFlow(
                workspace_id=ws_id,
                sale_order_code=_get(r, "sale_order_code"),
                order_number=_get(r, "order_number"),
                product_sku_code=_get(r, "product_sku_code"),
                invoice_number=_get(r, "invoice_number"),
                seller_order_id=_get(r, "seller_order_id"),
                packed_id=_get(r, "packed_id"),
                order_item_status=_get(r, "order_item_status"),
                return_type=_get(r, "return_type"),
                order_date=_get(r, "order_date", converter=_to_dt),
                packing_date=_get(r, "packing_date", converter=_to_dt),
                promised_delivery_date=_get(r, "promised_delivery_date", converter=_to_dt),
                actual_delivery_date=_get(r, "actual_delivery_date", converter=_to_dt),
                return_date=_get(r, "return_date", converter=_to_dt),
                restocked_date=_get(r, "restocked_date", converter=_to_dt),
                promised_settlement_date=_get(r, "promised_settlement_date", converter=_to_dt),
                currency=_get(r, "currency") or "INR",
                seller_paid_amount=_get(r, "seller_paid_amount", converter=_to_float),
                postpaid_amount=_get(r, "postpaid_amount", converter=_to_float),
                prepaid_amount=_get(r, "prepaid_amount", converter=_to_float),
                mrp=_get(r, "mrp", converter=_to_float),
                discount_amount=_get(r, "discount_amount", converter=_to_float),
                shipping_case=_get(r, "shipping_case"),
                tax_rate=_get(r, "tax_rate", converter=_to_float),
                igst_amount=_get(r, "igst_amount", converter=_to_float),
                cgst_amount=_get(r, "cgst_amount", converter=_to_float),
                sgst_amount=_get(r, "sgst_amount", converter=_to_float),
                tcs_igst_amt=_get(r, "tcs_igst_amt", converter=_to_float),
                tcs_sgst_amt=_get(r, "tcs_sgst_amt", converter=_to_float),
                tcs_cgst_amt=_get(r, "tcs_cgst_amt", converter=_to_float),
                taxable_amount=_get(r, "taxable_amount", converter=_to_float),
                minimum_commission=_get(r, "minimum_commission", converter=_to_float),
                commission_pct=_get(r, "commission_pct", converter=_to_float),
                commission_total_amount=_get(r, "commission_total_amount", converter=_to_float),
                commission_base_amount=_get(r, "commission_base_amount", converter=_to_float),
                commission_tax_amount=_get(r, "commission_tax_amount", converter=_to_float),
                total_commission_plus_tcs_deduction_fw=_get(r, "total_commission_plus_tcs_deduction_fw", converter=_to_float),
                logistics_deduction_fw=_get(r, "logistics_deduction_fw", converter=_to_float),
                customer_paid_amt_fw=_get(r, "customer_paid_amt_fw", converter=_to_float),
                total_settlement_fw=_get(r, "total_settlement_fw", converter=_to_float),
                amount_pending_settlement_fw=_get(r, "amount_pending_settlement_fw", converter=_to_float),
                total_commission_plus_tcs_deduction_rv=_get(r, "total_commission_plus_tcs_deduction_rv", converter=_to_float),
                logistics_deduction_rv=_get(r, "logistics_deduction_rv", converter=_to_float),
                total_settlement_rv=_get(r, "total_settlement_rv", converter=_to_float),
                amount_pending_settlement_rv=_get(r, "amount_pending_settlement_rv", converter=_to_float),
                brand=_get(r, "brand"),
                gender=_get(r, "gender"),
                article_type=_get(r, "article_type"),
                supply_type=_get(r, "supply_type"),
                is_try_and_buy=_get(r, "is_try_and_buy"),
                payment_method=_get(r, "payment_method"),
                courier_name=_get(r, "courier_name"),
                tracking_no=_get(r, "tracking_no"),
                hsn=_get(r, "hsn"),
                product_tax_category=_get(r, "product_tax_category"),
                e_commerce_portal_name=_get(r, "e_commerce_portal_name"),
                seller_gstn=_get(r, "seller_gstn"),
                seller_name=_get(r, "seller_name"),
                myntra_gstn=_get(r, "myntra_gstn"),
                customer_pincode=_get(r, "customer_pincode"),
                customer_state=_get(r, "customer_state"),
                total_customer_paid=_get(r, "total_customer_paid", converter=_to_float),
                raw_json=json.dumps(r),
                ingested_at=datetime.utcnow(),
            )
            objs.append(obj)

        db.bulk_save_objects(objs)
        db.commit()
        return {"ok": True, "inserted": len(objs), "workspace_slug": workspace_slug}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Order flow ingest failed: {e}")
    finally:
        db.close()


# ===========================================================================
# ANALYTICS ENDPOINTS
# ===========================================================================

# ---------------------------------------------------------------------------
# 1. Reconciliation Summary — the big picture
# ---------------------------------------------------------------------------

@router.get("/summary")
def recon_summary(workspace_slug: str = Query("default")):
    """
    High-level reconciliation summary:
    - Total forward sales (settled + unsettled)
    - Total reverse (returns/RTO settled + unsettled)
    - Commission, logistics, tax deductions
    - Net settlement, pending settlement
    - Non-order deductions
    """
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # Forward aggregates
        fw = db.query(
            func.count(MyntraPgForward.id).label("count"),
            func.coalesce(func.sum(MyntraPgForward.seller_product_amount), 0).label("total_seller_amount"),
            func.coalesce(func.sum(MyntraPgForward.mrp), 0).label("total_mrp"),
            func.coalesce(func.sum(MyntraPgForward.total_discount_amount), 0).label("total_discount"),
            # total_commission INCLUDES platform_fees — do not add both
            func.coalesce(func.sum(MyntraPgForward.total_commission), 0).label("total_commission"),
            # total_logistics_deduction INCLUDES shipping, pick&pack, fixed, pg fee
            func.coalesce(func.sum(MyntraPgForward.total_logistics_deduction), 0).label("total_logistics"),
            func.coalesce(func.sum(MyntraPgForward.shipping_fee), 0).label("total_shipping_fee"),
            func.coalesce(func.sum(MyntraPgForward.pick_and_pack_fee), 0).label("total_pick_pack"),
            func.coalesce(func.sum(MyntraPgForward.fixed_fee), 0).label("total_fixed_fee"),
            func.coalesce(func.sum(MyntraPgForward.payment_gateway_fee), 0).label("total_pg_fee"),
            func.coalesce(func.sum(MyntraPgForward.tcs_amount), 0).label("total_tcs"),
            func.coalesce(func.sum(MyntraPgForward.tds_amount), 0).label("total_tds"),
            func.coalesce(func.sum(MyntraPgForward.total_actual_settlement), 0).label("total_settled"),
            func.coalesce(func.sum(MyntraPgForward.amount_pending_settlement), 0).label("total_pending"),
        ).filter(MyntraPgForward.workspace_id == ws_id).first()

        # Reverse aggregates
        rv = db.query(
            func.count(MyntraPgReverse.id).label("count"),
            func.coalesce(func.sum(MyntraPgReverse.seller_product_amount), 0).label("total_seller_amount"),
            func.coalesce(func.sum(MyntraPgReverse.total_commission), 0).label("total_commission"),
            func.coalesce(func.sum(MyntraPgReverse.total_logistics_deduction), 0).label("total_logistics"),
            func.coalesce(func.sum(MyntraPgReverse.tcs_amount), 0).label("total_tcs"),
            func.coalesce(func.sum(MyntraPgReverse.tds_amount), 0).label("total_tds"),
            func.coalesce(func.sum(MyntraPgReverse.total_actual_settlement), 0).label("total_settled"),
            func.coalesce(func.sum(MyntraPgReverse.amount_pending_settlement), 0).label("total_pending"),
        ).filter(MyntraPgReverse.workspace_id == ws_id).first()

        # Non-order
        no = db.query(
            func.count(MyntraNonOrderSettlement.id).label("count"),
            func.coalesce(func.sum(MyntraNonOrderSettlement.settlement_amount), 0).label("total_amount"),
        ).filter(MyntraNonOrderSettlement.workspace_id == ws_id).first()

        return {
            "workspace_slug": workspace_slug,
            "forward": {
                "orders": fw.count,
                "total_mrp": round(fw.total_mrp, 2),
                "total_discount": round(fw.total_discount, 2),
                "total_seller_amount": round(fw.total_seller_amount, 2),
                "deductions": {
                    "commission": round(abs(fw.total_commission), 2),
                    "logistics": round(abs(fw.total_logistics), 2),
                    "logistics_breakdown": {
                        "shipping_fee": round(abs(fw.total_shipping_fee), 2),
                        "pick_and_pack": round(abs(fw.total_pick_pack), 2),
                        "fixed_fee": round(abs(fw.total_fixed_fee), 2),
                        "payment_gateway": round(abs(fw.total_pg_fee), 2),
                    },
                    "tcs": round(abs(fw.total_tcs), 2),
                    "tds": round(abs(fw.total_tds), 2),
                    "total": round(abs(fw.total_commission) + abs(fw.total_logistics) + abs(fw.total_tcs) + abs(fw.total_tds), 2),
                },
                "settled": round(fw.total_settled, 2),
                "pending": round(fw.total_pending, 2),
            },
            "reverse": {
                "orders": rv.count,
                "total_seller_amount": round(rv.total_seller_amount, 2),
                "deductions": {
                    "commission": round(abs(rv.total_commission), 2),
                    "logistics": round(abs(rv.total_logistics), 2),
                    "tcs": round(abs(rv.total_tcs), 2),
                    "tds": round(abs(rv.total_tds), 2),
                },
                "settled": round(rv.total_settled, 2),
                "pending": round(rv.total_pending, 2),
            },
            "non_order": {
                "count": no.count,
                "total_amount": round(no.total_amount, 2),
            },
            "net_settlement": round(
                fw.total_settled + rv.total_settled + no.total_amount, 2
            ),
            "total_pending": round(fw.total_pending + rv.total_pending, 2),
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 2. Commission Audit — verify commission rates
# ---------------------------------------------------------------------------

@router.get("/commission-audit")
def commission_audit(
    workspace_slug: str = Query("default"),
    expected_rate: float = Query(None, description="Expected commission % to compare against"),
):
    """
    Compare actual commission charged per order vs expected rate.
    Flags orders where commission_percentage differs from expected.
    """
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        q = db.query(
            MyntraPgForward.order_release_id,
            MyntraPgForward.sku_code,
            MyntraPgForward.brand,
            MyntraPgForward.article_type,
            MyntraPgForward.seller_product_amount,
            MyntraPgForward.commission_percentage,
            MyntraPgForward.total_commission,
            MyntraPgForward.platform_fees,
            MyntraPgForward.total_logistics_deduction,
            MyntraPgForward.total_actual_settlement,
        ).filter(
            MyntraPgForward.workspace_id == ws_id,
            MyntraPgForward.commission_percentage.isnot(None),
        ).order_by(MyntraPgForward.commission_percentage.desc())

        rows = q.all()

        # Group by commission rate
        rate_groups = {}
        mismatches = []
        for r in rows:
            rate = round(r.commission_percentage or 0, 1)
            if rate not in rate_groups:
                rate_groups[rate] = {"count": 0, "total_commission": 0, "total_seller_amount": 0}
            rate_groups[rate]["count"] += 1
            rate_groups[rate]["total_commission"] += abs(r.total_commission or 0)
            rate_groups[rate]["total_seller_amount"] += abs(r.seller_product_amount or 0)

            if expected_rate is not None and abs(rate - expected_rate) > 0.5:
                mismatches.append({
                    "order_release_id": r.order_release_id,
                    "sku_code": r.sku_code,
                    "brand": r.brand,
                    "article_type": r.article_type,
                    "seller_amount": r.seller_product_amount,
                    "actual_commission_pct": rate,
                    "expected_commission_pct": expected_rate,
                    "commission_charged": abs(r.total_commission or 0),
                })

        return {
            "workspace_slug": workspace_slug,
            "total_orders": len(rows),
            "rate_distribution": {
                str(k): {
                    "count": v["count"],
                    "total_commission": round(v["total_commission"], 2),
                    "avg_commission": round(v["total_commission"] / v["count"], 2) if v["count"] else 0,
                }
                for k, v in sorted(rate_groups.items())
            },
            "mismatches": mismatches[:100] if expected_rate else [],
            "mismatch_count": len(mismatches) if expected_rate else 0,
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 3. SKU-level P&L
# ---------------------------------------------------------------------------

@router.get("/sku-pnl")
def sku_pnl(
    workspace_slug: str = Query("default"),
    top_n: int = Query(50),
    sort_by: str = Query("net_profit"),
    sort_dir: str = Query("desc"),
):
    """
    True P&L per SKU with seller SKU mapping.
    Commission = total_commission (already includes platform_fees — NOT double counted).
    Logistics = total_logistics_deduction (already includes shipping, pick&pack, etc.).
    """
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # Build SKU map lookup
        sku_map = {}
        map_rows = db.query(MyntraSkuMap).filter(MyntraSkuMap.workspace_id == ws_id).all()
        for m in map_rows:
            sku_map[m.sku_code] = {
                "seller_sku_code": m.seller_sku_code,
                "style_id": m.style_id,
                "style_name": m.style_name,
                "size": m.size,
            }

        # Forward (sales) by SKU
        fw_q = db.query(
            MyntraPgForward.sku_code,
            MyntraPgForward.brand,
            MyntraPgForward.article_type,
            func.count(MyntraPgForward.id).label("fw_orders"),
            func.coalesce(func.sum(MyntraPgForward.seller_product_amount), 0).label("fw_revenue"),
            func.coalesce(func.sum(MyntraPgForward.mrp), 0).label("fw_mrp"),
            func.coalesce(func.sum(MyntraPgForward.total_discount_amount), 0).label("fw_discount"),
            func.coalesce(func.sum(MyntraPgForward.total_commission), 0).label("fw_commission"),
            func.coalesce(func.sum(MyntraPgForward.total_logistics_deduction), 0).label("fw_logistics"),
            func.coalesce(func.sum(MyntraPgForward.tcs_amount), 0).label("fw_tcs"),
            func.coalesce(func.sum(MyntraPgForward.tds_amount), 0).label("fw_tds"),
            func.coalesce(func.sum(MyntraPgForward.total_actual_settlement), 0).label("fw_settled"),
            func.coalesce(func.sum(MyntraPgForward.amount_pending_settlement), 0).label("fw_pending"),
        ).filter(
            MyntraPgForward.workspace_id == ws_id
        ).group_by(
            MyntraPgForward.sku_code,
            MyntraPgForward.brand,
            MyntraPgForward.article_type,
        ).all()

        # Reverse (returns) by SKU
        rv_map = {}
        rv_q = db.query(
            MyntraPgReverse.sku_code,
            func.count(MyntraPgReverse.id).label("rv_orders"),
            func.coalesce(func.sum(MyntraPgReverse.seller_product_amount), 0).label("rv_amount"),
            func.coalesce(func.sum(MyntraPgReverse.total_commission), 0).label("rv_commission"),
            func.coalesce(func.sum(MyntraPgReverse.total_logistics_deduction), 0).label("rv_logistics"),
            func.coalesce(func.sum(MyntraPgReverse.total_actual_settlement), 0).label("rv_settled"),
        ).filter(
            MyntraPgReverse.workspace_id == ws_id
        ).group_by(MyntraPgReverse.sku_code).all()

        for rv in rv_q:
            rv_map[rv.sku_code] = {
                "rv_orders": rv.rv_orders,
                "rv_amount": rv.rv_amount,
                "rv_commission": rv.rv_commission,
                "rv_logistics": rv.rv_logistics,
                "rv_settled": rv.rv_settled,
            }

        results = []
        for fw in fw_q:
            rv = rv_map.get(fw.sku_code, {})
            rv_orders = rv.get("rv_orders", 0)
            rv_amount = rv.get("rv_amount", 0)

            gross_revenue = fw.fw_revenue
            fw_commission = abs(fw.fw_commission)
            fw_logistics = abs(fw.fw_logistics)
            tcs = abs(fw.fw_tcs)
            tds = abs(fw.fw_tds)
            total_deductions = fw_commission + fw_logistics + tcs + tds

            # Net Profit = Revenue - Deductions + Return adjustments
            net_profit = gross_revenue - total_deductions - abs(rv_amount)
            total_settled = fw.fw_settled + rv.get("rv_settled", 0)

            mapped = sku_map.get(fw.sku_code, {})

            results.append({
                "sku_code": fw.sku_code,
                "seller_sku_code": mapped.get("seller_sku_code"),
                "style_name": mapped.get("style_name"),
                "size": mapped.get("size"),
                "brand": fw.brand,
                "article_type": fw.article_type,
                "forward_orders": fw.fw_orders,
                "return_orders": rv_orders,
                "return_pct": round(rv_orders / fw.fw_orders * 100, 1) if fw.fw_orders else 0,
                "mrp_total": round(fw.fw_mrp, 2),
                "discount_total": round(abs(fw.fw_discount), 2),
                "gross_revenue": round(gross_revenue, 2),
                "return_deduction": round(-abs(rv_amount), 2),
                "net_revenue": round(gross_revenue - abs(rv_amount), 2),
                "commission": round(fw_commission, 2),
                "logistics": round(fw_logistics, 2),
                "tcs": round(tcs, 2),
                "tds": round(tds, 2),
                "total_deductions": round(total_deductions, 2),
                "net_profit": round(net_profit, 2),
                "margin_pct": round(net_profit / gross_revenue * 100, 1) if gross_revenue else 0,
                "settled": round(total_settled, 2),
                "asp": round(gross_revenue / fw.fw_orders, 2) if fw.fw_orders else 0,
            })

        # Sort
        reverse = sort_dir.lower() != "asc"
        results.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=reverse)

        totals = {
            "forward_orders": sum(r["forward_orders"] for r in results),
            "return_orders": sum(r["return_orders"] for r in results),
            "gross_revenue": round(sum(r["gross_revenue"] for r in results), 2),
            "return_deduction": round(sum(r["return_deduction"] for r in results), 2),
            "commission": round(sum(r["commission"] for r in results), 2),
            "logistics": round(sum(r["logistics"] for r in results), 2),
            "net_profit": round(sum(r["net_profit"] for r in results), 2),
        }

        return {
            "workspace_slug": workspace_slug,
            "total_skus": len(results),
            "totals": totals,
            "rows": results[:top_n],
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 4. Settlement Tracker — pending payments
# ---------------------------------------------------------------------------

@router.get("/settlement-tracker")
def settlement_tracker(workspace_slug: str = Query("default")):
    """Track settled vs pending amounts, days to settlement."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # Settled forward orders with settlement dates
        settled = db.query(
            func.count(MyntraPgForward.id).label("count"),
            func.coalesce(func.sum(MyntraPgForward.total_actual_settlement), 0).label("amount"),
        ).filter(
            MyntraPgForward.workspace_id == ws_id,
            MyntraPgForward.settlement_status == "settled",
        ).first()

        # Unsettled forward orders
        unsettled = db.query(
            func.count(MyntraPgForward.id).label("count"),
            func.coalesce(func.sum(MyntraPgForward.amount_pending_settlement), 0).label("amount"),
        ).filter(
            MyntraPgForward.workspace_id == ws_id,
            MyntraPgForward.settlement_status == "unsettled",
        ).first()

        # Non-order settlements
        non_order = db.query(
            func.count(MyntraNonOrderSettlement.id).label("count"),
            func.coalesce(func.sum(MyntraNonOrderSettlement.settlement_amount), 0).label("amount"),
        ).filter(
            MyntraNonOrderSettlement.workspace_id == ws_id,
        ).first()

        return {
            "workspace_slug": workspace_slug,
            "forward_settled": {
                "count": settled.count,
                "amount": round(settled.amount, 2),
            },
            "forward_unsettled": {
                "count": unsettled.count,
                "amount": round(unsettled.amount, 2),
            },
            "non_order_adjustments": {
                "count": non_order.count,
                "amount": round(non_order.amount, 2),
            },
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 5. Penalty / Non-Order Audit
# ---------------------------------------------------------------------------

@router.get("/penalty-audit")
def penalty_audit(workspace_slug: str = Query("default")):
    """List all non-order deductions grouped by description/type."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        rows = db.query(
            MyntraNonOrderSettlement.settlement_description,
            MyntraNonOrderSettlement.settlement_type,
            func.count(MyntraNonOrderSettlement.id).label("count"),
            func.sum(MyntraNonOrderSettlement.settlement_amount).label("total_amount"),
        ).filter(
            MyntraNonOrderSettlement.workspace_id == ws_id,
        ).group_by(
            MyntraNonOrderSettlement.settlement_description,
            MyntraNonOrderSettlement.settlement_type,
        ).order_by(func.sum(MyntraNonOrderSettlement.settlement_amount)).all()

        return {
            "workspace_slug": workspace_slug,
            "rows": [
                {
                    "description": r.settlement_description,
                    "type": r.settlement_type,
                    "count": r.count,
                    "total_amount": round(r.total_amount or 0, 2),
                }
                for r in rows
            ],
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Ingest: SKU Map (Listings Report)
# ---------------------------------------------------------------------------

@router.post("/ingest/myntra/sku-map")
def ingest_sku_map(
    workspace_slug: str = Query("default"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    """Ingest Myntra Listings Report to build sku_code -> seller_sku_code mapping."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        rows_data = _read_csv(content)
        if not rows_data:
            return {"ok": True, "inserted": 0}

        if replace:
            db.query(MyntraSkuMap).filter(MyntraSkuMap.workspace_id == ws_id).delete(synchronize_session=False)
            db.commit()

        objs = []
        for r in rows_data:
            sku_code = _get(r, "sku_code", "skucode", "sku code")
            if not sku_code:
                continue
            obj = MyntraSkuMap(
                workspace_id=ws_id,
                sku_code=sku_code,
                sku_id=_get(r, "sku_id", "skuid", "sku id"),
                seller_sku_code=_get(r, "seller_sku_code", "sellerskucode", "seller sku code"),
                style_id=_get(r, "style_id", "styleid", "style id"),
                style_name=_get(r, "style_name", "stylename", "style name"),
                brand=_get(r, "brand"),
                article_type=_get(r, "article_type", "articletype", "article type"),
                size=_get(r, "size"),
                mrp=_get(r, "mrp", converter=_to_float),
                ingested_at=datetime.utcnow(),
            )
            objs.append(obj)

        db.bulk_save_objects(objs)
        db.commit()
        return {"ok": True, "inserted": len(objs), "workspace_slug": workspace_slug}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"SKU map ingest failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Download: SKU P&L as CSV
# ---------------------------------------------------------------------------

@router.get("/sku-pnl/download")
def sku_pnl_download(
    workspace_slug: str = Query("default"),
    sort_by: str = Query("net_profit"),
    sort_dir: str = Query("desc"),
):
    """Download SKU P&L as CSV."""
    import csv as csv_mod
    from fastapi.responses import StreamingResponse

    data = sku_pnl(workspace_slug=workspace_slug, top_n=9999, sort_by=sort_by, sort_dir=sort_dir)
    rows = data["rows"]

    output = io.StringIO()
    writer = csv_mod.writer(output)
    writer.writerow([
        "SKU Code", "Seller SKU", "Style Name", "Size", "Brand", "Article Type",
        "Orders", "Returns", "Return%", "MRP Total", "Discount", "Gross Revenue",
        "Return Deduction", "Net Revenue", "Commission", "Logistics", "TCS", "TDS",
        "Total Deductions", "Net Profit", "Margin%", "ASP", "Settled"
    ])
    for r in rows:
        writer.writerow([
            r["sku_code"], r.get("seller_sku_code", ""), r.get("style_name", ""),
            r.get("size", ""), r["brand"], r["article_type"],
            r["forward_orders"], r["return_orders"], r["return_pct"],
            r.get("mrp_total", ""), r.get("discount_total", ""), r["gross_revenue"],
            r["return_deduction"], r["net_revenue"], r["commission"],
            r["logistics"], r.get("tcs", ""), r.get("tds", ""), r.get("total_deductions", ""),
            r["net_profit"], r["margin_pct"], r.get("asp", ""), r.get("settled", ""),
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=sku_pnl_{workspace_slug}.csv"},
    )