# backend/flipkart_recon_routes.py
# Flipkart Payment Reconciliation — Ingestion + Analytics API

from __future__ import annotations

import io
import json
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func

from backend.db import SessionLocal, resolve_workspace_id
from backend.flipkart_recon_models import (
    FlipkartSkuPnl,
    FlipkartOrderPnl,
    FlipkartPaymentReport,
)

router = APIRouter(prefix="/db/recon/flipkart", tags=["flipkart-reconciliation"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tf(v):
    """To float, safely."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except:
        return None

def _ti(v):
    f = _tf(v)
    return int(f) if f is not None else None

def _ts(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().strip('"').strip("'")
    return s if s and s != "nan" else None

def _td(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return pd.to_datetime(v, errors="coerce").to_pydatetime().replace(tzinfo=None)
    except:
        return None


def _read_excel_sheet(content: bytes, sheet_name: str, skip_rows: int = 2):
    """Read Excel sheet, skip header rows, return DataFrame."""
    buf = io.BytesIO(content)
    df = pd.read_excel(buf, sheet_name=sheet_name, header=None, skiprows=skip_rows)
    return df


# ---------------------------------------------------------------------------
# Ingest: PNL Report - SKU-level P&L
# ---------------------------------------------------------------------------

@router.post("/ingest/sku-pnl")
def ingest_fk_sku_pnl(
    workspace_slug: str = Query("default"),
    report_month: str = Query(None, description="Report month YYYY-MM, e.g. 2026-01"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    """Ingest Flipkart PNL Report (SKU-level P&L sheet)."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        df = _read_excel_sheet(content, "SKU-level P&L", skip_rows=2)
        if df.empty:
            return {"ok": True, "inserted": 0}

        # Assign column names based on known structure
        col_names = [
            "sku_id", "sku_name", "gross_units", "returned_cancelled_units",
            "rto_units", "rvp_units", "cancelled_units", "net_units", "_net_units2",
            "estimated_net_sales", "_est2", "accounted_net_sales",
            "total_expenses", "commission_fee", "collection_fee", "fixed_fee",
            "pick_and_pack_fee", "forward_shipping_fee", "offer_adjustments",
            "reverse_shipping_fee", "storage_fee", "recall_fee",
            "no_cost_emi_fee", "installation_fee", "tech_visit_fee",
            "uninstallation_fee", "customer_addons_recovery", "franchise_fee",
            "shopsy_marketing_fee", "product_cancellation_fee",
            "taxes_gst", "taxes_tcs", "taxes_tds",
            "rewards_other_benefits", "rewards", "order_spf", "non_order_spf",
            "bank_settlement_projected", "input_tax_credits",
            "input_tax_gst_tcs", "input_tax_tds",
            "net_earnings", "earnings_per_unit", "net_margins_pct", "_net_margins2",
            "_bank_settlement2", "amount_settled", "amount_pending",
        ]
        df.columns = col_names[:len(df.columns)]

        # Filter out empty rows
        df = df.dropna(subset=["sku_id"])

        if replace:
            db.query(FlipkartSkuPnl).filter(FlipkartSkuPnl.workspace_id == ws_id).delete(synchronize_session=False)
            db.commit()

        objs = []
        for _, r in df.iterrows():
            obj = FlipkartSkuPnl(
                workspace_id=ws_id,
                sku_id=_ts(r.get("sku_id")),
                sku_name=_ts(r.get("sku_name")),
                gross_units=_ti(r.get("gross_units")),
                returned_cancelled_units=_ti(r.get("returned_cancelled_units")),
                rto_units=_ti(r.get("rto_units")),
                rvp_units=_ti(r.get("rvp_units")),
                cancelled_units=_ti(r.get("cancelled_units")),
                net_units=_ti(r.get("net_units")),
                estimated_net_sales=_tf(r.get("estimated_net_sales")),
                accounted_net_sales=_tf(r.get("accounted_net_sales")),
                total_expenses=_tf(r.get("total_expenses")),
                commission_fee=_tf(r.get("commission_fee")),
                collection_fee=_tf(r.get("collection_fee")),
                fixed_fee=_tf(r.get("fixed_fee")),
                pick_and_pack_fee=_tf(r.get("pick_and_pack_fee")),
                forward_shipping_fee=_tf(r.get("forward_shipping_fee")),
                offer_adjustments=_tf(r.get("offer_adjustments")),
                reverse_shipping_fee=_tf(r.get("reverse_shipping_fee")),
                storage_fee=_tf(r.get("storage_fee")),
                recall_fee=_tf(r.get("recall_fee")),
                no_cost_emi_fee=_tf(r.get("no_cost_emi_fee")),
                installation_fee=_tf(r.get("installation_fee")),
                tech_visit_fee=_tf(r.get("tech_visit_fee")),
                uninstallation_fee=_tf(r.get("uninstallation_fee")),
                customer_addons_recovery=_tf(r.get("customer_addons_recovery")),
                franchise_fee=_tf(r.get("franchise_fee")),
                shopsy_marketing_fee=_tf(r.get("shopsy_marketing_fee")),
                product_cancellation_fee=_tf(r.get("product_cancellation_fee")),
                taxes_gst=_tf(r.get("taxes_gst")),
                taxes_tcs=_tf(r.get("taxes_tcs")),
                taxes_tds=_tf(r.get("taxes_tds")),
                rewards_other_benefits=_tf(r.get("rewards_other_benefits")),
                rewards=_tf(r.get("rewards")),
                order_spf=_tf(r.get("order_spf")),
                non_order_spf=_tf(r.get("non_order_spf")),
                bank_settlement_projected=_tf(r.get("bank_settlement_projected")),
                input_tax_credits=_tf(r.get("input_tax_credits")),
                input_tax_gst_tcs=_tf(r.get("input_tax_gst_tcs")),
                input_tax_tds=_tf(r.get("input_tax_tds")),
                net_earnings=_tf(r.get("net_earnings")),
                earnings_per_unit=_tf(r.get("earnings_per_unit")),
                net_margins_pct=_tf(r.get("net_margins_pct")),
                amount_settled=_tf(r.get("amount_settled")),
                amount_pending=_tf(r.get("amount_pending")),
                report_month=report_month,
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
        raise HTTPException(500, f"FK SKU PNL ingest failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Ingest: PNL Report - Orders P&L
# ---------------------------------------------------------------------------

@router.post("/ingest/order-pnl")
def ingest_fk_order_pnl(
    workspace_slug: str = Query("default"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    """Ingest Flipkart PNL Report (Orders P&L sheet)."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        df = _read_excel_sheet(content, "Orders P&L", skip_rows=2)
        if df.empty:
            return {"ok": True, "inserted": 0}

        col_names = [
            "order_date", "order_id", "order_item_id", "sku_id", "fulfilment_type",
            "channel_of_sale", "mode_of_payment", "shipping_zone", "order_status", "_blank1",
            "gross_units", "returned_cancelled_units", "rto_units", "rvp_units", "cancelled_units",
            "net_units", "_blank2",
            "sale_amount", "seller_burn_offer", "customer_addons_amount",
            "estimated_net_sales", "_est2", "accounted_net_sales",
            "total_expenses",
            "commission_fee", "collection_fee", "fixed_fee", "pick_and_pack_fee",
            "forward_shipping_fee", "offer_adjustments", "reverse_shipping_fee",
            "no_cost_emi_fee", "installation_fee", "tech_visit_fee",
            "uninstallation_fee", "customer_addons_recovery", "franchise_fee",
            "shopsy_marketing_fee", "product_cancellation_fee",
            "storage_fee", "recall_fee",
            "taxes_gst", "taxes_tcs", "taxes_tds",
            "rewards_other_benefits", "rewards", "order_spf", "non_order_spf",
            "bank_settlement_projected", "input_tax_credits",
            "input_tax_gst_tcs", "input_tax_tds",
            "net_earnings", "earnings_per_unit", "net_margins_pct", "_net_margins2",
            "_bank_settlement2", "amount_settled", "amount_pending",
        ]
        df.columns = col_names[:len(df.columns)]
        df = df.dropna(subset=["order_id"])

        if replace:
            db.query(FlipkartOrderPnl).filter(FlipkartOrderPnl.workspace_id == ws_id).delete(synchronize_session=False)
            db.commit()

        objs = []
        for _, r in df.iterrows():
            obj = FlipkartOrderPnl(
                workspace_id=ws_id,
                order_date=_td(r.get("order_date")),
                order_id=_ts(r.get("order_id")),
                order_item_id=_ts(r.get("order_item_id")),
                sku_id=_ts(r.get("sku_id")),
                fulfilment_type=_ts(r.get("fulfilment_type")),
                channel_of_sale=_ts(r.get("channel_of_sale")),
                mode_of_payment=_ts(r.get("mode_of_payment")),
                shipping_zone=_ts(r.get("shipping_zone")),
                order_status=_ts(r.get("order_status")),
                gross_units=_ti(r.get("gross_units")),
                returned_cancelled_units=_ti(r.get("returned_cancelled_units")),
                rto_units=_ti(r.get("rto_units")),
                rvp_units=_ti(r.get("rvp_units")),
                cancelled_units=_ti(r.get("cancelled_units")),
                net_units=_ti(r.get("net_units")),
                sale_amount=_tf(r.get("sale_amount")),
                seller_burn_offer=_tf(r.get("seller_burn_offer")),
                customer_addons_amount=_tf(r.get("customer_addons_amount")),
                estimated_net_sales=_tf(r.get("estimated_net_sales")),
                accounted_net_sales=_tf(r.get("accounted_net_sales")),
                total_expenses=_tf(r.get("total_expenses")),
                commission_fee=_tf(r.get("commission_fee")),
                collection_fee=_tf(r.get("collection_fee")),
                fixed_fee=_tf(r.get("fixed_fee")),
                pick_and_pack_fee=_tf(r.get("pick_and_pack_fee")),
                forward_shipping_fee=_tf(r.get("forward_shipping_fee")),
                offer_adjustments=_tf(r.get("offer_adjustments")),
                reverse_shipping_fee=_tf(r.get("reverse_shipping_fee")),
                storage_fee=_tf(r.get("storage_fee")),
                recall_fee=_tf(r.get("recall_fee")),
                no_cost_emi_fee=_tf(r.get("no_cost_emi_fee")),
                product_cancellation_fee=_tf(r.get("product_cancellation_fee")),
                taxes_gst=_tf(r.get("taxes_gst")),
                taxes_tcs=_tf(r.get("taxes_tcs")),
                taxes_tds=_tf(r.get("taxes_tds")),
                rewards_other_benefits=_tf(r.get("rewards_other_benefits")),
                rewards=_tf(r.get("rewards")),
                order_spf=_tf(r.get("order_spf")),
                non_order_spf=_tf(r.get("non_order_spf")),
                bank_settlement_projected=_tf(r.get("bank_settlement_projected")),
                input_tax_credits=_tf(r.get("input_tax_credits")),
                net_earnings=_tf(r.get("net_earnings")),
                earnings_per_unit=_tf(r.get("earnings_per_unit")),
                net_margins_pct=_tf(r.get("net_margins_pct")),
                amount_settled=_tf(r.get("amount_settled")),
                amount_pending=_tf(r.get("amount_pending")),
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
        raise HTTPException(500, f"FK Order PNL ingest failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Ingest: Payment Report
# ---------------------------------------------------------------------------

@router.post("/ingest/payment-report")
def ingest_fk_payment_report(
    workspace_slug: str = Query("default"),
    replace: bool = Query(True),
    file: UploadFile = File(...),
):
    """Ingest Flipkart Payment Report (Orders sheet)."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        df = _read_excel_sheet(content, "Orders", skip_rows=3)
        if df.empty:
            return {"ok": True, "inserted": 0}

        col_names = [
            "neft_id", "neft_type", "payment_date", "bank_settlement_value",
            "input_gst_tcs_credits", "income_tax_tds_credits", "_b1",
            "order_id", "order_item_id", "sale_amount", "total_offer_amount",
            "my_share", "customer_addons_amount", "marketplace_fee", "taxes",
            "offer_adjustments", "protection_fund", "refund", "_b2",
            "tier", "commission_rate_pct", "commission", "fixed_fee", "collection_fee",
            "pick_and_pack_fee", "shipping_fee", "reverse_shipping_fee",
            "no_cost_emi_fee", "installation_fee", "tech_visit_fee",
            "uninstallation_fee", "customer_addons_recovery", "franchise_fee",
            "shopsy_marketing_fee", "product_cancellation_fee", "_b3",
            "tcs", "tds", "gst_on_mp_fees", "_b4",
            "offer_discount_settled", "item_gst_rate_pct", "discount_in_mp_fees",
            "gst_on_discount", "total_discount_mp_fee", "offer_adjustment_2", "_b5",
            "dead_weight", "lbh", "volumetric_weight", "chargeable_weight_source",
            "chargeable_weight_type", "chargeable_wt_slab", "shipping_zone", "_b6",
            "order_date", "dispatch_date", "fulfilment_type", "seller_sku",
            "quantity", "product_sub_category", "additional_info",
            "return_type", "shopsy_order", "item_return_status",
        ]
        df.columns = col_names[:len(df.columns)]
        df = df.dropna(subset=["order_id"])

        if replace:
            db.query(FlipkartPaymentReport).filter(FlipkartPaymentReport.workspace_id == ws_id).delete(synchronize_session=False)
            db.commit()

        objs = []
        for _, r in df.iterrows():
            obj = FlipkartPaymentReport(
                workspace_id=ws_id,
                neft_id=_ts(r.get("neft_id")),
                neft_type=_ts(r.get("neft_type")),
                payment_date=_td(r.get("payment_date")),
                bank_settlement_value=_tf(r.get("bank_settlement_value")),
                input_gst_tcs_credits=_tf(r.get("input_gst_tcs_credits")),
                income_tax_tds_credits=_tf(r.get("income_tax_tds_credits")),
                order_id=_ts(r.get("order_id")),
                order_item_id=_ts(r.get("order_item_id")),
                sale_amount=_tf(r.get("sale_amount")),
                total_offer_amount=_tf(r.get("total_offer_amount")),
                my_share=_tf(r.get("my_share")),
                customer_addons_amount=_tf(r.get("customer_addons_amount")),
                marketplace_fee=_tf(r.get("marketplace_fee")),
                taxes=_tf(r.get("taxes")),
                offer_adjustments=_tf(r.get("offer_adjustments")),
                protection_fund=_tf(r.get("protection_fund")),
                refund=_tf(r.get("refund")),
                tier=_ts(r.get("tier")),
                commission_rate_pct=_tf(r.get("commission_rate_pct")),
                commission=_tf(r.get("commission")),
                fixed_fee=_tf(r.get("fixed_fee")),
                collection_fee=_tf(r.get("collection_fee")),
                pick_and_pack_fee=_tf(r.get("pick_and_pack_fee")),
                shipping_fee=_tf(r.get("shipping_fee")),
                reverse_shipping_fee=_tf(r.get("reverse_shipping_fee")),
                no_cost_emi_fee=_tf(r.get("no_cost_emi_fee")),
                product_cancellation_fee=_tf(r.get("product_cancellation_fee")),
                tcs=_tf(r.get("tcs")),
                tds=_tf(r.get("tds")),
                gst_on_mp_fees=_tf(r.get("gst_on_mp_fees")),
                shipping_zone=_ts(r.get("shipping_zone")),
                chargeable_wt_slab=_ts(r.get("chargeable_wt_slab")),
                order_date=_td(r.get("order_date")),
                dispatch_date=_td(r.get("dispatch_date")),
                fulfilment_type=_ts(r.get("fulfilment_type")),
                seller_sku=_ts(r.get("seller_sku")),
                quantity=_ti(r.get("quantity")),
                product_sub_category=_ts(r.get("product_sub_category")),
                return_type=_ts(r.get("return_type")),
                item_return_status=_ts(r.get("item_return_status")),
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
        raise HTTPException(500, f"FK Payment report ingest failed: {e}")
    finally:
        db.close()


# ===========================================================================
# ANALYTICS ENDPOINTS
# ===========================================================================

@router.get("/available-months")
def fk_available_months(workspace_slug: str = Query("default")):
    """Get list of months with Flipkart data."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        rows = db.query(
            FlipkartSkuPnl.report_month,
            func.count(FlipkartSkuPnl.id).label("skus"),
            func.coalesce(func.sum(FlipkartSkuPnl.accounted_net_sales), 0).label("revenue"),
        ).filter(
            FlipkartSkuPnl.workspace_id == ws_id,
            FlipkartSkuPnl.report_month.isnot(None),
        ).group_by(FlipkartSkuPnl.report_month).order_by(FlipkartSkuPnl.report_month).all()

        return {
            "months": [{"month": r.report_month, "orders": r.skus, "revenue": round(r.revenue, 2)} for r in rows]
        }
    finally:
        db.close()


@router.get("/summary")
def fk_recon_summary(workspace_slug: str = Query("default"), month: Optional[str] = Query(None)):
    """Flipkart reconciliation summary from SKU-level PNL."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        q = db.query(
            func.count(FlipkartSkuPnl.id).label("sku_count"),
            func.coalesce(func.sum(FlipkartSkuPnl.gross_units), 0).label("gross_units"),
            func.coalesce(func.sum(FlipkartSkuPnl.returned_cancelled_units), 0).label("returned_units"),
            func.coalesce(func.sum(FlipkartSkuPnl.net_units), 0).label("net_units"),
            func.coalesce(func.sum(FlipkartSkuPnl.accounted_net_sales), 0).label("net_sales"),
            func.coalesce(func.sum(FlipkartSkuPnl.total_expenses), 0).label("total_expenses"),
            func.coalesce(func.sum(FlipkartSkuPnl.commission_fee), 0).label("commission"),
            func.coalesce(func.sum(FlipkartSkuPnl.collection_fee), 0).label("collection_fee"),
            func.coalesce(func.sum(FlipkartSkuPnl.fixed_fee), 0).label("fixed_fee"),
            func.coalesce(func.sum(FlipkartSkuPnl.pick_and_pack_fee), 0).label("pick_and_pack"),
            func.coalesce(func.sum(FlipkartSkuPnl.forward_shipping_fee), 0).label("fwd_shipping"),
            func.coalesce(func.sum(FlipkartSkuPnl.reverse_shipping_fee), 0).label("rev_shipping"),
            func.coalesce(func.sum(FlipkartSkuPnl.offer_adjustments), 0).label("offer_adj"),
            func.coalesce(func.sum(FlipkartSkuPnl.taxes_gst), 0).label("gst"),
            func.coalesce(func.sum(FlipkartSkuPnl.taxes_tcs), 0).label("tcs"),
            func.coalesce(func.sum(FlipkartSkuPnl.taxes_tds), 0).label("tds"),
            func.coalesce(func.sum(FlipkartSkuPnl.rewards_other_benefits), 0).label("rewards"),
            func.coalesce(func.sum(FlipkartSkuPnl.net_earnings), 0).label("net_earnings"),
            func.coalesce(func.sum(FlipkartSkuPnl.amount_settled), 0).label("settled"),
            func.coalesce(func.sum(FlipkartSkuPnl.amount_pending), 0).label("pending"),
        ).filter(FlipkartSkuPnl.workspace_id == ws_id)
        if month:
            q = q.filter(FlipkartSkuPnl.report_month == month)
        q = q.first()

        return {
            "workspace_slug": workspace_slug,
            "sku_count": q.sku_count,
            "units": {
                "gross": q.gross_units,
                "returned": q.returned_units,
                "net": q.net_units,
            },
            "net_sales": round(q.net_sales, 2),
            "expenses": {
                "total": round(abs(q.total_expenses), 2),
                "commission": round(abs(q.commission), 2),
                "collection_fee": round(abs(q.collection_fee), 2),
                "fixed_fee": round(abs(q.fixed_fee), 2),
                "pick_and_pack": round(abs(q.pick_and_pack), 2),
                "forward_shipping": round(abs(q.fwd_shipping), 2),
                "reverse_shipping": round(abs(q.rev_shipping), 2),
                "offer_adjustments": round(abs(q.offer_adj), 2),
                "gst": round(abs(q.gst), 2),
                "tcs": round(abs(q.tcs), 2),
                "tds": round(abs(q.tds), 2),
            },
            "rewards": round(q.rewards, 2),
            "net_earnings": round(q.net_earnings, 2),
            "settlement": {
                "settled": round(q.settled, 2),
                "pending": round(q.pending, 2),
            },
        }
    finally:
        db.close()


@router.get("/sku-pnl")
def fk_sku_pnl(
    workspace_slug: str = Query("default"),
    top_n: int = Query(100),
    sort_by: str = Query("net_earnings"),
    sort_dir: str = Query("desc"),
    month: Optional[str] = Query(None),
):
    """Flipkart SKU-level P&L from ingested data."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        q = db.query(FlipkartSkuPnl).filter(FlipkartSkuPnl.workspace_id == ws_id)
        if month:
            q = q.filter(FlipkartSkuPnl.report_month == month)
        rows = q.all()

        results = []
        for r in rows:
            results.append({
                "sku_id": r.sku_id,
                "sku_name": r.sku_name,
                "gross_units": r.gross_units or 0,
                "returned_units": r.returned_cancelled_units or 0,
                "rto_units": r.rto_units or 0,
                "rvp_units": r.rvp_units or 0,
                "cancelled_units": r.cancelled_units or 0,
                "net_units": r.net_units or 0,
                "return_pct": round((r.returned_cancelled_units or 0) / (r.gross_units or 1) * 100, 1),
                "net_sales": round(r.accounted_net_sales or 0, 2),
                "total_expenses": round(abs(r.total_expenses or 0), 2),
                "commission": round(abs(r.commission_fee or 0), 2),
                "collection_fee": round(abs(r.collection_fee or 0), 2),
                "fixed_fee": round(abs(r.fixed_fee or 0), 2),
                "forward_shipping": round(abs(r.forward_shipping_fee or 0), 2),
                "reverse_shipping": round(abs(r.reverse_shipping_fee or 0), 2),
                "pick_and_pack": round(abs(r.pick_and_pack_fee or 0), 2),
                "offer_adjustments": round(abs(r.offer_adjustments or 0), 2),
                "gst": round(abs(r.taxes_gst or 0), 2),
                "tcs": round(abs(r.taxes_tcs or 0), 2),
                "tds": round(abs(r.taxes_tds or 0), 2),
                "rewards": round(r.rewards_other_benefits or 0, 2),
                "net_earnings": round(r.net_earnings or 0, 2),
                "margin_pct": round(r.net_margins_pct or 0, 1) if r.net_margins_pct else (
                    round((r.net_earnings or 0) / (r.accounted_net_sales or 1) * 100, 1) if r.accounted_net_sales else 0
                ),
                "amount_settled": round(r.amount_settled or 0, 2),
                "amount_pending": round(r.amount_pending or 0, 2),
                "asp": round((r.accounted_net_sales or 0) / (r.net_units or 1), 2) if r.net_units else 0,
            })

        reverse = sort_dir.lower() != "asc"
        results.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=reverse)

        totals = {
            "gross_units": sum(r["gross_units"] for r in results),
            "returned_units": sum(r["returned_units"] for r in results),
            "net_units": sum(r["net_units"] for r in results),
            "net_sales": round(sum(r["net_sales"] for r in results), 2),
            "total_expenses": round(sum(r["total_expenses"] for r in results), 2),
            "commission": round(sum(r["commission"] for r in results), 2),
            "reverse_shipping": round(sum(r["reverse_shipping"] for r in results), 2),
            "net_earnings": round(sum(r["net_earnings"] for r in results), 2),
        }

        return {
            "workspace_slug": workspace_slug,
            "total_skus": len(results),
            "totals": totals,
            "rows": results[:top_n],
        }
    finally:
        db.close()


@router.get("/sku-pnl/download")
def fk_sku_pnl_download(
    workspace_slug: str = Query("default"),
    sort_by: str = Query("net_earnings"),
    sort_dir: str = Query("desc"),
):
    """Download Flipkart SKU P&L as CSV."""
    import csv as csv_mod
    data = fk_sku_pnl(workspace_slug=workspace_slug, top_n=9999, sort_by=sort_by, sort_dir=sort_dir)
    rows = data["rows"]

    output = io.StringIO()
    writer = csv_mod.writer(output)
    writer.writerow([
        "SKU ID", "SKU Name", "Gross Units", "Returns", "RTO", "RVP", "Cancelled",
        "Net Units", "Return%", "Net Sales", "Total Expenses", "Commission",
        "Collection Fee", "Fixed Fee", "Fwd Shipping", "Rev Shipping", "Pick&Pack",
        "GST", "TCS", "TDS", "Rewards", "Net Earnings", "Margin%", "ASP",
        "Settled", "Pending"
    ])
    for r in rows:
        writer.writerow([
            r["sku_id"], r.get("sku_name", ""), r["gross_units"], r["returned_units"],
            r["rto_units"], r["rvp_units"], r["cancelled_units"], r["net_units"],
            r["return_pct"], r["net_sales"], r["total_expenses"], r["commission"],
            r["collection_fee"], r["fixed_fee"], r["forward_shipping"], r["reverse_shipping"],
            r["pick_and_pack"], r["gst"], r["tcs"], r["tds"], r["rewards"],
            r["net_earnings"], r["margin_pct"], r["asp"], r["amount_settled"], r["amount_pending"],
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=flipkart_sku_pnl_{workspace_slug}.csv"},
    )


# ---------------------------------------------------------------------------
# DELETE: Clear all Flipkart recon data
# ---------------------------------------------------------------------------

@router.delete("/clear-all")
def clear_fk_recon(workspace_slug: str = Query("default")):
    """Delete ALL Flipkart reconciliation data for a workspace."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        counts = {}
        for model, name in [
            (FlipkartSkuPnl, "sku_pnl"),
            (FlipkartOrderPnl, "order_pnl"),
            (FlipkartPaymentReport, "payment_report"),
        ]:
            c = db.query(model).filter(model.workspace_id == ws_id).delete(synchronize_session=False)
            counts[name] = c
        db.commit()
        return {"ok": True, "deleted": counts, "workspace_slug": workspace_slug}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Clear failed: {e}")
    finally:
        db.close()