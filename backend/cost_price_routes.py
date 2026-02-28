# backend/cost_price_routes.py
# Cost Price Management — download template, upload costs, true P&L

from __future__ import annotations

import io
import csv as csv_mod
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func

from backend.db import SessionLocal, resolve_workspace_id
from backend.cost_price_models import SkuCostPrice

router = APIRouter(prefix="/db/recon/cost-price", tags=["cost-price"])


# ---------------------------------------------------------------------------
# Download: SKU Template (pre-filled with seller SKU codes)
# ---------------------------------------------------------------------------

@router.get("/template")
def download_cost_template(
    workspace_slug: str = Query("default"),
    platform: str = Query("all", description="myntra, flipkart, or all"),
):
    """
    Download Excel-ready CSV template pre-filled with all unique seller SKU codes.
    Two columns: seller_sku_code (filled), cost_price (empty for user to fill).
    """
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        skus = set()

        # Collect SKUs from Myntra
        if platform in ("myntra", "all"):
            try:
                from backend.reconciliation_models import MyntraPgForward, MyntraSkuMap
                # First try mapped seller SKUs
                mapped = db.query(
                    MyntraSkuMap.seller_sku_code,
                    MyntraSkuMap.sku_code,
                    MyntraSkuMap.style_name,
                    MyntraSkuMap.brand,
                ).filter(
                    MyntraSkuMap.workspace_id == ws_id,
                    MyntraSkuMap.seller_sku_code.isnot(None),
                ).all()

                for m in mapped:
                    skus.add((m.seller_sku_code, m.style_name or "", m.brand or "", "myntra"))

                # Also get unmapped Myntra SKUs
                myntra_skus = db.query(
                    MyntraPgForward.sku_code,
                    MyntraPgForward.brand,
                    MyntraPgForward.article_type,
                ).filter(
                    MyntraPgForward.workspace_id == ws_id
                ).distinct().all()

                mapped_codes = {m.sku_code for m in mapped}
                for ms in myntra_skus:
                    if ms.sku_code not in mapped_codes:
                        skus.add((ms.sku_code, ms.article_type or "", ms.brand or "", "myntra"))
            except ImportError:
                pass

        # Collect SKUs from Flipkart
        if platform in ("flipkart", "all"):
            try:
                from backend.flipkart_recon_models import FlipkartSkuPnl
                fk_skus = db.query(
                    FlipkartSkuPnl.sku_id,
                    FlipkartSkuPnl.sku_name,
                ).filter(
                    FlipkartSkuPnl.workspace_id == ws_id
                ).distinct().all()

                for fs in fk_skus:
                    skus.add((fs.sku_id, fs.sku_name or "", "", "flipkart"))
            except ImportError:
                pass

        # Also include existing cost prices (so user can update them)
        existing = {}
        costs = db.query(SkuCostPrice).filter(SkuCostPrice.workspace_id == ws_id).all()
        for c in costs:
            existing[c.seller_sku_code] = c.cost_price

        # Build CSV
        output = io.StringIO()
        writer = csv_mod.writer(output)
        writer.writerow(["seller_sku_code", "cost_price", "sku_name", "brand", "platform"])

        for sku, name, brand, platform_name in sorted(skus):
            cp = existing.get(sku, "")
            writer.writerow([sku, cp, name, brand, platform_name])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=cost_price_template_{workspace_slug}.csv"},
        )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Upload: Cost Prices
# ---------------------------------------------------------------------------

@router.post("/upload")
def upload_cost_prices(
    workspace_slug: str = Query("default"),
    file: UploadFile = File(...),
):
    """Upload filled cost price CSV/Excel. Expects columns: seller_sku_code, cost_price."""
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        content = file.file.read()
        if not content:
            raise HTTPException(400, "Empty file")

        # Try CSV first, then Excel
        try:
            df = pd.read_csv(io.BytesIO(content))
        except Exception:
            try:
                df = pd.read_excel(io.BytesIO(content))
            except Exception:
                raise HTTPException(400, "Could not parse file. Upload CSV or Excel.")

        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        if "seller_sku_code" not in df.columns:
            # Try alternative names
            for alt in ["sku", "sku_id", "sku_code", "seller_sku"]:
                if alt in df.columns:
                    df = df.rename(columns={alt: "seller_sku_code"})
                    break

        if "seller_sku_code" not in df.columns:
            raise HTTPException(400, "Missing column: seller_sku_code")

        if "cost_price" not in df.columns:
            for alt in ["cost", "cp", "purchase_price", "buying_price"]:
                if alt in df.columns:
                    df = df.rename(columns={alt: "cost_price"})
                    break

        if "cost_price" not in df.columns:
            raise HTTPException(400, "Missing column: cost_price")

        # Filter valid rows
        df = df.dropna(subset=["seller_sku_code", "cost_price"])
        df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
        df = df.dropna(subset=["cost_price"])
        df = df[df["cost_price"] > 0]

        if df.empty:
            return {"ok": True, "inserted": 0, "message": "No valid cost prices found"}

        # Upsert: update existing, insert new (don't wipe other platform's costs)
        updated = 0
        inserted = 0
        for _, r in df.iterrows():
            sku = str(r["seller_sku_code"]).strip()
            cp = float(r["cost_price"])
            existing = db.query(SkuCostPrice).filter(
                SkuCostPrice.workspace_id == ws_id,
                SkuCostPrice.seller_sku_code == sku,
            ).first()

            if existing:
                existing.cost_price = cp
                existing.updated_at = datetime.utcnow()
                if pd.notna(r.get("sku_name")) and str(r.get("sku_name", "")).strip():
                    existing.sku_name = str(r["sku_name"]).strip()
                if pd.notna(r.get("brand")) and str(r.get("brand", "")).strip():
                    existing.brand = str(r["brand"]).strip()
                updated += 1
            else:
                obj = SkuCostPrice(
                    workspace_id=ws_id,
                    seller_sku_code=sku,
                    cost_price=cp,
                    sku_name=str(r.get("sku_name", "")).strip() if pd.notna(r.get("sku_name")) else None,
                    brand=str(r.get("brand", "")).strip() if pd.notna(r.get("brand")) else None,
                    category=str(r.get("category", "")).strip() if pd.notna(r.get("category")) else None,
                    updated_at=datetime.utcnow(),
                )
                db.add(obj)
                inserted += 1

        db.commit()
        return {"ok": True, "inserted": inserted, "updated": updated, "total": inserted + updated, "workspace_slug": workspace_slug}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Cost price upload failed: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Analytics: True P&L (with cost price)
# ---------------------------------------------------------------------------

@router.get("/true-pnl")
def true_pnl(
    workspace_slug: str = Query("default"),
    platform: str = Query("all", description="myntra, flipkart, or all"),
    sort_by: str = Query("true_profit"),
    sort_dir: str = Query("desc"),
):
    """
    True P&L per SKU = Marketplace Earnings - (Cost Price × Units Sold).
    Combines data from Myntra and/or Flipkart with cost prices.
    """
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # Load cost prices
        cost_map = {}
        costs = db.query(SkuCostPrice).filter(SkuCostPrice.workspace_id == ws_id).all()
        for c in costs:
            cost_map[c.seller_sku_code] = c.cost_price

        results = []

        # Flipkart data
        if platform in ("flipkart", "all"):
            try:
                from backend.flipkart_recon_models import FlipkartSkuPnl
                fk_rows = db.query(FlipkartSkuPnl).filter(FlipkartSkuPnl.workspace_id == ws_id).all()
                for r in fk_rows:
                    sku = r.sku_id
                    cp = cost_map.get(sku)
                    net_units = r.net_units or 0
                    net_sales = r.accounted_net_sales or 0
                    marketplace_earnings = r.net_earnings or 0
                    total_expenses = abs(r.total_expenses or 0)

                    cogs = (cp * net_units) if cp and net_units else None
                    true_profit = (marketplace_earnings - cogs) if cogs is not None else None
                    true_margin = (true_profit / net_sales * 100) if true_profit is not None and net_sales else None

                    results.append({
                        "platform": "Flipkart",
                        "seller_sku_code": sku,
                        "sku_name": r.sku_name,
                        "gross_units": r.gross_units or 0,
                        "returned_units": r.returned_cancelled_units or 0,
                        "net_units": net_units,
                        "return_pct": round((r.returned_cancelled_units or 0) / max(r.gross_units or 1, 1) * 100, 1),
                        "net_sales": round(net_sales, 2),
                        "marketplace_expenses": round(total_expenses, 2),
                        "marketplace_earnings": round(marketplace_earnings, 2),
                        "cost_price": cp,
                        "cogs": round(cogs, 2) if cogs is not None else None,
                        "true_profit": round(true_profit, 2) if true_profit is not None else None,
                        "true_margin_pct": round(true_margin, 1) if true_margin is not None else None,
                        "asp": round(net_sales / max(net_units, 1), 2) if net_units else 0,
                    })
            except ImportError:
                pass

        # Myntra data
        if platform in ("myntra", "all"):
            try:
                from backend.reconciliation_models import MyntraPgForward, MyntraPgReverse, MyntraSkuMap

                # SKU map for seller codes
                sku_map = {}
                map_rows = db.query(MyntraSkuMap).filter(MyntraSkuMap.workspace_id == ws_id).all()
                for m in map_rows:
                    sku_map[m.sku_code] = m.seller_sku_code

                # Forward by SKU
                fw_q = db.query(
                    MyntraPgForward.sku_code,
                    MyntraPgForward.brand,
                    MyntraPgForward.article_type,
                    func.count(MyntraPgForward.id).label("fw_orders"),
                    func.coalesce(func.sum(MyntraPgForward.seller_product_amount), 0).label("fw_revenue"),
                    func.coalesce(func.sum(MyntraPgForward.total_commission), 0).label("fw_commission"),
                    func.coalesce(func.sum(MyntraPgForward.total_logistics_deduction), 0).label("fw_logistics"),
                    func.coalesce(func.sum(MyntraPgForward.tcs_amount), 0).label("fw_tcs"),
                    func.coalesce(func.sum(MyntraPgForward.tds_amount), 0).label("fw_tds"),
                ).filter(MyntraPgForward.workspace_id == ws_id).group_by(
                    MyntraPgForward.sku_code, MyntraPgForward.brand, MyntraPgForward.article_type
                ).all()

                # Reverse by SKU
                rv_map = {}
                rv_q = db.query(
                    MyntraPgReverse.sku_code,
                    func.count(MyntraPgReverse.id).label("rv_orders"),
                    func.coalesce(func.sum(MyntraPgReverse.seller_product_amount), 0).label("rv_amount"),
                ).filter(MyntraPgReverse.workspace_id == ws_id).group_by(MyntraPgReverse.sku_code).all()

                for rv in rv_q:
                    rv_map[rv.sku_code] = {"rv_orders": rv.rv_orders, "rv_amount": rv.rv_amount}

                for fw in fw_q:
                    rv = rv_map.get(fw.sku_code, {})
                    seller_sku = sku_map.get(fw.sku_code, fw.sku_code)
                    rv_orders = rv.get("rv_orders", 0)

                    gross_revenue = fw.fw_revenue
                    deductions = abs(fw.fw_commission) + abs(fw.fw_logistics) + abs(fw.fw_tcs) + abs(fw.fw_tds)
                    rv_amount = abs(rv.get("rv_amount", 0))
                    marketplace_earnings = gross_revenue - deductions - rv_amount
                    net_units = fw.fw_orders - rv_orders

                    cp = cost_map.get(seller_sku)
                    cogs = (cp * net_units) if cp and net_units > 0 else None
                    true_profit = (marketplace_earnings - cogs) if cogs is not None else None
                    true_margin = (true_profit / gross_revenue * 100) if true_profit is not None and gross_revenue else None

                    results.append({
                        "platform": "Myntra",
                        "seller_sku_code": seller_sku,
                        "sku_name": fw.article_type,
                        "gross_units": fw.fw_orders,
                        "returned_units": rv_orders,
                        "net_units": net_units,
                        "return_pct": round(rv_orders / max(fw.fw_orders, 1) * 100, 1),
                        "net_sales": round(gross_revenue, 2),
                        "marketplace_expenses": round(deductions, 2),
                        "marketplace_earnings": round(marketplace_earnings, 2),
                        "cost_price": cp,
                        "cogs": round(cogs, 2) if cogs is not None else None,
                        "true_profit": round(true_profit, 2) if true_profit is not None else None,
                        "true_margin_pct": round(true_margin, 1) if true_margin is not None else None,
                        "asp": round(gross_revenue / max(fw.fw_orders, 1), 2),
                    })
            except ImportError:
                pass

        # Sort
        reverse = sort_dir.lower() != "asc"
        results.sort(key=lambda x: x.get(sort_by) if x.get(sort_by) is not None else -999999, reverse=reverse)

        # Totals (only for items with cost price)
        costed = [r for r in results if r["true_profit"] is not None]
        uncosted = len(results) - len(costed)

        totals = {
            "total_skus": len(results),
            "costed_skus": len(costed),
            "uncosted_skus": uncosted,
            "net_sales": round(sum(r["net_sales"] for r in results), 2),
            "marketplace_earnings": round(sum(r["marketplace_earnings"] for r in results), 2),
            "total_cogs": round(sum(r["cogs"] for r in costed), 2) if costed else 0,
            "total_true_profit": round(sum(r["true_profit"] for r in costed), 2) if costed else 0,
        }

        return {
            "workspace_slug": workspace_slug,
            "platform": platform,
            "totals": totals,
            "rows": results,
        }
    finally:
        db.close()


@router.get("/true-pnl/download")
def true_pnl_download(
    workspace_slug: str = Query("default"),
    platform: str = Query("all"),
    sort_by: str = Query("true_profit"),
    sort_dir: str = Query("desc"),
):
    """Download True P&L as CSV."""
    data = true_pnl(workspace_slug=workspace_slug, platform=platform, sort_by=sort_by, sort_dir=sort_dir)
    rows = data["rows"]

    output = io.StringIO()
    writer = csv_mod.writer(output)
    writer.writerow([
        "Platform", "Seller SKU", "SKU Name", "Gross Units", "Returns", "Net Units",
        "Return%", "Net Sales", "MP Expenses", "MP Earnings", "Cost Price",
        "COGS", "True Profit", "True Margin%", "ASP"
    ])
    for r in rows:
        writer.writerow([
            r["platform"], r["seller_sku_code"], r.get("sku_name", ""),
            r["gross_units"], r["returned_units"], r["net_units"],
            r["return_pct"], r["net_sales"], r["marketplace_expenses"],
            r["marketplace_earnings"], r.get("cost_price", ""),
            r.get("cogs", ""), r.get("true_profit", ""), r.get("true_margin_pct", ""),
            r["asp"],
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=true_pnl_{workspace_slug}.csv"},
    )