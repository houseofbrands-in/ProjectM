# backend/main.py

from __future__ import annotations

import io
import json
import re
import uuid
import ast
from sqlalchemy import or_


from datetime import date, datetime, time, timedelta
from typing import Optional

from datetime import date
from sqlalchemy import func, cast, Date
from backend.models import StyleMonthly
from sqlalchemy import exists, and_

from sqlalchemy import cast, String
from sqlalchemy import select

from sqlalchemy.exc import IntegrityError
from sqlalchemy import case, text, and_
from sqlalchemy.sql import select

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import case, func, text, cast
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy import and_, or_, Float
from sqlalchemy import cast, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from backend.db import SessionLocal, Base, engine
from backend.models import CatalogRaw, ReturnsRaw, SalesRaw, Workspace, MyntraWeeklyPerfRaw, StockRaw
# ensure tables exist (simple dev-mode migration)
Base.metadata.create_all(bind=engine)

from datetime import datetime, timedelta
from sqlalchemy import and_
from backend.models import CatalogRaw
from sqlalchemy.dialects.postgresql import insert

from pydantic import BaseModel, Field
from backend.models import FlipkartTrafficRaw

from dateutil.relativedelta import relativedelta

from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import JSONB

app = FastAPI(title="Project M API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_origin_regex=r"^https:\/\/.*\.(github\.dev|app\.github\.dev)$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Return Reason Normalizer
# =========================

RETURN_REASON_MAP = {
    "SIZE_TOO_BIG": [
        "size too big",
        "size is too large",
    ],
    "SIZE_TOO_SMALL": [
        "size too small",
        "size is too small",
    ],
    "SIZE_DIFFERENT": [
        "size is different",
    ],
    "FIT_NOT_LIKED": [
        "i did not like the fit",
    ],
    "QUALITY_DEFECT_DAMAGE": [
        "product was defective",
        "defective product was delivered",
        "product was damaged",
        "received a poor quality product",
        "product looked old",
        "product was dirty and had stains",
    ],
    "WRONG_PRODUCT_DELIVERED": [
        "received a completely different product",
        "received a different product",
        "different product was delivered",
    ],
    "NOT_AS_EXPECTED_COLOR_IMAGE": [
        "color is different",
        "product image was better than the actual product",
    ],
    "FOUND_BETTER_PRICE": [
        "found a better price elsewhere",
        "found a better price on myntra",
    ],
    "DELIVERY_DELAYED": [
        "delivery was delayed",
    ],
    "CUSTOMER_CHANGED_MIND": [
    "i do not need it anymore",
    "it did not look good on me",
    ],
    "GENERIC_OTHER": [
        "generic return reason",
    ],
}

# build reverse lookup once
_REASON_LOOKUP = {}
for clean, arr in RETURN_REASON_MAP.items():
    for raw in arr:
        _REASON_LOOKUP[raw.strip().lower()] = clean


def clean_return_reason(raw_reason: str | None, return_type: str | None = None) -> str:
    s = (raw_reason or "").strip().lower()
    rt = (return_type or "").strip().upper()

    if not s:
        # Myntra RTO often has no reason in file
        if rt == "RTO":
            return "RTO_NO_REASON"
        return "UNKNOWN"

    return _REASON_LOOKUP.get(s, "OTHER")


def heatmap_reason_key(raw_reason: str | None, return_type: str | None = None, portal: str | None = None) -> str:
    """Heatmap reason label depends on portal.

    - Myntra: use existing clean_return_reason bucketing.
    - Flipkart: use return_sub_reason/return_reason codes as-is (normalized).
    """
    p = _portal_norm(portal)
    rt = (return_type or "").strip().upper()
    s = (raw_reason or "").strip()

    if p == "flipkart":
        if not s:
            return "RTO_NO_REASON" if rt == "RTO" else "UNKNOWN"
        return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").upper()

    return clean_return_reason(raw_reason, return_type)

import sqlalchemy  # add near imports if not already

def _reason_bucket_expr(reason_text_expr):
    whens = []
    for bucket, phrases in RETURN_REASON_MAP.items():
        for p in phrases:
            whens.append((reason_text_expr == p, bucket))

    if not whens:
        return "GENERIC_OTHER"

    major = int(sqlalchemy.__version__.split(".")[0])
    if major >= 2:
        # SQLAlchemy 2.x expects positional whens
        return case(*whens, else_="GENERIC_OTHER")
    else:
        # SQLAlchemy 1.4 accepts list-of-tuples
        return case(whens, else_="GENERIC_OTHER")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def require_col(df: pd.DataFrame, name: str) -> str:
    """Require an exact header match ignoring spaces/case/symbols."""
    target = _norm(name)
    for c in df.columns:
        if _norm(c) == target:
            return c
    raise KeyError(f"Missing required column: '{name}'. Found: {list(df.columns)[:30]}...")


def optional_col(df: pd.DataFrame, name: str) -> Optional[str]:
    target = _norm(name)
    for c in df.columns:
        if _norm(c) == target:
            return c
    return None


def parse_dt_series(s: pd.Series) -> pd.Series:
    """Parse date/datetime. Handles dd-mm, yyyy-mm, timestamps etc."""
    raw = s.astype(str)
    d1 = pd.to_datetime(raw, errors="coerce", dayfirst=True)
    d10 = pd.to_datetime(raw.str.slice(0, 10), errors="coerce", dayfirst=True)
    return d1.fillna(d10)

def parse_date_any(x) -> Optional[datetime]:
    """
    Parse a single date/datetime value coming from CSV/Excel.
    Returns a Python datetime (naive) or None.
    """
    if x is None:
        return None

    # already date/datetime
    if isinstance(x, datetime):
        return x.replace(tzinfo=None) if x.tzinfo else x
    if isinstance(x, date):
        return datetime.combine(x, time.min)

    s = str(x).strip()
    if not s:
        return None

    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            dt = pd.to_datetime(s[:10], errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None

        py_dt = dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt
        if isinstance(py_dt, datetime) and py_dt.tzinfo is not None:
            py_dt = py_dt.replace(tzinfo=None)
        return py_dt
    except Exception:
        return None


def normalize_return_type(x: str) -> str:
    t = str(x).strip().lower()
    if "rto" in t:
        return "RTO"
    if "return" in t:
        return "Return"
    if t == "":
        return "(Unknown)"
    return t


def resolve_workspace_id(db, slug_or_id: Optional[str]) -> uuid.UUID:
    """Accepts workspace_slug, ensures it exists, returns UUID."""
    s = (slug_or_id or "default").strip()
    # UUID passed
    try:
        return uuid.UUID(s)
    except Exception:
        pass

    slug = s.lower()

    row = (
        db.execute(
            text("SELECT id FROM workspaces WHERE slug=:slug LIMIT 1"),
            {"slug": slug},
        )
        .mappings()
        .first()
    )
    if row and row.get("id"):
        return row["id"]

    created = (
        db.execute(
            text(
                """
                INSERT INTO workspaces (id, slug, name, created_at)
                VALUES (gen_random_uuid(), :slug, :name, NOW())
                RETURNING id
                """
            ),
            {"slug": slug, "name": ("Default Workspace" if slug == "default" else slug.title())},
        )
        .mappings()
        .first()
    )
    db.commit()
    return created["id"]

def _portal_norm(portal: str | None) -> str:
    p = (portal or "").strip().lower()
    return p or "myntra"

def _fk_prefix(ws_slug: str) -> str:
    return f"fk:{ws_slug}:"

def _apply_portal_sales(q, ws_slug: str, portal: str | None):
    p = _portal_norm(portal)
    if p == "flipkart":
        return q.filter(SalesRaw.order_line_id.like("fk:%"))
    if p == "myntra":
        return q.filter(sqlalchemy.not_(SalesRaw.order_line_id.like("fk:%")))
    return q


def _apply_portal_returns(q, ws_slug: str, portal: str | None):
    p = _portal_norm(portal)
    if p == "flipkart":
        return q.filter(ReturnsRaw.order_line_id.like("fk:%"))
    if p == "myntra":
        return q.filter(sqlalchemy.not_(ReturnsRaw.order_line_id.like("fk:%")))
    return q


def _apply_portal_catalog(q, portal: str | None):
    p = _portal_norm(portal)
    # Flipkart styles are stored like "fk:<FSN>"
    if p == "flipkart":
        return q.filter(func.lower(func.trim(CatalogRaw.style_key)).like("fk:%"))
    if p == "myntra":
        return q.filter(sqlalchemy.not_(func.lower(func.trim(CatalogRaw.style_key)).like("fk:%")))
    return q


def sales_orders_expr():
    """Sales has no qty column => 1 row = 1 unit/order."""
    return func.count(SalesRaw.id)


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/")
def home():
    return {"status": "ok", "message": "Project M backend is running"}


@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------------------------------------------------------------
# Workspaces (for dropdown)
# -----------------------------------------------------------------------------
@app.get("/db/workspaces")
def db_list_workspaces():
    db = SessionLocal()
    try:
        rows = db.query(Workspace).order_by(Workspace.name.asc()).all()
        return [{"id": str(r.id), "slug": r.slug, "name": r.name} for r in rows]
    finally:
        db.close()

# -----------------------------------------------------------------------------
# Workspaces (for dropdown)
# -----------------------------------------------------------------------------
    
    try:
        rows = db.query(Workspace).order_by(Workspace.name.asc()).all()
        return [{"id": str(r.id), "slug": r.slug, "name": r.name} for r in rows]
    finally:
        db.close()


class WorkspaceCreate(BaseModel):
    slug: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=128)




@app.post("/db/workspaces")
def db_create_workspace(payload: WorkspaceCreate):
    db = SessionLocal()
    try:
        slug = (payload.slug or "").strip().lower()
        name = (payload.name or "").strip()

        if not slug or not name:
            raise HTTPException(status_code=400, detail="slug and name are required")

        if not re.match(r"^[a-z0-9][a-z0-9_-]{0,63}$", slug):
            raise HTTPException(
                status_code=400,
                detail="Invalid slug. Use lowercase letters/numbers and - or _ (max 64 chars). Example: rekha_maniyar",
            )

        # slug already exists?
        exists_ws = db.query(Workspace).filter(Workspace.slug == slug).first()
        if exists_ws:
            raise HTTPException(status_code=409, detail=f"Workspace already exists: {slug}")

        ws = Workspace(slug=slug, name=name)

        # ✅ IMPORTANT: created_at is NOT NULL in DB, so set it here
        if hasattr(ws, "created_at") and getattr(ws, "created_at", None) is None:
            ws.created_at = datetime.utcnow()

        db.add(ws)

        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise HTTPException(status_code=409, detail=f"Workspace already exists: {slug}")

        db.refresh(ws)
        return {"id": str(ws.id), "slug": ws.slug, "name": ws.name}

    finally:
        db.close()


@app.delete("/db/workspaces/{workspace_slug}")
def db_delete_workspace(
    workspace_slug: str,
    force: bool = Query(False, description="If true, deletes ALL data for this workspace before deleting workspace"),
):
    db = SessionLocal()
    try:
        slug = (workspace_slug or "").strip().lower()
        ws = db.query(Workspace).filter(Workspace.slug == slug).first()
        if not ws:
            raise HTTPException(status_code=404, detail=f"Workspace not found: {slug}")

        ws_id = ws.id

        # Count dependent rows (safe delete check)
        counts = {
            "sales_raw": int(db.query(func.count(SalesRaw.id)).filter(SalesRaw.workspace_id == ws_id).scalar() or 0),
            "returns_raw": int(db.query(func.count(ReturnsRaw.id)).filter(ReturnsRaw.workspace_id == ws_id).scalar() or 0),
            "catalog_raw": int(db.query(func.count(CatalogRaw.style_key)).filter(CatalogRaw.workspace_id == ws_id).scalar() or 0),
            "stock_raw": int(db.query(func.count(StockRaw.id)).filter(StockRaw.workspace_id == ws_id).scalar() or 0),
            "weekly_perf_raw": int(db.query(func.count(MyntraWeeklyPerfRaw.id)).filter(MyntraWeeklyPerfRaw.workspace_id == ws_id).scalar() or 0),
            "style_monthly": int(db.query(func.count(StyleMonthly.id)).filter(StyleMonthly.workspace_id == ws_id).scalar() or 0),
        }

        total = sum(counts.values())
        if total > 0 and not force:
            # IMPORTANT: keep detail as STRING (frontend error handler expects string)
            raise HTTPException(
                status_code=409,
                detail=f"Workspace has data and cannot be deleted without force=true. counts={json.dumps(counts)}",
            )

        # Force delete: delete children first (no FK cascade assumed)
        if total > 0:
            db.query(StyleMonthly).filter(StyleMonthly.workspace_id == ws_id).delete(synchronize_session=False)
            db.query(MyntraWeeklyPerfRaw).filter(MyntraWeeklyPerfRaw.workspace_id == ws_id).delete(synchronize_session=False)
            db.query(StockRaw).filter(StockRaw.workspace_id == ws_id).delete(synchronize_session=False)
            db.query(CatalogRaw).filter(CatalogRaw.workspace_id == ws_id).delete(synchronize_session=False)
            db.query(ReturnsRaw).filter(ReturnsRaw.workspace_id == ws_id).delete(synchronize_session=False)
            db.query(SalesRaw).filter(SalesRaw.workspace_id == ws_id).delete(synchronize_session=False)

        db.delete(ws)
        db.commit()

        return {
            "deleted": True,
            "workspace_slug": slug,
            "force": bool(force),
            "counts": counts,
        }
    finally:
        db.close()


def _month_start_dates_from_series(dt_series) -> list[date]:
    months = set()
    for ts in dt_series:
        if pd.isna(ts):
            continue
        d = ts.to_pydatetime().date()
        months.add(d.replace(day=1))
    return sorted(months)


def refresh_style_monthly(db, ws_id, months: list[date] | None = None, full_refresh: bool = False) -> None:
    sales_month = cast(func.date_trunc("month", SalesRaw.order_date), Date)
    returns_month = cast(func.date_trunc("month", ReturnsRaw.return_date), Date)

    if full_refresh:
        sales_months = (
            db.query(sales_month.label("m"))
            .filter(SalesRaw.workspace_id == ws_id, SalesRaw.order_date.isnot(None))
            .distinct()
            .all()
        )
        returns_months = (
            db.query(returns_month.label("m"))
            .filter(ReturnsRaw.workspace_id == ws_id, ReturnsRaw.return_date.isnot(None))
            .distinct()
            .all()
        )
        months = sorted({r[0] for r in (sales_months + returns_months) if r[0] is not None})

    if not months:
        return

    # delete snapshot rows for these months
    db.query(StyleMonthly).filter(
        StyleMonthly.workspace_id == ws_id,
        StyleMonthly.month_start.in_(months),
    ).delete(synchronize_session=False)
    db.commit()

    # sales aggregates
    sales_rows = (
        db.query(
            sales_month.label("month_start"),
            SalesRaw.style_key.label("style_key"),
            func.sum(SalesRaw.units).label("orders"),
            func.max(SalesRaw.order_date).label("last_order_date"),
        )
        .filter(
            SalesRaw.workspace_id == ws_id,
            SalesRaw.order_date.isnot(None),
            SalesRaw.style_key.isnot(None),
            sales_month.in_(months),
        )
        .group_by(sales_month, SalesRaw.style_key)
        .all()
    )

    # returns aggregates
    returns_rows = (
        db.query(
            returns_month.label("month_start"),
            ReturnsRaw.style_key.label("style_key"),
            func.sum(ReturnsRaw.units).label("returns"),
        )
        .filter(
            ReturnsRaw.workspace_id == ws_id,
            ReturnsRaw.return_date.isnot(None),
            ReturnsRaw.style_key.isnot(None),
            returns_month.in_(months),
        )
        .group_by(returns_month, ReturnsRaw.style_key)
        .all()
    )

    merged = {}
    for r in sales_rows:
        k = (r.month_start, r.style_key)
        merged[k] = {
            "workspace_id": ws_id,
            "month_start": r.month_start,
            "style_key": r.style_key,
            "orders": int(r.orders or 0),
            "returns": 0,
            "revenue": None,
            "last_order_date": r.last_order_date,
            "return_pct": None,
        }

    for r in returns_rows:
        k = (r.month_start, r.style_key)
        if k not in merged:
            merged[k] = {
                "workspace_id": ws_id,
                "month_start": r.month_start,
                "style_key": r.style_key,
                "orders": 0,
                "returns": int(r.returns or 0),
                "revenue": None,
                "last_order_date": None,
                "return_pct": None,
            }
        else:
            merged[k]["returns"] = int(r.returns or 0)

    for v in merged.values():
        o = int(v["orders"] or 0)
        rt = int(v["returns"] or 0)
        v["return_pct"] = (float(rt) / float(o) * 100.0) if o > 0 else None

    rows_to_insert = list(merged.values())
    if rows_to_insert:
        db.bulk_insert_mappings(StyleMonthly, rows_to_insert)
        db.commit()


# -----------------------------------------------------------------------------
# Ingest: SALES (EXACT headers)
# Sales headers:
#  - order line id
#  - style id
#  - created on
# Qty: not present => each row = 1
# -----------------------------------------------------------------------------
@app.post("/db/ingest/sales")
async def db_ingest_sales(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    try:
        col_olid = require_col(df, "order line id")
        col_style = require_col(df, "style id")
        col_date = require_col(df, "created on")
        col_sku = optional_col(df, "seller sku code")
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    order_dt = parse_dt_series(df[col_date])

    style_key = (
        df[col_style].astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.lower()
    )
    order_line_id = (
        df[col_olid].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    )
    seller_sku = (
        df[col_sku].astype(str).str.strip().str.lower() if col_sku else None
    )

    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        if replace:
            db.query(SalesRaw).filter(SalesRaw.workspace_id == ws_id).delete()
            db.commit()

        rows = []
        for i in range(len(df)):
            raw_row = df.iloc[i].to_dict()
            rows.append(
                {
                    "workspace_id": ws_id,
                    "order_line_id": order_line_id.iat[i],
                    "style_key": style_key.iat[i],
                    "order_date": None if pd.isna(order_dt.iat[i]) else order_dt.iat[i].to_pydatetime(),
                    "seller_sku_code": None if seller_sku is None else seller_sku.iat[i],
                    "raw_json": json.dumps(raw_row, ensure_ascii=False),
                    "units": 1,  # Myntra: each row = 1 unit
                }
            )

        BATCH = 2000
        inserted = 0
        for start_i in range(0, len(rows), BATCH):
            chunk = rows[start_i : start_i + BATCH]
            db.bulk_insert_mappings(SalesRaw, chunk)
            inserted += len(chunk)

        db.commit()
        months = _month_start_dates_from_series(order_dt)
        refresh_style_monthly(db, ws_id, months=months, full_refresh=bool(replace))



        return {
            "filename": file.filename,
            "rows_in_file": int(len(df)),
            "inserted": int(inserted),
            "replace": bool(replace),
            "workspace_slug": workspace_slug,
            "detected": {
                "order_line_id": col_olid,
                "style_id": col_style,
                "created_on": col_date,
                "seller_sku_code": col_sku,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB ingest failed: {e}")
    finally:
        db.close()



# -----------------------------------------------------------------------------
# Ingest: RETURNS (EXACT headers)
# Returns headers:
#  - order_line_id
#  - style_id
#  - type
#  - quantity
#  - return_created_date (Return)
#  - order_rto_date (RTO)
# Logic:
#  - If type contains "RTO" => use order_rto_date
#  - Else => use return_created_date
# -----------------------------------------------------------------------------
@app.post("/db/ingest/returns")
async def db_ingest_returns(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    try:
        col_olid = require_col(df, "order_line_id")
        col_style = require_col(df, "style_id")
        col_type = require_col(df, "type")
        col_qty = require_col(df, "quantity")
        col_return_dt = require_col(df, "return_created_date")
        col_rto_dt = require_col(df, "order_rto_date")
        sku_col = optional_col(df, "seller_sku_code")  # optional
        reason_col = optional_col(df, "return_reason")  # optional (but present in Myntra)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    rtype = df[col_type].astype(str).map(normalize_return_type)
    qty = pd.to_numeric(df[col_qty], errors="coerce").fillna(0).astype(int)

    dt_return = parse_dt_series(df[col_return_dt])
    dt_rto = parse_dt_series(df[col_rto_dt])

    # choose date based on type
    is_rto = rtype.astype(str).str.upper().str.strip() == "RTO"
    chosen_dt = dt_return.where(~is_rto, dt_rto)

    style_key = (
        df[col_style]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.lower()
    )
    order_line_id = (
        df[col_olid].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    )
    seller_sku = df[sku_col].astype(str).str.strip().str.lower() if sku_col else None

    # reason series (optional)
    raw_reason_series = (
        df[reason_col].astype(str).str.strip() if reason_col else None
    )

    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        if replace:
            db.query(ReturnsRaw).filter(ReturnsRaw.workspace_id == ws_id).delete()
            db.commit()

        rows = []
        for i in range(len(df)):
            raw_row = df.iloc[i].to_dict()

            # enrich raw_json with cleaned reason (no DB migration needed)
            raw_reason = None if raw_reason_series is None else raw_reason_series.iat[i]
            clean_reason = clean_return_reason(raw_reason, rtype.iat[i])

            # keep both (useful for audits)
            raw_row["return_reason"] = raw_reason
            raw_row["clean_return_reason"] = clean_reason

            rows.append(
                {
                    "workspace_id": ws_id,
                    "order_line_id": order_line_id.iat[i],
                    "style_key": style_key.iat[i],
                    "return_date": None
                    if pd.isna(chosen_dt.iat[i])
                    else chosen_dt.iat[i].to_pydatetime(),
                    "return_type": rtype.iat[i],
                    "units": int(qty.iat[i]),
                    "seller_sku_code": None if seller_sku is None else seller_sku.iat[i],
                    "raw_json": json.dumps(raw_row, ensure_ascii=False),
                }
            )

        BATCH = 2000
        inserted = 0
        for start_i in range(0, len(rows), BATCH):
            chunk = rows[start_i : start_i + BATCH]
            db.bulk_insert_mappings(ReturnsRaw, chunk)
            inserted += len(chunk)
        db.commit()
        months = _month_start_dates_from_series(chosen_dt)
        refresh_style_monthly(db, ws_id, months=months, full_refresh=bool(replace))

    


        return {
            "filename": file.filename,
            "rows_in_file": int(len(df)),
            "inserted": int(inserted),
            "replace": bool(replace),
            "workspace_slug": workspace_slug,
            "detected": {
                "order_line_id": col_olid,
                "style_id": col_style,
                "type": col_type,
                "quantity": col_qty,
                "return_created_date": col_return_dt,
                "order_rto_date": col_rto_dt,
                "seller_sku_code": sku_col,
                "return_reason": reason_col,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB ingest failed: {e}")
    finally:
        db.close()



# -----------------------------------------------------------------------------
# Ingest: CATALOG / LISTING (EXACT headers)
# listing headers:
#  - style id
#  - style catalogued date
#  - brand
#  - style name   -> product_name
#  - seller sku code -> seller_sku_code
# -----------------------------------------------------------------------------
@app.post("/db/ingest/catalog")
async def db_ingest_catalog(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    try:
        col_style = require_col(df, "style id")
        col_live = require_col(df, "style catalogued date")
        col_brand = require_col(df, "brand")
        col_name = require_col(df, "style name")
        col_sku = require_col(df, "seller sku code")
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    style_key = df[col_style].astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.lower()
    live_dt = parse_dt_series(df[col_live])
    brand = df[col_brand].astype(str).str.strip()
    pname = df[col_name].astype(str).str.strip()
    sku = df[col_sku].astype(str).str.strip().str.lower()

    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        if replace:
            db.query(CatalogRaw).filter(CatalogRaw.workspace_id == ws_id).delete()
            db.commit()

        rows = []
        for i in range(len(df)):
            raw_row = df.iloc[i].to_dict()
            rows.append(
                {
                    "workspace_id": ws_id,
                    "style_key": style_key.iat[i],
                    "seller_sku_code": sku.iat[i] if sku.iat[i] else None,
                    "brand": brand.iat[i] if brand.iat[i] else None,
                    "product_name": pname.iat[i] if pname.iat[i] else None,
                    "style_catalogued_date": None if pd.isna(live_dt.iat[i]) else live_dt.iat[i].to_pydatetime(),
                    "raw_json": json.dumps(raw_row, ensure_ascii=False),
                }
            )

        BATCH = 2000
        inserted = 0
        for start_i in range(0, len(rows), BATCH):
            chunk = rows[start_i : start_i + BATCH]
            db.bulk_insert_mappings(CatalogRaw, chunk)
            inserted += len(chunk)
        db.commit()

        return {
            "filename": file.filename,
            "rows_in_file": int(len(df)),
            "inserted": int(inserted),
            "replace": bool(replace),
            "workspace_slug": workspace_slug,
            "detected": {
                "style_id": col_style,
                "style_catalogued_date": col_live,
                "brand": col_brand,
                "style_name": col_name,
                "seller_sku_code": col_sku,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB ingest failed: {e}")
    finally:
        db.close()

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import cast

@app.get("/db/brands")
def db_brands(
    workspace_slug: str = Query("default"),
    portal: str | None = Query(None),
):
    """
    Returns distinct brand list for a workspace.

    Priority:
    1) CatalogRaw.brand column (if populated)
    2) SalesRaw.raw_json["brand"] (from uploaded sales file)
    3) ReturnsRaw.raw_json["brand"] (from uploaded returns file)

    We only trim/normalize. We do NOT change ingestion here.
    """
    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "default").strip().strip('"').strip("'")
        ws_id = resolve_workspace_id(db, ws_slug)


        def json_brand_expr(model_raw_json_col):
            # raw_json is stored as TEXT in models -> cast to JSONB
            j = cast(model_raw_json_col, JSONB)
            # try common keys safely; whichever exists will come through
            b1 = func.nullif(func.trim(j["brand"].astext), "")
            b2 = func.nullif(func.trim(j["Brand"].astext), "")
            b3 = func.nullif(func.trim(j["BRAND"].astext), "")
            return func.coalesce(b1, b2, b3)

        brands: set[str] = set()

        # 1) Catalog table brand column
        cat_q = db.query(CatalogRaw.brand).filter(CatalogRaw.workspace_id == ws_id)
        cat_q = _apply_portal_catalog(cat_q, portal)

        cat_rows = (
            cat_q.filter(CatalogRaw.brand.isnot(None))
            .filter(func.length(func.trim(CatalogRaw.brand)) > 0)
            .distinct()
            .all()
        )
        for r in cat_rows:
            if r.brand:
                brands.add(str(r.brand).strip())

        # ✅ NEW: If catalog has brands, use ONLY catalog brands (avoid noisy sales/returns JSON)
        if brands:
            out = sorted(brands)
            return {"workspace_slug": ws_slug, "count": len(out), "brands": out}


        # 2) Sales raw_json brand
        
        sales_q = db.query(SalesRaw.raw_json).filter(SalesRaw.workspace_id == ws_id)
        sales_q = _apply_portal_sales(sales_q, ws_slug, portal)  
        
        sales_brand = json_brand_expr(SalesRaw.raw_json)
        
        # We need to apply the filter to the query structure compatible with the brand expr
        # Actually better to restructure the sales query to use the pre-filtered query object if possible, 
        # but _apply_portal_sales returns a query object.
        # The original code was doing a fresh query: db.query(sales_brand...)
        # We can chain it:
        
        sales_q_brand = db.query(sales_brand.label("brand")).filter(SalesRaw.workspace_id == ws_id)
        sales_q_brand = _apply_portal_sales(sales_q_brand, ws_slug, portal)
        
        sales_rows = (
            sales_q_brand
            .filter(SalesRaw.raw_json.isnot(None))
            .filter(sales_brand.isnot(None))
            .distinct()
            .all()
        )
        for r in sales_rows:
            if r.brand:
                brands.add(str(r.brand).strip())

        # 3) Returns raw_json brand

        ret_q_brand = db.query(json_brand_expr(ReturnsRaw.raw_json).label("brand")).filter(ReturnsRaw.workspace_id == ws_id)
        ret_q_brand = _apply_portal_returns(ret_q_brand, ws_slug, portal)

        returns_brand = json_brand_expr(ReturnsRaw.raw_json)
        returns_rows = (
             ret_q_brand
            .filter(ReturnsRaw.raw_json.isnot(None))
            .filter(returns_brand.isnot(None))
            .distinct()
            .all()
        )
        for r in returns_rows:
            if r.brand:
                brands.add(str(r.brand).strip())

        out = sorted(brands, key=lambda x: x.lower())

        return {
            "workspace_slug": ws_slug,
            "count": len(out),
            "brands": out,
        }
    finally:
        db.close()

  # -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Brand-wise GMV + ASP table (Seller Price) — Dashboard
# -----------------------------------------------------------------------------

@app.get("/db/kpi/house-gmv")
def db_kpi_house_gmv(
    start: date | None = Query(None, description="YYYY-MM-DD (optional). If provided, end is required."),
    end: date | None = Query(None, description="YYYY-MM-DD (optional). If provided, start is required."),
):
    """
    House GMV across ALL workspaces.
    - If start/end not provided => all-time
    - If start/end provided => date range on sales_raw.order_date
    GMV uses Seller Price (sellerprice) from sales_raw.raw_json * units
    """
    db = SessionLocal()
    try:
        if (start is None) != (end is None):
            raise HTTPException(status_code=400, detail="Provide both start and end, or neither.")

        start_dt = None
        end_dt_excl = None
        if start and end:
            start_dt = datetime.combine(start, time.min)
            end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        def to_float(x):
            try:
                s = str(x).replace(",", "").strip()
                if not s or s.lower() == "nan":
                    return None
                return float(s)
            except Exception:
                return None

        def pick_by_norm(d: dict, desired_norm: str):
            for k, v in d.items():
                if _norm(k) == desired_norm:
                    return v
            return None

        # Map workspace_id -> {orders, gmv}
        agg: dict[str, dict] = {}

        q = (
            db.query(SalesRaw.workspace_id, SalesRaw.units, SalesRaw.raw_json, SalesRaw.order_date)
            .filter(SalesRaw.order_date.isnot(None))
        )
        if start_dt and end_dt_excl:
            q = q.filter(SalesRaw.order_date >= start_dt).filter(SalesRaw.order_date < end_dt_excl)

        # stream-ish iteration (no big memory spike)
        for ws_id, units, raw_json, _od in q.yield_per(5000):
            ws_key = str(ws_id)

            u = int(units or 1)

            raw = {}
            v = raw_json
            if isinstance(v, dict):
                raw = v
            elif isinstance(v, str) and v.strip():
                try:
                    raw = json.loads(v)
                except Exception:
                    try:
                        raw = ast.literal_eval(v) if v.strip().startswith("{") else {}
                    except Exception:
                        raw = {}
                if not isinstance(raw, dict):
                    raw = {}
            else:
                raw = {}

            price_val = pick_by_norm(raw, "sellerprice")
            price = to_float(price_val) or 0.0
            gmv = price * u

            rec = agg.get(ws_key)
            if not rec:
                rec = {"orders": 0, "gmv": 0.0}
                agg[ws_key] = rec

            rec["orders"] += u
            rec["gmv"] += gmv

        # workspace meta
        ws_rows = db.query(Workspace.id, Workspace.slug, Workspace.name).all()
        ws_meta = {str(wid): {"slug": slug, "name": name} for wid, slug, name in ws_rows}

        total_gmv = sum(float(r["gmv"]) for r in agg.values()) if agg else 0.0
        total_orders = sum(int(r["orders"]) for r in agg.values()) if agg else 0

        rows_out = []
        for ws_key, rec in agg.items():
            meta = ws_meta.get(ws_key, {"slug": "(unknown)", "name": "(Unknown)"})
            gmv_val = float(rec["gmv"] or 0.0)
            share = (gmv_val / total_gmv * 100.0) if total_gmv > 0 else 0.0
            rows_out.append(
                {
                    "workspace_slug": meta["slug"],
                    "workspace_name": meta["name"],
                    "orders": int(rec["orders"] or 0),
                    "gmv": round(gmv_val, 2),
                    "share_pct": round(share, 2),
                }
            )

        rows_out.sort(key=lambda x: x["gmv"], reverse=True)

        return {
            "mode": "range" if (start and end) else "all_time",
            "window": {"start": None if not start else start.isoformat(), "end": None if not end else end.isoformat()},
            "total_gmv": round(total_gmv, 2),
            "total_orders": int(total_orders),
            "rows": rows_out,
        }
    finally:
        db.close()

@app.get("/db/kpi/house-summary")
def db_kpi_house_summary(
    start: date | None = Query(None, description="YYYY-MM-DD (optional). If provided, end required."),
    end: date | None = Query(None, description="YYYY-MM-DD (optional). If provided, start required."),
):
    """
    House Summary across ALL workspaces.
    - If start/end not provided => all-time
    - If start/end provided => date range (inclusive)
    GMV uses Seller Price (sellerprice) from SalesRaw.raw_json * units
    Returns units from ReturnsRaw.units (fallback 1)
    Return split: RTO vs CUSTOMER (everything not RTO treated as CUSTOMER)
    """
    db = SessionLocal()
    try:
        if (start is None) != (end is None):
            raise HTTPException(status_code=400, detail="Provide both start and end, or neither.")

        start_dt = None
        end_dt_excl = None
        if start and end:
            start_dt = datetime.combine(start, time.min)
            end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        def to_float(x):
            try:
                s = str(x).replace(",", "").strip()
                if not s or s.lower() == "nan":
                    return None
                return float(s)
            except Exception:
                return None

        def pick_by_norm(d: dict, desired_norm: str):
            for k, v in d.items():
                if _norm(k) == desired_norm:
                    return v
            return None

        # -----------------------
        # SALES aggregation (workspace-wise)
        # -----------------------
        sales_agg: dict[str, dict] = {}

        sq = (
            db.query(SalesRaw.workspace_id, SalesRaw.units, SalesRaw.raw_json, SalesRaw.order_date)
            .filter(SalesRaw.order_date.isnot(None))
        )
        if start_dt and end_dt_excl:
            sq = sq.filter(SalesRaw.order_date >= start_dt).filter(SalesRaw.order_date < end_dt_excl)

        for ws_id, units, raw_json, _od in sq.yield_per(5000):
            ws_key = str(ws_id)
            u = int(units or 1)

            raw = {}
            v = raw_json
            if isinstance(v, dict):
                raw = v
            elif isinstance(v, str) and v.strip():
                try:
                    raw = json.loads(v)
                except Exception:
                    raw = {}
                if not isinstance(raw, dict):
                    raw = {}
            else:
                raw = {}

            price_val = pick_by_norm(raw, "sellerprice")
            price = to_float(price_val) or 0.0
            gmv = price * u

            rec = sales_agg.get(ws_key)
            if not rec:
                rec = {"orders": 0, "gmv": 0.0}
                sales_agg[ws_key] = rec

            rec["orders"] += u
            rec["gmv"] += gmv

        # -----------------------
        # RETURNS aggregation (workspace-wise)
        # -----------------------
        returns_agg: dict[str, dict] = {}
        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rto_flag = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, ""))) == "RTO"

        rq = (
            db.query(
                ReturnsRaw.workspace_id.label("ws_id"),
                func.coalesce(func.sum(unit_expr), 0).label("total_units"),
                func.coalesce(func.sum(case((rto_flag, unit_expr), else_=0)), 0).label("rto_units"),
            )
            .filter(ReturnsRaw.return_date.isnot(None))
        )
        if start_dt and end_dt_excl:
            rq = rq.filter(ReturnsRaw.return_date >= start_dt).filter(ReturnsRaw.return_date < end_dt_excl)

        rq = rq.group_by(ReturnsRaw.workspace_id).all()

        for r in rq:
            ws_key = str(r.ws_id)
            total_u = int(r.total_units or 0)
            rto_u = int(r.rto_units or 0)
            cust_u = max(0, total_u - rto_u)
            returns_agg[ws_key] = {"returns_total": total_u, "returns_rto": rto_u, "returns_customer": cust_u}

        # workspace meta
        ws_rows = db.query(Workspace.id, Workspace.slug, Workspace.name).all()
        ws_meta = {str(wid): {"slug": slug, "name": name} for wid, slug, name in ws_rows}

        ws_keys = set(sales_agg.keys()) | set(returns_agg.keys())

        total_gmv = sum(float(sales_agg.get(k, {}).get("gmv", 0.0)) for k in ws_keys) if ws_keys else 0.0
        total_orders = sum(int(sales_agg.get(k, {}).get("orders", 0)) for k in ws_keys) if ws_keys else 0

        total_ret = sum(int(returns_agg.get(k, {}).get("returns_total", 0)) for k in ws_keys) if ws_keys else 0
        total_rto = sum(int(returns_agg.get(k, {}).get("returns_rto", 0)) for k in ws_keys) if ws_keys else 0
        total_cust = sum(int(returns_agg.get(k, {}).get("returns_customer", 0)) for k in ws_keys) if ws_keys else 0

        rows_out = []
        for k in ws_keys:
            meta = ws_meta.get(k, {"slug": "(unknown)", "name": "(Unknown)"})
            gmv_val = float(sales_agg.get(k, {}).get("gmv", 0.0))
            share = (gmv_val / total_gmv * 100.0) if total_gmv > 0 else 0.0

            rr = returns_agg.get(k, {})
            rows_out.append(
                {
                    "workspace_slug": meta["slug"],
                    "workspace_name": meta["name"],
                    "orders": int(sales_agg.get(k, {}).get("orders", 0)),
                    "gmv": round(gmv_val, 2),
                    "share_pct": round(share, 2),
                    "returns_total": int(rr.get("returns_total", 0)),
                    "returns_rto": int(rr.get("returns_rto", 0)),
                    "returns_customer": int(rr.get("returns_customer", 0)),
                }
            )

        rows_out.sort(key=lambda x: x["gmv"], reverse=True)

        return {
            "mode": "range" if (start and end) else "all_time",
            "window": {"start": None if not start else start.isoformat(), "end": None if not end else end.isoformat()},
            "totals": {
                "gmv": round(total_gmv, 2),
                "orders": int(total_orders),
                "returns_total": int(total_ret),
                "returns_rto": int(total_rto),
                "returns_customer": int(total_cust),
            },
            "rows": rows_out,
        }
    finally:
        db.close()


@app.get("/db/kpi/house-monthly")
def db_kpi_house_monthly(
    months: int = Query(12, ge=1, le=36, description="How many recent months to return"),
):
    """
    Last N months totals across ALL workspaces:
    GMV + Orders + Returns split (RTO vs Customer)
    """
    db = SessionLocal()
    try:
        months = int(months)

        def month_key(dt: datetime) -> str:
            return f"{dt.year:04d}-{dt.month:02d}"

        def shift_month(y: int, m: int, delta: int):
            mm = (y * 12 + (m - 1)) + delta
            ny = mm // 12
            nm = (mm % 12) + 1
            return ny, nm

        today = date.today()
        y0, m0 = today.year, today.month
        y_start, m_start = shift_month(y0, m0, -(months - 1))
        start_dt = datetime(y_start, m_start, 1, 0, 0, 0)

        def to_float(x):
            try:
                s = str(x).replace(",", "").strip()
                if not s or s.lower() == "nan":
                    return None
                return float(s)
            except Exception:
                return None

        def pick_by_norm(d: dict, desired_norm: str):
            for k, v in d.items():
                if _norm(k) == desired_norm:
                    return v
            return None

        agg: dict[str, dict] = {}

        # SALES (loop, consistent with your GMV logic)
        sq = (
            db.query(SalesRaw.units, SalesRaw.raw_json, SalesRaw.order_date)
            .filter(SalesRaw.order_date.isnot(None))
            .filter(SalesRaw.order_date >= start_dt)
        )
        for units, raw_json, od in sq.yield_per(5000):
            if not od:
                continue
            mk = month_key(od)

            rec = agg.get(mk)
            if not rec:
                rec = {"orders": 0, "gmv": 0.0, "returns_total": 0, "returns_rto": 0, "returns_customer": 0}
                agg[mk] = rec

            u = int(units or 1)

            raw = {}
            v = raw_json
            if isinstance(v, dict):
                raw = v
            elif isinstance(v, str) and v.strip():
                try:
                    raw = json.loads(v)
                except Exception:
                    raw = {}
                if not isinstance(raw, dict):
                    raw = {}
            else:
                raw = {}

            price_val = pick_by_norm(raw, "sellerprice")
            price = to_float(price_val) or 0.0

            rec["orders"] += u
            rec["gmv"] += price * u

        # RETURNS (SQL aggregate)
        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rto_flag = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, ""))) == "RTO"
        month_expr = func.to_char(ReturnsRaw.return_date, "YYYY-MM")

        rq = (
            db.query(
                month_expr.label("month_key"),
                func.coalesce(func.sum(unit_expr), 0).label("total_units"),
                func.coalesce(func.sum(case((rto_flag, unit_expr), else_=0)), 0).label("rto_units"),
            )
            .filter(ReturnsRaw.return_date.isnot(None))
            .filter(ReturnsRaw.return_date >= start_dt)
            .group_by(month_expr)
            .all()
        )

        for r in rq:
            mk = str(r.month_key)
            rec = agg.get(mk)
            if not rec:
                rec = {"orders": 0, "gmv": 0.0, "returns_total": 0, "returns_rto": 0, "returns_customer": 0}
                agg[mk] = rec
            total_u = int(r.total_units or 0)
            rto_u = int(r.rto_units or 0)
            rec["returns_total"] = total_u
            rec["returns_rto"] = rto_u
            rec["returns_customer"] = max(0, total_u - rto_u)

        # Build ordered list for last N months
        rows_out = []
        y, m = y_start, m_start
        for _ in range(months):
            mk = f"{y:04d}-{m:02d}"
            rec = agg.get(mk, {"orders": 0, "gmv": 0.0, "returns_total": 0, "returns_rto": 0, "returns_customer": 0})
            rows_out.append(
                {
                    "month": mk,
                    "orders": int(rec["orders"]),
                    "gmv": round(float(rec["gmv"]), 2),
                    "returns_total": int(rec["returns_total"]),
                    "returns_rto": int(rec["returns_rto"]),
                    "returns_customer": int(rec["returns_customer"]),
                }
            )
            y, m = shift_month(y, m, 1)

        return {"months": int(months), "rows": rows_out}
    finally:
        db.close()



@app.get("/db/kpi/brand-gmv-asp")
def db_kpi_brand_gmv_asp(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str | None = Query(None),
    workspace: str = Query("default"),  # backward compat
    brand: str | None = Query(None, description="Optional brand filter (catalog_raw.brand)"),
    top_n: int = Query(50, ge=1, le=500),
    portal: str | None = Query(None),
):
    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "").strip() or (workspace or "default")
        ws_id = resolve_workspace_id(db, ws_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        # -----------------------
        # Helpers
        # -----------------------
        def _norm(s: str) -> str:
            return "".join(ch for ch in str(s).strip().lower() if ch.isalnum())

        def pick_by_norm(d: dict, desired_norm: str):
            for k, v in (d or {}).items():
                if _norm(k) == desired_norm:
                    return v
            return None

        def to_float(x):
            try:
                s = str(x).replace(",", "").strip()
                if not s or s.lower() == "nan":
                    return None
                return float(s)
            except Exception:
                return None

        def parse_raw_json(v):
            """
            Supports:
            - dict (already parsed)
            - JSON string
            - python-dict-like string using single quotes
            """
            if isinstance(v, dict):
                return v
            if isinstance(v, str) and v.strip():
                try:
                    return json.loads(v)
                except Exception:
                    try:
                        obj = ast.literal_eval(v) if v.strip().startswith("{") else {}
                        return obj if isinstance(obj, dict) else {}
                    except Exception:
                        return {}
            return {}

        # -----------------------
        # Optional brand filter via CatalogRaw.brand -> style_keys
        # -----------------------
        brand_norm = (brand or "").strip().lower() if brand else None
        style_key_filter = None

        if brand_norm:
            brand_style_keys_sq = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)).label("style_key"))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
                .subquery()
            )
            style_key_filter = select(brand_style_keys_sq.c.style_key)

        # -----------------------
        # Pull sales rows in window
        # -----------------------
        sales_q = (
            db.query(SalesRaw.style_key, SalesRaw.units, SalesRaw.raw_json)
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
        )
        sales_q = _apply_portal_sales(sales_q, ws_slug, portal)

        if style_key_filter is not None:
            sales_q = sales_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        sales_rows = sales_q.all()

        # -----------------------
        # style_key -> brand map from catalog (preferred)
        # -----------------------
        style_keys = sorted(
            {
                str(r.style_key).strip().lower()
                for r in sales_rows
                if r.style_key and str(r.style_key).strip()
            }
        )

        brand_map: dict[str, str] = {}
        if style_keys:
            CHUNK = 1000
            for i in range(0, len(style_keys), CHUNK):
                chunk = style_keys[i : i + CHUNK]
                cat_rows = (
                    db.query(
                        func.lower(func.trim(CatalogRaw.style_key)).label("style_key"),
                        func.max(CatalogRaw.brand).label("brand"),
                    )
                    .filter(CatalogRaw.workspace_id == ws_id)
                    .filter(CatalogRaw.style_key.isnot(None))
                    .filter(func.lower(func.trim(CatalogRaw.style_key)).in_(chunk))
                    .group_by(func.lower(func.trim(CatalogRaw.style_key)))
                    .all()
                )
                for c in cat_rows:
                    if c.style_key and c.brand:
                        brand_map[str(c.style_key)] = str(c.brand).strip()

        # -----------------------
        # Aggregate per brand
        # -----------------------
        agg: dict[str, dict] = {}
        total_gmv = 0.0
        total_orders = 0

        for r in sales_rows:
            units = int(r.units or 1)
            style_norm = str(r.style_key).strip().lower() if r.style_key else ""

            raw = parse_raw_json(r.raw_json)

            # Brand: catalog first, fallback to raw_json['brand']
            b = brand_map.get(style_norm)
            if not b:
                b2 = pick_by_norm(raw, "brand")
                b = str(b2).strip() if b2 not in (None, "") else "(Unknown)"

            # GMV uses Seller Price
            price_val = pick_by_norm(raw, "sellerprice")
            price = to_float(price_val) or 0.0

            gmv = price * units
            total_gmv += gmv
            total_orders += units

            rec = agg.get(b)
            if not rec:
                rec = {"brand": b, "orders": 0, "gmv": 0.0}
                agg[b] = rec

            rec["orders"] += units
            rec["gmv"] += gmv

        rows = []
        for b, rec in agg.items():
            orders = int(rec["orders"] or 0)
            gmv = float(rec["gmv"] or 0.0)
            asp = (gmv / orders) if orders > 0 else 0.0
            share_pct = (gmv / total_gmv * 100.0) if total_gmv > 0 else 0.0

            rows.append(
                {
                    "brand": b,
                    "orders": orders,
                    "gmv": round(gmv, 2),
                    "asp": round(asp, 2),
                    "share_pct": round(share_pct, 2),
                }
            )

        # Default sort: GMV desc + apply top_n
        rows.sort(key=lambda x: x["gmv"], reverse=True)
        rows = rows[: int(top_n)]

        return {
            "workspace_slug": ws_slug,
            "window": {"start": str(start), "end": str(end)},
            "total_gmv": round(total_gmv, 2),
            "total_orders": int(total_orders),
            "rows": rows,
        }
    finally:
        db.close()

@app.get("/db/kpi/style-gmv-asp")
def db_kpi_style_gmv_asp(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str | None = Query(None),
    workspace: str = Query("default"),  # backward compat
    brand: str | None = Query(None),
    return_mode: str = Query("overall"),  # overall | same_month
    sort: str = Query("gmv"),  # gmv|orders|asp|returns|return_pct|return_amount
    dir: str = Query("desc"),  # asc|desc
    limit: int = Query(200, ge=1, le=2000),
    portal: str | None = Query(None),
    row_dim: str = Query("style", description="style|sku (flipkart should use sku)"),
):
    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "").strip() or (workspace or "default")
        ws_id = resolve_workspace_id(db, ws_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        mode = (return_mode or "overall").strip().lower()
        if mode not in ("overall", "same_month"):
            mode = "overall"

        p = _portal_norm(portal)
        dim = (row_dim or "style").strip().lower()
        is_sku = (dim == "sku") or (p == "flipkart")

        # -----------------------
        # Helpers
        # -----------------------
        def _norm(s: str) -> str:
            return "".join(ch for ch in str(s).strip().lower() if ch.isalnum())

        def pick_by_norm(d: dict, desired_norm: str):
            for k, v in (d or {}).items():
                if _norm(k) == desired_norm:
                    return v
            return None

        def to_float(x):
            try:
                s = str(x).replace(",", "").strip()
                if not s or s.lower() == "nan":
                    return None
                return float(s)
            except Exception:
                return None

        def parse_raw_json(v):
            if isinstance(v, dict):
                return v
            if isinstance(v, str) and v.strip():
                try:
                    return json.loads(v)
                except Exception:
                    try:
                        obj = ast.literal_eval(v) if v.strip().startswith("{") else {}
                        return obj if isinstance(obj, dict) else {}
                    except Exception:
                        return {}
            return {}

        # -----------------------
        # Brand filter list from catalog
        # -----------------------
        key_filter = None
        if brand:
            if is_sku:
                key_filter = [
                    (r[0] or "").strip().lower()
                    for r in (
                        db.query(func.lower(func.trim(CatalogRaw.seller_sku_code)))
                        .filter(CatalogRaw.workspace_id == ws_id)
                        .filter(CatalogRaw.seller_sku_code.isnot(None))
                        .filter(func.lower(func.trim(CatalogRaw.brand)) == brand.strip().lower())
                        .all()
                    )
                    if (r[0] or "").strip()
                ]
            else:
                key_filter = [
                    (r[0] or "").strip().lower()
                    for r in (
                        db.query(func.lower(func.trim(CatalogRaw.style_key)))
                        .filter(CatalogRaw.workspace_id == ws_id)
                        .filter(CatalogRaw.style_key.isnot(None))
                        .filter(func.lower(func.trim(CatalogRaw.brand)) == brand.strip().lower())
                        .all()
                    )
                    if (r[0] or "").strip()
                ]

        # -----------------------
        # Brand map for response
        # -----------------------
        if is_sku:
            brand_rows = (
                db.query(CatalogRaw.seller_sku_code, CatalogRaw.brand, func.max(CatalogRaw.style_key).label("style_key"))
                .filter(CatalogRaw.workspace_id == ws_id)
                .filter(CatalogRaw.seller_sku_code.isnot(None))
            )
            brand_rows = _apply_portal_catalog(brand_rows, portal)
            if key_filter is not None:
                brand_rows = brand_rows.filter(func.lower(func.trim(CatalogRaw.seller_sku_code)).in_(key_filter))
            brand_rows = brand_rows.group_by(CatalogRaw.seller_sku_code, CatalogRaw.brand).all()

            brand_map = {
                (r.seller_sku_code or "").strip().lower(): {
                    "brand": (r.brand or "(Unknown)"),
                    "style_key": (r.style_key or ""),
                }
                for r in brand_rows
            }
        else:
            brand_rows = (
                db.query(CatalogRaw.style_key, CatalogRaw.brand)
                .filter(CatalogRaw.workspace_id == ws_id)
                .filter(CatalogRaw.style_key.isnot(None))
            )
            brand_rows = _apply_portal_catalog(brand_rows, portal)
            if key_filter is not None:
                brand_rows = brand_rows.filter(func.lower(func.trim(CatalogRaw.style_key)).in_(key_filter))
            brand_map = {(r.style_key or "").strip().lower(): (r.brand or "(Unknown)") for r in brand_rows.all()}

        # -----------------------
        # Sales rows in window
        # -----------------------
        sales_rows_q = (
            db.query(
                SalesRaw.style_key,
                SalesRaw.seller_sku_code,
                SalesRaw.order_line_id,
                SalesRaw.order_date,
                SalesRaw.raw_json,
                SalesRaw.units,
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
        )
        sales_rows_q = _apply_portal_sales(sales_rows_q, ws_slug, portal)

        if key_filter is not None:
            if is_sku:
                sales_rows_q = sales_rows_q.filter(func.lower(func.trim(SalesRaw.seller_sku_code)).in_(key_filter))
            else:
                sales_rows_q = sales_rows_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(key_filter))

        sales_rows = sales_rows_q.all()

        agg: dict[str, dict] = {}
        sales_by_olid: dict[str, dict] = {}

        for r in sales_rows:
            key = (r.seller_sku_code if is_sku else r.style_key) or ""
            key = key.strip()
            if not key:
                continue

            units = int(r.units or 1)
            raw = parse_raw_json(r.raw_json)

            price_val = pick_by_norm(raw, "sellerprice")
            if price_val is None:
                price_val = raw.get("Seller Price") if isinstance(raw, dict) else None
            price = to_float(price_val) or 0.0

            kkey = key.lower()
            rec = agg.get(kkey)
            if not rec:
                rec = {
                    "key": key,
                    "style_key": (r.style_key or "").strip(),
                    "seller_sku_code": (r.seller_sku_code or "").strip(),
                    "orders": 0,
                    "gmv": 0.0,
                    "returns": 0,
                    "return_units": 0,
                    "rto_units": 0,
                    "return_amount": 0.0,
                }
                agg[kkey] = rec

            rec["orders"] += units
            rec["gmv"] += price * units

            olid = str(r.order_line_id).strip() if r.order_line_id else ""
            if olid and olid not in sales_by_olid:
                sales_by_olid[olid] = {
                    "key": key,
                    "style_key": (r.style_key or "").strip(),
                    "seller_sku_code": (r.seller_sku_code or "").strip(),
                    "order_date": r.order_date,
                    "seller_price": price,
                }

        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

        # -----------------------
        # Returns
        # -----------------------
        if mode == "overall":
            if is_sku:
                returns_q = (
                    db.query(
                        ReturnsRaw.seller_sku_code.label("key"),
                        func.coalesce(func.sum(unit_expr), 0).label("returns_total_units"),
                        func.coalesce(
                            func.sum(case((rtype_norm.in_(["RETURN", "CUSTOMER_RETURN"]), unit_expr), else_=0)),
                            0,
                        ).label("return_units"),
                        func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                        func.max(ReturnsRaw.style_key).label("style_key"),
                    )
                    .filter(ReturnsRaw.workspace_id == ws_id)
                    .filter(ReturnsRaw.return_date >= start_dt)
                    .filter(ReturnsRaw.return_date < end_dt_excl)
                    .filter(ReturnsRaw.seller_sku_code.isnot(None))
                )
            else:
                returns_q = (
                    db.query(
                        ReturnsRaw.style_key.label("key"),
                        func.coalesce(func.sum(unit_expr), 0).label("returns_total_units"),
                        func.coalesce(
                            func.sum(case((rtype_norm.in_(["RETURN", "CUSTOMER_RETURN"]), unit_expr), else_=0)),
                            0,
                        ).label("return_units"),
                        func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                    )
                    .filter(ReturnsRaw.workspace_id == ws_id)
                    .filter(ReturnsRaw.return_date >= start_dt)
                    .filter(ReturnsRaw.return_date < end_dt_excl)
                    .filter(ReturnsRaw.style_key.isnot(None))
                )

            returns_q = _apply_portal_returns(returns_q, ws_slug, portal)

            if key_filter is not None:
                if is_sku:
                    returns_q = returns_q.filter(func.lower(func.trim(ReturnsRaw.seller_sku_code)).in_(key_filter))
                else:
                    returns_q = returns_q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(key_filter))

            if is_sku:
                returns_rows = returns_q.group_by(ReturnsRaw.seller_sku_code).all()
            else:
                returns_rows = returns_q.group_by(ReturnsRaw.style_key).all()

            for rr in returns_rows:
                key = (rr.key or "").strip()
                if not key:
                    continue
                kkey = key.lower()

                rec = agg.get(kkey)
                if not rec:
                    rec = {
                        "key": key,
                        "style_key": (getattr(rr, "style_key", "") or "").strip(),
                        "seller_sku_code": key if is_sku else "",
                        "orders": 0,
                        "gmv": 0.0,
                        "returns": 0,
                        "return_units": 0,
                        "rto_units": 0,
                        "return_amount": 0.0,
                    }
                    agg[kkey] = rec

                rec["returns"] += int(rr.returns_total_units or 0)
                rec["return_units"] += int(rr.return_units or 0)
                rec["rto_units"] += int(rr.rto_units or 0)

        else:
            returns_rows_q = (
                db.query(
                    ReturnsRaw.order_line_id,
                    ReturnsRaw.style_key,
                    ReturnsRaw.seller_sku_code,
                    ReturnsRaw.units,
                    ReturnsRaw.return_type,
                    ReturnsRaw.return_date,
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
            )
            returns_rows_q = _apply_portal_returns(returns_rows_q, ws_slug, portal)
            returns_rows = returns_rows_q.all()

            for rr in returns_rows:
                olid = str(rr.order_line_id).strip() if rr.order_line_id else ""
                if not olid:
                    continue
                sale = sales_by_olid.get(olid)
                if not sale:
                    continue

                od = sale.get("order_date")
                rd = rr.return_date
                if not od or not rd:
                    continue
                if od.year != rd.year or od.month != rd.month:
                    continue

                key = (sale.get("key") or (rr.seller_sku_code if is_sku else rr.style_key) or "").strip()
                if not key:
                    continue

                ret_units = int(rr.units if rr.units is not None else 1)
                rt = (str(rr.return_type).strip().upper() if rr.return_type is not None else "")

                kkey = key.lower()
                rec = agg.get(kkey)
                if not rec:
                    rec = {
                        "key": key,
                        "style_key": (sale.get("style_key") or rr.style_key or "").strip(),
                        "seller_sku_code": (sale.get("seller_sku_code") or rr.seller_sku_code or "").strip(),
                        "orders": 0,
                        "gmv": 0.0,
                        "returns": 0,
                        "return_units": 0,
                        "rto_units": 0,
                        "return_amount": 0.0,
                    }
                    agg[kkey] = rec

                price = float(sale.get("seller_price") or 0.0)

                rec["returns"] += ret_units
                if rt in ("RETURN", "CUSTOMER_RETURN"):
                    rec["return_units"] += ret_units
                elif rt == "RTO":
                    rec["rto_units"] += ret_units

                rec["return_amount"] += price * ret_units

        # -----------------------
        # Build response rows
        # -----------------------
        rows = []
        for _, rec in agg.items():
            orders = int(rec["orders"] or 0)
            gmv = float(rec["gmv"] or 0.0)
            asp = (gmv / orders) if orders > 0 else 0.0

            ret_units = int(rec["returns"] or 0)
            ret_pct = (ret_units / orders * 100.0) if orders > 0 else 0.0

            if is_sku:
                meta = brand_map.get((rec.get("seller_sku_code") or "").strip().lower(), {})
                bname = meta.get("brand") or "(Unknown)"
                style_key_from_cat = meta.get("style_key") or rec.get("style_key") or ""
            else:
                bname = brand_map.get(rec.get("style_key") or "", "(Unknown)")
                style_key_from_cat = rec.get("style_key") or ""

            row = {
                "style_key": style_key_from_cat,
                "seller_sku_code": rec.get("seller_sku_code") or None,
                "brand": bname,
                "orders": orders,
                "gmv": round(gmv, 2),
                "asp": round(asp, 2),

                "returns": ret_units,
                "returns_total_units": ret_units,
                "return_units": int(rec.get("return_units") or 0),
                "rto_units": int(rec.get("rto_units") or 0),

                "return_pct": round(ret_pct, 2),
                "return_only_pct": round(((int(rec.get("return_units") or 0) / orders * 100.0) if orders > 0 else 0.0), 2),
                "rto_pct": round(((int(rec.get("rto_units") or 0) / orders * 100.0) if orders > 0 else 0.0), 2),
            }

            if mode == "same_month":
                row["return_amount"] = round(float(rec.get("return_amount") or 0.0), 2)

            rows.append(row)

        allowed = {"gmv", "orders", "asp", "returns", "return_pct", "return_amount"}
        key = sort if sort in allowed else "gmv"
        rev = (dir or "desc").lower() != "asc"

        if mode == "overall" and key == "return_amount":
            key = "gmv"

        rows.sort(key=lambda x: (x.get(key) or 0), reverse=rev)
        rows = rows[: int(limit)]

        return {
            "workspace_slug": ws_slug,
            "count": len(rows),
            "rows": rows,
            "return_mode": mode,
            "row_dim": ("sku" if is_sku else "style"),
            "portal": p or None,
        }

    finally:
        db.close()



# -----------------------------------------------------------------------------
# Style-wise Orders + GMV + ASP + Returns + Return Amount (Option A)
# Return Amount = Seller Price (from Sales) for those order_line_id that got returned
# -----------------------------------------------------------------------------


@app.get("/db/returns/summary")
def db_returns_summary(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str = Query("default"),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        # Orders (sales has no qty column => count rows)
        orders = (
            db.query(func.count(SalesRaw.id))
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
            .scalar()
        ) or 0

        rtype_norm = func.upper(func.trim(ReturnsRaw.return_type))

        returns_agg = (
            db.query(
                func.coalesce(func.sum(ReturnsRaw.units), 0).label("returns_units"),
                func.coalesce(
                    func.sum(case((rtype_norm == "RTO", ReturnsRaw.units), else_=0)), 0
                ).label("rto_units"),
                func.coalesce(
                    func.sum(case((rtype_norm == "RETURN", ReturnsRaw.units), else_=0)), 0
                ).label("return_units"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start_dt)
            .filter(ReturnsRaw.return_date < end_dt_excl)
            .one()
        )

        returns_units = int(returns_agg.returns_units or 0)
        rto_units = int(returns_agg.rto_units or 0)
        return_units = int(returns_agg.return_units or 0)

        return_pct = (returns_units / orders * 100.0) if orders else 0.0
        rto_pct = (rto_units / orders * 100.0) if orders else 0.0
        return_only_pct = (return_units / orders * 100.0) if orders else 0.0

        # -----------------------------
        # Return Amount = sum(seller_price for returned orders)
        # seller price is inside SalesRaw.raw_json under key "seller price"
        # -----------------------------
        seller_price_txt = cast(SalesRaw.raw_json, JSONB)["seller price"].astext
        seller_price_num = cast(func.nullif(func.replace(seller_price_txt, ",", ""), ""), Float)

        units_num = func.coalesce(ReturnsRaw.units, 1)

        return_amount = (
            db.query(
                func.coalesce(func.sum(func.coalesce(seller_price_num, 0.0) * units_num), 0.0)
            )
            .select_from(ReturnsRaw)
            .outerjoin(
                SalesRaw,
                and_(
                    SalesRaw.order_line_id == ReturnsRaw.order_line_id,
                    SalesRaw.workspace_id == ws_id,
                ),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start_dt)
            .filter(ReturnsRaw.return_date < end_dt_excl)
            .scalar()
        ) or 0.0

        return {
            "workspace_slug": workspace_slug,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "orders": int(orders),
            "returns_units": returns_units,
            "return_pct": float(return_pct),
            "rto_units": rto_units,
            "rto_pct": float(rto_pct),
            "return_units": return_units,
            "return_only_pct": float(return_only_pct),
            "return_amount": float(return_amount),
        }
    finally:
        db.close()


@app.post("/db/ingest/myntra-weekly-perf")
async def db_ingest_myntra_weekly_perf(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    # required columns
    try:
        col_style = require_col(df, "style id")
        col_impr = require_col(df, "impressions")
        col_clicks = require_col(df, "clicks")
        col_atc = require_col(df, "add to carts")
        col_purch = require_col(df, "purchases")
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # optional columns (keep flexible)
    col_seller = optional_col(df, "seller id")
    col_article = optional_col(df, "article type")
    col_brand = optional_col(df, "brand")
    col_gender = optional_col(df, "gender")
    col_mrp = optional_col(df, "seller mrp")
    col_age = optional_col(df, "inventory age")
    col_rplc = optional_col(df, "rplc")
    col_ret = optional_col(df, "return %")
    col_cons = optional_col(df, "consideration %")
    col_conv = optional_col(df, "conversion %")
    col_rating = optional_col(df, "rating")

    # normalize style_key same as others
    style_key = (
        df[col_style]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.lower()
    )

    def to_int(s):
        return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

    def to_float(s):
        return pd.to_numeric(s, errors="coerce")

    def to_pct_float(s):
        # handles "12.3" or "12.3%" or blank
        s2 = s.astype(str).str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(s2, errors="coerce")

    impressions = to_int(df[col_impr])
    clicks = to_int(df[col_clicks])
    add_to_carts = to_int(df[col_atc])
    purchases = to_int(df[col_purch])

    seller_id = to_int(df[col_seller]) if col_seller else None
    article_type = df[col_article].astype(str).str.strip() if col_article else None
    brand = df[col_brand].astype(str).str.strip() if col_brand else None
    gender = df[col_gender].astype(str).str.strip() if col_gender else None

    seller_mrp = to_float(df[col_mrp]) if col_mrp else None
    inventory_age = to_int(df[col_age]) if col_age else None
    rplc = to_float(df[col_rplc]) if col_rplc else None

    return_pct = to_pct_float(df[col_ret]) if col_ret else None
    consideration_pct = to_pct_float(df[col_cons]) if col_cons else None
    conversion_pct = to_pct_float(df[col_conv]) if col_conv else None
    rating = to_float(df[col_rating]) if col_rating else None

    ingested_at = datetime.utcnow()

    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # optional: allow clearing history if needed
        if replace:
            db.query(MyntraWeeklyPerfRaw).filter(MyntraWeeklyPerfRaw.workspace_id == ws_id).delete()
            db.commit()

        rows = []
        for i in range(len(df)):
            raw_row = df.iloc[i].to_dict()
            rows.append(
                {
                    "workspace_id": ws_id,
                    "style_key": style_key.iat[i],
                    "seller_id": None if seller_id is None else int(seller_id.iat[i]),
                    "article_type": None if article_type is None else article_type.iat[i],
                    "brand": None if brand is None else brand.iat[i],
                    "gender": None if gender is None else gender.iat[i],
                    "seller_mrp": None if seller_mrp is None or pd.isna(seller_mrp.iat[i]) else float(seller_mrp.iat[i]),
                    "inventory_age": None if inventory_age is None else int(inventory_age.iat[i]),
                    "rplc": None if rplc is None or pd.isna(rplc.iat[i]) else float(rplc.iat[i]),
                    "impressions": int(impressions.iat[i]),
                    "clicks": int(clicks.iat[i]),
                    "add_to_carts": int(add_to_carts.iat[i]),
                    "purchases": int(purchases.iat[i]),
                    "return_pct": None if return_pct is None or pd.isna(return_pct.iat[i]) else float(return_pct.iat[i]),
                    "consideration_pct": None if consideration_pct is None or pd.isna(consideration_pct.iat[i]) else float(consideration_pct.iat[i]),
                    "conversion_pct": None if conversion_pct is None or pd.isna(conversion_pct.iat[i]) else float(conversion_pct.iat[i]),
                    "rating": None if rating is None or pd.isna(rating.iat[i]) else float(rating.iat[i]),
                    "ingested_at": ingested_at,
                    "raw_json": json.dumps(raw_row, ensure_ascii=False),
                }
            )

        BATCH = 2000
        inserted = 0
        for start_i in range(0, len(rows), BATCH):
            chunk = rows[start_i : start_i + BATCH]
            db.bulk_insert_mappings(MyntraWeeklyPerfRaw, chunk)
            inserted += len(chunk)
        db.commit()

        return {
            "filename": file.filename,
            "rows_in_file": int(len(df)),
            "inserted": int(inserted),
            "replace": bool(replace),
            "workspace_slug": workspace_slug,
            "ingested_at": ingested_at.isoformat() + "Z",
            "detected": {
                "style_id": col_style,
                "impressions": col_impr,
                "clicks": col_clicks,
                "add_to_carts": col_atc,
                "purchases": col_purch,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB ingest failed: {e}")
    finally:
        db.close()

@app.post("/db/ingest/stock")
async def db_ingest_stock(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    content = await file.read()

    # Stock upload: CSV only (keeps backend lightweight)
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")


    try:
        col_sku = require_col(df, "seller_sku_code")
        col_qty = require_col(df, "qty")
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    sku = (
        df[col_sku]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    qty = pd.to_numeric(df[col_qty], errors="coerce").fillna(0).astype(int)

    ingested_at = datetime.utcnow()

    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        if replace:
            db.query(StockRaw).filter(StockRaw.workspace_id == ws_id).delete()
            db.commit()

        rows = []
        for i in range(len(df)):
            raw_row = df.iloc[i].to_dict()
            rows.append(
                {
                    "workspace_id": ws_id,
                    "seller_sku_code": sku.iat[i],
                    "qty": int(qty.iat[i]),
                    "ingested_at": ingested_at,
                    "raw_json": json.dumps(raw_row, ensure_ascii=False),
                }
            )

        BATCH = 2000
        inserted = 0
        for start_i in range(0, len(rows), BATCH):
            chunk = rows[start_i : start_i + BATCH]
            db.bulk_insert_mappings(StockRaw, chunk)
            inserted += len(chunk)
        db.commit()

        return {
            "filename": file.filename,
            "rows_in_file": int(len(df)),
            "inserted": int(inserted),
            "replace": bool(replace),
            "workspace_slug": workspace_slug,
            "ingested_at": ingested_at.isoformat() + "Z",
            "detected": {"seller_sku_code": col_sku, "qty": col_qty},
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB ingest failed: {e}")
    finally:
        db.close()




# -----------------------------------------------------------------------------
# Flipkart ingestion
#   - Events file: Sale + Return in ONE sheet
#   - Listing file: Catalog + Stock in ONE sheet
# -----------------------------------------------------------------------------

def _fk_norm(s: str | None) -> str:
    return (s or "").strip()

def _fk_norm_l(s: str | None) -> str:
    return (s or "").strip().lower()

def _fk_to_int(x, default=0) -> int:
    try:
        s = str(x).replace(",", "").strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default

def _fk_to_float(x, default=0.0) -> float:
    try:
        s = str(x).replace(",", "").strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


@app.post("/db/ingest/flipkart/events")
async def db_ingest_flipkart_events(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    """
    Flipkart Events Upload (ONE file contains Sale + Return)
    Rules:
    - Date column to use: Buyer Invoice Date
    - Join id: Order Item ID  (we store as order_line_id with fk prefix to keep uniqueness)
    - FSN is unique → style_key = fk:{fsn}
    - SKU → seller_sku_code = fk:{sku}
    - Units: Item Quantity
    - Amount: Final Invoice Amount (Price after discount+Shipping Charges)
      For Sales GMV: we store sellerprice per-unit = final_amount / qty
    - Returns:
        Event Type = Return
        Event Sub Type:
          Return -> CUSTOMER_RETURN
          Cancellation -> RTO
          Return Cancellation -> IGNORE
    """
    content = await file.read()
    filename = (file.filename or "").lower()

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    required = [
        "Order Item ID",
        "FSN",
        "SKU",
        "Buyer Invoice Date",
        "Event Type",
        "Event Sub Type",
        "Item Quantity",
        "Final Invoice Amount (Price after discount+Shipping Charges)",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing columns: {missing}")

    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "default").strip().lower() or "default"
        ws_id = resolve_workspace_id(db, ws_slug)

        # Replace only Flipkart rows for THIS workspace (safe)
        if replace:
            prefix = f"fk:{ws_slug}:"
            db.query(ReturnsRaw).filter(
                ReturnsRaw.workspace_id == ws_id,
                ReturnsRaw.order_line_id.like(prefix + "%"),
            ).delete(synchronize_session=False)

            db.query(SalesRaw).filter(
                SalesRaw.workspace_id == ws_id,
                SalesRaw.order_line_id.like(prefix + "%"),
            ).delete(synchronize_session=False)

            db.commit()

        sales_rows = []
        return_rows = []

        for i in range(len(df)):
            r = df.iloc[i].to_dict()

            order_item_id = _fk_norm(r.get("Order Item ID"))
            if not order_item_id:
                continue

            fsn = _fk_norm_l(r.get("FSN"))
            sku = _fk_norm_l(r.get("SKU"))

            # keep style_key distinct from Myntra keys
            style_key = f"fk:{fsn}" if fsn else None
            seller_sku_code = f"fk:{sku}" if sku else None

            # Use Buyer Invoice Date
            dt = parse_date_any(r.get("Buyer Invoice Date"))

            event_type = _fk_norm_l(r.get("Event Type"))
            event_sub = _fk_norm_l(r.get("Event Sub Type"))

            qty = _fk_to_int(r.get("Item Quantity"), default=0)
            qty = max(0, qty)

            final_amt = _fk_to_float(r.get("Final Invoice Amount (Price after discount+Shipping Charges)"), default=0.0)
            final_amt = max(0.0, final_amt)

            unit_price = (final_amt / float(qty)) if qty > 0 else 0.0

            # Global uniqueness safety for SalesRaw.order_line_id
            order_line_id = f"fk:{ws_slug}:{order_item_id}"

            # Store enriched raw_json
            enriched = dict(r)
            enriched["portal"] = "flipkart"
            enriched["sellerprice"] = unit_price  # IMPORTANT: GMV logic expects sellerprice
            enriched["final_invoice_amount"] = final_amt
            enriched["unit_price"] = unit_price

            if event_type == "sale":
                # Insert into sales_raw
                sales_rows.append(
                    {
                        "order_line_id": order_line_id,
                        "style_key": style_key,
                        "order_date": dt,
                        "seller_sku_code": seller_sku_code,
                        "raw_json": json.dumps(enriched, ensure_ascii=False),
                        "workspace_id": ws_id,
                        "units": qty if qty > 0 else 1,
                    }
                )

            elif event_type == "return":
                # Ignore return cancellation explicitly
                if event_sub == "return cancellation":
                    continue

                if event_sub == "return":
                    rtype = "CUSTOMER_RETURN"
                elif event_sub == "cancellation":
                    rtype = "RTO"
                else:
                    # unknown subtype -> keep but label it
                    rtype = (event_sub or "RETURN").upper()

                enriched["return_amount"] = final_amt  # FK returns have amount
                enriched["return_type_norm"] = rtype

                return_rows.append(
                    {
                        "order_line_id": order_line_id,   # must match sales for joins
                        "style_key": style_key,
                        "return_date": dt,
                        "return_type": rtype,
                        "units": qty if qty > 0 else 1,
                        "seller_sku_code": seller_sku_code,
                        "raw_json": json.dumps(enriched, ensure_ascii=False),
                        "workspace_id": ws_id,
                    }
                )

            else:
                # ignore other event types
                continue

        inserted_sales = 0
        inserted_returns = 0

        if sales_rows:
            db.execute(SalesRaw.__table__.insert(), sales_rows)
            inserted_sales = len(sales_rows)

        if return_rows:
            db.execute(ReturnsRaw.__table__.insert(), return_rows)
            inserted_returns = len(return_rows)

        db.commit()

        return {
            "workspace_slug": ws_slug,
            "inserted_sales": inserted_sales,
            "inserted_returns": inserted_returns,
        }

    except IntegrityError as e:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"Duplicate detected (Order Item ID already ingested). Use Replace toggle if re-uploading same period. ({e})",
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Flipkart ingest failed: {e}")
    finally:
        db.close()


@app.post("/db/ingest/flipkart/listing")
async def db_ingest_flipkart_listing(
    file: UploadFile = File(...),
    replace: bool = False,
    workspace_slug: str = Query("default"),
):
    """
    Flipkart Listing Upload (ONE file contains Catalog + Stock)
    Uses:
      - FSN column: Flipkart Serial Number  (fallback: FSN)
      - SKU column: Seller SKU Id           (fallback: SKU)
      - Stock column priority:
          Current stock count for your product
          Your Stock Count
          System Stock count
    """
    content = await file.read()
    filename = (file.filename or "").lower()

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    fsn_col = "Flipkart Serial Number" if "Flipkart Serial Number" in df.columns else ("FSN" if "FSN" in df.columns else None)
    sku_col = "Seller SKU Id" if "Seller SKU Id" in df.columns else ("SKU" if "SKU" in df.columns else None)

    if not fsn_col:
        raise HTTPException(status_code=400, detail="Missing FSN column: expected 'Flipkart Serial Number' (or 'FSN')")
    if not sku_col:
        raise HTTPException(status_code=400, detail="Missing SKU column: expected 'Seller SKU Id' (or 'SKU')")

    stock_col = None
    for c in ["Current stock count for your product", "Your Stock Count", "System Stock count"]:
        if c in df.columns:
            stock_col = c
            break

    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "default").strip().lower() or "default"
        ws_id = resolve_workspace_id(db, ws_slug)

        if replace:
            # remove only Flipkart-tagged catalog + fk-prefixed stock for this workspace
            db.query(CatalogRaw).filter(
                CatalogRaw.workspace_id == ws_id,
                CatalogRaw.style_key.like("fk:%"),
            ).delete(synchronize_session=False)

            db.query(StockRaw).filter(
                StockRaw.workspace_id == ws_id,
                func.lower(func.trim(StockRaw.seller_sku_code)).like("fk:%"),
            ).delete(synchronize_session=False)

            db.commit()

        # ---- Catalog upsert by style_key (CatalogRaw PK is style_key)
        upserts = 0
        stock_rows = 0
        ingested_at = datetime.utcnow()

        # Aggregate stock by SKU
        stock_map = {}

        for i in range(len(df)):
            r = df.iloc[i].to_dict()

            fsn = _fk_norm_l(r.get(fsn_col))
            if not fsn:
                continue

            sku = _fk_norm_l(r.get(sku_col))
            style_key = f"fk:{fsn}"
            seller_sku_code = f"fk:{sku}" if sku else None

            product_name = _fk_norm(r.get("Product Title")) if "Product Title" in df.columns else None
            brand = None  # FK listing sample doesn’t provide stable brand

            raw = dict(r)
            raw["portal"] = "flipkart"
            raw_json = json.dumps(raw, ensure_ascii=False)

            stmt = insert(CatalogRaw).values(
                style_key=style_key,
                seller_sku_code=seller_sku_code,
                brand=brand,
                product_name=product_name,
                style_catalogued_date=None,   # FK doesn't have live date
                raw_json=raw_json,
                workspace_id=ws_id,
            ).on_conflict_do_update(
    index_elements=["workspace_id", "style_key", "seller_sku_code"],
    set_=dict(
        product_name=product_name,
        raw_json=raw_json,
    ),
)


            db.execute(stmt)
            upserts += 1

            if stock_col and seller_sku_code:
                qty = _fk_to_int(r.get(stock_col), default=0)
                stock_map[seller_sku_code] = stock_map.get(seller_sku_code, 0) + max(0, qty)

        # Insert stock snapshot rows
        if stock_map:
            rows = []
            for sku_norm, qty in stock_map.items():
                rows.append(
                    {
                        "seller_sku_code": sku_norm,
                        "qty": int(qty),
                        "ingested_at": ingested_at,
                        "raw_json": json.dumps({"portal": "flipkart", "source": "listing"}, ensure_ascii=False),
                        "workspace_id": ws_id,
                    }
                )
            db.execute(StockRaw.__table__.insert(), rows)
            stock_rows = len(rows)

        db.commit()

        return {
            "workspace_slug": ws_slug,
            "inserted_catalog": int(upserts),
            "inserted_stock": int(stock_rows),
            "stock_snapshot_at": ingested_at.isoformat(),
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Flipkart listing ingest failed: {e}")
    finally:
        db.close()


# -----------------------------------------------------------------------------
# KPI endpoints (unchanged behavior, just workspace_slug)
# -----------------------------------------------------------------------------
@app.get("/db/kpi/summary")
def db_kpi_summary(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str | None = Query(None),
    workspace: str = Query("default"),  # backward compat
    brand: str | None = Query(None),
    return_mode: str = Query("overall"),  # overall | same_month
    portal: str | None = Query(None),
):
    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "").strip() or (workspace or "default")
        ws_id = resolve_workspace_id(db, ws_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        mode = (return_mode or "overall").strip().lower()
        if mode not in ("overall", "same_month"):
            mode = "overall"

        # Optional brand filter: restrict to styles from catalog
        style_key_filter = None
        if brand:
            style_key_filter = [
                (r[0] or "").strip().lower()
                for r in (
                    db.query(func.lower(func.trim(CatalogRaw.style_key)))
                    .filter(CatalogRaw.workspace_id == ws_id)
                    .filter(func.lower(func.trim(CatalogRaw.brand)) == brand.strip().lower())
                    .all()
                )
                if (r[0] or "").strip()
            ]
            if not style_key_filter:
                return {
                    "workspace_slug": ws_slug,
                    "start": str(start),
                    "end": str(end),
                    "window": {"start": str(start), "end": str(end)},
                    "orders": 0,
                    "returns_total_units": 0,
                    "return_units": 0,
                    "rto_units": 0,
                    "returns": 0,
                    "rto": 0,
                    "return_pct": 0.0,
                    "return_only_pct": 0.0,
                    "rto_pct": 0.0,
                    "return_mode": mode,
                }

        # Orders (sales) in window
        sales_q = (
            db.query(func.coalesce(func.sum(SalesRaw.units), 0))
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
        )
        sales_q = _apply_portal_sales(sales_q, ws_slug, portal)

        if style_key_filter is not None:
            sales_q = sales_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        orders_units = int(sales_q.scalar() or 0)

        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

        # Returns in window
        if mode == "overall":
            # overall: by return_date only (no sale-date linking)

            returns_total_units_q = (
                db.query(func.coalesce(func.sum(unit_expr), 0))
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
            )
            returns_total_units_q = _apply_portal_returns(returns_total_units_q, ws_slug, portal)

            if style_key_filter is not None:
                returns_total_units_q = returns_total_units_q.filter(
                    func.lower(func.trim(ReturnsRaw.style_key)).in_(style_key_filter)
                )
            returns_total_units = int(returns_total_units_q.scalar() or 0)

            rto_units_q = (
                db.query(func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0))
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
            )
            rto_units_q = _apply_portal_returns(rto_units_q, ws_slug, portal)

            if style_key_filter is not None:
                rto_units_q = rto_units_q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(style_key_filter))
            rto_units = int(rto_units_q.scalar() or 0)

            # Customer returns: RETURN (Myntra) + CUSTOMER_RETURN (Flipkart)
            return_units_q = (
                db.query(
                    func.coalesce(
                        func.sum(case((rtype_norm.in_(["RETURN", "CUSTOMER_RETURN"]), unit_expr), else_=0)),
                        0,
                    )
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
            )
            return_units_q = _apply_portal_returns(return_units_q, ws_slug, portal)

            if style_key_filter is not None:
                return_units_q = return_units_q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(style_key_filter))
            return_units = int(return_units_q.scalar() or 0)

        else:
            # same_month: only returns where sales month == return month, and both sale+return in window
            # IMPORTANT: Dedup sales by order_line_id to avoid multiple-month matches inflating same_month

            sales_one_q = (
                db.query(
                    SalesRaw.order_line_id.label("order_line_id"),
                    func.min(SalesRaw.order_date).label("order_date"),
                    func.max(SalesRaw.style_key).label("style_key"),
                )
                .filter(SalesRaw.workspace_id == ws_id)
                .filter(SalesRaw.order_date >= start_dt)
                .filter(SalesRaw.order_date < end_dt_excl)
            )

            # Apply portal filter on sales side BEFORE grouping
            sales_one_q = _apply_portal_sales(sales_one_q, ws_slug, portal)

            # Apply brand/style filter on sales side (if present)
            if style_key_filter is not None:
                sales_one_q = sales_one_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

            sales_one = sales_one_q.group_by(SalesRaw.order_line_id).subquery()

            base = (
                db.query(ReturnsRaw)
                .join(sales_one, sales_one.c.order_line_id == ReturnsRaw.order_line_id)
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
                .filter(func.date_trunc("month", sales_one.c.order_date) == func.date_trunc("month", ReturnsRaw.return_date))
            )

            # Apply portal filter on returns side
            base = _apply_portal_returns(base, ws_slug, portal)

            returns_total_units = int(base.with_entities(func.coalesce(func.sum(unit_expr), 0)).scalar() or 0)
            rto_units = int(
                base.with_entities(func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0)).scalar()
                or 0
            )
            return_units = int(
                base.with_entities(
                    func.coalesce(
                        func.sum(case((rtype_norm.in_(["RETURN", "CUSTOMER_RETURN"]), unit_expr), else_=0)),
                        0,
                    )
                ).scalar()
                or 0
            )

        return_pct = (returns_total_units / orders_units * 100.0) if orders_units > 0 else 0.0
        rto_pct = (rto_units / orders_units * 100.0) if orders_units > 0 else 0.0
        return_only_pct = (return_units / orders_units * 100.0) if orders_units > 0 else 0.0

        return {
            # Backward compat
            "workspace_slug": ws_slug,
            "start": str(start),
            "end": str(end),

            # New structured window (preferred)
            "window": {"start": str(start), "end": str(end)},

            "orders": orders_units,

            # Units
            "returns_total_units": returns_total_units,  # all returns (customer + RTO)
            "return_units": return_units,                # customer returns only
            "rto_units": rto_units,                      # RTO only

            # Backward compat keys
            "returns": returns_total_units,
            "rto": rto_units,

            # Percentages (vs orders)
            "return_pct": round(return_pct, 2),           # total returns %
            "return_only_pct": round(return_only_pct, 2), # customer returns %
            "rto_pct": round(rto_pct, 2),                 # RTO %

            "return_mode": mode,
        }
    finally:
        db.close()

@app.get("/db/kpi/returns-trend")
def db_kpi_returns_trend(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str | None = Query(None),
    workspace: str = Query("default"),
    brand: str | None = Query(None),
    return_mode: str = Query("overall"),  # overall | same_month
    portal: str | None = Query(None),
):
    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "").strip() or (workspace or "default")
        ws_id = resolve_workspace_id(db, ws_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        mode = (return_mode or "overall").strip().lower()
        if mode not in ("overall", "same_month"):
            mode = "overall"

        style_key_filter = None
        if brand:
            style_key_filter = [
                (r[0] or "").strip().lower()
                for r in (
                    db.query(func.lower(func.trim(CatalogRaw.style_key)))
                    .filter(CatalogRaw.workspace_id == ws_id)
                    .filter(func.lower(func.trim(CatalogRaw.brand)) == brand.strip().lower())
                    .all()
                )
                if (r[0] or "").strip()
            ]
            if not style_key_filter:
                return {
                    "workspace_slug": ws_slug,
                    "window": {"start": str(start), "end": str(end)},
                    "series": [],
                    "return_mode": mode,
                }

        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

        if mode == "overall":
            q = (
                db.query(
                    func.date(ReturnsRaw.return_date).label("d"),
                    func.coalesce(func.sum(unit_expr), 0).label("returns_total_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RETURN", unit_expr), else_=0)), 0).label("return_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
            )
            q = _apply_portal_returns(q, ws_slug, portal)

            if style_key_filter is not None:
                q = q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(style_key_filter))
            q = q.group_by(func.date(ReturnsRaw.return_date)).order_by(func.date(ReturnsRaw.return_date))

        else:
            q = (
                db.query(
                    func.date(ReturnsRaw.return_date).label("d"),
                    func.coalesce(func.sum(unit_expr), 0).label("returns_total_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RETURN", unit_expr), else_=0)), 0).label("return_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                )
                .select_from(ReturnsRaw)
                .join(SalesRaw, SalesRaw.order_line_id == ReturnsRaw.order_line_id)
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(SalesRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
                .filter(SalesRaw.order_date >= start_dt)
                .filter(SalesRaw.order_date < end_dt_excl)
                .filter(func.date_trunc("month", SalesRaw.order_date) == func.date_trunc("month", ReturnsRaw.return_date))
            )
            q = _apply_portal_sales(q, ws_slug, portal)
            q = _apply_portal_returns(q, ws_slug, portal)

            if style_key_filter is not None:
                q = q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

            q = q.group_by(func.date(ReturnsRaw.return_date)).order_by(func.date(ReturnsRaw.return_date))

        rows = q.all()
        series = [
            {
                "date": str(r.d),
                # Backward compat
                "returns": int(getattr(r, "returns_total_units", 0) or 0),
                # Preferred
                "returns_total_units": int(getattr(r, "returns_total_units", 0) or 0),
                "return_units": int(getattr(r, "return_units", 0) or 0),
                "rto_units": int(getattr(r, "rto_units", 0) or 0),
            }
            for r in rows
        ]

        return {
            "workspace_slug": ws_slug,
            "window": {"start": str(start), "end": str(end)},
            "series": series,
            "return_mode": mode,
        }
    finally:
        db.close()


@app.get("/db/kpi/top-return-styles")
def db_kpi_top_return_styles(
    start: str = Query(...),
    end: str = Query(...),
    workspace_slug: str = Query("default"),
    top_n: int = Query(50, ge=1, le=1000),
    min_orders: int = Query(10, ge=0),
    mode: str = Query("month"),  # kept for compatibility (unused)
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    return_mode: str = Query("same_month"),  # overall | same_month
    portal: str | None = Query(None),
):
    # Parse dates
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt_excl = datetime.fromisoformat(end) + timedelta(days=1)
    except Exception:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    mode_norm = (return_mode or "same_month").strip().lower()
    if mode_norm not in ("overall", "same_month"):
        mode_norm = "same_month"

    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # ---------- Brand -> style_keys filter (prevents join duplication)
        style_key_filter = None
        brand_norm = (brand or "").strip().lower() if brand else None
        if brand_norm:
            brand_style_keys_sq = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)).label("style_key"))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
                .subquery()
            )
            style_key_filter = select(brand_style_keys_sq.c.style_key)

        # ---------- Sales: orders per style (sales window)
        sales_q = (
            db.query(
                func.lower(func.trim(SalesRaw.style_key)).label("style_key"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders"),
                func.max(SalesRaw.order_date).label("last_order_date"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
            .filter(SalesRaw.style_key.isnot(None))
        )
        sales_q = _apply_portal_sales(sales_q, workspace_slug, portal)

        if style_key_filter is not None:
            sales_q = sales_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        sales_by_style = sales_q.group_by(func.lower(func.trim(SalesRaw.style_key))).subquery()

        # ---------- Returns aggregation
        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

        if mode_norm == "overall":
            # overall: returns in window grouped by ReturnsRaw.style_key (no sale-linking)
            ret_q = (
                db.query(
                    func.lower(func.trim(ReturnsRaw.style_key)).label("style_key"),
                    func.coalesce(func.sum(unit_expr), 0).label("returns_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RETURN", unit_expr), else_=0)), 0).label("return_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
                .filter(ReturnsRaw.style_key.isnot(None))
            )
            ret_q = _apply_portal_returns(ret_q, workspace_slug, portal)

            if style_key_filter is not None:
                ret_q = ret_q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(style_key_filter))

            returns_by_style = ret_q.group_by(func.lower(func.trim(ReturnsRaw.style_key))).subquery()

        else:
            # same_month: join by order_line_id, and require sale month == return month
            ret_q = (
                db.query(
                    func.lower(func.trim(SalesRaw.style_key)).label("style_key"),
                    func.coalesce(func.sum(unit_expr), 0).label("returns_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RETURN", unit_expr), else_=0)), 0).label("return_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                )
                .select_from(ReturnsRaw)
                .join(SalesRaw, SalesRaw.order_line_id == ReturnsRaw.order_line_id)
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(SalesRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
                .filter(SalesRaw.order_date >= start_dt)
                .filter(SalesRaw.order_date < end_dt_excl)
                .filter(func.date_trunc("month", SalesRaw.order_date) == func.date_trunc("month", ReturnsRaw.return_date))
                .filter(SalesRaw.style_key.isnot(None))
            )
            ret_q = _apply_portal_sales(ret_q, workspace_slug, portal)
            ret_q = _apply_portal_returns(ret_q, workspace_slug, portal)

            if style_key_filter is not None:
                ret_q = ret_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

            returns_by_style = ret_q.group_by(func.lower(func.trim(SalesRaw.style_key))).subquery()

        # ---------- Catalog: one row per style
        cat = (
            db.query(
                func.lower(func.trim(CatalogRaw.style_key)).label("style_key"),
                func.max(CatalogRaw.brand).label("brand"),
                func.max(CatalogRaw.product_name).label("product_name"),
            )
            .filter(CatalogRaw.workspace_id == ws_id)
            .filter(CatalogRaw.style_key.isnot(None))
            .group_by(func.lower(func.trim(CatalogRaw.style_key)))
            .subquery()
        )

        # ---------- Final query
        q = (
            db.query(
                sales_by_style.c.style_key,
                cat.c.brand,
                cat.c.product_name,
                sales_by_style.c.orders,
                func.coalesce(returns_by_style.c.returns_units, 0).label("returns_units"),
                func.coalesce(returns_by_style.c.return_units, 0).label("return_units"),
                func.coalesce(returns_by_style.c.rto_units, 0).label("rto_units"),
                (
                    func.coalesce(returns_by_style.c.returns_units, 0)
                    / func.nullif(sales_by_style.c.orders, 0)
                ).label("return_pct"),
                sales_by_style.c.last_order_date,
            )
            .outerjoin(returns_by_style, returns_by_style.c.style_key == sales_by_style.c.style_key)
            .outerjoin(cat, cat.c.style_key == sales_by_style.c.style_key)
            .filter(sales_by_style.c.orders >= min_orders)
            .order_by(text("return_pct DESC NULLS LAST"), text("orders DESC"))
            .limit(top_n)
        )

        rows = []
        for r in q.all():
            rows.append(
                {
                    "style_key": r.style_key,
                    "brand": r.brand,
                    "product_name": r.product_name,
                    "orders": int(r.orders or 0),
                    "returns_units": int(r.returns_units or 0),
                    "return_units": int(r.return_units or 0),
                    "rto_units": int(r.rto_units or 0),
                    "return_pct": float(r.return_pct or 0.0),
                    "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                    "return_mode": mode_norm,
                }
            )

        return rows
    finally:
        db.close()



@app.get("/db/kpi/top-return-skus")
def db_kpi_top_return_skus(
    start: str = Query(...),
    end: str = Query(...),
    workspace_slug: str = Query("default"),
    top_n: int = Query(50, ge=1, le=1000),
    min_orders: int = Query(10, ge=0),
    mode: str = Query("month"),  # kept for compatibility (unused)
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    return_mode: str = Query("same_month"),  # overall | same_month
    portal: str | None = Query(None),
):
    # Parse dates
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt_excl = datetime.fromisoformat(end) + timedelta(days=1)
    except Exception:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    mode_norm = (return_mode or "same_month").strip().lower()
    if mode_norm not in ("overall", "same_month"):
        mode_norm = "same_month"

    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # ---------- Brand -> style_keys filter (prevents join duplication)
        style_key_filter = None
        brand_norm = (brand or "").strip().lower() if brand else None
        if brand_norm:
            brand_style_keys_sq = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)).label("style_key"))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
                .subquery()
            )
            style_key_filter = select(brand_style_keys_sq.c.style_key)

        # ---------- Sales: orders per SKU+style (sales window)
        sales_q = (
            db.query(
                func.lower(func.trim(SalesRaw.seller_sku_code)).label("seller_sku_code"),
                func.lower(func.trim(SalesRaw.style_key)).label("style_key"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders"),
                func.max(SalesRaw.order_date).label("last_order_date"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
            .filter(SalesRaw.seller_sku_code.isnot(None))
            .filter(SalesRaw.style_key.isnot(None))
        )
        sales_q = _apply_portal_sales(sales_q, workspace_slug, portal)

        if style_key_filter is not None:
            sales_q = sales_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        sales_by_sku = sales_q.group_by(
            func.lower(func.trim(SalesRaw.seller_sku_code)),
            func.lower(func.trim(SalesRaw.style_key)),
        ).subquery()

        # ---------- Returns aggregation
        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

        if mode_norm == "overall":
            # overall: returns in window grouped by ReturnsRaw.seller_sku_code (and style_key if present)
            ret_q = (
                db.query(
                    func.lower(func.trim(ReturnsRaw.seller_sku_code)).label("seller_sku_code"),
                    func.lower(func.trim(ReturnsRaw.style_key)).label("style_key"),
                    func.coalesce(func.sum(unit_expr), 0).label("returns_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RETURN", unit_expr), else_=0)), 0).label("return_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
                .filter(ReturnsRaw.seller_sku_code.isnot(None))
                .filter(ReturnsRaw.style_key.isnot(None))
            )
            ret_q = _apply_portal_returns(ret_q, workspace_slug, portal)

            if style_key_filter is not None:
                ret_q = ret_q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(style_key_filter))

            returns_by_sku = ret_q.group_by(
                func.lower(func.trim(ReturnsRaw.seller_sku_code)),
                func.lower(func.trim(ReturnsRaw.style_key)),
            ).subquery()

        else:
            # same_month: join by order_line_id, and require sale month == return month
            ret_q = (
                db.query(
                    func.lower(func.trim(SalesRaw.seller_sku_code)).label("seller_sku_code"),
                    func.lower(func.trim(SalesRaw.style_key)).label("style_key"),
                    func.coalesce(func.sum(unit_expr), 0).label("returns_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RETURN", unit_expr), else_=0)), 0).label("return_units"),
                    func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units"),
                )
                .select_from(ReturnsRaw)
                .join(SalesRaw, SalesRaw.order_line_id == ReturnsRaw.order_line_id)
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(SalesRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start_dt)
                .filter(ReturnsRaw.return_date < end_dt_excl)
                .filter(SalesRaw.order_date >= start_dt)
                .filter(SalesRaw.order_date < end_dt_excl)
                .filter(func.date_trunc("month", SalesRaw.order_date) == func.date_trunc("month", ReturnsRaw.return_date))
                .filter(SalesRaw.seller_sku_code.isnot(None))
                .filter(SalesRaw.style_key.isnot(None))
            )
            ret_q = _apply_portal_sales(ret_q, workspace_slug, portal)
            ret_q = _apply_portal_returns(ret_q, workspace_slug, portal)

            if style_key_filter is not None:
                ret_q = ret_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

            returns_by_sku = ret_q.group_by(
                func.lower(func.trim(SalesRaw.seller_sku_code)),
                func.lower(func.trim(SalesRaw.style_key)),
            ).subquery()

        # ---------- Catalog: one row per style (brand/product_name context)
        cat = (
            db.query(
                func.lower(func.trim(CatalogRaw.style_key)).label("style_key"),
                func.max(CatalogRaw.brand).label("brand"),
                func.max(CatalogRaw.product_name).label("product_name"),
            )
            .filter(CatalogRaw.workspace_id == ws_id)
            .filter(CatalogRaw.style_key.isnot(None))
            .group_by(func.lower(func.trim(CatalogRaw.style_key)))
            .subquery()
        )

        # ---------- Final query
        q = (
            db.query(
                sales_by_sku.c.seller_sku_code,
                sales_by_sku.c.style_key,
                cat.c.brand,
                cat.c.product_name,
                sales_by_sku.c.orders,
                func.coalesce(returns_by_sku.c.returns_units, 0).label("returns_units"),
                func.coalesce(returns_by_sku.c.return_units, 0).label("return_units"),
                func.coalesce(returns_by_sku.c.rto_units, 0).label("rto_units"),
                (
                    func.coalesce(returns_by_sku.c.returns_units, 0)
                    / func.nullif(sales_by_sku.c.orders, 0)
                ).label("return_pct"),
                sales_by_sku.c.last_order_date,
            )
            .outerjoin(
                returns_by_sku,
                and_(
                    returns_by_sku.c.seller_sku_code == sales_by_sku.c.seller_sku_code,
                    returns_by_sku.c.style_key == sales_by_sku.c.style_key,
                ),
            )
            .outerjoin(cat, cat.c.style_key == sales_by_sku.c.style_key)
            .filter(sales_by_sku.c.orders >= min_orders)
            .order_by(text("return_pct DESC NULLS LAST"), text("orders DESC"))
            .limit(top_n)
        )

        rows = []
        for r in q.all():
            rows.append(
                {
                    "seller_sku_code": r.seller_sku_code,
                    "style_key": r.style_key,
                    "brand": r.brand,
                    "product_name": r.product_name,
                    "orders": int(r.orders or 0),
                    "returns_units": int(r.returns_units or 0),
                    "return_units": int(r.return_units or 0),
                    "rto_units": int(r.rto_units or 0),
                    "return_pct": float(r.return_pct or 0.0),
                    "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                    "return_mode": mode_norm,
                }
            )

        return rows
    finally:
        db.close()



from datetime import datetime, timedelta
from fastapi import Query, HTTPException
from sqlalchemy import func, and_, case
from sqlalchemy.orm import Session


@app.get("/db/kpi/returns-cohort")
def db_kpi_returns_cohort(
    start: str = Query(...),
    end: str = Query(...),
    workspace_slug: str = Query("default"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    portal: str | None = Query(None),
):
    """
    Cohort = sales_month (order_date month) x return_month (return_date month)
    Joined via order_line_id so returns are attributed to original sale month.
    """
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end) + timedelta(days=1)
    except Exception:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # Brand -> style_keys subquery
        brand_norm = (brand or "").strip().lower() if brand else None
        style_key_filter = None
        if brand_norm:
            brand_style_keys_sq = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)).label("style_key"))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
                .subquery()
            )
            style_key_filter = select(brand_style_keys_sq.c.style_key)

        sales_dt = SalesRaw.order_date
        ret_dt = ReturnsRaw.return_date

        # month buckets
        sale_month = func.date_trunc("month", sales_dt).label("sale_month")
        return_month = func.date_trunc("month", ret_dt).label("return_month")

        q = (
            db.query(
                sale_month,
                return_month,
                func.coalesce(func.sum(ReturnsRaw.units), 0).label("returns_units"),
                func.coalesce(
                    func.sum(
                        case(
                            (func.upper(func.trim(ReturnsRaw.return_type)) == "RETURN", ReturnsRaw.units),
                            else_=0,
                        )
                    ),
                    0,
                ).label("return_units"),
                func.coalesce(
                    func.sum(
                        case(
                            (func.upper(func.trim(ReturnsRaw.return_type)) == "RTO", ReturnsRaw.units),
                            else_=0,
                        )
                    ),
                    0,
                ).label("rto_units"),
            )
            .join(
                ReturnsRaw,
                and_(
                    ReturnsRaw.workspace_id == SalesRaw.workspace_id,
                    ReturnsRaw.order_line_id == SalesRaw.order_line_id,
                ),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(sales_dt >= start_dt, sales_dt < end_dt)
            .filter(ret_dt.isnot(None))
            .filter(ret_dt >= start_dt, ret_dt < end_dt)
        )
        q = _apply_portal_sales(q, workspace_slug, portal)
        q = _apply_portal_returns(q, workspace_slug, portal)

        if style_key_filter is not None:
            q = q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        rows = (
            q.group_by(sale_month, return_month)
             .order_by(sale_month, return_month)
             .all()
        )

        # return as flat rows (frontend can pivot)
        out = []
        for r in rows:
            out.append(
                {
                    "sale_month": r.sale_month.date().isoformat() if r.sale_month else None,
                    "return_month": r.return_month.date().isoformat() if r.return_month else None,
                    "returns_units": int(r.returns_units or 0),
                    "return_units": int(r.return_units or 0),
                    "rto_units": int(r.rto_units or 0),
                }
            )
        return out
    finally:
        db.close()


@app.get("/db/style/details")
def db_style_details(
    workspace_slug: str = Query("default"),
    style_key: str = Query(..., description="Style ID / style_key"),
    start: str | None = Query(None, description="YYYY-MM-DD (optional)"),
    end: str | None = Query(None, description="YYYY-MM-DD (optional)"),
    monthly_limit: int = Query(12, ge=1, le=36),
):
    """
    Drawer-friendly style details:
    - catalog info (brand, product_name, live date)
    - range totals (orders/returns/rto/return% for start-end if provided)
    - monthly history from style_monthly (last N months)
    """
    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        sk = (style_key or "").strip().lower()

        # Catalog info (single row)
        cat = (
            db.query(
                func.max(CatalogRaw.brand).label("brand"),
                func.max(CatalogRaw.product_name).label("product_name"),
                func.max(CatalogRaw.style_catalogued_date).label("live_date"),
            )
            .filter(CatalogRaw.workspace_id == ws_id, CatalogRaw.style_key == sk)
            .first()
        )

        # Overall last order date (helpful in drawer)
        last_order_date = (
            db.query(func.max(SalesRaw.order_date))
            .filter(SalesRaw.workspace_id == ws_id, SalesRaw.style_key == sk)
            .scalar()
        )

        # Monthly history (snapshot table)
        mh = (
            db.query(
                StyleMonthly.month_start,
                StyleMonthly.orders,
                StyleMonthly.returns,
                StyleMonthly.return_pct,
                StyleMonthly.last_order_date,
            )
            .filter(StyleMonthly.workspace_id == ws_id, StyleMonthly.style_key == sk)
            .order_by(StyleMonthly.month_start.desc())
            .limit(monthly_limit)
            .all()
        )

        monthly = [
            {
                "month_start": r.month_start.isoformat() if r.month_start else None,
                "orders": int(r.orders or 0),
                "returns": int(r.returns or 0),
                "return_pct": r.return_pct,
                "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
            }
            for r in mh
        ]

        range_block = None
        if start and end:
            try:
                start_dt = datetime.fromisoformat(start)  # YYYY-MM-DD
                end_dt_excl = datetime.fromisoformat(end) + timedelta(days=1)
            except Exception:
                raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

            # Orders in range (sales)
            orders = (
                db.query(func.coalesce(func.sum(SalesRaw.units), 0))
                .filter(
                    SalesRaw.workspace_id == ws_id,
                    SalesRaw.style_key == sk,
                    SalesRaw.order_date.isnot(None),
                    SalesRaw.order_date >= start_dt,
                    SalesRaw.order_date < end_dt_excl,
                )
                .scalar()
            ) or 0

            # Returns in range (returns_raw) — treat null units as 1
            units_expr = func.coalesce(ReturnsRaw.units, 1)
            rtype = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

            returns_units = (
                db.query(func.coalesce(func.sum(units_expr), 0))
                .filter(
                    ReturnsRaw.workspace_id == ws_id,
                    ReturnsRaw.style_key == sk,
                    ReturnsRaw.return_date.isnot(None),
                    ReturnsRaw.return_date >= start_dt,
                    ReturnsRaw.return_date < end_dt_excl,
                )
                .scalar()
            ) or 0

            return_units = (
                db.query(func.coalesce(func.sum(units_expr), 0))
                .filter(
                    ReturnsRaw.workspace_id == ws_id,
                    ReturnsRaw.style_key == sk,
                    ReturnsRaw.return_date.isnot(None),
                    ReturnsRaw.return_date >= start_dt,
                    ReturnsRaw.return_date < end_dt_excl,
                    rtype == "RETURN",
                )
                .scalar()
            ) or 0

            rto_units = (
                db.query(func.coalesce(func.sum(units_expr), 0))
                .filter(
                    ReturnsRaw.workspace_id == ws_id,
                    ReturnsRaw.style_key == sk,
                    ReturnsRaw.return_date.isnot(None),
                    ReturnsRaw.return_date >= start_dt,
                    ReturnsRaw.return_date < end_dt_excl,
                    rtype == "RTO",
                )
                .scalar()
            ) or 0

            range_block = {
                "start": start,
                "end": end,
                "orders": int(orders),
                "returns_units": int(returns_units),
                "return_units": int(return_units),
                "rto_units": int(rto_units),
                "return_pct": (float(returns_units) / float(orders) * 100.0) if orders > 0 else None,
            }


        return {
            "workspace_slug": workspace_slug,
            "style_key": sk,
            "brand": cat.brand if cat else None,
            "product_name": cat.product_name if cat else None,
            "live_date": cat.live_date.isoformat() if (cat and cat.live_date) else None,
            "last_order_date": last_order_date.isoformat() if last_order_date else None,
            "range": range_block,
            "monthly": monthly,
        }
    finally:
        db.close()


from sqlalchemy import func, and_
from sqlalchemy.sql import exists
from fastapi import Query, HTTPException

from sqlalchemy import and_, func, select
from datetime import date

@app.get("/db/style-monthly")
def db_style_monthly(
    workspace_slug: str = Query("default"),
    month_start: str | None = Query(None, description="YYYY-MM-01; if omitted returns month totals only"),
    start: str | None = Query(None, description="YYYY-MM-01 (optional range start)"),
    end: str | None = Query(None, description="YYYY-MM-01 (optional range end)"),
    top_n: int = Query(50, ge=1, le=500),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    return_mode: str = Query("overall", description="overall | same_month"),
):
    """
    Read monthly totals.
    - overall: returns counted by return_date month (existing style_monthly behavior)
    - same_month: returns counted only when sale_month == return_month (computed from raw tables)
    """
    def _parse_month(s: str | None) -> date | None:
        if not s:
            return None
        return date.fromisoformat(s)  # expects YYYY-MM-DD like 2025-12-01

    mode = (return_mode or "overall").strip().lower()
    if mode not in ("overall", "same_month"):
        mode = "overall"

    db = SessionLocal()
    try:
        ws = db.query(Workspace).filter(Workspace.slug == workspace_slug).first()
        if not ws:
            raise HTTPException(status_code=404, detail=f"Workspace not found: {workspace_slug}")

        # -----------------------
        # Brand filter -> style_keys
        # -----------------------
        brand_norm = (brand or "").strip().lower() if brand else None
        brand_style_keys_sq = None
        if brand_norm:
            brand_style_keys_sq = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)).label("style_key"))
                .filter(
                    CatalogRaw.workspace_id == ws.id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
                .subquery()
            )

        # Optional month range filter
        start_d = _parse_month(start)
        end_d = _parse_month(end)
        month_start_d = _parse_month(month_start)

        # -----------------------
        # Month totals
        # -----------------------
        totals_rows = []

        if mode == "overall":
            # Use style_monthly snapshot (fast)
            q = db.query(StyleMonthly).filter(StyleMonthly.workspace_id == ws.id)

            if brand_style_keys_sq is not None:
                q = q.filter(func.lower(func.trim(StyleMonthly.style_key)).in_(select(brand_style_keys_sq.c.style_key)))

            if start_d:
                q = q.filter(StyleMonthly.month_start >= start_d)
            if end_d:
                q = q.filter(StyleMonthly.month_start <= end_d)

            totals = (
                q.with_entities(
                    StyleMonthly.month_start.label("month_start"),
                    func.sum(StyleMonthly.orders).label("orders"),
                    func.sum(StyleMonthly.returns).label("returns"),
                )
                .group_by(StyleMonthly.month_start)
                .order_by(StyleMonthly.month_start.desc())
                .all()
            )

            for t in totals:
                orders = int(t.orders or 0)
                returns = int(t.returns or 0)
                totals_rows.append(
                    {
                        "month_start": str(t.month_start),
                        "orders": orders,
                        "returns": returns,
                        "return_pct": (returns / orders * 100.0) if orders > 0 else None,
                        "return_mode": "overall",
                    }
                )

        else:
            # same_month: compute from raw tables (accurate)
            # month bucket = sale month
            sale_month = func.date_trunc("month", SalesRaw.order_date)

            # Orders by sale month
            orders_q = (
                db.query(
                    cast(sale_month, Date).label("month_start"),
                    func.coalesce(func.sum(SalesRaw.units), 0).label("orders"),
                )
                .filter(SalesRaw.workspace_id == ws.id)
                .filter(SalesRaw.order_date.isnot(None))
            )

            # Apply month range on sale_month using sale dates
            if start_d:
                orders_q = orders_q.filter(SalesRaw.order_date >= datetime.combine(start_d, time.min))
            if end_d:
                # end_d is month-start; include that month, so go to next month start exclusive
                end_next = (end_d.replace(day=1) + timedelta(days=32)).replace(day=1)
                orders_q = orders_q.filter(SalesRaw.order_date < datetime.combine(end_next, time.min))

            if brand_style_keys_sq is not None:
                orders_q = orders_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(select(brand_style_keys_sq.c.style_key)))

            orders_q = orders_q.group_by(cast(sale_month, Date)).subquery()

            # Returns (same_month) by sale month, join by order_line_id
            units_expr = func.coalesce(ReturnsRaw.units, 1)
            ret_q = (
                db.query(
                    cast(sale_month, Date).label("month_start"),
                    func.coalesce(func.sum(units_expr), 0).label("returns"),
                )
                .select_from(ReturnsRaw)
                .join(
                    SalesRaw,
                    and_(
                        SalesRaw.workspace_id == ws.id,
                        ReturnsRaw.workspace_id == ws.id,
                        SalesRaw.order_line_id == ReturnsRaw.order_line_id,
                    ),
                )
                .filter(SalesRaw.order_date.isnot(None))
                .filter(ReturnsRaw.return_date.isnot(None))
                .filter(func.date_trunc("month", SalesRaw.order_date) == func.date_trunc("month", ReturnsRaw.return_date))
            )

            if start_d:
                ret_q = ret_q.filter(SalesRaw.order_date >= datetime.combine(start_d, time.min))
            if end_d:
                end_next = (end_d.replace(day=1) + timedelta(days=32)).replace(day=1)
                ret_q = ret_q.filter(SalesRaw.order_date < datetime.combine(end_next, time.min))

            if brand_style_keys_sq is not None:
                ret_q = ret_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(select(brand_style_keys_sq.c.style_key)))

            ret_q = ret_q.group_by(cast(sale_month, Date)).subquery()

            # Join orders + returns
            joined = (
                db.query(
                    orders_q.c.month_start,
                    orders_q.c.orders,
                    func.coalesce(ret_q.c.returns, 0).label("returns"),
                )
                .outerjoin(ret_q, ret_q.c.month_start == orders_q.c.month_start)
                .order_by(orders_q.c.month_start.desc())
                .all()
            )

            for r in joined:
                orders = int(r.orders or 0)
                returns = int(r.returns or 0)
                totals_rows.append(
                    {
                        "month_start": str(r.month_start),
                        "orders": orders,
                        "returns": returns,
                        "return_pct": (returns / orders * 100.0) if orders > 0 else None,
                        "return_mode": "same_month",
                    }
                )

        # -----------------------
        # Optional: Top styles for a specific month
        # (We keep existing snapshot logic; for same_month top-styles use /db/kpi/top-return-styles)
        # -----------------------
        style_rows = []
        if month_start_d:
            q = db.query(StyleMonthly).filter(
                StyleMonthly.workspace_id == ws.id,
                StyleMonthly.month_start == month_start_d,
            )

            if brand_style_keys_sq is not None:
                q = q.filter(func.lower(func.trim(StyleMonthly.style_key)).in_(select(brand_style_keys_sq.c.style_key)))

            q = q.order_by(StyleMonthly.orders.desc()).limit(top_n)

            for r in q.all():
                style_rows.append(
                    {
                        "month_start": str(r.month_start),
                        "style_key": r.style_key,
                        "orders": int(r.orders or 0),
                        "returns": int(r.returns or 0),
                        "return_pct": r.return_pct,
                        "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                    }
                )

        return {
            "workspace_slug": workspace_slug,
            "filters": {
                "start": start,
                "end": end,
                "month_start": month_start,
                "top_n": int(top_n),
                "brand": brand,
                "return_mode": mode,
            },
            "month_totals": totals_rows,
            "rows": style_rows,
        }

    finally:
        db.close()


from datetime import date, datetime, time, timedelta
from typing import Optional

from fastapi import HTTPException, Query
from sqlalchemy import Integer, func

@app.get("/db/kpi/zero-sales-since-live")
def db_kpi_zero_sales_since_live(
    min_days_live: int = Query(7, ge=0),
    top_n: int = Query(100, ge=1, le=1000),
    workspace_slug: str = Query("default"),
    portal: str = Query("myntra", description="myntra|flipkart"),
    brand: Optional[str] = Query(None, description="Optional brand filter (catalog_raw.brand)"),
    sort_dir: str = Query("desc", pattern="^(asc|desc)$", description="Sort by days_live"),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        LIVE_COL = CatalogRaw.style_catalogued_date

        # sales counts per style
        sales_counts = (
            db.query(
                SalesRaw.style_key.label("StyleKey"),
                func.count().label("Orders"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .group_by(SalesRaw.style_key)
            .subquery()
        )

        # catalog one row per style (+ optional brand filter)
        cat_q = (
            db.query(
                CatalogRaw.style_key.label("StyleKey"),
                func.max(CatalogRaw.brand).label("Brand"),
                func.max(CatalogRaw.product_name).label("ProductName"),
                func.max(LIVE_COL).label("LiveDate"),
            )
            .filter(CatalogRaw.workspace_id == ws_id)
        )

        if brand and brand.strip():
            b = brand.strip().lower()
            cat_q = cat_q.filter(
                CatalogRaw.brand.isnot(None),
                func.lower(func.trim(CatalogRaw.brand)) == b,
            )

        cat = cat_q.group_by(CatalogRaw.style_key).subquery()

        days_live_expr = func.date_part("day", func.now() - cat.c.LiveDate).cast(Integer)

        q = (
            db.query(
                cat.c.StyleKey,
                cat.c.Brand,
                cat.c.ProductName,
                cat.c.LiveDate,
                days_live_expr.label("DaysLive"),
                func.coalesce(sales_counts.c.Orders, 0).label("Orders"),
            )
            .outerjoin(sales_counts, sales_counts.c.StyleKey == cat.c.StyleKey)
            .filter(cat.c.LiveDate.isnot(None))
            .filter(days_live_expr >= min_days_live)
            .filter(func.coalesce(sales_counts.c.Orders, 0) == 0)
        )

        if sort_dir == "asc":
            q = q.order_by(days_live_expr.asc())
        else:
            q = q.order_by(days_live_expr.desc())

        q = q.limit(top_n)

        result = []
        for r in q.all():
            result.append(
                {
                    "StyleKey": r.StyleKey,
                    "Brand": r.Brand,
                    "ProductName": r.ProductName,
                    "LiveDate": r.LiveDate.isoformat() if r.LiveDate else None,
                    "DaysLive": int(r.DaysLive or 0),
                    "Orders": int(r.Orders or 0),
                }
            )

        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"zero-sales failed: {e}")
    finally:
        db.close()


@app.get("/db/action-board")
def db_action_board(
    workspace_slug: str = Query("default"),
    portal: str | None = Query(None, description="myntra|flipkart"),
    row_dim: str = Query("style", description="style|sku (flipkart will auto-force sku)"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    month_start: str | None = Query(None, description="YYYY-MM-01; if omitted uses latest month"),
    min_orders: int = Query(1, ge=1, description="Minimum orders in the month to consider a row"),
    good_return_pct: float = Query(30.0, ge=0, le=100, description="Max return% to qualify as Scale Now"),
    high_return_pct: float = Query(30.0, ge=0, le=100, description="Min return% to qualify as Profit Leak"),
    top_n: int = Query(50, ge=1, le=500),
    new_days: int = Query(30, ge=0, le=365, description="Catalog live days window"),
    new_min_orders: int | None = Query(None, ge=1, description="Min orders for New Potential; defaults to min_orders"),
    new_ref: str = Query("today", pattern="^(today|month_start)$", description="Reference for new_days window"),
):
    db = SessionLocal()
    try:
        ws = db.query(Workspace).filter(Workspace.slug == workspace_slug).first()
        if not ws:
            raise HTTPException(status_code=404, detail=f"Workspace not found: {workspace_slug}")

        # --- normalize portal ---
        p = (portal or "").strip().lower()
        if p in ("fk", "flipkart"):
            p = "flipkart"
        elif p in ("mn", "myntra"):
            p = "myntra"
        else:
            p = None  # no portal filter

        # Flipkart must be SKU-first
        effective_dim = "sku" if p == "flipkart" else (row_dim or "style").strip().lower()
        if effective_dim not in ("style", "sku"):
            effective_dim = "style"

        # helper: next month start
        def _next_month_start(d: date) -> date:
            # jump to next month by overshooting end of month
            tmp = d.replace(day=28) + timedelta(days=4)
            return tmp.replace(day=1)

        # choose month_start if missing
        if not month_start:
            if effective_dim == "style":
                latest = (
                    db.query(func.max(StyleMonthly.month_start))
                    .filter(StyleMonthly.workspace_id == ws.id)
                    .scalar()
                )
                if not latest:
                    return {
                        "workspace_slug": workspace_slug,
                        "month_start": None,
                        "params": {
                            "portal": portal,
                            "row_dim": effective_dim,
                            "brand": brand,
                            "min_orders": min_orders,
                            "good_return_pct": good_return_pct,
                            "high_return_pct": high_return_pct,
                            "top_n": top_n,
                            "new_days": new_days,
                            "new_min_orders": new_min_orders,
                            "new_ref": new_ref,
                        },
                        "scale_now": [],
                        "profit_leak": [],
                        "new_potential": [],
                        "note": "No style_monthly data yet for this workspace",
                    }
                month_start = str(latest)
            else:
                # SKU mode: derive latest month from sales_raw
                latest_month_ts_q = db.query(func.max(func.date_trunc("month", SalesRaw.order_date))).filter(
                    SalesRaw.workspace_id == ws.id
                )
                latest_month_ts_q = _apply_portal_sales(latest_month_ts_q, workspace_slug, portal)
                latest_month_ts = latest_month_ts_q.scalar()

                if not latest_month_ts:
                    return {
                        "workspace_slug": workspace_slug,
                        "month_start": None,
                        "params": {
                            "portal": portal,
                            "row_dim": effective_dim,
                            "brand": brand,
                            "min_orders": min_orders,
                            "good_return_pct": good_return_pct,
                            "high_return_pct": high_return_pct,
                            "top_n": top_n,
                            "new_days": new_days,
                            "new_min_orders": new_min_orders,
                            "new_ref": new_ref,
                        },
                        "scale_now": [],
                        "profit_leak": [],
                        "new_potential": [],
                        "note": "No sales_raw data yet for this workspace",
                    }

                # latest_month_ts is a datetime-like at first day of month
                month_start = latest_month_ts.date().isoformat()

        # Parse month_start + compute month window
        ms_date = datetime.fromisoformat(month_start).date()
        ms_date = ms_date.replace(day=1)
        me_date = _next_month_start(ms_date)

        ms_dt = datetime.combine(ms_date, time.min)
        me_dt_excl = datetime.combine(me_date, time.min)

        # ref date for "new_days"
        ref_date = datetime.utcnow().date() if new_ref == "today" else ms_date
        live_cutoff = ref_date - timedelta(days=new_days)

        if new_min_orders is None:
            new_min_orders = min_orders

        # ---------- Brand filter ----------
        brand_norm = (brand or "").strip().lower() if brand else None
        brand_style_keys: list[str] | None = None

        if brand_norm:
            brand_q = (
                db.query(func.lower(func.trim(cast(CatalogRaw.style_key, String))))
                .filter(
                    CatalogRaw.workspace_id == ws.id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
            )

            # portal filter on catalog style_key prefix (fk:)
            if p == "flipkart":
                brand_q = brand_q.filter(func.trim(cast(CatalogRaw.style_key, String)).ilike("fk:%"))
            elif p == "myntra":
                brand_q = brand_q.filter(sqlalchemy.not_(func.trim(cast(CatalogRaw.style_key, String)).ilike("fk:%")))

            brand_style_keys = [r[0] for r in brand_q.distinct().all() if r and r[0]]

            if not brand_style_keys:
                return {
                    "workspace_slug": workspace_slug,
                    "month_start": ms_date.isoformat(),
                    "params": {
                        "portal": portal,
                        "row_dim": effective_dim,
                        "brand": brand,
                        "min_orders": min_orders,
                        "good_return_pct": good_return_pct,
                        "high_return_pct": high_return_pct,
                        "top_n": top_n,
                        "new_days": new_days,
                        "new_min_orders": new_min_orders,
                        "new_ref": new_ref,
                    },
                    "scale_now": [],
                    "profit_leak": [],
                    "new_potential": [],
                }

        # ==========================
        # STYLE MODE (existing logic)
        # ==========================
        if effective_dim == "style":
            base = (
                db.query(StyleMonthly)
                .filter(
                    StyleMonthly.workspace_id == ws.id,
                    StyleMonthly.month_start == ms_date.isoformat(),
                    StyleMonthly.orders >= min_orders,
                )
            )

            # portal filter on style_monthly.style_key
            if p == "flipkart":
                base = base.filter(func.trim(cast(StyleMonthly.style_key, String)).ilike("fk:%"))
            elif p == "myntra":
                base = base.filter(sqlalchemy.not_(func.trim(cast(StyleMonthly.style_key, String)).ilike("fk:%")))

            if brand_style_keys is not None:
                base = base.filter(func.lower(func.trim(cast(StyleMonthly.style_key, String))).in_(brand_style_keys))

            scale_now_q = (
                base.filter((StyleMonthly.return_pct.is_(None)) | (StyleMonthly.return_pct <= good_return_pct))
                .order_by(StyleMonthly.orders.desc())
                .limit(top_n)
            )

            profit_leak_q = (
                base.filter(StyleMonthly.return_pct.isnot(None), StyleMonthly.return_pct >= high_return_pct)
                .order_by(StyleMonthly.orders.desc())
                .limit(top_n)
            )

            # New Potential (join catalog for live date)
            new_potential_q = (
                db.query(StyleMonthly, CatalogRaw.style_catalogued_date)
                .join(
                    CatalogRaw,
                    and_(
                        CatalogRaw.workspace_id == ws.id,
                        CatalogRaw.style_key == StyleMonthly.style_key,
                    ),
                )
                .filter(
                    StyleMonthly.workspace_id == ws.id,
                    StyleMonthly.month_start == ms_date.isoformat(),
                    StyleMonthly.orders >= new_min_orders,
                    CatalogRaw.style_catalogued_date.isnot(None),
                    CatalogRaw.style_catalogued_date >= live_cutoff,
                )
            )

            if brand_norm:
                new_potential_q = new_potential_q.filter(
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )

            # portal filter on catalog in new_potential
            if p == "flipkart":
                new_potential_q = new_potential_q.filter(func.trim(cast(CatalogRaw.style_key, String)).ilike("fk:%"))
            elif p == "myntra":
                new_potential_q = new_potential_q.filter(
                    sqlalchemy.not_(func.trim(cast(CatalogRaw.style_key, String)).ilike("fk:%"))
                )

            new_potential_q = new_potential_q.order_by(StyleMonthly.orders.desc()).limit(top_n)

            def row_style(sm: StyleMonthly):
                return {
                    "style_key": sm.style_key,
                    "orders": int(sm.orders or 0),
                    "returns": int(sm.returns or 0),
                    "return_pct": sm.return_pct,
                    "last_order_date": sm.last_order_date.isoformat() if sm.last_order_date else None,
                }

            new_potential_rows = []
            for sm, scd in new_potential_q.all():
                new_potential_rows.append(
                    {
                        "style_key": sm.style_key,
                        "orders": int(sm.orders or 0),
                        "returns": int(sm.returns or 0),
                        "return_pct": sm.return_pct,
                        "last_order_date": sm.last_order_date.isoformat() if sm.last_order_date else None,
                        "style_catalogued_date": scd.isoformat() if scd else None,
                    }
                )

            return {
                "workspace_slug": workspace_slug,
                "month_start": ms_date.isoformat(),
                "params": {
                    "portal": portal,
                    "row_dim": effective_dim,
                    "brand": brand,
                    "min_orders": min_orders,
                    "good_return_pct": good_return_pct,
                    "high_return_pct": high_return_pct,
                    "top_n": top_n,
                    "new_days": new_days,
                    "new_min_orders": new_min_orders,
                    "new_ref": new_ref,
                },
                "scale_now": [row_style(r) for r in scale_now_q.all()],
                "profit_leak": [row_style(r) for r in profit_leak_q.all()],
                "new_potential": new_potential_rows,
            }

        # ==========================
        # SKU MODE (Flipkart default)
        # ==========================
        # local portal filter for returns (avoid depending on external helper)
        def _apply_portal_returns_local(q):
            if p == "flipkart":
                return q.filter(ReturnsRaw.order_line_id.like("fk:%"))
            if p == "myntra":
                return q.filter(sqlalchemy.not_(ReturnsRaw.order_line_id.like("fk:%")))
            return q

        # Sales per SKU (month)
        sales_q = (
            db.query(
                SalesRaw.seller_sku_code.label("seller_sku_code"),
                func.max(SalesRaw.style_key).label("style_key"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders"),
                func.max(SalesRaw.order_date).label("last_order_date"),
            )
            .filter(
                SalesRaw.workspace_id == ws.id,
                SalesRaw.order_date >= ms_dt,
                SalesRaw.order_date < me_dt_excl,
                SalesRaw.seller_sku_code.isnot(None),
                func.trim(SalesRaw.seller_sku_code) != "",
            )
        )
        sales_q = _apply_portal_sales(sales_q, workspace_slug, portal)

        if brand_style_keys is not None:
            sales_q = sales_q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys))

        sales_sub = sales_q.group_by(SalesRaw.seller_sku_code).subquery()

        # Returns per SKU (month) by return_date
        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        returns_q = (
            db.query(
                ReturnsRaw.seller_sku_code.label("seller_sku_code"),
                func.max(ReturnsRaw.style_key).label("style_key"),
                func.coalesce(func.sum(unit_expr), 0).label("returns"),
            )
            .filter(
                ReturnsRaw.workspace_id == ws.id,
                ReturnsRaw.return_date >= ms_dt,
                ReturnsRaw.return_date < me_dt_excl,
                ReturnsRaw.seller_sku_code.isnot(None),
                func.trim(ReturnsRaw.seller_sku_code) != "",
            )
        )
        returns_q = _apply_portal_returns_local(returns_q)

        if brand_style_keys is not None:
            returns_q = returns_q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(brand_style_keys))

        returns_sub = returns_q.group_by(ReturnsRaw.seller_sku_code).subquery()

        # Join sales + returns
        rows_q = (
            db.query(
                sales_sub.c.seller_sku_code,
                sales_sub.c.style_key,
                sales_sub.c.orders,
                func.coalesce(returns_sub.c.returns, 0).label("returns"),
                sales_sub.c.last_order_date,
            )
            .outerjoin(returns_sub, returns_sub.c.seller_sku_code == sales_sub.c.seller_sku_code)
            .filter(sales_sub.c.orders >= min_orders)
        )

        rows = rows_q.all()

        def to_row(r):
            orders = int(r.orders or 0)
            rets = int(r.returns or 0)
            pct = (rets * 100.0 / orders) if orders > 0 else 0.0
            return {
                "style_key": (r.style_key or ""),  # keep style_key for linking if needed
                "seller_sku_code": r.seller_sku_code,
                "orders": orders,
                "returns": rets,
                "return_pct": float(pct),
                "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
            }

        all_rows = [to_row(r) for r in rows]

        scale_now = [x for x in all_rows if (x["return_pct"] is None) or (x["return_pct"] <= good_return_pct)]
        profit_leak = [x for x in all_rows if (x["return_pct"] is not None) and (x["return_pct"] >= high_return_pct)]

        scale_now.sort(key=lambda x: x["orders"], reverse=True)
        profit_leak.sort(key=lambda x: x["orders"], reverse=True)

        # New Potential (SKU) via catalog live date using the SKU's max style_key
        new_potential = []
        if new_days >= 0:
            np_q = (
                db.query(
                    sales_sub.c.seller_sku_code,
                    sales_sub.c.style_key,
                    sales_sub.c.orders,
                    func.coalesce(returns_sub.c.returns, 0).label("returns"),
                    sales_sub.c.last_order_date,
                    func.max(CatalogRaw.style_catalogued_date).label("style_catalogued_date"),
                )
                .outerjoin(returns_sub, returns_sub.c.seller_sku_code == sales_sub.c.seller_sku_code)
                .join(
                    CatalogRaw,
                    and_(
                        CatalogRaw.workspace_id == ws.id,
                        CatalogRaw.style_key == sales_sub.c.style_key,
                    ),
                )
                .filter(
                    sales_sub.c.orders >= new_min_orders,
                    CatalogRaw.style_catalogued_date.isnot(None),
                    CatalogRaw.style_catalogued_date >= live_cutoff,
                )
            )

            if brand_norm:
                np_q = np_q.filter(
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )

            # portal filter on catalog rows
            if p == "flipkart":
                np_q = np_q.filter(func.trim(cast(CatalogRaw.style_key, String)).ilike("fk:%"))
            elif p == "myntra":
                np_q = np_q.filter(sqlalchemy.not_(func.trim(cast(CatalogRaw.style_key, String)).ilike("fk:%")))

            np_q = np_q.group_by(
                sales_sub.c.seller_sku_code,
                sales_sub.c.style_key,
                sales_sub.c.orders,
                returns_sub.c.returns,
                sales_sub.c.last_order_date,
            ).order_by(sales_sub.c.orders.desc()).limit(top_n)

            for r in np_q.all():
                orders = int(r.orders or 0)
                rets = int(r.returns or 0)
                pct = (rets * 100.0 / orders) if orders > 0 else 0.0
                new_potential.append(
                    {
                        "style_key": (r.style_key or ""),
                        "seller_sku_code": r.seller_sku_code,
                        "orders": orders,
                        "returns": rets,
                        "return_pct": float(pct),
                        "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                        "style_catalogued_date": r.style_catalogued_date.isoformat() if r.style_catalogued_date else None,
                    }
                )

        return {
            "workspace_slug": workspace_slug,
            "month_start": ms_date.isoformat(),
            "params": {
                "portal": portal,
                "row_dim": effective_dim,
                "brand": brand,
                "min_orders": min_orders,
                "good_return_pct": good_return_pct,
                "high_return_pct": high_return_pct,
                "top_n": top_n,
                "new_days": new_days,
                "new_min_orders": new_min_orders,
                "new_ref": new_ref,
            },
            "scale_now": scale_now[:top_n],
            "profit_leak": profit_leak[:top_n],
            "new_potential": new_potential[:top_n],
        }

    finally:
        db.close()


@app.get("/db/ads/recommendations")
def db_ads_recommendations(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    workspace_slug: str = Query("default"),
    portal: str | None = Query(None),
    brand: str | None = Query(None, description="Optional brand filter (catalog_raw.brand)"),
    new_age_days: int = Query(60, ge=0),
    min_orders: int = Query(2, ge=0),
    high_return_pct: float = Query(0.35, ge=0.0),
    in_stock_only: bool = Query(False, description="If true, only styles with total stock qty > 0"),
):
    """
    Recommendations using:
    - SalesRaw (orders)
    - ReturnsRaw (return + rto units)
    - CatalogRaw (live date)
    - MyntraWeeklyPerfRaw (latest snapshot) ONLY when portal=myntra
    - FlipkartTrafficRaw (datewise traffic) ONLY when portal=flipkart

    Flipkart rules:
    - Canonical key = fk:<seller_sku_code> (lowercased)
    - SalesRaw.style_key is listing_id -> mapped to SKU via catalog_raw
    - ReturnsRaw.style_key is mostly blank for Flipkart -> use ReturnsRaw.seller_sku_code
    """
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except Exception:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")

    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        p = _portal_norm(portal)

        # windows relative to end date (as-of)
        end_excl = end_dt + timedelta(days=1)
        recent_start = end_dt - timedelta(days=30)
        prev_start = end_dt - timedelta(days=60)
        prev_end_excl = recent_start  # end of prev window (exclusive)

        brand_norm = (brand or "").strip().lower()
        has_brand = bool(brand_norm)

        def norm_key(x: str | None) -> str:
            return (x or "").strip().lower()

        def norm_fk_sku(x: str | None) -> str:
            k = norm_key(x)
            if not k:
                return ""
            if k.startswith("fk:"):
                return k
            return "fk:" + k

        # ---------------------------------------------------------------------
        # Catalog base rows (used for live_map + Flipkart key mapping)
        # ---------------------------------------------------------------------
        cat_q = (
            db.query(
                CatalogRaw.style_key,
                CatalogRaw.seller_sku_code,
                CatalogRaw.brand,
                CatalogRaw.product_name,
                CatalogRaw.style_catalogued_date,
            )
            .filter(CatalogRaw.workspace_id == ws_id)
        )
        if has_brand:
            cat_q = cat_q.filter(
                CatalogRaw.brand.isnot(None),
                func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
            )

        cat_rows = cat_q.all()

        # ---------------------------------------------------------------------
        # Build live_map + key mapping
        # live_map keys:
        #   - myntra: style_key (normalized)
        #   - flipkart: fk:<seller_sku_code> (normalized) ✅
        # map_to_sku:
        #   - maps listing_id->sku and sku->sku (both normalized)
        # ---------------------------------------------------------------------
        live_map: dict[str, dict] = {}
        map_to_sku: dict[str, str] = {}

        if p == "flipkart":
            for r in cat_rows:
                listing_id = norm_key(r.style_key)        # listing id like fk:ktah...
                sku = norm_fk_sku(r.seller_sku_code)      # fk:mx-em...

                if not sku:
                    continue

                if listing_id:
                    map_to_sku[listing_id] = sku
                map_to_sku[sku] = sku

                meta = live_map.get(sku)
                if meta is None:
                    live_map[sku] = {
                        "live_date": r.style_catalogued_date,
                        "brand": r.brand,
                        "product_name": r.product_name,
                        "seller_sku_code": r.seller_sku_code,
                        "listing_id": r.style_key,
                    }
                else:
                    ld = meta.get("live_date")
                    if ld is None or (r.style_catalogued_date is not None and r.style_catalogued_date < ld):
                        meta["live_date"] = r.style_catalogued_date
                    if not meta.get("brand") and r.brand:
                        meta["brand"] = r.brand
                    if not meta.get("product_name") and r.product_name:
                        meta["product_name"] = r.product_name
                    if not meta.get("listing_id") and r.style_key:
                        meta["listing_id"] = r.style_key
        else:
            for r in cat_rows:
                sk = norm_key(r.style_key)
                if not sk:
                    continue
                meta = live_map.get(sk)
                if meta is None:
                    live_map[sk] = {
                        "live_date": r.style_catalogued_date,
                        "brand": r.brand,
                        "product_name": r.product_name,
                        "style_key_raw": r.style_key,
                    }
                else:
                    ld = meta.get("live_date")
                    if ld is None or (r.style_catalogued_date is not None and r.style_catalogued_date < ld):
                        meta["live_date"] = r.style_catalogued_date
                    if not meta.get("brand") and r.brand:
                        meta["brand"] = r.brand
                    if not meta.get("product_name") and r.product_name:
                        meta["product_name"] = r.product_name

        style_keys = set(live_map.keys())

        # ---------------------------------------------------------------------
        # Sales 30d (portal-safe)
        # ---------------------------------------------------------------------
        sales_30_q = (
            db.query(
                SalesRaw.style_key.label("style_key"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders_30d"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date.isnot(None))
            .filter(SalesRaw.order_date >= recent_start, SalesRaw.order_date < end_excl)
        )
        sales_30_q = _apply_portal_sales(sales_30_q, workspace_slug, portal)
        sales_30 = sales_30_q.group_by(SalesRaw.style_key).all()

        sales_30_map: dict[str, int] = {}
        for r in sales_30:
            k = norm_key(r.style_key)
            if not k:
                continue
            if p == "flipkart":
                k = map_to_sku.get(k, k)  # listing -> fk:sku OR already fk:sku
            sales_30_map[k] = sales_30_map.get(k, 0) + int(r.orders_30d or 0)

        # ---------------------------------------------------------------------
        # Sales prev 30d (portal-safe)
        # ---------------------------------------------------------------------
        sales_prev_q = (
            db.query(
                SalesRaw.style_key.label("style_key"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders_prev_30d"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date.isnot(None))
            .filter(SalesRaw.order_date >= prev_start, SalesRaw.order_date < prev_end_excl)
        )
        sales_prev_q = _apply_portal_sales(sales_prev_q, workspace_slug, portal)
        sales_prev = sales_prev_q.group_by(SalesRaw.style_key).all()

        sales_prev_map: dict[str, int] = {}
        for r in sales_prev:
            k = norm_key(r.style_key)
            if not k:
                continue
            if p == "flipkart":
                k = map_to_sku.get(k, k)
            sales_prev_map[k] = sales_prev_map.get(k, 0) + int(r.orders_prev_30d or 0)

        # ---------------------------------------------------------------------
        # Orders since live (FAST - single query)
        # ---------------------------------------------------------------------
        orders_since_live_map: dict[str, int] = {k: 0 for k in style_keys}

        if p == "flipkart":
            listing_map_q = (
                db.query(
                    func.lower(func.trim(CatalogRaw.style_key)).label("listing_norm"),
                    func.lower(func.trim(CatalogRaw.seller_sku_code)).label("sku_norm"),
                    func.min(CatalogRaw.style_catalogued_date).label("live_date"),
                )
                .filter(CatalogRaw.workspace_id == ws_id)
                .filter(CatalogRaw.style_key.isnot(None))
                .filter(CatalogRaw.seller_sku_code.isnot(None))
            )

            if has_brand:
                listing_map_q = listing_map_q.filter(
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )

            listing_subq = (
                listing_map_q.group_by(
                    func.lower(func.trim(CatalogRaw.style_key)),
                    func.lower(func.trim(CatalogRaw.seller_sku_code)),
                ).subquery()
            )

            q_cnt = (
                db.query(
                    listing_subq.c.sku_norm.label("k"),
                    func.coalesce(func.sum(SalesRaw.units), 0).label("orders_since_live"),
                )
                .filter(SalesRaw.workspace_id == ws_id)
                .filter(SalesRaw.order_date.isnot(None))
                .join(
                    listing_subq,
                    func.lower(func.trim(SalesRaw.style_key)) == listing_subq.c.listing_norm,
                )
                .filter(SalesRaw.order_date >= listing_subq.c.live_date)
                .group_by(listing_subq.c.sku_norm)
            )

            q_cnt = _apply_portal_sales(q_cnt, workspace_slug, portal)

            for r in q_cnt.all():
                k = norm_fk_sku(r.k)
                if k:
                    orders_since_live_map[k] = int(r.orders_since_live or 0)

        else:
            live_q = (
                db.query(
                    func.lower(func.trim(CatalogRaw.style_key)).label("sk_norm"),
                    func.min(CatalogRaw.style_catalogued_date).label("live_date"),
                )
                .filter(CatalogRaw.workspace_id == ws_id)
                .filter(CatalogRaw.style_key.isnot(None))
            )

            if has_brand:
                live_q = live_q.filter(
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )

            live_subq = live_q.group_by(func.lower(func.trim(CatalogRaw.style_key))).subquery()

            q_cnt = (
                db.query(
                    live_subq.c.sk_norm.label("k"),
                    func.coalesce(func.sum(SalesRaw.units), 0).label("orders_since_live"),
                )
                .filter(SalesRaw.workspace_id == ws_id)
                .filter(SalesRaw.order_date.isnot(None))
                .join(
                    live_subq,
                    func.lower(func.trim(SalesRaw.style_key)) == live_subq.c.sk_norm,
                )
                .filter(SalesRaw.order_date >= live_subq.c.live_date)
                .group_by(live_subq.c.sk_norm)
            )

            q_cnt = _apply_portal_sales(q_cnt, workspace_slug, portal)

            for r in q_cnt.all():
                k = norm_key(r.k)
                if k:
                    orders_since_live_map[k] = int(r.orders_since_live or 0)

        # ---------------------------------------------------------------------
        # Returns 30d
        # Flipkart: use seller_sku_code (style_key is mostly blank)
        # Myntra: use style_key
        # ---------------------------------------------------------------------
        unit_expr = func.coalesce(ReturnsRaw.units, 1)
        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))

        ret_key_col = ReturnsRaw.seller_sku_code if p == "flipkart" else ReturnsRaw.style_key

        returns_30_q = (
            db.query(
                ret_key_col.label("k"),
                func.coalesce(func.sum(unit_expr), 0).label("returns_units_30d"),
                func.coalesce(
                    func.sum(case((rtype_norm.in_(["RETURN", "CUSTOMER_RETURN"]), unit_expr), else_=0)),
                    0,
                ).label("return_units_30d"),
                func.coalesce(func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0).label("rto_units_30d"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date.isnot(None))
            .filter(ReturnsRaw.return_date >= recent_start, ReturnsRaw.return_date < end_excl)
        )

        # IMPORTANT: returns_raw has NO portal column -> do NOT call _apply_portal_returns()
        returns_30 = returns_30_q.group_by(ret_key_col).all()

        returns_map: dict[str, dict] = {}
        for r in returns_30:
            k = norm_fk_sku(r.k) if p == "flipkart" else norm_key(r.k)
            if not k:
                continue
            rec = returns_map.get(k)
            if rec is None:
                returns_map[k] = {
                    "returns_units_30d": int(r.returns_units_30d or 0),
                    "return_units_30d": int(r.return_units_30d or 0),
                    "rto_units_30d": int(r.rto_units_30d or 0),
                }
            else:
                rec["returns_units_30d"] += int(r.returns_units_30d or 0)
                rec["return_units_30d"] += int(r.return_units_30d or 0)
                rec["rto_units_30d"] += int(r.rto_units_30d or 0)

        # ---------------------------------------------------------------------
        # Myntra weekly snapshot ONLY when portal=myntra
        # ---------------------------------------------------------------------
        latest_myntra_ingested = None
        weekly_map: dict[str, dict] = {}

        if p == "myntra":
            latest_myntra_ingested = (
                db.query(func.max(MyntraWeeklyPerfRaw.ingested_at))
                .filter(MyntraWeeklyPerfRaw.workspace_id == ws_id)
                .scalar()
            )

            if latest_myntra_ingested is not None:
                weekly = (
                    db.query(
                        MyntraWeeklyPerfRaw.style_key.label("style_key"),
                        func.sum(MyntraWeeklyPerfRaw.impressions).label("impressions"),
                        func.sum(MyntraWeeklyPerfRaw.clicks).label("clicks"),
                        func.sum(MyntraWeeklyPerfRaw.add_to_carts).label("add_to_carts"),
                        func.sum(MyntraWeeklyPerfRaw.purchases).label("purchases"),
                    )
                    .filter(MyntraWeeklyPerfRaw.workspace_id == ws_id)
                    .filter(MyntraWeeklyPerfRaw.ingested_at == latest_myntra_ingested)
                    .group_by(MyntraWeeklyPerfRaw.style_key)
                    .all()
                )

                weekly_map = {
                    norm_key(w.style_key): {
                        "snapshot_at": latest_myntra_ingested,
                        "impressions": int(w.impressions or 0),
                        "clicks": int(w.clicks or 0),
                        "add_to_carts": int(w.add_to_carts or 0),
                        "purchases": int(w.purchases or 0),
                    }
                    for w in weekly
                }

        # ---------------------------------------------------------------------
        # Flipkart traffic (datewise) ONLY when portal=flipkart
        # ---------------------------------------------------------------------
        latest_fk_traffic_ingested = None
        fk_traffic_map: dict[str, dict] = {}

        if p == "flipkart":
            latest_fk_traffic_ingested = (
                db.query(func.max(FlipkartTrafficRaw.ingested_at))
                .filter(FlipkartTrafficRaw.workspace_id == ws_id)
                .scalar()
            )

            # Aggregate traffic over the dashboard date window (start_dt..end_dt)
            traffic_rows = (
                db.query(
                    func.lower(func.trim(FlipkartTrafficRaw.seller_sku_code)).label("sku"),
                    func.coalesce(func.sum(FlipkartTrafficRaw.product_views), 0).label("impressions"),
                    func.coalesce(func.sum(FlipkartTrafficRaw.product_clicks), 0).label("clicks"),
                    func.coalesce(func.sum(FlipkartTrafficRaw.sales_qty), 0).label("purchases"),
                )
                .filter(FlipkartTrafficRaw.workspace_id == ws_id)
                .filter(FlipkartTrafficRaw.impression_date.isnot(None))
                .filter(FlipkartTrafficRaw.impression_date >= start_dt.date())
                .filter(FlipkartTrafficRaw.impression_date <= end_dt.date())
                .group_by(func.lower(func.trim(FlipkartTrafficRaw.seller_sku_code)))
                .all()
            )

            for t in traffic_rows:
                k = norm_fk_sku(t.sku)
                if k:
                    fk_traffic_map[k] = {
                        "snapshot_at": latest_fk_traffic_ingested,
                        "impressions": int(t.impressions or 0),
                        "clicks": int(t.clicks or 0),
                        "purchases": int(t.purchases or 0),
                    }

        # ---------------------------------------------------------------------
        # Latest stock snapshot
        # ---------------------------------------------------------------------
        latest_stock_ingested = (
            db.query(func.max(StockRaw.ingested_at))
            .filter(StockRaw.workspace_id == ws_id)
            .scalar()
        )

        style_stock_qty_map: dict[str, int] = {}
        if latest_stock_ingested is not None:
            if p == "flipkart":
                stock_style_rows = (
                    db.query(
                        CatalogRaw.seller_sku_code.label("k"),
                        func.sum(StockRaw.qty).label("style_total_qty"),
                    )
                    .join(
                        StockRaw,
                        and_(
                            StockRaw.workspace_id == CatalogRaw.workspace_id,
                            StockRaw.seller_sku_code == CatalogRaw.seller_sku_code,
                        ),
                    )
                    .filter(CatalogRaw.workspace_id == ws_id)
                    .filter(StockRaw.workspace_id == ws_id)
                    .filter(StockRaw.ingested_at == latest_stock_ingested)
                    .group_by(CatalogRaw.seller_sku_code)
                    .all()
                )
                style_stock_qty_map = {norm_fk_sku(r.k): int(r.style_total_qty or 0) for r in stock_style_rows}
            else:
                stock_style_rows = (
                    db.query(
                        CatalogRaw.style_key.label("k"),
                        func.sum(StockRaw.qty).label("style_total_qty"),
                    )
                    .join(
                        StockRaw,
                        and_(
                            StockRaw.workspace_id == CatalogRaw.workspace_id,
                            StockRaw.seller_sku_code == CatalogRaw.seller_sku_code,
                        ),
                    )
                    .filter(CatalogRaw.workspace_id == ws_id)
                    .filter(StockRaw.workspace_id == ws_id)
                    .filter(StockRaw.ingested_at == latest_stock_ingested)
                    .group_by(CatalogRaw.style_key)
                    .all()
                )
                style_stock_qty_map = {norm_key(r.k): int(r.style_total_qty or 0) for r in stock_style_rows}

        def safe_pct(num: float, den: float):
            if den is None or den == 0:
                return None
            return float(num) / float(den)

        def momentum(orders_30: int, orders_prev_30: int):
            if orders_prev_30 and orders_prev_30 > 0:
                return (orders_30 - orders_prev_30) / float(orders_prev_30)
            if orders_30 and orders_30 > 0:
                return 1.0
            return 0.0

        rank = {
            "STOP (High Returns)": 0,
            "SCALE": 1,
            "TRENDING PUSH": 2,
            "PUSH (New Discovery)": 3,
            "PUSH (Zero-Sale)": 4,
            "WATCH": 5,
        }

        out = []
        for sk in sorted(style_keys):
            meta = live_map.get(sk) or {}

            style_total_qty = int(style_stock_qty_map.get(sk, 0))
            in_stock = style_total_qty > 0
            if in_stock_only and not in_stock:
                continue

            live_date = meta.get("live_date")
            age_days = None
            if live_date is not None:
                age_days = (end_dt.date() - live_date.date()).days

            o30 = int(sales_30_map.get(sk, 0))
            oprev = int(sales_prev_map.get(sk, 0))
            mom = momentum(o30, oprev)

            r = returns_map.get(sk, {"returns_units_30d": 0, "return_units_30d": 0, "rto_units_30d": 0})
            ret_units = int(r["returns_units_30d"])
            ret_only = int(r["return_units_30d"])
            rto_units = int(r["rto_units_30d"])

            return_pct_30d = safe_pct(ret_units, o30)
            rto_share = safe_pct(rto_units, ret_units)
            return_share = safe_pct(ret_only, ret_units)

            # traffic / weekly metrics
            if p == "myntra":
                wk = weekly_map.get(sk, {})
                impressions = int(wk.get("impressions", 0))
                clicks = int(wk.get("clicks", 0))
                add_to_carts = int(wk.get("add_to_carts", 0))
                purchases = int(wk.get("purchases", 0))
                snapshot_at = None if not wk else wk.get("snapshot_at").isoformat()
            elif p == "flipkart":
                tk = fk_traffic_map.get(sk, {})
                impressions = int(tk.get("impressions", 0))
                clicks = int(tk.get("clicks", 0))
                add_to_carts = 0  # Flipkart traffic file doesn't have ATC
                purchases = int(tk.get("purchases", 0))
                snapshot_at = None if not tk or tk.get("snapshot_at") is None else tk.get("snapshot_at").isoformat()
            else:
                impressions = clicks = add_to_carts = purchases = 0
                snapshot_at = None

            orders_since_live = int(orders_since_live_map.get(sk, 0))

            tag = "WATCH"
            why = "Monitor performance"

            if o30 >= min_orders and return_pct_30d is not None and return_pct_30d >= high_return_pct:
                tag = "STOP (High Returns)"
                if rto_share is not None and rto_share >= 0.6:
                    why = "High returns — RTO heavy"
                else:
                    why = "High returns — post-delivery returns"
            elif o30 >= min_orders and return_pct_30d is not None and return_pct_30d < high_return_pct:
                tag = "SCALE"
                why = "Good orders & low returns"
            elif o30 >= min_orders and mom is not None and mom >= 0.2:
                tag = "TRENDING PUSH"
                why = "Momentum up & demand building"
            elif (
                p == "myntra"
                and latest_myntra_ingested is not None
                and age_days is not None
                and age_days <= new_age_days
                and o30 < min_orders
                and impressions == 0
            ):
                tag = "PUSH (New Discovery)"
                why = "New style — needs exposure (0 impressions)"
            elif age_days is not None and age_days > new_age_days and orders_since_live == 0:
                tag = "PUSH (Zero-Sale)"
                why = "Live but 0 orders — push discovery"

            display_key = meta.get("seller_sku_code") if p == "flipkart" else (meta.get("style_key_raw") or sk)

            out.append(
                {
                    "style_key": display_key,
                    "listing_id": meta.get("listing_id") if p == "flipkart" else None,
                    "brand": meta.get("brand"),
                    "product_name": meta.get("product_name"),
                    "live_date": None if live_date is None else live_date.isoformat(),
                    "age_days": age_days,
                    "orders_30d": o30,
                    "orders_prev_30d": oprev,
                    "momentum": mom,
                    "returns_units_30d": ret_units,
                    "return_units_30d": ret_only,
                    "rto_units_30d": rto_units,
                    "return_pct_30d": return_pct_30d,
                    "rto_share_30d": rto_share,
                    "return_share_30d": return_share,
                    "orders_since_live": orders_since_live,
                    "snapshot_at": snapshot_at,
                    "impressions": impressions,
                    "clicks": clicks,
                    "add_to_carts": add_to_carts,
                    "purchases": purchases,
                    "tag": tag,
                    "why": why,
                    "style_total_qty": style_total_qty,
                    "in_stock": in_stock,
                }
            )

        out.sort(key=lambda x: (rank.get(x["tag"], 99), -(x["orders_30d"] or 0), (x["style_key"] or "")))

        return {
            "workspace_slug": workspace_slug,
            "as_of": end,
            "params": {
                "portal": p,
                "brand": brand,
                "new_age_days": new_age_days,
                "min_orders": min_orders,
                "high_return_pct": high_return_pct,
                "latest_myntra_snapshot_at": None
                if latest_myntra_ingested is None
                else latest_myntra_ingested.isoformat(),
                "latest_flipkart_traffic_snapshot_at": None
                if latest_fk_traffic_ingested is None
                else latest_fk_traffic_ingested.isoformat(),
                "latest_stock_snapshot_at": None
                if latest_stock_ingested is None
                else latest_stock_ingested.isoformat(),
            },
            "rows": out,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ads recommendations failed: {e}")
    finally:
        db.close()



# -----------------------------------------------------------------------------
# PHASE 10A — RETURNS INSIGHTS
# -----------------------------------------------------------------------------

@app.get("/db/returns/summary")
def db_returns_summary(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str = Query("default"),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        # Orders (sales has no qty column => count rows)
        orders = (
            db.query(func.count(SalesRaw.id))
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
            .scalar()
        ) or 0

        rtype_norm = func.upper(func.trim(ReturnsRaw.return_type))

        returns_agg = (
            db.query(
                func.coalesce(func.sum(ReturnsRaw.units), 0).label("returns_units"),
                func.coalesce(func.sum(case((rtype_norm == "RTO", ReturnsRaw.units), else_=0)), 0).label("rto_units"),
                func.coalesce(func.sum(case((rtype_norm == "RETURN", ReturnsRaw.units), else_=0)), 0).label("return_units"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start_dt)
            .filter(ReturnsRaw.return_date < end_dt_excl)
            .first()
        )

        returns_units = int(returns_agg.returns_units or 0)
        rto_units = int(returns_agg.rto_units or 0)
        return_units = int(returns_agg.return_units or 0)

        if orders > 0:
            return_pct = (returns_units / orders) * 100.0
            rto_pct = (rto_units / orders) * 100.0
            return_only_pct = (return_units / orders) * 100.0
        else:
            return_pct = rto_pct = return_only_pct = 0.0

        return {
            "workspace_slug": workspace_slug,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "orders": int(orders),
            "returns_units": returns_units,
            "return_pct": float(return_pct),
            "rto_units": rto_units,
            "rto_pct": float(rto_pct),
            "return_units": return_units,
            "return_only_pct": float(return_only_pct),
        }
    finally:
        db.close()


@app.get("/db/returns/reasons")
def db_returns_reasons(
    start: date = Query(...),
    end: date = Query(...),
    top_n: int = Query(20, ge=1, le=200),
    workspace_slug: str = Query("default"),
    portal: str = Query("myntra", description="myntra|flipkart"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    style_key: str | None = Query(None, description="Optional style_key to filter"),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        portal_norm = (portal or "myntra").strip().lower()

        rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))
        unit_expr = func.coalesce(ReturnsRaw.units, 1)

        # -------------------------
        # Choose reason field by portal
        # -------------------------
        raw_json = cast(ReturnsRaw.raw_json, JSONB)

        if portal_norm == "flipkart":
            # Flipkart: use return_sub_reason
            reason_raw = raw_json["return_sub_reason"].astext
            reason_norm = func.lower(func.trim(func.coalesce(reason_raw, "")))

            # For FK we show raw subreason (no Myntra bucketing)
            reason = case(
                (rtype_norm == "RTO", "RTO_NO_REASON"),
                else_=func.coalesce(func.nullif(func.trim(reason_raw), ""), "NO_REASON"),
            )
        else:
            # Myntra: use return_reason + bucket mapping
            reason_raw = raw_json["return_reason"].astext
            reason_norm = func.lower(func.trim(func.coalesce(reason_raw, "")))

            reason = case(
                (rtype_norm == "RTO", "RTO_NO_REASON"),
                else_=_reason_bucket_expr(reason_norm),
            )

        # -------------------------
        # Base query (with portal filter)
        # -------------------------
        q = (
            db.query(
                reason.label("reason"),
                func.coalesce(func.sum(unit_expr), 0).label("returns_units"),
                func.coalesce(
                    func.sum(case((rtype_norm == "RTO", unit_expr), else_=0)), 0
                ).label("rto_units"),
                func.coalesce(
                    func.sum(
                        case(
                            (rtype_norm.in_(["CUSTOMER_RETURN", "RETURN"]), unit_expr),
                            else_=0,
                        )
                    ),
                    0,
                ).label("return_units"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start_dt)
            .filter(ReturnsRaw.return_date < end_dt_excl)
        )

        # ✅ apply portal filter (this fixes Myntra showing Flipkart reasons)
        q = _apply_portal_returns(q, workspace_slug, portal_norm)

        # Optional style_key filter
        if style_key:
            sk = style_key.strip().lower()
            q = q.filter(func.lower(func.trim(ReturnsRaw.style_key)) == sk)

        # Optional brand filter via catalog style_key set
        if brand:
            b = brand.strip().lower()
            brand_style_keys_q = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == b,
                )
                .distinct()
            )
            brand_style_keys_q = _apply_portal_catalog(brand_style_keys_q, portal_norm)

            q = q.filter(func.lower(func.trim(ReturnsRaw.style_key)).in_(brand_style_keys_q))

        q = (
            q.group_by(reason)
             .order_by(func.coalesce(func.sum(unit_expr), 0).desc())
             .limit(top_n)
        )

        rows = q.all()
        total_units = sum(int(r.returns_units or 0) for r in rows) or 0

        out = []
        for r in rows:
            units = int(r.returns_units or 0)
            pct = (units * 100.0 / total_units) if total_units > 0 else 0.0
            out.append(
                {
                    "reason": r.reason,
                    "returns_units": units,
                    "rto_units": int(r.rto_units or 0),
                    "return_units": int(r.return_units or 0),
                    "pct_of_top": float(pct),
                }
            )

        return {
            "workspace_slug": workspace_slug,
            "portal": portal_norm,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "top_n": int(top_n),
            "rows": out,
        }
    finally:
        db.close()


@app.get("/db/returns/style-wise")
def db_returns_style_wise(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str = Query("default"),
    portal: str = Query("myntra"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    top_n: int = Query(50, ge=1, le=500),
    min_orders: int = Query(10, ge=0, le=1000000),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        brand_norm = (brand or "").strip().lower() if brand else None

        # -------------------------
        # Brand style-key set from catalog (optional)
        # -------------------------
        brand_style_keys_q = None
        if brand_norm:
            brand_style_keys_q = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
            )
            # portal filter (exclude fk rows when portal=myntra, include only fk when portal=flipkart)
            brand_style_keys_q = _apply_portal_catalog(brand_style_keys_q, portal)

        # -------------------------
        # Sales counts per style
        # -------------------------
        sales_counts_q = (
            db.query(
                SalesRaw.style_key.label("style_key"),
                func.count(SalesRaw.id).label("orders"),
                func.max(SalesRaw.order_date).label("last_order_date"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
        )

        sales_counts_q = _apply_portal_sales(sales_counts_q, workspace_slug, portal)

        if brand_style_keys_q is not None:
            sales_counts_q = sales_counts_q.filter(
                func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys_q)
            )

        sales_counts = sales_counts_q.group_by(SalesRaw.style_key).subquery()

        # -------------------------
        # Returns counts per style
        # -------------------------
        rtype_norm = func.upper(func.trim(ReturnsRaw.return_type))

        returns_counts_q = (
            db.query(
                ReturnsRaw.style_key.label("style_key"),
                func.coalesce(func.sum(ReturnsRaw.units), 0).label("returns_units"),
                func.coalesce(
                    func.sum(case((rtype_norm == "RTO", ReturnsRaw.units), else_=0)),
                    0,
                ).label("rto_units"),
                func.coalesce(
                    func.sum(
                        case(
                            (rtype_norm.in_(["CUSTOMER_RETURN", "RETURN"]), ReturnsRaw.units),
                            else_=0,
                        )
                    ),
                    0,
                ).label("return_units"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start_dt)
            .filter(ReturnsRaw.return_date < end_dt_excl)
        )

        returns_counts_q = _apply_portal_returns(returns_counts_q, workspace_slug, portal)

        if brand_style_keys_q is not None:
            returns_counts_q = returns_counts_q.filter(
                func.lower(func.trim(ReturnsRaw.style_key)).in_(brand_style_keys_q)
            )

        returns_counts = returns_counts_q.group_by(ReturnsRaw.style_key).subquery()

        # -------------------------
        # Catalog (brand + product)
        # -------------------------
        cat_q = (
            db.query(
                CatalogRaw.style_key.label("style_key"),
                func.max(CatalogRaw.brand).label("brand"),
                func.max(CatalogRaw.product_name).label("product_name"),
            )
            .filter(CatalogRaw.workspace_id == ws_id)
        )

        cat_q = _apply_portal_catalog(cat_q, portal)

        if brand_norm:
            cat_q = cat_q.filter(
                CatalogRaw.brand.isnot(None),
                func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
            )

        cat = cat_q.group_by(CatalogRaw.style_key).subquery()

        # -------------------------
        # Final output
        # -------------------------
        q = (
            db.query(
                sales_counts.c.style_key,
                func.coalesce(cat.c.brand, "").label("brand"),
                func.coalesce(cat.c.product_name, "").label("product_name"),
                sales_counts.c.orders,
                func.coalesce(returns_counts.c.returns_units, 0).label("returns_units"),
                func.coalesce(returns_counts.c.return_units, 0).label("return_units"),
                func.coalesce(returns_counts.c.rto_units, 0).label("rto_units"),
                sales_counts.c.last_order_date,
            )
            .outerjoin(returns_counts, returns_counts.c.style_key == sales_counts.c.style_key)
            .outerjoin(cat, cat.c.style_key == sales_counts.c.style_key)
            .filter(sales_counts.c.orders >= min_orders)
        )

        rows = q.all()

        out = []
        for r in rows:
            orders = int(r.orders or 0)
            returns_units = int(r.returns_units or 0)
            pct = (returns_units * 100.0 / orders) if orders > 0 else 0.0

            out.append(
                {
                    "style_key": r.style_key,
                    "brand": r.brand,
                    "product_name": r.product_name,
                    "orders": orders,
                    "returns_units": returns_units,
                    "return_units": int(r.return_units or 0),
                    "rto_units": int(r.rto_units or 0),
                    "return_pct": float(pct),
                    "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                    "window": {"start": start.isoformat(), "end": end.isoformat()},
                }
            )

        out.sort(key=lambda x: x["return_pct"], reverse=True)
        return out[:top_n]

    finally:
        db.close()

from sqlalchemy import func, case

@app.get("/db/returns/size-kpi")
def returns_size_kpi(
    start: str = Query(...),
    end: str = Query(...),
    workspace_slug: str = Query("default"),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        # --- 1) ORDERS by SKU (sales_raw) ---
        sales_rows = (
            db.query(
                SalesRaw.seller_sku_code.label("sku"),
                func.count().label("orders"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start)
            .filter(SalesRaw.order_date <= end)
            .filter(SalesRaw.seller_sku_code.isnot(None))
            .group_by(SalesRaw.seller_sku_code)
            .all()
        )

        orders_by_size = {}
        for r in sales_rows:
            size = _extract_size_from_sku(r.sku)
            orders_by_size[size] = orders_by_size.get(size, 0) + int(r.orders or 0)

        # --- 2) RETURNS by SKU (returns_raw) ---
        returns_rows = (
            db.query(
                ReturnsRaw.seller_sku_code.label("sku"),
                func.coalesce(func.sum(ReturnsRaw.units), 0).label("returns_units"),
                func.coalesce(
                    func.sum(case((func.upper(ReturnsRaw.return_type) == "RTO", ReturnsRaw.units), else_=0)),
                    0,
                ).label("rto_units"),
                func.coalesce(
                    func.sum(case((func.upper(ReturnsRaw.return_type) == "RETURN", ReturnsRaw.units), else_=0)),
                    0,
                ).label("return_units"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start)
            .filter(ReturnsRaw.return_date <= end)
            .filter(ReturnsRaw.seller_sku_code.isnot(None))
            .group_by(ReturnsRaw.seller_sku_code)
            .all()
        )

        returns_by_size = {}
        rto_by_size = {}
        ret_by_size = {}

        for r in returns_rows:
            size = _extract_size_from_sku(r.sku)
            returns_by_size[size] = returns_by_size.get(size, 0) + int(r.returns_units or 0)
            rto_by_size[size] = rto_by_size.get(size, 0) + int(r.rto_units or 0)
            ret_by_size[size] = ret_by_size.get(size, 0) + int(r.return_units or 0)

        # --- 3) Build output (union sizes) ---
        sizes = set(orders_by_size.keys()) | set(returns_by_size.keys())

        size_order = {"XS": 1, "S": 2, "M": 3, "L": 4, "XL": 5, "XXL": 6, "XXXL": 7, "NO_SIZE": 99}

        out = []
        for size in sizes:
            orders = int(orders_by_size.get(size, 0))
            returns_units = int(returns_by_size.get(size, 0))
            rto_units = int(rto_by_size.get(size, 0))
            return_units = int(ret_by_size.get(size, 0))

            return_pct = (returns_units / orders * 100.0) if orders > 0 else None
            rto_pct = (rto_units / orders * 100.0) if orders > 0 else None
            return_only_pct = (return_units / orders * 100.0) if orders > 0 else None

            out.append(
                {
                    "size": size,
                    "orders": orders,
                    "returns_units": returns_units,
                    "rto_units": rto_units,
                    "return_units": return_units,
                    "return_pct": return_pct,
                    "rto_pct": rto_pct,
                    "return_only_pct": return_only_pct,
                }
            )

        out.sort(key=lambda r: size_order.get(r["size"], 50))

        return {
            "workspace_slug": workspace_slug,
            "window": {"start": start, "end": end},
            "rows": out,
        }

    finally:
        db.close()

def _json_reason_expr():
    # returns_raw.raw_json is TEXT, cast to JSONB and extract return_reason
    return func.jsonb_extract_path_text(cast(ReturnsRaw.raw_json, JSONB), "return_reason")


def _build_reason_heatmap(
    db,
    ws_id: str,
    workspace_slug: str,
    start: str,
    end: str,
    row_dim: str,  # "style" or "sku"
    portal: str | None = None,
    top_reasons: int = 10,
    top_rows: int = 30,
    brand: str | None = None,
):
    p = _portal_norm(portal)
    is_fk = (p == "flipkart")

    reason_expr = (
        func.jsonb_extract_path_text(cast(ReturnsRaw.raw_json, JSONB), "return_sub_reason")
        if p == "flipkart"
        else _json_reason_expr()
    )

    brand_norm = (brand or "").strip().lower() if brand else None

    # Brand -> style_keys subquery
    brand_style_keys_q = None
    if brand_norm:
        brand_style_keys_q = (
            db.query(func.lower(func.trim(CatalogRaw.style_key)))
            .filter(
                CatalogRaw.workspace_id == ws_id,
                CatalogRaw.style_key.isnot(None),
                CatalogRaw.brand.isnot(None),
                func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
            )
            .distinct()
        )

    if row_dim == "style":
        row_key_col = ReturnsRaw.style_key
        row_key_label = "style_key"

        orders_q = (
            db.query(
                SalesRaw.style_key.label("row_key"),
                func.count().label("orders"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start)
            .filter(SalesRaw.order_date <= end)
        )

        orders_q = _apply_portal_sales(orders_q, workspace_slug, portal)


        if brand_style_keys_q is not None:
            orders_q = orders_q.filter(
                SalesRaw.style_key.isnot(None),
                func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys_q),
            )

        orders_q = orders_q.group_by(SalesRaw.style_key)

    else:
        row_key_col = ReturnsRaw.seller_sku_code
        row_key_label = "seller_sku_code"

        orders_q = (
            db.query(
                SalesRaw.seller_sku_code.label("row_key"),
                func.count().label("orders"),
                func.max(SalesRaw.style_key).label("style_key"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start)
            .filter(SalesRaw.order_date <= end)
            .filter(SalesRaw.seller_sku_code.isnot(None))
        )

        if brand_style_keys_q is not None:
            orders_q = orders_q.filter(
                SalesRaw.style_key.isnot(None),
                func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys_q),
            )

        orders_q = orders_q.group_by(SalesRaw.seller_sku_code)

    orders_rows = orders_q.all()
    orders_map = {str(r.row_key): int(r.orders or 0) for r in orders_rows if r.row_key is not None}

    sku_style_map = {}
    if row_dim == "sku":
        for r in orders_rows:
            if r.row_key is None:
                continue
            sku_style_map[str(r.row_key)] = str(r.style_key) if getattr(r, "style_key", None) is not None else None

    base = (
        db.query(
            row_key_col.label("row_key"),
            reason_expr.label("raw_reason"),
            ReturnsRaw.return_type.label("return_type"),
            func.coalesce(func.sum(ReturnsRaw.units), 0).label("units"),
            func.max(ReturnsRaw.style_key).label("style_key"),
        )
        .filter(ReturnsRaw.workspace_id == ws_id)
        .filter(ReturnsRaw.return_date >= start)
        .filter(ReturnsRaw.return_date <= end)
    )

    base = _apply_portal_returns(base, workspace_slug, portal)

    if row_dim == "sku":
        base = base.filter(ReturnsRaw.seller_sku_code.isnot(None))

    if brand_style_keys_q is not None:
        base = base.filter(
            ReturnsRaw.style_key.isnot(None),
            func.lower(func.trim(ReturnsRaw.style_key)).in_(brand_style_keys_q),
        )

    agg = base.group_by(row_key_col, reason_expr, ReturnsRaw.return_type).all()

    totals = {}
    for r in agg:
        rk = str(r.row_key) if r.row_key is not None else None
        if not rk:
            continue
        clean = heatmap_reason_key(r.raw_reason, r.return_type, portal)
        totals[clean] = totals.get(clean, 0) + int(r.units or 0)

    top_reason_keys = [k for k, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:top_reasons]]

    row_totals = {}
    cell_units = {}

    for r in agg:
        rk = str(r.row_key) if r.row_key is not None else None
        if not rk:
            continue
        clean = heatmap_reason_key(r.raw_reason, r.return_type, portal)
        if clean not in top_reason_keys:
            continue

        u = int(r.units or 0)
        cell_units[(rk, clean)] = cell_units.get((rk, clean), 0) + u
        row_totals[rk] = row_totals.get(rk, 0) + u

    top_row_keys = [k for k, _ in sorted(row_totals.items(), key=lambda x: x[1], reverse=True)[:top_rows]]

    style_meta = {}
    if top_row_keys:
        style_keys = set()
        if row_dim == "style":
            style_keys = set(top_row_keys)
        else:
            for sku in top_row_keys:
                st = sku_style_map.get(sku)
                if st:
                    style_keys.add(st)

        if style_keys:
            cats_q = (
                db.query(
                    CatalogRaw.style_key,
                    CatalogRaw.brand,
                    CatalogRaw.product_name,
                )
                .filter(CatalogRaw.workspace_id == ws_id)
                .filter(CatalogRaw.style_key.in_(list(style_keys)))
            )

            if brand_norm:
                cats_q = cats_q.filter(
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )

            cats = cats_q.all()

            for c in cats:
                style_meta[str(c.style_key)] = {
                    "brand": c.brand,
                    "product_name": c.product_name,
                }

    rows = []
    for rk in top_row_keys:
        if row_dim == "style":
            st = rk
        else:
            st = sku_style_map.get(rk)

        meta = style_meta.get(st or "", {}) if st else {}
        rows.append(
            {
                row_key_label: rk,
                "style_key": st,
                "brand": meta.get("brand"),
                "product_name": meta.get("product_name"),
                "orders": int(orders_map.get(rk, 0)),
                "returns_units": int(row_totals.get(rk, 0)),
            }
        )

    cols = [{"reason": r} for r in top_reason_keys]

    matrix_units = []
    matrix_pct = []
    for rk in top_row_keys:
        o = int(orders_map.get(rk, 0))
        urow = []
        prow = []
        for reason in top_reason_keys:
            u = int(cell_units.get((rk, reason), 0))
            urow.append(u)
            prow.append((u / o * 100.0) if o > 0 else None)
        matrix_units.append(urow)
        matrix_pct.append(prow)

    return {
        "row_dim": row_dim,
        "window": {"start": start, "end": end},
        "top_reasons": int(top_reasons),
        "top_rows": int(top_rows),
        "rows": rows,
        "cols": cols,
        "matrix_units": matrix_units,
        "matrix_pct": matrix_pct,
    }
def _build_reason_heatmap(
    db,
    ws_id: str,
    workspace_slug: str,
    start: str,
    end: str,
    row_dim: str,  # "style" or "sku"
    portal: str | None = None,
    top_reasons: int = 10,
    top_rows: int = 30,
    brand: str | None = None,
):
    p = _portal_norm(portal)

    # Flipkart: use return_sub_reason directly from json
    # Myntra: use existing reason expr (already supports bucketing via heatmap_reason_key)
    reason_expr = (
        func.jsonb_extract_path_text(cast(ReturnsRaw.raw_json, JSONB), "return_sub_reason")
        if p == "flipkart"
        else _json_reason_expr()
    )

    brand_norm = (brand or "").strip().lower() if brand else None

    # Brand -> style_keys subquery (from catalog)
    brand_style_keys_q = None
    if brand_norm:
        brand_style_keys_q = (
            db.query(func.lower(func.trim(CatalogRaw.style_key)))
            .filter(
                CatalogRaw.workspace_id == ws_id,
                CatalogRaw.style_key.isnot(None),
                CatalogRaw.brand.isnot(None),
                func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
            )
            .distinct()
        )
        # IMPORTANT: portal-safe catalog filtering
        brand_style_keys_q = _apply_portal_catalog(brand_style_keys_q, portal)

    # -------------------------
    # ORDERS MAP (from sales)
    # -------------------------
    if row_dim == "style":
        row_key_col = ReturnsRaw.style_key
        row_key_label = "style_key"

        orders_q = (
            db.query(
                SalesRaw.style_key.label("row_key"),
                func.count().label("orders"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start)
            .filter(SalesRaw.order_date <= end)
        )

        # IMPORTANT: apply portal filter ONCE
        orders_q = _apply_portal_sales(orders_q, workspace_slug, portal)

        if brand_style_keys_q is not None:
            orders_q = orders_q.filter(
                SalesRaw.style_key.isnot(None),
                func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys_q),
            )

        orders_q = orders_q.group_by(SalesRaw.style_key)

    else:
        row_key_col = ReturnsRaw.seller_sku_code
        row_key_label = "seller_sku_code"

        orders_q = (
            db.query(
                SalesRaw.seller_sku_code.label("row_key"),
                func.count().label("orders"),
                func.max(SalesRaw.style_key).label("style_key"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start)
            .filter(SalesRaw.order_date <= end)
            .filter(SalesRaw.seller_sku_code.isnot(None))
        )

        # IMPORTANT: portal-safe sales filtering for SKU path (this was missing)
        orders_q = _apply_portal_sales(orders_q, workspace_slug, portal)

        if brand_style_keys_q is not None:
            orders_q = orders_q.filter(
                SalesRaw.style_key.isnot(None),
                func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys_q),
            )

        orders_q = orders_q.group_by(SalesRaw.seller_sku_code)

    orders_rows = orders_q.all()
    orders_map = {str(r.row_key): int(r.orders or 0) for r in orders_rows if r.row_key is not None}

    sku_style_map = {}
    if row_dim == "sku":
        for r in orders_rows:
            if r.row_key is None:
                continue
            sku_style_map[str(r.row_key)] = str(r.style_key) if getattr(r, "style_key", None) is not None else None

    # -------------------------
    # RETURNS BASE (from returns)
    # -------------------------
    base = (
        db.query(
            row_key_col.label("row_key"),
            reason_expr.label("raw_reason"),
            ReturnsRaw.return_type.label("return_type"),
            func.coalesce(func.sum(ReturnsRaw.units), 0).label("units"),
            func.max(ReturnsRaw.style_key).label("style_key"),
        )
        .filter(ReturnsRaw.workspace_id == ws_id)
        .filter(ReturnsRaw.return_date >= start)
        .filter(ReturnsRaw.return_date <= end)
    )

    # IMPORTANT: portal-safe returns filtering
    base = _apply_portal_returns(base, workspace_slug, portal)

    if row_dim == "sku":
        base = base.filter(ReturnsRaw.seller_sku_code.isnot(None))

    if brand_style_keys_q is not None:
        base = base.filter(
            ReturnsRaw.style_key.isnot(None),
            func.lower(func.trim(ReturnsRaw.style_key)).in_(brand_style_keys_q),
        )

    agg = base.group_by(row_key_col, reason_expr, ReturnsRaw.return_type).all()

    # top reasons
    totals = {}
    for r in agg:
        rk = str(r.row_key) if r.row_key is not None else None
        if not rk:
            continue
        clean = heatmap_reason_key(r.raw_reason, r.return_type, portal)
        totals[clean] = totals.get(clean, 0) + int(r.units or 0)

    top_reason_keys = [k for k, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:top_reasons]]

    # top rows
    row_totals = {}
    cell_units = {}

    for r in agg:
        rk = str(r.row_key) if r.row_key is not None else None
        if not rk:
            continue
        clean = heatmap_reason_key(r.raw_reason, r.return_type, portal)
        if clean not in top_reason_keys:
            continue

        u = int(r.units or 0)
        cell_units[(rk, clean)] = cell_units.get((rk, clean), 0) + u
        row_totals[rk] = row_totals.get(rk, 0) + u

    top_row_keys = [k for k, _ in sorted(row_totals.items(), key=lambda x: x[1], reverse=True)[:top_rows]]

    # -------------------------
    # CATALOG META (brand/product_name)
    # -------------------------
    style_meta = {}
    if top_row_keys:
        style_keys = set()
        if row_dim == "style":
            style_keys = set(top_row_keys)
        else:
            for sku in top_row_keys:
                st = sku_style_map.get(sku)
                if st:
                    style_keys.add(st)

        if style_keys:
            cats_q = (
                db.query(
                    CatalogRaw.style_key,
                    CatalogRaw.brand,
                    CatalogRaw.product_name,
                )
                .filter(CatalogRaw.workspace_id == ws_id)
                .filter(CatalogRaw.style_key.in_(list(style_keys)))
            )

            # IMPORTANT: portal-safe catalog filtering (this was missing)
            cats_q = _apply_portal_catalog(cats_q, portal)

            if brand_norm:
                cats_q = cats_q.filter(
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )

            cats = cats_q.all()

            for c in cats:
                style_meta[str(c.style_key)] = {
                    "brand": c.brand,
                    "product_name": c.product_name,
                }

    # output rows
    rows = []
    for rk in top_row_keys:
        if row_dim == "style":
            st = rk
        else:
            st = sku_style_map.get(rk)

        meta = style_meta.get(st or "", {}) if st else {}
        rows.append(
            {
                row_key_label: rk,
                "style_key": st,
                "brand": meta.get("brand"),
                "product_name": meta.get("product_name"),
                "orders": int(orders_map.get(rk, 0)),
                "returns_units": int(row_totals.get(rk, 0)),
            }
        )

    cols = [{"reason": r} for r in top_reason_keys]

    matrix_units = []
    matrix_pct = []
    for rk in top_row_keys:
        o = int(orders_map.get(rk, 0))
        urow = []
        prow = []
        for reason in top_reason_keys:
            u = int(cell_units.get((rk, reason), 0))
            urow.append(u)
            prow.append((u / o * 100.0) if o > 0 else None)
        matrix_units.append(urow)
        matrix_pct.append(prow)

    return {
        "row_dim": row_dim,
        "window": {"start": start, "end": end},
        "top_reasons": int(top_reasons),
        "top_rows": int(top_rows),
        "rows": rows,
        "cols": cols,
        "matrix_units": matrix_units,
        "matrix_pct": matrix_pct,
    }

@app.get("/db/returns/heatmap/style-reason")
def returns_heatmap_style_reason(
    start: str = Query(...),
    end: str = Query(...),
    workspace_slug: str = Query("default"),
    portal: str = Query("myntra", description="myntra|flipkart"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    top_reasons: int = Query(10, ge=1, le=10),
    top_rows: int = Query(30, ge=5, le=200),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        data = _build_reason_heatmap(
            db=db,
            ws_id=ws_id,
            workspace_slug=workspace_slug,
            portal=portal,
            start=start,
            end=end,
            row_dim="style",
            top_reasons=top_reasons,
            top_rows=top_rows,
            brand=brand,
        )
        return {"workspace_slug": workspace_slug, **data}
    finally:
        db.close()



@app.get("/db/returns/heatmap/sku-reason")
def returns_heatmap_sku_reason(
    start: str = Query(...),
    end: str = Query(...),
    workspace_slug: str = Query("default"),
    portal: str = Query("myntra", description="myntra|flipkart"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    top_reasons: int = Query(10, ge=1, le=10),
    top_rows: int = Query(30, ge=5, le=200),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        data = _build_reason_heatmap(
            db=db,
            ws_id=ws_id,
            workspace_slug=workspace_slug,
            portal=portal,
            start=start,
            end=end,
            row_dim="sku",
            top_reasons=top_reasons,
            top_rows=top_rows,
            brand=brand,
        )
        return {"workspace_slug": workspace_slug, **data}
    finally:
        db.close()


from fastapi import Query
from sqlalchemy import func, case
import json

def _json_get(raw_json: str | None, key: str) -> str | None:
    """Safely parse raw_json and fetch key; returns None if missing."""
    if not raw_json:
        return None
    try:
        obj = json.loads(raw_json)
        v = obj.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None
    except Exception:
        return None
def _get_reason_clean(raw_json: str | None) -> str | None:
    # prefer clean reason if present
    v = _json_get(raw_json, "clean_return_reason")
    if v:
        return v
    return _json_get(raw_json, "return_reason")

if False:
    # Legacy duplicate heatmap endpoints (disabled; kept for reference)
    @app.get("/db/returns/heatmap/style-reason")
    def returns_heatmap_style_reason(
        start: str = Query(...),
        end: str = Query(...),
        workspace_slug: str = Query("default"),
        top_reasons: int = Query(12, ge=1, le=50),
        top_rows: int = Query(50, ge=1, le=500),
    ):
        db = SessionLocal()
        try:
            ws_id = resolve_workspace_id(db, workspace_slug)

            # Pull base rows in window
            q = (
                db.query(
                    ReturnsRaw.style_key.label("style_key"),
                    ReturnsRaw.return_type.label("return_type"),
                    ReturnsRaw.units.label("units"),
                    ReturnsRaw.raw_json.label("raw_json"),
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start)
                .filter(ReturnsRaw.return_date <= end)
            )

            rows = q.all()

            # Build reason list + aggregate
            agg = {}  # style_key -> reason -> {total, rto, ret}
            reason_totals = {}

            for r in rows:
                style_key = r.style_key or "(Unknown)"
                reason = _get_reason_clean(r.raw_json) or "(Unknown)"
                units = int(r.units or 0)
                rtype = (r.return_type or "").strip()

                agg.setdefault(style_key, {})
                agg[style_key].setdefault(reason, {"total": 0, "rto": 0, "ret": 0})

                agg[style_key][reason]["total"] += units
                if rtype.lower() == "rto":
                    agg[style_key][reason]["rto"] += units
                else:
                    agg[style_key][reason]["ret"] += units

                reason_totals[reason] = reason_totals.get(reason, 0) + units

            # Top reasons by volume
            top_reason_list = sorted(reason_totals.items(), key=lambda x: x[1], reverse=True)[:top_reasons]
            columns = [x[0] for x in top_reason_list]

            # Score styles by sum(top reasons)
            scored = []
            for style_key, mp in agg.items():
                score = sum(mp.get(c, {}).get("total", 0) for c in columns)
                total_all = sum(v["total"] for v in mp.values())
                scored.append((style_key, score, total_all))
            scored.sort(key=lambda x: x[1], reverse=True)
            scored = scored[:top_rows]

            out_rows = []
            for style_key, score, total_all in scored:
                mp = agg.get(style_key, {})
                row = {
                    "style_key": style_key,
                    "total_top": score,
                    "total_all": total_all,
                }
                for c in columns:
                    row[c] = mp.get(c, {}).get("total", 0)
                out_rows.append(row)

            return {
                "workspace_slug": workspace_slug,
                "window": {"start": start, "end": end},
                "columns": columns,
                "rows": out_rows,
            }
        finally:
            db.close()


    @app.get("/db/returns/heatmap/sku-reason")
    def returns_heatmap_sku_reason(
        start: str = Query(...),
        end: str = Query(...),
        workspace_slug: str = Query("default"),
        top_reasons: int = Query(12, ge=1, le=50),
        top_rows: int = Query(50, ge=1, le=500),
    ):
        db = SessionLocal()
        try:
            ws_id = resolve_workspace_id(db, workspace_slug)

            q = (
                db.query(
                    ReturnsRaw.seller_sku_code.label("seller_sku_code"),
                    ReturnsRaw.return_type.label("return_type"),
                    ReturnsRaw.units.label("units"),
                    ReturnsRaw.raw_json.label("raw_json"),
                )
                .filter(ReturnsRaw.workspace_id == ws_id)
                .filter(ReturnsRaw.return_date >= start)
                .filter(ReturnsRaw.return_date <= end)
            )

            rows = q.all()

            agg = {}  # sku -> reason -> {total,rto,ret}
            reason_totals = {}

            for r in rows:
                sku = r.seller_sku_code or "(Unknown)"
                reason = _get_reason_clean(r.raw_json) or "(Unknown)"
                units = int(r.units or 0)
                rtype = (r.return_type or "").strip()

                agg.setdefault(sku, {})
                agg[sku].setdefault(reason, {"total": 0, "rto": 0, "ret": 0})

                agg[sku][reason]["total"] += units
                if rtype.lower() == "rto":
                    agg[sku][reason]["rto"] += units
                else:
                    agg[sku][reason]["ret"] += units

                reason_totals[reason] = reason_totals.get(reason, 0) + units

            top_reason_list = sorted(reason_totals.items(), key=lambda x: x[1], reverse=True)[:top_reasons]
            columns = [x[0] for x in top_reason_list]

            scored = []
            for sku, mp in agg.items():
                score = sum(mp.get(c, {}).get("total", 0) for c in columns)
                total_all = sum(v["total"] for v in mp.values())
                scored.append((sku, score, total_all))
            scored.sort(key=lambda x: x[1], reverse=True)
            scored = scored[:top_rows]

            out_rows = []
            for sku, score, total_all in scored:
                mp = agg.get(sku, {})
                row = {
                    "seller_sku_code": sku,
                    "total_top": score,
                    "total_all": total_all,
                }
                for c in columns:
                    row[c] = mp.get(c, {}).get("total", 0)
                out_rows.append(row)

            return {
                "workspace_slug": workspace_slug,
                "window": {"start": start, "end": end},
                "columns": columns,
                "rows": out_rows,
            }
        finally:
            db.close()


@app.get("/db/returns/sku-wise")
def db_returns_sku_wise(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str = Query("default"),
    portal: str = Query("myntra"),
    brand: str | None = Query(None, description="Optional brand filter (from catalog_raw.brand)"),
    top_n: int = Query(50, ge=1, le=500),
    min_orders: int = Query(10, ge=0, le=1000000),
):
    db = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        brand_norm = (brand or "").strip().lower() if brand else None

        # -------------------------
        # Brand style-key set from catalog (optional)
        # -------------------------
        brand_style_keys_q = None
        if brand_norm:
            brand_style_keys_q = (
                db.query(func.lower(func.trim(CatalogRaw.style_key)))
                .filter(
                    CatalogRaw.workspace_id == ws_id,
                    CatalogRaw.style_key.isnot(None),
                    CatalogRaw.brand.isnot(None),
                    func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
                )
                .distinct()
            )
            brand_style_keys_q = _apply_portal_catalog(brand_style_keys_q, portal)

        # -------------------------
        # Sales counts per SKU
        # -------------------------
        sales_counts_q = (
            db.query(
                SalesRaw.seller_sku_code.label("seller_sku_code"),
                func.max(SalesRaw.style_key).label("style_key"),
                func.count(SalesRaw.id).label("orders"),
                func.max(SalesRaw.order_date).label("last_order_date"),
            )
            .filter(SalesRaw.workspace_id == ws_id)
            .filter(SalesRaw.order_date >= start_dt)
            .filter(SalesRaw.order_date < end_dt_excl)
        )

        sales_counts_q = _apply_portal_sales(sales_counts_q, workspace_slug, portal)

        if brand_style_keys_q is not None:
            sales_counts_q = sales_counts_q.filter(
                func.lower(func.trim(SalesRaw.style_key)).in_(brand_style_keys_q)
            )

        sales_counts = sales_counts_q.group_by(SalesRaw.seller_sku_code).subquery()

        # -------------------------
        # Returns counts per SKU
        # -------------------------
        rtype_norm = func.upper(func.trim(ReturnsRaw.return_type))

        returns_counts_q = (
            db.query(
                ReturnsRaw.seller_sku_code.label("seller_sku_code"),
                func.max(ReturnsRaw.style_key).label("style_key"),
                func.coalesce(func.sum(ReturnsRaw.units), 0).label("returns_units"),
                func.coalesce(
                    func.sum(case((rtype_norm == "RTO", ReturnsRaw.units), else_=0)),
                    0,
                ).label("rto_units"),
                func.coalesce(
                    func.sum(
                        case(
                            (rtype_norm.in_(["CUSTOMER_RETURN", "RETURN"]), ReturnsRaw.units),
                            else_=0,
                        )
                    ),
                    0,
                ).label("return_units"),
            )
            .filter(ReturnsRaw.workspace_id == ws_id)
            .filter(ReturnsRaw.return_date >= start_dt)
            .filter(ReturnsRaw.return_date < end_dt_excl)
        )

        returns_counts_q = _apply_portal_returns(returns_counts_q, workspace_slug, portal)

        if brand_style_keys_q is not None:
            returns_counts_q = returns_counts_q.filter(
                func.lower(func.trim(ReturnsRaw.style_key)).in_(brand_style_keys_q)
            )

        returns_counts = returns_counts_q.group_by(ReturnsRaw.seller_sku_code).subquery()

        # -------------------------
        # Catalog details (join via style_key)
        # -------------------------
        cat_q = (
            db.query(
                CatalogRaw.style_key.label("style_key"),
                func.max(CatalogRaw.brand).label("brand"),
                func.max(CatalogRaw.product_name).label("product_name"),
            )
            .filter(CatalogRaw.workspace_id == ws_id)
        )

        cat_q = _apply_portal_catalog(cat_q, portal)

        if brand_norm:
            cat_q = cat_q.filter(
                CatalogRaw.brand.isnot(None),
                func.lower(func.trim(CatalogRaw.brand)) == brand_norm,
            )

        cat = cat_q.group_by(CatalogRaw.style_key).subquery()

        # -------------------------
        # Final output
        # -------------------------
        q = (
            db.query(
                sales_counts.c.seller_sku_code,
                func.coalesce(sales_counts.c.style_key, returns_counts.c.style_key).label("style_key"),
                func.coalesce(cat.c.brand, "").label("brand"),
                func.coalesce(cat.c.product_name, "").label("product_name"),
                sales_counts.c.orders,
                func.coalesce(returns_counts.c.returns_units, 0).label("returns_units"),
                func.coalesce(returns_counts.c.return_units, 0).label("return_units"),
                func.coalesce(returns_counts.c.rto_units, 0).label("rto_units"),
                sales_counts.c.last_order_date,
            )
            .outerjoin(returns_counts, returns_counts.c.seller_sku_code == sales_counts.c.seller_sku_code)
            .outerjoin(cat, cat.c.style_key == func.coalesce(sales_counts.c.style_key, returns_counts.c.style_key))
            .filter(sales_counts.c.orders >= min_orders)
        )

        rows = q.all()

        out = []
        for r in rows:
            orders = int(r.orders or 0)
            returns_units = int(r.returns_units or 0)
            pct = (returns_units * 100.0 / orders) if orders > 0 else 0.0

            out.append(
                {
                    "seller_sku_code": r.seller_sku_code,
                    "style_key": r.style_key,
                    "brand": r.brand,
                    "product_name": r.product_name,
                    "orders": orders,
                    "returns_units": returns_units,
                    "return_units": int(r.return_units or 0),
                    "rto_units": int(r.rto_units or 0),
                    "return_pct": float(pct),
                    "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                    "window": {"start": start.isoformat(), "end": end.isoformat()},
                }
            )

        out.sort(key=lambda x: x["return_pct"], reverse=True)
        return out[:top_n]

    finally:
        db.close()

def _extract_size_from_sku(sku: str | None) -> str:
    """
    Extract size from seller_sku_code like:
    xyz-m, abc-l, item-xl, code-xxl
    If not found, return "NO_SIZE".
    """
    if not sku:
        return "NO_SIZE"
    s = str(sku).strip().lower()

    # take last token after "-" or "_" (common pattern)
    last = re.split(r"[-_]", s)[-1].strip()

    # normalize common sizes
    mapping = {
        "xs": "XS",
        "s": "S",
        "m": "M",
        "l": "L",
        "xl": "XL",
        "xxl": "XXL",
        "xxxl": "XXXL",
        "2xl": "XXL",
        "3xl": "XXXL",
    }
    if last in mapping:
        return mapping[last]

    return "NO_SIZE"

from typing import Any


from typing import Any

@app.get("/db/style/size-forecast")
def db_style_size_forecast(
    workspace_slug: str = Query("default"),
    style_key: str = Query(..., description="Style ID / style_key"),
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    brand: str | None = Query(None, description="Optional brand filter (exact match, case-insensitive)"),

    # NEW forecast inputs (all optional)
    forecast_days: int = Query(30, ge=1, le=120, description="Forecast horizon days (e.g. next month days)"),
    sales_days: int = Query(0, ge=0, le=31, description="Number of sales days in the horizon"),
    spike_multiplier: float = Query(1.0, ge=0.5, le=20.0, description="Multiplier during sales days (e.g. 2.0)"),
    lead_time_days: int = Query(0, ge=0, le=120, description="Lead time days"),
    target_cover_days: int = Query(0, ge=0, le=180, description="Target cover days"),
    safety_stock_pct: float = Query(0.0, ge=0.0, le=500.0, description="Safety stock percent"),
    exclude_rto: bool = Query(False, description="If true, subtract RTO units (in same window) from demand"),
):
    """
    Size mix + stock + forecast recommendation for ONE style.
    Works even if stock snapshot is missing (then stock=0 but recommendation still returns).
    """
    import re

    def norm_sku(s: str | None) -> str:
        return (s or "").strip().lower()

    SIZE_TOKENS = {"xs", "s", "m", "l", "xl", "xxl", "xxxl", "3xl", "4xl", "5xl", "free", "fs"}

    def extract_size_from_sku(sku_norm: str) -> str:
        if not sku_norm:
            return "NO_SIZE"
        parts = re.split(r"[-_]+", sku_norm)
        last = parts[-1] if parts else ""
        if last in SIZE_TOKENS:
            return last.upper()
        return "NO_SIZE"

    def clamp_int(x: int, lo: int, hi: int) -> int:
        return max(lo, min(hi, int(x)))

    def safe_float(x: float) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        sk = (style_key or "").strip().lower()

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)
        hist_days = max(1, (end_dt_excl.date() - start_dt.date()).days)

        # normalize inputs
        forecast_days = clamp_int(forecast_days, 1, 120)
        sales_days = clamp_int(sales_days, 0, 31)
        sales_days = min(sales_days, forecast_days)
        spike_multiplier = safe_float(spike_multiplier)
        lead_time_days = clamp_int(lead_time_days, 0, 120)
        target_cover_days = clamp_int(target_cover_days, 0, 180)
        safety_stock_pct = safe_float(safety_stock_pct)

        # -----------------------
        # 1) Orders by SKU in range
        # -----------------------
        sku_norm_sales = func.lower(func.trim(SalesRaw.seller_sku_code))

        brand_norm = (brand or "").strip().lower() if brand else None
        sales_brand_expr = func.lower(func.trim(cast(SalesRaw.raw_json, JSONB)["brand"].astext))

        sales_rows = (
            db.query(
                sku_norm_sales.label("sku_norm"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders"),
            )
            .filter(
                SalesRaw.workspace_id == ws_id,
                SalesRaw.style_key == sk,
                SalesRaw.order_date.isnot(None),
                SalesRaw.order_date >= start_dt,
                SalesRaw.order_date < end_dt_excl,
                SalesRaw.seller_sku_code.isnot(None),
                *([sales_brand_expr == brand_norm] if brand_norm else []),
            )
            .group_by(sku_norm_sales)
            .all()
        )

        sku_orders_map: dict[str, int] = {}
        orders_by_size: dict[str, int] = {}

        for r in sales_rows:
            sku = norm_sku(r.sku_norm)
            if not sku:
                continue
            o = int(r.orders or 0)
            sku_orders_map[sku] = sku_orders_map.get(sku, 0) + o

            sz = extract_size_from_sku(sku)
            orders_by_size[sz] = orders_by_size.get(sz, 0) + o

        gross_orders = int(sum(orders_by_size.values()) or 0)

        # -----------------------
        # 1b) Optional: subtract RTO demand (same window, same style)
        # -----------------------
        rto_by_size: dict[str, int] = {}
        rto_total = 0

        if exclude_rto:
            rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))
            unit_expr = func.coalesce(ReturnsRaw.units, 1)
            rto_rows = (
                db.query(
                    func.lower(func.trim(ReturnsRaw.seller_sku_code)).label("sku_norm"),
                    func.coalesce(func.sum(unit_expr), 0).label("rto_units"),
                )
                .filter(
                    ReturnsRaw.workspace_id == ws_id,
                    ReturnsRaw.style_key == sk,
                    ReturnsRaw.return_date.isnot(None),
                    ReturnsRaw.return_date >= start_dt,
                    ReturnsRaw.return_date < end_dt_excl,
                    ReturnsRaw.seller_sku_code.isnot(None),
                    rtype_norm == "RTO",
                )
                .group_by(func.lower(func.trim(ReturnsRaw.seller_sku_code)))
                .all()
            )
            for rr in rto_rows:
                sku = norm_sku(rr.sku_norm)
                u = int(rr.rto_units or 0)
                if not sku or u <= 0:
                    continue
                sz = extract_size_from_sku(sku)
                rto_by_size[sz] = rto_by_size.get(sz, 0) + u
                rto_total += u

            # apply subtraction size-wise (clamped)
            for sz, u in rto_by_size.items():
                if sz in orders_by_size:
                    orders_by_size[sz] = max(0, int(orders_by_size[sz]) - int(u))

        net_orders = int(sum(orders_by_size.values()) or 0)

        # -----------------------
        # 2) Latest stock snapshot by SKU -> size
        # -----------------------
        latest_stock_ingested = (
            db.query(func.max(StockRaw.ingested_at))
            .filter(StockRaw.workspace_id == ws_id)
            .scalar()
        )
        has_stock_snapshot = latest_stock_ingested is not None

        stock_by_size: dict[str, int] = {}

        if has_stock_snapshot:
            cat_skus = [
                norm_sku(r[0])
                for r in (
                    db.query(CatalogRaw.seller_sku_code)
                    .filter(
                        CatalogRaw.workspace_id == ws_id,
                        CatalogRaw.style_key == sk,
                        CatalogRaw.seller_sku_code.isnot(None),
                    )
                    .distinct()
                    .all()
                )
            ]
            cat_skus = [s for s in cat_skus if s]

            candidate_skus = cat_skus if cat_skus else list(sku_orders_map.keys())

            if candidate_skus:
                sku_norm_stock = func.lower(func.trim(StockRaw.seller_sku_code))
                stock_rows = (
                    db.query(
                        sku_norm_stock.label("sku_norm"),
                        func.coalesce(func.sum(StockRaw.qty), 0).label("qty"),
                    )
                    .filter(
                        StockRaw.workspace_id == ws_id,
                        StockRaw.ingested_at == latest_stock_ingested,
                        StockRaw.seller_sku_code.isnot(None),
                        sku_norm_stock.in_(candidate_skus),
                    )
                    .group_by(sku_norm_stock)
                    .all()
                )
                for r in stock_rows:
                    sku = norm_sku(r.sku_norm)
                    if not sku:
                        continue
                    q = int(r.qty or 0)
                    sz = extract_size_from_sku(sku)
                    stock_by_size[sz] = stock_by_size.get(sz, 0) + q

        total_stock = int(sum(stock_by_size.values()) if stock_by_size else 0)

        # -----------------------
        # 3) Forecast math (totals)
        # -----------------------
        avg_daily = (net_orders / float(hist_days)) if hist_days > 0 else 0.0

        sales_days_eff = min(int(sales_days), int(forecast_days)) 
        base_days = max(0, int(forecast_days) - sales_days_eff)

        forecast_units = (
         avg_daily * float(base_days)
         + avg_daily * float(spike_multiplier) * float(sales_days_eff)
         )
        forecast_units = max(0.0, float(forecast_units))

        forecast_avg_daily = (forecast_units / float(forecast_days)) if forecast_days > 0 else 0.0

        cover_days = int(lead_time_days + target_cover_days)
        required_on_hand = (
        forecast_avg_daily * float(cover_days) * (1.0 + (safety_stock_pct / 100.0)) )
        required_on_hand = max(0.0, float(required_on_hand))

        gap_total = max(0.0, required_on_hand - float(total_stock))


        # -----------------------
        # 4) Merge output by size (include required + gap)
        # -----------------------
        all_sizes = set(orders_by_size.keys()) | set(stock_by_size.keys())
        size_order = {"XS": 1, "S": 2, "M": 3, "L": 4, "XL": 5, "XXL": 6, "XXXL": 7, "NO_SIZE": 99}

        rows_out: list[dict[str, Any]] = []
        total_orders_for_share = max(1, net_orders)  # prevent div0

        for sz in all_sizes:
            o = int(orders_by_size.get(sz, 0))
            stock = int(stock_by_size.get(sz, 0))

            share = (o * 100.0 / float(total_orders_for_share)) if net_orders > 0 else 0.0

            bucket_required = (required_on_hand * (share / 100.0)) if net_orders > 0 else 0.0
            bucket_gap = max(0.0, bucket_required - float(stock))

            avg_daily_bucket = (o / float(hist_days)) if hist_days > 0 else 0.0
            days_cover = (stock / avg_daily_bucket) if avg_daily_bucket > 0 else None

            if not has_stock_snapshot:
                risk = "NO_STOCK_SNAPSHOT"
            elif stock <= 0:
                risk = "OOS"
            elif days_cover is not None and days_cover < 7:
                risk = "LOW_STOCK"
            else:
                risk = "OK"

            rows_out.append(
                {
                    "size": sz,
                    "orders": o,
                    "share_orders": float(share),
                    "stock_qty": stock,
                    "avg_daily_orders": float(avg_daily_bucket),
                    "days_cover": None if days_cover is None else float(days_cover),
                    "risk": risk,

                    # NEW
                    "required_qty": float(bucket_required),
                    "gap_qty": float(bucket_gap),
                }
            )

        rows_out.sort(key=lambda r: size_order.get(r["size"], 50))

        return {
            "workspace_slug": workspace_slug,
            "style_key": sk,
            "window": {"start": start.isoformat(), "end": end.isoformat(), "days": int(hist_days)},
            "latest_stock_snapshot_at": None if latest_stock_ingested is None else latest_stock_ingested.isoformat(),

            "inputs": {
                "forecast_days": int(forecast_days),
                "sales_days": int(sales_days),
                "spike_multiplier": float(spike_multiplier),
                "lead_time_days": int(lead_time_days),
                "target_cover_days": int(target_cover_days),
                "safety_stock_pct": float(safety_stock_pct),
                "exclude_rto": bool(exclude_rto),
            },

            "totals": {
                "orders_gross": int(gross_orders),
                "rto_units_subtracted": int(rto_total),
                "orders_net": int(net_orders),
                "stock_qty": int(total_stock),

                "avg_daily": float(avg_daily),
                "forecast_units": float(forecast_units),
                "required_on_hand": float(required_on_hand),
                "gap_qty": float(gap_total),
            },
            "rows": rows_out,
        }
    finally:
        db.close()

from typing import Any


@app.get("/db/style/sku-forecast")
def db_style_sku_forecast(
    workspace_slug: str = Query("default"),
    style_key: str = Query(..., description="Style ID / style_key"),
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
    brand: str | None = Query(None, description="Optional brand filter (exact match, case-insensitive)"),

    # NEW forecast inputs
    forecast_days: int = Query(30, ge=1, le=120),
    sales_days: int = Query(0, ge=0, le=31),
    spike_multiplier: float = Query(1.0, ge=0.5, le=20.0),
    lead_time_days: int = Query(0, ge=0, le=120),
    target_cover_days: int = Query(0, ge=0, le=180),
    safety_stock_pct: float = Query(0.0, ge=0.0, le=500.0),
    exclude_rto: bool = Query(False),
):
    """
    SKU mix + stock + forecast recommendation for ONE style.
    Works for both sized + non-sized styles.
    """
    def norm_sku(s: str | None) -> str:
        return (s or "").strip().lower()

    def clamp_int(x: int, lo: int, hi: int) -> int:
        return max(lo, min(hi, int(x)))

    def safe_float(x: float) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)
        sk = (style_key or "").strip().lower()

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)
        hist_days = max(1, (end_dt_excl.date() - start_dt.date()).days)

        # normalize inputs
        forecast_days = clamp_int(forecast_days, 1, 120)
        sales_days = clamp_int(sales_days, 0, 31)
        sales_days = min(sales_days, forecast_days)
        spike_multiplier = safe_float(spike_multiplier)
        lead_time_days = clamp_int(lead_time_days, 0, 120)
        target_cover_days = clamp_int(target_cover_days, 0, 180)
        safety_stock_pct = safe_float(safety_stock_pct)

        # -----------------------
        # 1) Orders by SKU
        # -----------------------
        sku_norm_sales = func.lower(func.trim(SalesRaw.seller_sku_code))

        brand_norm = (brand or "").strip().lower() if brand else None
        sales_brand_expr = func.lower(func.trim(cast(SalesRaw.raw_json, JSONB)["brand"].astext))

        sales_rows = (
            db.query(
                sku_norm_sales.label("sku_norm"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("orders"),
            )
            .filter(
                SalesRaw.workspace_id == ws_id,
                SalesRaw.style_key == sk,
                SalesRaw.order_date.isnot(None),
                SalesRaw.order_date >= start_dt,
                SalesRaw.order_date < end_dt_excl,
                SalesRaw.seller_sku_code.isnot(None),
                *([sales_brand_expr == brand_norm] if brand_norm else []),
            )
            .group_by(sku_norm_sales)
            .all()
        )

        sku_orders_map: dict[str, int] = {}
        for r in sales_rows:
            sku = norm_sku(r.sku_norm)
            if not sku:
                continue
            sku_orders_map[sku] = sku_orders_map.get(sku, 0) + int(r.orders or 0)

        gross_orders = int(sum(sku_orders_map.values()) or 0)

        # -----------------------
        # 1b) subtract RTO per SKU (optional)
        # -----------------------
        rto_total = 0
        if exclude_rto:
            rtype_norm = func.upper(func.trim(func.coalesce(ReturnsRaw.return_type, "")))
            unit_expr = func.coalesce(ReturnsRaw.units, 1)
            rto_rows = (
                db.query(
                    func.lower(func.trim(ReturnsRaw.seller_sku_code)).label("sku_norm"),
                    func.coalesce(func.sum(unit_expr), 0).label("rto_units"),
                )
                .filter(
                    ReturnsRaw.workspace_id == ws_id,
                    ReturnsRaw.style_key == sk,
                    ReturnsRaw.return_date.isnot(None),
                    ReturnsRaw.return_date >= start_dt,
                    ReturnsRaw.return_date < end_dt_excl,
                    ReturnsRaw.seller_sku_code.isnot(None),
                    rtype_norm == "RTO",
                )
                .group_by(func.lower(func.trim(ReturnsRaw.seller_sku_code)))
                .all()
            )
            for rr in rto_rows:
                sku = norm_sku(rr.sku_norm)
                u = int(rr.rto_units or 0)
                if not sku or u <= 0:
                    continue
                if sku in sku_orders_map:
                    sku_orders_map[sku] = max(0, int(sku_orders_map[sku]) - u)
                rto_total += u

        net_orders = int(sum(sku_orders_map.values()) or 0)

        # -----------------------
        # 2) Latest stock snapshot by SKU
        # -----------------------
        latest_stock_ingested = (
            db.query(func.max(StockRaw.ingested_at))
            .filter(StockRaw.workspace_id == ws_id)
            .scalar()
        )
        has_stock_snapshot = latest_stock_ingested is not None

        stock_by_sku: dict[str, int] = {}
        if has_stock_snapshot:
            cat_skus = [
                norm_sku(r[0])
                for r in (
                    db.query(CatalogRaw.seller_sku_code)
                    .filter(
                        CatalogRaw.workspace_id == ws_id,
                        CatalogRaw.style_key == sk,
                        CatalogRaw.seller_sku_code.isnot(None),
                    )
                    .distinct()
                    .all()
                )
            ]
            cat_skus = [s for s in cat_skus if s]

            candidate_skus = set(sku_orders_map.keys())
            candidate_skus.update(cat_skus)

            if candidate_skus:
                sku_norm_stock = func.lower(func.trim(StockRaw.seller_sku_code))
                stock_rows = (
                    db.query(
                        sku_norm_stock.label("sku_norm"),
                        func.coalesce(func.sum(StockRaw.qty), 0).label("qty"),
                    )
                    .filter(
                        StockRaw.workspace_id == ws_id,
                        StockRaw.ingested_at == latest_stock_ingested,
                        StockRaw.seller_sku_code.isnot(None),
                        sku_norm_stock.in_(list(candidate_skus)),
                    )
                    .group_by(sku_norm_stock)
                    .all()
                )
                for r in stock_rows:
                    sku = norm_sku(r.sku_norm)
                    if not sku:
                        continue
                    stock_by_sku[sku] = stock_by_sku.get(sku, 0) + int(r.qty or 0)

        total_stock = int(sum(stock_by_sku.values()) if stock_by_sku else 0)

        # -----------------------
        # 3) Forecast math totals
        # -----------------------
        avg_daily = (net_orders / float(hist_days)) if hist_days > 0 else 0.0
        forecast_units = avg_daily * float(forecast_days - sales_days) + avg_daily * float(spike_multiplier) * float(sales_days)
        forecast_units = max(0.0, float(forecast_units))
        forecast_avg_daily = (forecast_units / float(forecast_days)) if forecast_days > 0 else 0.0

        cover_days = int(lead_time_days + target_cover_days)
        required_on_hand = forecast_avg_daily * float(cover_days) * (1.0 + (safety_stock_pct / 100.0))
        required_on_hand = max(0.0, float(required_on_hand))

        gap_total = max(0.0, required_on_hand - float(total_stock))

        # -----------------------
        # 4) Rows by SKU (+ required/gap)
        # -----------------------
        all_skus = set(sku_orders_map.keys()) | set(stock_by_sku.keys())
        total_orders_for_share = max(1, net_orders)

        rows_out: list[dict[str, Any]] = []
        for sku in all_skus:
            o = int(sku_orders_map.get(sku, 0))
            stock = int(stock_by_sku.get(sku, 0))

            share = (o * 100.0 / float(total_orders_for_share)) if net_orders > 0 else 0.0
            bucket_required = (required_on_hand * (share / 100.0)) if net_orders > 0 else 0.0
            bucket_gap = max(0.0, bucket_required - float(stock))

            avg_daily_bucket = (o / float(hist_days)) if hist_days > 0 else 0.0
            days_cover = (stock / avg_daily_bucket) if avg_daily_bucket > 0 else None

            if not has_stock_snapshot:
                risk = "NO_STOCK_SNAPSHOT"
            elif stock <= 0:
                risk = "OOS"
            elif days_cover is not None and days_cover < 7:
                risk = "LOW_STOCK"
            else:
                risk = "OK"

            rows_out.append(
                {
                    "sku": sku,
                    "orders": o,
                    "share_orders": float(share),
                    "stock_qty": stock,
                    "avg_daily_orders": float(avg_daily_bucket),
                    "days_cover": None if days_cover is None else float(days_cover),
                    "risk": risk,

                    "required_qty": float(bucket_required),
                    "gap_qty": float(bucket_gap),
                }
            )

        rows_out.sort(key=lambda r: (-(r["orders"] or 0), r["sku"]))

        return {
            "workspace_slug": workspace_slug,
            "style_key": sk,
            "window": {"start": start.isoformat(), "end": end.isoformat(), "days": int(hist_days)},
            "latest_stock_snapshot_at": None if latest_stock_ingested is None else latest_stock_ingested.isoformat(),
            "inputs": {
                "forecast_days": int(forecast_days),
                "sales_days": int(sales_days),
                "spike_multiplier": float(spike_multiplier),
                "lead_time_days": int(lead_time_days),
                "target_cover_days": int(target_cover_days),
                "safety_stock_pct": float(safety_stock_pct),
                "exclude_rto": bool(exclude_rto),
            },
            "totals": {
                "orders_gross": int(gross_orders),
                "rto_units_subtracted": int(rto_total),
                "orders_net": int(net_orders),
                "stock_qty": int(total_stock),

                "avg_daily": float(avg_daily),
                "forecast_units": float(forecast_units),
                "required_on_hand": float(required_on_hand),
                "gap_qty": float(gap_total),
            },
            "rows": rows_out,
        }
    finally:
        db.close()


@app.get("/db/kpi/gmv-asp")
def db_kpi_gmv_asp(
    start: date = Query(...),
    end: date = Query(...),
    workspace_slug: str | None = Query(None),
    workspace: str = Query("default"),  # backward compat
    brand: str | None = Query(None),
    portal: str | None = Query(None),   # ✅ add portal
):
    db = SessionLocal()
    try:
        ws_slug = (workspace_slug or "").strip() or (workspace or "default")
        ws_id = resolve_workspace_id(db, ws_slug)

        start_dt = datetime.combine(start, time.min)
        end_dt_excl = datetime.combine(end + timedelta(days=1), time.min)

        # optional brand filter via catalog -> style_key list
        style_key_filter = None
        if brand:
            style_key_filter = [
                (r[0] or "").strip().lower()
                for r in (
                    db.query(func.lower(func.trim(CatalogRaw.style_key)))
                    .filter(CatalogRaw.workspace_id == ws_id)
                    .filter(func.lower(func.trim(CatalogRaw.brand)) == brand.strip().lower())
                    .all()
                )
                if (r[0] or "").strip()
            ]
            if not style_key_filter:
                return {
                    "gmv": 0.0,
                    "orders": 0,
                    "units": 0,
                    "asp": 0.0,
                    "prev_gmv": 0.0,
                    "prev_orders": 0,
                    "prev_units": 0,
                    "prev_asp": 0.0,
                    "gmv_change_pct": None,
                    "asp_change_pct": None,
                }

        # seller price from raw_json (Text -> JSONB)
        seller_price_txt = cast(SalesRaw.raw_json, JSONB).op("->>")("seller price")
        seller_price_num = cast(
            func.nullif(func.trim(func.replace(seller_price_txt, ",", "")), ""),
            Numeric,
        )

        q = (
            db.query(
                func.coalesce(func.sum(seller_price_num * SalesRaw.units), 0).label("gmv"),
                func.count(SalesRaw.id).label("orders"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("units"),
            )
            .filter(
                SalesRaw.workspace_id == ws_id,
                SalesRaw.order_date >= start_dt,
                SalesRaw.order_date < end_dt_excl,
                SalesRaw.raw_json.isnot(None),
                SalesRaw.raw_json.like("{%"),  # safety: only JSON-looking rows
            )
        )

        # ✅ apply portal filter (fixes Myntra ASP)
        q = _apply_portal_sales(q, ws_slug, portal)

        if style_key_filter is not None:
            q = q.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        row = q.one()
        gmv = float(row.gmv or 0)
        orders = int(row.orders or 0)
        units = int(row.units or 0)
        asp = float(gmv / units) if units > 0 else 0.0

        # Previous window (same length)
        window_days = (end - start).days + 1
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=window_days - 1)

        prev_start_dt = datetime.combine(prev_start, time.min)
        prev_end_dt_excl = datetime.combine(prev_end + timedelta(days=1), time.min)

        q2 = (
            db.query(
                func.coalesce(func.sum(seller_price_num * SalesRaw.units), 0).label("gmv"),
                func.count(SalesRaw.id).label("orders"),
                func.coalesce(func.sum(SalesRaw.units), 0).label("units"),
            )
            .filter(
                SalesRaw.workspace_id == ws_id,
                SalesRaw.order_date >= prev_start_dt,
                SalesRaw.order_date < prev_end_dt_excl,
                SalesRaw.raw_json.isnot(None),
                SalesRaw.raw_json.like("{%"),
            )
        )

        # ✅ apply portal filter to prev window too
        q2 = _apply_portal_sales(q2, ws_slug, portal)

        if style_key_filter is not None:
            q2 = q2.filter(func.lower(func.trim(SalesRaw.style_key)).in_(style_key_filter))

        prev = q2.one()
        prev_gmv = float(prev.gmv or 0)
        prev_orders = int(prev.orders or 0)
        prev_units = int(prev.units or 0)
        prev_asp = float(prev_gmv / prev_units) if prev_units > 0 else 0.0

        def pct_change(curr: float, old: float):
            if old == 0:
                return None
            return float(((curr - old) / old) * 100.0)

        return {
            "gmv": gmv,
            "orders": orders,
            "units": units,
            "asp": asp,
            "prev_gmv": prev_gmv,
            "prev_orders": prev_orders,
            "prev_units": prev_units,
            "prev_asp": prev_asp,
            "gmv_change_pct": pct_change(gmv, prev_gmv),
            "asp_change_pct": pct_change(asp, prev_asp),
        }

    finally:
        db.close()


from fastapi import UploadFile, File, Query, HTTPException
import pandas as pd
import json
from datetime import datetime

@app.post("/db/ingest/flipkart-traffic")
def ingest_flipkart_traffic(
    workspace_slug: str = Query("default"),
    replace_history: bool = Query(False),
    file: UploadFile = File(...),
):
    """
    Ingest Flipkart Search Traffic Report (daily rows by SKU).

    Confirmed columns in your file:
      - Impression Date (yyyy-mm-dd)
      - SKU Id
      - Listing Id
      - Product Title
      - Product Views
      - Product Clicks
      - Sales
      - Revenue
      - Click Through Rate
      - Conversion Rate
      - (optional) Average Selling Price, Category, Brand, Vertical, etc.

    Notes:
      - We normalize SKU into the same format used everywhere else for Flipkart:
          "MX-EM-001-Wine-L" -> "fk:mx-em-001-wine-l"
      - returns_raw for Flipkart has blank style_key for most rows, but seller_sku_code is populated,
        so matching traffic by seller_sku_code is the correct long-term plan.
    """
    db: Session = SessionLocal()
    try:
        ws_id = resolve_workspace_id(db, workspace_slug)

        if not file.filename.lower().endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="Please upload an Excel (.xlsx/.xls) file")

        df = pd.read_excel(file.file)

        def ckey(x: str) -> str:
            return str(x or "").strip().lower()

        cols = {ckey(c): c for c in df.columns}

        def pick_col(*names: str) -> str | None:
            for name in names:
                k = name.strip().lower()
                if k in cols:
                    return cols[k]
            return None

        # Required (based on your file)
        impression_date_col = pick_col("Impression Date")
        sku_col = pick_col("SKU Id")
        views_col = pick_col("Product Views")
        sales_col = pick_col("Sales")
        revenue_col = pick_col("Revenue")

        # Optional (present in your file but still guard)
        listing_col = pick_col("Listing Id")
        title_col = pick_col("Product Title")
        clicks_col = pick_col("Product Clicks")
        ctr_col = pick_col("Click Through Rate", "CTR (%)", "CTR")
        conv_col = pick_col("Conversion Rate", "Conversion Rate (%)", "Conversion (%)", "CVR")

        missing = []
        if impression_date_col is None:
            missing.append("Impression Date")
        if sku_col is None:
            missing.append("SKU Id")
        if views_col is None:
            missing.append("Product Views")
        if sales_col is None:
            missing.append("Sales")
        if revenue_col is None:
            missing.append("Revenue")
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing column(s): {', '.join(missing)}")

        # Parse date
        df[impression_date_col] = pd.to_datetime(df[impression_date_col], errors="coerce").dt.date
        df = df[df[impression_date_col].notna()]

        if replace_history:
            db.query(FlipkartTrafficRaw).filter(FlipkartTrafficRaw.workspace_id == ws_id).delete()
            db.commit()

        def norm_fk_sku(x: object) -> str:
            s = str(x or "").strip()
            if not s:
                return ""
            s = s.lower()
            # Ensure consistent Flipkart namespace
            if not s.startswith("fk:"):
                s = "fk:" + s
            return s

        def norm_fk_listing_id(x: object) -> str | None:
            """
            Optional: normalize Listing Id to a stable internal form.
            Your Listing Id looks like: LSTKTAH6WUWPZAH2NUQVLNNLG
            Your UI / sales style_key often looks like: fk:ktah6wuwpzah2nuq (derived)
            We'll store a best-effort normalized version for debugging/use later.
            """
            s = str(x or "").strip()
            if not s:
                return None
            s_up = s.upper()
            if s_up.startswith("LST"):
                s = s[3:]  # remove LST
            s = s.strip().lower()
            if not s:
                return None
            # match the shorter id pattern you've been using (first 16 chars)
            s = s[:16]
            if not s.startswith("fk:"):
                s = "fk:" + s
            return s

        def to_int(v: object) -> int:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return 0
            try:
                return int(float(str(v).replace(",", "").strip()))
            except Exception:
                return 0

        def to_float(v: object) -> float:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return 0.0
            try:
                s = str(v).strip()
                if s.endswith("%"):
                    s = s[:-1].strip()
                return float(s.replace(",", ""))
            except Exception:
                return 0.0

        def to_pct_or_none(v: object) -> float | None:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            try:
                s = str(v).strip()
                if not s:
                    return None
                if s.endswith("%"):
                    s = s[:-1].strip()
                return float(s.replace(",", ""))
            except Exception:
                return None

        now = datetime.utcnow()
        rows: list[FlipkartTrafficRaw] = []

        for _, r in df.iterrows():
            sku_norm = norm_fk_sku(r.get(sku_col))
            if not sku_norm:
                continue

            rec = FlipkartTrafficRaw(
                workspace_id=ws_id,
                impression_date=r[impression_date_col],
                seller_sku_code=sku_norm,
                listing_id=None if listing_col is None else norm_fk_listing_id(r.get(listing_col)),
                product_title=None if title_col is None else (str(r.get(title_col) or "").strip() or None),

                product_views=to_int(r.get(views_col)),
                product_clicks=0 if clicks_col is None else to_int(r.get(clicks_col)),
                sales_qty=to_int(r.get(sales_col)),
                revenue=to_float(r.get(revenue_col)),

                # IMPORTANT: In your file these are already "percent numbers":
                # CTR example: 17.55 (means 17.55%)
                # Conversion example: 0.53 (means 0.53%)
                ctr_pct=to_pct_or_none(None if ctr_col is None else r.get(ctr_col)),
                conversion_pct=to_pct_or_none(None if conv_col is None else r.get(conv_col)),

                raw_json=json.dumps({k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}, default=str),
                ingested_at=now,
            )
            rows.append(rec)

        if not rows:
            return {"ok": True, "inserted": 0, "workspace_slug": workspace_slug}

        # bulk insert
        db.bulk_save_objects(rows)
        db.commit()

        return {"ok": True, "inserted": len(rows), "workspace_slug": workspace_slug}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"flipkart traffic ingest failed: {e}")
    finally:
        db.close()
