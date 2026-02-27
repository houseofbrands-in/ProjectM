# backend/models.py

import uuid

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text, Date, UniqueConstraint


from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID

from backend.db import Base


class Workspace(Base):
    __tablename__ = "workspaces"

    # DB column is UUID (not text)
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    slug = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)

    created_at = Column(DateTime, nullable=False)

    style_monthly = relationship("StyleMonthly", back_populates="workspace")
    sales = relationship("SalesRaw", back_populates="workspace")
    returns = relationship("ReturnsRaw", back_populates="workspace")
    catalog = relationship("CatalogRaw", back_populates="workspace")


from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import text

# backend/models.py

from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

class SalesRaw(Base):
    __tablename__ = "sales_raw"

    id = Column(Integer, primary_key=True, index=True)

    order_line_id = Column(String, unique=True, index=True, nullable=False)
    style_key = Column(String, index=True, nullable=True)

    order_date = Column(DateTime, nullable=True)
    seller_sku_code = Column(String, nullable=True)
    raw_json = Column(Text, nullable=True)

    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    workspace = relationship("Workspace", back_populates="sales")

    # Each row is 1 unit for Myntra sales
    units = Column(Integer, nullable=False, server_default=text("1"))





class ReturnsRaw(Base):
    __tablename__ = "returns_raw"

    id = Column(Integer, primary_key=True, index=True)

    order_line_id = Column(String, unique=True, index=True, nullable=False)
    style_key = Column(String, index=True, nullable=True)

    # timestamp without time zone
    return_date = Column(DateTime, nullable=True)

    return_type = Column(String, nullable=True)
    units = Column(Integer, nullable=True)
    seller_sku_code = Column(String, nullable=True)
    raw_json = Column(String, nullable=True)

    # DB column is UUID (matches workspaces.id)
    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    workspace = relationship("Workspace", back_populates="returns")


class CatalogRaw(Base):
    __tablename__ = "catalog_raw"

    # No id in this table (style_key is PK)
    style_key = Column(String, primary_key=True, index=True)

    seller_sku_code = Column(String, nullable=True)
    brand = Column(String, nullable=True)
    product_name = Column(String, nullable=True)

    # Live Date
    style_catalogued_date = Column(DateTime, nullable=True)

    raw_json = Column(String, nullable=True)

    # DB column is UUID (matches workspaces.id)
    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    workspace = relationship("Workspace", back_populates="catalog")

class MyntraWeeklyPerfRaw(Base):
    __tablename__ = "myntra_weekly_perf_raw"

    id = Column(Integer, primary_key=True, index=True)

    # from report: "Style ID"
    style_key = Column(String, index=True, nullable=False)

    # report fields (as-is)
    seller_id = Column(Integer, nullable=True)
    article_type = Column(String, nullable=True)
    brand = Column(String, nullable=True)
    gender = Column(String, nullable=True)

    seller_mrp = Column(Float, nullable=True)
    inventory_age = Column(Integer, nullable=True)
    rplc = Column(Float, nullable=True)

    impressions = Column(Integer, nullable=True)
    clicks = Column(Integer, nullable=True)
    add_to_carts = Column(Integer, nullable=True)
    purchases = Column(Integer, nullable=True)

    return_pct = Column(Float, nullable=True)
    consideration_pct = Column(Float, nullable=True)
    conversion_pct = Column(Float, nullable=True)
    rating = Column(Float, nullable=True)

    # when we ingested this file
    ingested_at = Column(DateTime, nullable=True)

    raw_json = Column(String, nullable=True)

    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)

class StockRaw(Base):
    __tablename__ = "stock_raw"

    id = Column(Integer, primary_key=True, index=True)

    # join key (matches CatalogRaw.seller_sku_code, includes size suffix)
    seller_sku_code = Column(String, index=True, nullable=False)

    qty = Column(Integer, nullable=False, default=0)

    # snapshot timestamp (each upload creates a new snapshot)
    ingested_at = Column(DateTime, nullable=True)

    raw_json = Column(Text, nullable=True)

    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)


class StyleMonthly(Base):
    __tablename__ = "style_monthly"

    id = Column(Integer, primary_key=True, index=True)

    # Month bucket (store as the 1st day of month, e.g. 2025-11-01)
    month_start = Column(Date, nullable=False, index=True)

    style_key = Column(String, nullable=False, index=True)

    orders = Column(Integer, nullable=False, server_default=text("0"))
    returns = Column(Integer, nullable=False, server_default=text("0"))

    # Optional, can be filled later
    revenue = Column(Float, nullable=True)

    # Optional convenience fields
    last_order_date = Column(DateTime, nullable=True)

    # Optional, we can compute later; keeping nullable avoids forcing logic now
    return_pct = Column(Float, nullable=True)

    updated_at = Column(DateTime, nullable=False, server_default=text("now()"))

    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    workspace = relationship("Workspace", back_populates="style_monthly")

    __table_args__ = (
        UniqueConstraint("workspace_id", "month_start", "style_key", name="uq_style_monthly_ws_month_style"),
    )

from sqlalchemy import Column, Integer, Text, Date, DateTime, Float
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime

class FlipkartTrafficRaw(Base):
    __tablename__ = "flipkart_traffic_raw"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    impression_date = Column(Date, nullable=False, index=True)

    # keys
    seller_sku_code = Column(Text, nullable=True, index=True)  # your "SKU Id"
    listing_id = Column(Text, nullable=True, index=True)       # your "Listing Id"
    product_title = Column(Text, nullable=True)

    # metrics
    product_views = Column(Integer, nullable=True)  # impressions
    product_clicks = Column(Integer, nullable=True)
    sales_qty = Column(Integer, nullable=True)
    revenue = Column(Float, nullable=True)

    ctr_pct = Column(Float, nullable=True)
    conversion_pct = Column(Float, nullable=True)

    raw_json = Column(Text, nullable=True)

    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
class FlipkartGstrSalesRaw(Base):
    __tablename__ = "flipkart_gstr_sales_raw"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    # keys
    order_id = Column(Text, nullable=True, index=True)
    order_item_id = Column(Text, nullable=True, index=True)
    seller_sku_code = Column(Text, nullable=True, index=True)  # SKU in GSTR (Flipkart: acts as Style+SKU)

    # dates (we use Order Date as timeline)
    order_date = Column(Date, nullable=True, index=True)
    buyer_invoice_date = Column(Date, nullable=True, index=True)

    # qty + amount
    item_quantity = Column(Integer, nullable=True)
    buyer_invoice_amount = Column(Float, nullable=True)  # Buyer Invoice Amount (can be negative)

    # event info
    event_type = Column(Text, nullable=True, index=True)
    event_sub_type = Column(Text, nullable=True, index=True)

    product_title = Column(Text, nullable=True)

    raw_json = Column(Text, nullable=True)

    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)