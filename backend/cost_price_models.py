# backend/cost_price_models.py
# Cost Price Master — shared across Myntra & Flipkart

from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
from backend.db import Base


class SkuCostPrice(Base):
    """Seller's cost price per SKU — used for true profitability analysis."""
    __tablename__ = "sku_cost_price"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    seller_sku_code = Column(String, nullable=False, index=True)
    cost_price = Column(Float, nullable=False)

    # Optional metadata
    sku_name = Column(String)
    brand = Column(String)
    category = Column(String)

    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)