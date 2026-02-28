# backend/flipkart_recon_models.py
# Flipkart Payment Reconciliation - Database Models

from sqlalchemy import Column, Integer, String, Float, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
from backend.db import Base


class FlipkartSkuPnl(Base):
    """Flipkart PNL Report - SKU-level P&L (from pnl_report.xlsx, sheet 'SKU-level P&L')"""
    __tablename__ = "flipkart_sku_pnl"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    sku_id = Column(String, nullable=False, index=True)
    sku_name = Column(String)

    # Units
    gross_units = Column(Integer)
    returned_cancelled_units = Column(Integer)
    rto_units = Column(Integer)
    rvp_units = Column(Integer)
    cancelled_units = Column(Integer)
    net_units = Column(Integer)

    # Sales
    estimated_net_sales = Column(Float)
    accounted_net_sales = Column(Float)

    # Expenses (all typically negative)
    total_expenses = Column(Float)
    commission_fee = Column(Float)
    collection_fee = Column(Float)
    fixed_fee = Column(Float)
    pick_and_pack_fee = Column(Float)
    forward_shipping_fee = Column(Float)
    offer_adjustments = Column(Float)
    reverse_shipping_fee = Column(Float)
    storage_fee = Column(Float)
    recall_fee = Column(Float)
    no_cost_emi_fee = Column(Float)
    installation_fee = Column(Float)
    tech_visit_fee = Column(Float)
    uninstallation_fee = Column(Float)
    customer_addons_recovery = Column(Float)
    franchise_fee = Column(Float)
    shopsy_marketing_fee = Column(Float)
    product_cancellation_fee = Column(Float)

    # Taxes
    taxes_gst = Column(Float)
    taxes_tcs = Column(Float)
    taxes_tds = Column(Float)

    # Rewards & Benefits
    rewards_other_benefits = Column(Float)
    rewards = Column(Float)
    order_spf = Column(Float)
    non_order_spf = Column(Float)

    # Settlement
    bank_settlement_projected = Column(Float)
    input_tax_credits = Column(Float)
    input_tax_gst_tcs = Column(Float)
    input_tax_tds = Column(Float)
    net_earnings = Column(Float)
    earnings_per_unit = Column(Float)
    net_margins_pct = Column(Float)
    amount_settled = Column(Float)
    amount_pending = Column(Float)

    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class FlipkartOrderPnl(Base):
    """Flipkart PNL Report - Order-level P&L (from pnl_report.xlsx, sheet 'Orders P&L')"""
    __tablename__ = "flipkart_order_pnl"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    order_date = Column(DateTime, index=True)
    order_id = Column(String, index=True)
    order_item_id = Column(String, index=True)
    sku_id = Column(String, index=True)
    fulfilment_type = Column(String)
    channel_of_sale = Column(String)
    mode_of_payment = Column(String)
    shipping_zone = Column(String)
    order_status = Column(String, index=True)

    # Units
    gross_units = Column(Integer)
    returned_cancelled_units = Column(Integer)
    rto_units = Column(Integer)
    rvp_units = Column(Integer)
    cancelled_units = Column(Integer)
    net_units = Column(Integer)

    # Sales
    sale_amount = Column(Float)
    seller_burn_offer = Column(Float)
    customer_addons_amount = Column(Float)
    estimated_net_sales = Column(Float)
    accounted_net_sales = Column(Float)

    # Expenses
    total_expenses = Column(Float)
    commission_fee = Column(Float)
    collection_fee = Column(Float)
    fixed_fee = Column(Float)
    pick_and_pack_fee = Column(Float)
    forward_shipping_fee = Column(Float)
    offer_adjustments = Column(Float)
    reverse_shipping_fee = Column(Float)
    storage_fee = Column(Float)
    recall_fee = Column(Float)
    no_cost_emi_fee = Column(Float)
    product_cancellation_fee = Column(Float)

    # Taxes
    taxes_gst = Column(Float)
    taxes_tcs = Column(Float)
    taxes_tds = Column(Float)

    # Rewards
    rewards_other_benefits = Column(Float)
    rewards = Column(Float)
    order_spf = Column(Float)
    non_order_spf = Column(Float)

    # Settlement
    bank_settlement_projected = Column(Float)
    input_tax_credits = Column(Float)
    net_earnings = Column(Float)
    earnings_per_unit = Column(Float)
    net_margins_pct = Column(Float)
    amount_settled = Column(Float)
    amount_pending = Column(Float)

    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class FlipkartPaymentReport(Base):
    """Flipkart Payment/Settlement Report - per order payment details"""
    __tablename__ = "flipkart_payment_report"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    # Payment
    neft_id = Column(String, index=True)
    neft_type = Column(String)
    payment_date = Column(DateTime, index=True)
    bank_settlement_value = Column(Float)
    input_gst_tcs_credits = Column(Float)
    income_tax_tds_credits = Column(Float)

    # Order
    order_id = Column(String, index=True)
    order_item_id = Column(String, index=True)
    sale_amount = Column(Float)
    total_offer_amount = Column(Float)
    my_share = Column(Float)
    customer_addons_amount = Column(Float)
    marketplace_fee = Column(Float)
    taxes = Column(Float)
    offer_adjustments = Column(Float)
    protection_fund = Column(Float)
    refund = Column(Float)

    # Fee breakdown
    tier = Column(String)
    commission_rate_pct = Column(Float)
    commission = Column(Float)
    fixed_fee = Column(Float)
    collection_fee = Column(Float)
    pick_and_pack_fee = Column(Float)
    shipping_fee = Column(Float)
    reverse_shipping_fee = Column(Float)
    no_cost_emi_fee = Column(Float)
    product_cancellation_fee = Column(Float)

    # Taxes
    tcs = Column(Float)
    tds = Column(Float)
    gst_on_mp_fees = Column(Float)

    # Shipping
    shipping_zone = Column(String)
    chargeable_wt_slab = Column(String)

    # Order details
    order_date = Column(DateTime, index=True)
    dispatch_date = Column(DateTime)
    fulfilment_type = Column(String)
    seller_sku = Column(String, index=True)
    quantity = Column(Integer)
    product_sub_category = Column(String)
    return_type = Column(String)
    item_return_status = Column(String)

    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)