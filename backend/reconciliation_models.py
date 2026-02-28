# backend/reconciliation_models.py
# Myntra Payment Reconciliation - Database Models

from sqlalchemy import (
    Column, Integer, String, Float, Text, Date, DateTime,
    ForeignKey, Index
)
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime

from backend.db import Base


class MyntraPgForward(Base):
    """PG Forward Settled + Unsettled — every forward (sale) payment line."""
    __tablename__ = "myntra_pg_forward"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    # Settlement status: 'settled' or 'unsettled'
    settlement_status = Column(String, nullable=False, index=True)

    # Order identifiers
    order_release_id = Column(String, index=True)
    order_line_id = Column(String, index=True)
    sku_code = Column(String, index=True)
    packet_id = Column(String)
    invoice_number = Column(String)
    hsn_code = Column(String)
    product_tax_category = Column(String)
    seller_order_id = Column(String)

    # Dates
    packing_date = Column(DateTime)
    delivery_date = Column(DateTime)

    # Amounts
    currency = Column(String, default="INR")
    seller_product_amount = Column(Float)
    postpaid_amount = Column(Float)
    prepaid_amount = Column(Float)
    mrp = Column(Float)
    total_discount_amount = Column(Float)
    customer_paid_amt = Column(Float)

    # Tax
    shipping_case = Column(String)
    total_tax_rate = Column(Float)
    igst_amount = Column(Float)
    cgst_amount = Column(Float)
    sgst_amount = Column(Float)
    tcs_amount = Column(Float)
    tds_amount = Column(Float)
    taxable_amount = Column(Float)
    igst_rate = Column(Float)
    cgst_rate = Column(Float)
    sgst_rate = Column(Float)
    cess_amount = Column(Float)
    cess_rate = Column(Float)
    tcs_igst_rate = Column(Float)
    tcs_sgst_rate = Column(Float)
    tcs_cgst_rate = Column(Float)
    tds_rate = Column(Float)

    # Commission & Fees
    commission_percentage = Column(Float)
    minimum_commission = Column(Float)
    platform_fees = Column(Float)
    total_commission = Column(Float)
    total_commission_plus_tcs_tds_deduction = Column(Float)
    commission_base_amount = Column(Float)
    commission_tax_amount = Column(Float)
    commission_discount = Column(Float)
    sjit_incentive_amount = Column(Float)

    # Logistics
    total_logistics_deduction = Column(Float)
    shipping_fee = Column(Float)
    fixed_fee = Column(Float)
    pick_and_pack_fee = Column(Float)
    payment_gateway_fee = Column(Float)
    total_tax_on_logistics = Column(Float)

    # Settlement
    article_level = Column(Integer)
    shipment_zone_classification = Column(String)
    total_expected_settlement = Column(Float)
    total_actual_settlement = Column(Float)
    amount_pending_settlement = Column(Float)

    # Prepaid settlement breakdown
    prepaid_commission_deduction = Column(Float)
    prepaid_logistics_deduction = Column(Float)
    prepaid_payment = Column(Float)
    settlement_date_prepaid_comm_deduction = Column(DateTime)
    settlement_date_prepaid_logistics_deduction = Column(DateTime)
    settlement_date_prepaid_payment = Column(DateTime)
    bank_utr_no_prepaid_comm_deduction = Column(String)
    bank_utr_no_prepaid_logistics_deduction = Column(String)
    bank_utr_no_prepaid_payment = Column(String)

    # Postpaid settlement breakdown
    postpaid_commission_deduction = Column(Float)
    postpaid_logistics_deduction = Column(Float)
    postpaid_payment = Column(Float)
    settlement_date_postpaid_comm_deduction = Column(DateTime)
    settlement_date_postpaid_logistics_deduction = Column(DateTime)
    settlement_date_postpaid_payment = Column(DateTime)
    bank_utr_no_postpaid_comm_deduction = Column(String)
    bank_utr_no_postpaid_logistics_deduction = Column(String)
    bank_utr_no_postpaid_payment = Column(String)

    # Detailed commission (prepaid/postpaid split)
    prepaid_commission_percentage = Column(Float)
    prepaid_minimum_commission = Column(Float)
    prepaid_platform_fees = Column(Float)
    prepaid_total_commission = Column(Float)
    postpaid_commission_percentage = Column(Float)
    postpaid_minimum_commission = Column(Float)
    postpaid_platform_fees = Column(Float)
    postpaid_total_commission = Column(Float)

    # Royalty & Marketing charges
    royaltyCharges_prepaid = Column(Float)
    royaltyCharges_postpaid = Column(Float)
    royaltyPercent_prepaid = Column(Float)
    royaltyPercent_postpaid = Column(Float)
    marketingCharges_prepaid = Column(Float)
    marketingCharges_postpaid = Column(Float)
    marketingPercent_prepaid = Column(Float)
    marketingPercent_postpaid = Column(Float)
    marketingContribution_prepaid = Column(Float)
    marketingContribution_postpaid = Column(Float)
    techEnablement_prepaid = Column(Float)
    techEnablement_postpaid = Column(Float)
    airLogistics_prepaid = Column(Float)
    airLogistics_postpaid = Column(Float)
    forwardAdditionalCharges_prepaid = Column(Float)
    forwardAdditionalCharges_postpaid = Column(Float)

    # Product info
    brand = Column(String, index=True)
    gender = Column(String)
    brand_type = Column(String)
    article_type = Column(String)
    supply_type = Column(String)
    try_and_buy_purchase = Column(String)
    seller_tier = Column(String)

    # Shipping info
    seller_gstn = Column(String)
    seller_name = Column(String)
    myntra_gstn = Column(String)
    shipping_city = Column(String)
    shipping_pin_code = Column(String)
    shipping_state = Column(String)
    shipping_state_code = Column(String)

    # Other amounts
    postpaid_amount_other = Column(Float)
    prepaid_amount_other = Column(Float)
    shipping_amount = Column(Float)
    gift_amount = Column(Float)
    additional_amount = Column(Float)

    # Raw + metadata
    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class MyntraPgReverse(Base):
    """PG Reverse Settled + Unsettled — every reverse (return/RTO) payment line."""
    __tablename__ = "myntra_pg_reverse"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    settlement_status = Column(String, nullable=False, index=True)

    # Order identifiers
    order_release_id = Column(String, index=True)
    order_line_id = Column(String, index=True)
    sku_code = Column(String, index=True)
    packet_id = Column(String)
    invoice_number = Column(String)
    hsn_code = Column(String)
    product_tax_category = Column(String)
    seller_order_id = Column(String)
    return_id = Column(String, index=True)

    # Return info
    return_type = Column(String, index=True)
    return_date = Column(DateTime)
    packing_date = Column(DateTime)
    delivery_date = Column(DateTime)

    # Amounts (typically negative for reverse)
    currency = Column(String, default="INR")
    seller_product_amount = Column(Float)
    postpaid_amount = Column(Float)
    prepaid_amount = Column(Float)
    mrp = Column(Float)
    total_discount_amount = Column(Float)
    customer_paid_amt = Column(Float)

    # Tax
    shipping_case = Column(String)
    total_tax_rate = Column(Float)
    igst_amount = Column(Float)
    cgst_amount = Column(Float)
    sgst_amount = Column(Float)
    tcs_amount = Column(Float)
    tds_amount = Column(Float)
    taxable_amount = Column(Float)
    igst_rate = Column(Float)
    cgst_rate = Column(Float)
    sgst_rate = Column(Float)
    cess_amount = Column(Float)
    cess_rate = Column(Float)
    tcs_igst_rate = Column(Float)
    tcs_sgst_rate = Column(Float)
    tcs_cgst_rate = Column(Float)
    tds_rate = Column(Float)

    # Commission & Fees
    commission_percentage = Column(Float)
    minimum_commission = Column(Float)
    platform_fees = Column(Float)
    total_commission = Column(Float)
    total_commission_plus_tcs_tds_deduction = Column(Float)
    commission_base_amount = Column(Float)
    commission_tax_amount = Column(Float)
    commission_discount = Column(Float)
    sjit_incentive_amount = Column(Float)

    # Logistics
    total_logistics_deduction = Column(Float)
    shipping_fee = Column(Float)
    fixed_fee = Column(Float)
    pick_and_pack_fee = Column(Float)
    payment_gateway_fee = Column(Float)
    total_tax_on_logistics = Column(Float)

    # Settlement
    article_level = Column(Integer)
    shipment_zone_classification = Column(String)
    total_settlement = Column(Float)
    total_actual_settlement = Column(Float)
    amount_pending_settlement = Column(Float)

    # Prepaid settlement breakdown
    prepaid_commission_deduction = Column(Float)
    prepaid_logistics_deduction = Column(Float)
    prepaid_payment = Column(Float)
    settlement_date_prepaid_comm_deduction = Column(DateTime)
    settlement_date_prepaid_logistics_deduction = Column(DateTime)
    settlement_date_prepaid_payment = Column(DateTime)
    bank_utr_no_prepaid_comm_deduction = Column(String)
    bank_utr_no_prepaid_logistics_deduction = Column(String)
    bank_utr_no_prepaid_payment = Column(String)

    # Postpaid settlement breakdown
    postpaid_commission_deduction = Column(Float)
    postpaid_logistics_deduction = Column(Float)
    postpaid_payment = Column(Float)
    settlement_date_postpaid_comm_deduction = Column(DateTime)
    settlement_date_postpaid_logistics_deduction = Column(DateTime)
    settlement_date_postpaid_payment = Column(DateTime)
    bank_utr_no_postpaid_comm_deduction = Column(String)
    bank_utr_no_postpaid_logistics_deduction = Column(String)
    bank_utr_no_postpaid_payment = Column(String)

    # Detailed commission
    prepaid_commission_percentage = Column(Float)
    prepaid_minimum_commission = Column(Float)
    prepaid_platform_fees = Column(Float)
    prepaid_total_commission = Column(Float)
    postpaid_commission_percentage = Column(Float)
    postpaid_minimum_commission = Column(Float)
    postpaid_platform_fees = Column(Float)
    postpaid_total_commission = Column(Float)

    # Royalty & Marketing charges
    royaltyCharges_prepaid = Column(Float)
    royaltyCharges_postpaid = Column(Float)
    royaltyPercent_prepaid = Column(Float)
    royaltyPercent_postpaid = Column(Float)
    marketingCharges_prepaid = Column(Float)
    marketingCharges_postpaid = Column(Float)
    marketingPercent_prepaid = Column(Float)
    marketingPercent_postpaid = Column(Float)
    marketingContribution_prepaid = Column(Float)
    marketingContribution_postpaid = Column(Float)
    reverseAdditionalCharges_prepaid = Column(Float)
    reverseAdditionalCharges_postpaid = Column(Float)

    # Product info
    brand = Column(String, index=True)
    gender = Column(String)
    brand_type = Column(String)
    article_type = Column(String)
    supply_type = Column(String)
    try_and_buy_purchase = Column(String)
    seller_tier = Column(String)

    # Shipping info
    seller_gstn = Column(String)
    seller_name = Column(String)
    myntra_gstn = Column(String)
    shipping_city = Column(String)
    shipping_pin_code = Column(String)
    shipping_state = Column(String)
    shipping_state_code = Column(String)

    # Other
    postpaid_amount_other = Column(Float)
    prepaid_amount_other = Column(Float)
    shipping_amount = Column(Float)
    gift_amount = Column(Float)
    additional_amount = Column(Float)

    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class MyntraNonOrderSettlement(Base):
    """Non-order settlements — penalties, SPF claims, adjustments."""
    __tablename__ = "myntra_non_order_settlement"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    seller_name = Column(String)
    settlement_amount = Column(Float)
    settlement_type = Column(String, index=True)  # 'debit' / 'credit'
    utr = Column(String)
    invoice_ref = Column(String, index=True)
    settlement_date = Column(DateTime)
    settlement_description = Column(String)

    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class MyntraOrderFlow(Base):
    """Order Flow — master lifecycle view linking forward + reverse."""
    __tablename__ = "myntra_order_flow"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    # Order identifiers
    sale_order_code = Column(String, index=True)
    order_number = Column(String, index=True)
    product_sku_code = Column(String, index=True)
    invoice_number = Column(String)
    seller_order_id = Column(String)
    packed_id = Column(String)

    # Status
    order_item_status = Column(String, index=True)
    return_type = Column(String, index=True)

    # Dates
    order_date = Column(DateTime, index=True)
    packing_date = Column(DateTime)
    promised_delivery_date = Column(DateTime)
    actual_delivery_date = Column(DateTime)
    return_date = Column(DateTime)
    restocked_date = Column(DateTime)
    promised_settlement_date = Column(DateTime)

    # Amounts
    currency = Column(String, default="INR")
    seller_paid_amount = Column(Float)
    postpaid_amount = Column(Float)
    prepaid_amount = Column(Float)
    mrp = Column(Float)
    discount_amount = Column(Float)
    customer_paid_amt_fw = Column(Float)
    customer_paid_amt_rv = Column(Float)

    # Tax
    shipping_case = Column(String)
    tax_rate = Column(Float)
    igst_amount = Column(Float)
    cgst_amount = Column(Float)
    sgst_amount = Column(Float)
    tcs_igst_amt = Column(Float)
    tcs_sgst_amt = Column(Float)
    tcs_cgst_amt = Column(Float)
    taxable_amount = Column(Float)
    igst_rate = Column(Float)
    cgst_rate = Column(Float)
    sgst_rate = Column(Float)
    tcs_igst_rate = Column(Float)
    tcs_sgst_rate = Column(Float)
    tcs_cgst_rate = Column(Float)

    # Commission
    minimum_commission = Column(Float)
    commission_pct = Column(Float)
    commission_total_amount = Column(Float)
    commission_base_amount = Column(Float)
    commission_tax_amount = Column(Float)

    # Forward settlement
    total_commission_plus_tcs_deduction_fw = Column(Float)
    logistics_deduction_fw = Column(Float)
    total_settlement_fw = Column(Float)
    amount_pending_settlement_fw = Column(Float)
    prepaid_commission_deduction_fw = Column(Float)
    prepaid_logistics_deduction_fw = Column(Float)
    prepaid_payment_fw = Column(Float)
    postpaid_commission_deduction_fw = Column(Float)
    postpaid_logistics_deduction_fw = Column(Float)
    postpaid_payment_fw = Column(Float)

    # Forward settlement dates & UTR
    settlement_date_prepaid_comm_deduction_fw = Column(DateTime)
    settlement_date_prepaid_logistics_deduction_fw = Column(DateTime)
    settlement_date_prepaid_payment_fw = Column(DateTime)
    settlement_date_postpaid_comm_deduction_fw = Column(DateTime)
    settlement_date_postpaid_logistics_deduction_fw = Column(DateTime)
    settlement_date_postpaid_payment_fw = Column(DateTime)
    bank_utr_no_prepaid_comm_deduction_fw = Column(String)
    bank_utr_no_prepaid_logistics_deduction_fw = Column(String)
    bank_utr_no_prepaid_payment_fw = Column(String)
    bank_utr_no_postpaid_comm_deduction_fw = Column(String)
    bank_utr_no_postpaid_logistics_deduction_fw = Column(String)
    bank_utr_no_postpaid_payment_fw = Column(String)

    # Reverse settlement
    total_commission_plus_tcs_deduction_rv = Column(Float)
    logistics_deduction_rv = Column(Float)
    customer_paid_amt_rv_2 = Column(Float)
    total_settlement_rv = Column(Float)
    amount_pending_settlement_rv = Column(Float)
    prepaid_commission_deduction_rv = Column(Float)
    prepaid_logistics_deduction_rv = Column(Float)
    prepaid_payment_rv = Column(Float)
    postpaid_commission_deduction_rv = Column(Float)
    postpaid_logistics_deduction_rv = Column(Float)
    postpaid_payment_rv = Column(Float)

    # Reverse settlement dates & UTR
    settlement_date_prepaid_comm_deduction_rv = Column(DateTime)
    settlement_date_prepaid_logistics_deduction_rv = Column(DateTime)
    settlement_date_prepaid_payment_rv = Column(DateTime)
    settlement_date_postpaid_comm_deduction_rv = Column(DateTime)
    settlement_date_postpaid_logistics_deduction_rv = Column(DateTime)
    settlement_date_postpaid_payment_rv = Column(DateTime)
    bank_utr_no_prepaid_comm_deduction_rv = Column(String)
    bank_utr_no_prepaid_logistics_deduction_rv = Column(String)
    bank_utr_no_prepaid_payment_rv = Column(String)
    bank_utr_no_postpaid_comm_deduction_rv = Column(String)
    bank_utr_no_postpaid_logistics_deduction_rv = Column(String)
    bank_utr_no_postpaid_payment_rv = Column(String)

    # Product info
    brand = Column(String, index=True)
    gender = Column(String)
    article_type = Column(String)
    supply_type = Column(String)
    is_try_and_buy = Column(String)
    payment_method = Column(String)
    courier_name = Column(String)
    tracking_no = Column(String)

    # GST / Seller info
    hsn = Column(String)
    product_tax_category = Column(String)
    e_commerce_portal_name = Column(String)
    seller_gstn = Column(String)
    seller_name = Column(String)
    seller_state_code = Column(String)
    myntra_gstn = Column(String)

    # Customer info
    customer_name = Column(String)
    customer_pincode = Column(String)
    customer_state = Column(String)

    # Other amounts
    additional_amount = Column(Float)
    postpaid_amount_other = Column(Float)
    prepaid_amount_other = Column(Float)
    shipping_amount = Column(Float)
    gift_amount = Column(Float)
    cart_discount = Column(Float)
    coupon_discount = Column(Float)
    total_customer_paid = Column(Float)

    raw_json = Column(Text)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class MyntraSkuMap(Base):
    """Mapping: Myntra sku_code -> seller_sku_code (from Listings Report)"""
    __tablename__ = "myntra_sku_map"

    id = Column(Integer, primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    sku_code = Column(String, nullable=False, index=True)
    sku_id = Column(String)
    seller_sku_code = Column(String, index=True)
    style_id = Column(String, index=True)
    style_name = Column(String)
    brand = Column(String)
    article_type = Column(String)
    size = Column(String)
    mrp = Column(Float)

    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow)