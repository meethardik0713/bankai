"""
core/gstr1.py
─────────────
GSTR-1 Auto-Fill Engine
- Bank PDF transactions + Invoice Excel cross-match
- GSTIN validation
- Table 4 (B2B), Table 5 (B2C Large), Table 12 (HSN Summary)
- Reconciliation: Declared sales vs Bank receipts
"""

import re
import pandas as pd
from collections import defaultdict

# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════

GSTIN_REGEX = re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z][Z][0-9A-Z]$')

# Basic HSN map — merchant keyword → HSN code + description
HSN_MAP = {
    'restaurant': ('9963', 'Food & Beverage Services'),
    'hotel':      ('9963', 'Food & Beverage Services'),
    'pizza':      ('9963', 'Food & Beverage Services'),
    'food':       ('9963', 'Food & Beverage Services'),
    'pharma':     ('3004', 'Medicaments'),
    'medicine':   ('3004', 'Medicaments'),
    'software':   ('8523', 'Software/IT Services'),
    'it ':        ('8523', 'Software/IT Services'),
    'tech':       ('8523', 'Software/IT Services'),
    'consulting': ('9983', 'Professional Services'),
    'legal':      ('9982', 'Legal Services'),
    'transport':  ('9965', 'Goods Transport'),
    'logistics':  ('9965', 'Goods Transport'),
    'courier':    ('9965', 'Goods Transport'),
    'interior':   ('9954', 'Construction/Interior'),
    'construct':  ('9954', 'Construction/Interior'),
    'builder':    ('9954', 'Construction/Interior'),
    'retail':     ('9999', 'Retail Trade'),
    'wholesale':  ('9999', 'Wholesale Trade'),
    'cloth':      ('6203', 'Apparel'),
    'garment':    ('6203', 'Apparel'),
    'fashion':    ('6203', 'Apparel'),
    'machinery':  ('8431', 'Machinery Parts'),
    'electric':   ('8544', 'Electrical Equipment'),
    'fertilizer': ('3105', 'Fertilizers'),
    'steel':      ('7208', 'Steel Products'),
    'cement':     ('2523', 'Cement'),
}

GST_RATES = [0, 5, 12, 18, 28]  # Standard GST slabs


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def validate_gstin(gstin: str) -> bool:
    if not gstin or not isinstance(gstin, str):
        return False
    return bool(GSTIN_REGEX.match(gstin.strip().upper()))


def get_state_from_gstin(gstin: str) -> str:
    STATE_CODES = {
        '01': 'Jammu & Kashmir', '02': 'Himachal Pradesh', '03': 'Punjab',
        '04': 'Chandigarh', '05': 'Uttarakhand', '06': 'Haryana',
        '07': 'Delhi', '08': 'Rajasthan', '09': 'Uttar Pradesh',
        '10': 'Bihar', '11': 'Sikkim', '12': 'Arunachal Pradesh',
        '13': 'Nagaland', '14': 'Manipur', '15': 'Mizoram',
        '16': 'Tripura', '17': 'Meghalaya', '18': 'Assam',
        '19': 'West Bengal', '20': 'Jharkhand', '21': 'Odisha',
        '22': 'Chhattisgarh', '23': 'Madhya Pradesh', '24': 'Gujarat',
        '26': 'Dadra & Nagar Haveli', '27': 'Maharashtra', '28': 'Andhra Pradesh',
        '29': 'Karnataka', '30': 'Goa', '31': 'Lakshadweep',
        '32': 'Kerala', '33': 'Tamil Nadu', '34': 'Puducherry',
        '35': 'Andaman & Nicobar', '36': 'Telangana', '37': 'Andhra Pradesh (New)',
    }
    try:
        code = gstin[:2]
        return STATE_CODES.get(code, 'Unknown')
    except:
        return 'Unknown'


def infer_hsn(description: str) -> tuple:
    if not description:
        return ('9999', 'General Trade')
    lower = description.lower()
    for keyword, (hsn, desc) in HSN_MAP.items():
        if keyword in lower:
            return (hsn, desc)
    return ('9999', 'General Trade')


def infer_gst_rate(cgst: float, sgst: float, igst: float, taxable: float) -> float:
    total_tax = cgst + sgst + igst
    if taxable <= 0:
        return 18.0
    rate = round((total_tax / taxable) * 100)
    # Snap to nearest GST slab
    nearest = min(GST_RATES, key=lambda x: abs(x - rate))
    return float(nearest)


# ═══════════════════════════════════════════════════════════
#  INVOICE PARSER
# ═══════════════════════════════════════════════════════════

def parse_invoice_excel(filepath: str) -> dict:
    """
    Parse uploaded invoice Excel.
    Expected columns (flexible matching):
    Invoice No | Date | Customer Name | Customer GSTIN | Taxable Value | CGST | SGST | IGST
    """
    try:
        df = pd.read_excel(filepath, sheet_name=0)
    except Exception as e:
        return {'error': f'Excel read failed: {str(e)}', 'invoices': []}

    # Normalize column names
    df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]

    # Flexible column mapping — GSTIN must be checked before 'customer'
    col_map = {}
    for col in df.columns:
        if 'cgst' in col:
            col_map['cgst'] = col
        elif 'sgst' in col:
            col_map['sgst'] = col
        elif 'igst' in col:
            col_map['igst'] = col
        elif any(k in col for k in ['gstin', 'gst_no', 'gst_number', 'gstn']):
            col_map['gstin'] = col
        elif any(k in col for k in ['invoice', 'inv_no', 'inv_num', 'bill_no']):
            col_map['invoice_no'] = col
        elif any(k in col for k in ['date', 'dt']):
            col_map['date'] = col
        elif any(k in col for k in ['taxable', 'basic']):
            col_map['taxable_value'] = col
        elif any(k in col for k in ['customer', 'client', 'buyer', 'party']):
            col_map['customer_name'] = col
        elif any(k in col for k in ['total', 'invoice_value', 'gross']):
            col_map['total'] = col
        elif any(k in col for k in ['desc', 'item', 'product', 'narration']):
            col_map['description'] = col

    invoices = []
    errors   = []

    for idx, row in df.iterrows():
        try:
            inv_no    = str(row.get(col_map.get('invoice_no', ''), f'INV-{idx+1}')).strip()
            date      = str(row.get(col_map.get('date', ''), '')).strip()
            cust_name_raw = row.get(col_map.get('customer_name', ''), '')
            cust_name = str(cust_name_raw).strip() if str(cust_name_raw).strip() not in ('nan', '', 'None') else 'Unknown'
            gstin_raw = str(row.get(col_map.get('gstin', ''), '')).strip().upper()
            desc      = str(row.get(col_map.get('description', ''), '')).strip()

            def to_float(val):
                try:
                    return float(str(val).replace(',', '').replace('₹', '').strip())
                except:
                    return 0.0

            taxable = to_float(row.get(col_map.get('taxable_value', ''), 0))
            cgst    = to_float(row.get(col_map.get('cgst', ''), 0))
            sgst    = to_float(row.get(col_map.get('sgst', ''), 0))
            igst    = to_float(row.get(col_map.get('igst', ''), 0))
            total   = to_float(row.get(col_map.get('total', ''), 0))

            if total == 0:
                total = taxable + cgst + sgst + igst

            if taxable == 0 and total > 0:
                taxable = round(total / 1.18, 2)

            gstin_valid = validate_gstin(gstin_raw)
            gst_rate    = infer_gst_rate(cgst, sgst, igst, taxable)
            hsn, hsn_desc = infer_hsn(desc or cust_name)
            supply_type = 'B2B' if gstin_valid else 'B2C'
            # Debug log
            import logging
            logging.getLogger(__name__).info("INV %s GSTIN=%s valid=%s type=%s", inv_no, gstin_raw, gstin_valid, supply_type)
            state       = get_state_from_gstin(gstin_raw) if gstin_valid else 'Unknown'
            is_interstate = igst > 0

            invoices.append({
                'invoice_no':    inv_no,
                'date':          date,
                'customer_name': cust_name,
                'gstin':         gstin_raw if gstin_valid else '',
                'gstin_valid':   gstin_valid,
                'gstin_raw':     gstin_raw,
                'taxable_value': round(taxable, 2),
                'cgst':          round(cgst, 2),
                'sgst':          round(sgst, 2),
                'igst':          round(igst, 2),
                'total':         round(total, 2),
                'gst_rate':      gst_rate,
                'hsn_code':      hsn,
                'hsn_desc':      hsn_desc,
                'supply_type':   supply_type,
                'state':         state,
                'is_interstate': is_interstate,
                'description':   desc,
                'bank_matched':  False,
                'bank_amount':   0.0,
                'reconciled':    False,
            })

        except Exception as e:
            errors.append(f'Row {idx+1}: {str(e)}')

    return {
        'invoices': invoices,
        'errors':   errors,
        'total_invoices': len(invoices),
        'columns_detected': col_map,
    }


# ═══════════════════════════════════════════════════════════
#  BANK RECONCILIATION
# ═══════════════════════════════════════════════════════════

def reconcile_with_bank(invoices: list, transactions: list) -> list:
    """
    Match invoice totals against bank CR transactions.
    Fuzzy match on amount ± 1% tolerance.
    """
    cr_txns = [t for t in transactions if t.get('type') == 'CR' and t.get('amount')]

    for inv in invoices:
        inv_total = inv['total']
        if inv_total <= 0:
            continue

        for txn in cr_txns:
            bank_amt = txn['amount']
            # Amount match within 1% tolerance
            if abs(bank_amt - inv_total) / max(inv_total, 1) <= 0.01:
                inv['bank_matched'] = True
                inv['bank_amount']  = bank_amt
                inv['bank_date']    = txn.get('date', '')
                inv['reconciled']   = True
                break

    return invoices


# ═══════════════════════════════════════════════════════════
#  GSTR-1 TABLES
# ═══════════════════════════════════════════════════════════

def generate_table4(invoices: list) -> list:
    """Table 4: B2B Invoices (GSTIN holders)"""
    return [inv for inv in invoices if inv['supply_type'] == 'B2B' and inv['gstin_valid']]


def generate_table5(invoices: list, seller_state_code: str = '27') -> list:
    """Table 5: B2C Large — Interstate B2C invoices > ₹2.5L"""
    result = []
    for inv in invoices:
        if inv['supply_type'] == 'B2C' and inv['total'] > 250000:
            # Interstate = different state OR IGST present
            if inv['is_interstate'] or inv['igst'] > 0:
                result.append(inv)
    return result


def generate_table12(invoices: list) -> list:
    """Table 12: HSN Summary"""
    hsn_summary = defaultdict(lambda: {
        'hsn_code': '', 'hsn_desc': '', 'total_value': 0,
        'taxable_value': 0, 'cgst': 0, 'sgst': 0, 'igst': 0, 'count': 0
    })

    for inv in invoices:
        hsn = inv['hsn_code']
        hsn_summary[hsn]['hsn_code']     = hsn
        hsn_summary[hsn]['hsn_desc']     = inv['hsn_desc']
        hsn_summary[hsn]['total_value']  += inv['total']
        hsn_summary[hsn]['taxable_value']+= inv['taxable_value']
        hsn_summary[hsn]['cgst']         += inv['cgst']
        hsn_summary[hsn]['sgst']         += inv['sgst']
        hsn_summary[hsn]['igst']         += inv['igst']
        hsn_summary[hsn]['count']        += 1

    return [
        {**v, 'total_value': round(v['total_value'], 2),
         'taxable_value': round(v['taxable_value'], 2),
         'cgst': round(v['cgst'], 2), 'sgst': round(v['sgst'], 2),
         'igst': round(v['igst'], 2)}
        for v in hsn_summary.values()
    ]


# ═══════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════

def run_gstr1(invoice_filepath: str, transactions: list) -> dict:
    # Parse invoices
    parsed      = parse_invoice_excel(invoice_filepath)
    invoices    = parsed.get('invoices', [])
    parse_errors= parsed.get('errors', [])

    if not invoices:
        return {'error': 'No invoices parsed', 'parse_errors': parse_errors}

    # Reconcile with bank
    invoices = reconcile_with_bank(invoices, transactions)

    # Generate tables
    table4 = generate_table4(invoices)
    table5 = generate_table5(invoices)
    table12= generate_table12(invoices)

    # Summary numbers
    total_declared    = round(sum(inv['total'] for inv in invoices), 2)
    total_b2b         = round(sum(inv['total'] for inv in table4), 2)
    total_b2c         = round(sum(inv['total'] for inv in invoices if inv['supply_type'] == 'B2C'), 2)
    total_bank_cr     = round(sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount')), 2)
    matched_count     = sum(1 for inv in invoices if inv['bank_matched'])
    unmatched_invoices= [inv for inv in invoices if not inv['bank_matched']]
    gstin_invalid     = [inv for inv in invoices if inv['gstin_raw'] and not inv['gstin_valid']]
    recon_gap         = round(total_declared - total_bank_cr, 2)

    total_cgst = round(sum(inv['cgst'] for inv in invoices), 2)
    total_sgst = round(sum(inv['sgst'] for inv in invoices), 2)
    total_igst = round(sum(inv['igst'] for inv in invoices), 2)
    total_tax  = round(total_cgst + total_sgst + total_igst, 2)

    return {
        'invoices':           invoices,
        'table4':             table4,
        'table5':             table5,
        'table12':            table12,
        'parse_errors':       parse_errors,
        'total_invoices':     len(invoices),
        'total_declared':     total_declared,
        'total_b2b':          total_b2b,
        'total_b2c':          total_b2c,
        'total_bank_cr':      total_bank_cr,
        'matched_count':      matched_count,
        'unmatched_invoices': unmatched_invoices,
        'gstin_invalid':      gstin_invalid,
        'recon_gap':          recon_gap,
        'total_cgst':         total_cgst,
        'total_sgst':         total_sgst,
        'total_igst':         total_igst,
        'total_tax':          total_tax,
        'b2b_count':          len(table4),
        'b2c_large_count':    len(table5),
        'filing_ready':       len(gstin_invalid) == 0 and len(unmatched_invoices) == 0,
    }
