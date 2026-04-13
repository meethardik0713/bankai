"""
core/gstr3b.py
──────────────
GSTR-3B Auto-Fill Engine
- Bank PDF + Sales Invoice Excel + Purchase Invoice Excel
- Table 3.1: Outward Supplies
- Table 3.2: Interstate Supplies (state-wise)
- Table 4:   ITC Available
- Table 5:   Exempt/Nil/Non-GST Inward Supplies
- Table 6.1: Tax Payable vs Paid
- Net Tax Liability Calculation
"""

import re
import pandas as pd
from collections import defaultdict

# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════

GSTIN_REGEX = re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z][Z][0-9A-Z]$')

GST_KEYWORDS = [
    'gst', 'igst', 'cgst', 'sgst', 'tax invoice', 'gstin',
]

PURCHASE_KEYWORDS = [
    'vendor', 'supplier', 'purchase', 'raw material', 'stock',
    'b2b', 'invoice', 'bill', 'goods', 'material',
]

EXEMPT_KEYWORDS = [
    'interest', 'dividend', 'salary', 'wages', 'rent received',
    'agricultural', 'exempted', 'nil rated',
]

EXPORT_KEYWORDS = [
    'export', 'swift', 'foreign', 'usd', 'eur', 'gbp',
    'paypal', 'stripe', 'wise', 'remittance', 'lut',
]

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


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def validate_gstin(gstin: str) -> bool:
    if not gstin or not isinstance(gstin, str):
        return False
    return bool(GSTIN_REGEX.match(gstin.strip().upper()))


def get_state(gstin: str) -> str:
    try:
        return STATE_CODES.get(gstin[:2], 'Unknown')
    except:
        return 'Unknown'


def to_float(val) -> float:
    try:
        return float(str(val).replace(',', '').replace('₹', '').strip())
    except:
        return 0.0


def normalize_cols(df):
    df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
    return df


def map_columns(df):
    col_map = {}
    for col in df.columns:
        if 'cgst' in col:
            col_map['cgst'] = col
        elif 'sgst' in col:
            col_map['sgst'] = col
        elif 'igst' in col:
            col_map['igst'] = col
        elif any(k in col for k in ['gstin', 'gst_no', 'gstn']):
            col_map['gstin'] = col
        elif any(k in col for k in ['invoice', 'inv_no', 'bill_no']):
            col_map['invoice_no'] = col
        elif any(k in col for k in ['date', 'dt']):
            col_map['date'] = col
        elif any(k in col for k in ['taxable', 'basic', 'value']):
            col_map['taxable_value'] = col
        elif any(k in col for k in ['customer', 'client', 'buyer', 'vendor', 'supplier', 'party']):
            col_map['party_name'] = col
        elif any(k in col for k in ['total', 'gross', 'invoice_value']):
            col_map['total'] = col
        elif any(k in col for k in ['desc', 'item', 'product', 'narration']):
            col_map['description'] = col
        elif any(k in col for k in ['exempt', 'nil', 'non_gst']):
            col_map['supply_nature'] = col
    return col_map


# ═══════════════════════════════════════════════════════════
#  PARSERS
# ═══════════════════════════════════════════════════════════

def parse_sales_excel(filepath: str) -> dict:
    try:
        df = pd.read_excel(filepath, sheet_name=0)
    except Exception as e:
        return {'error': str(e), 'invoices': []}

    df = normalize_cols(df)
    col_map = map_columns(df)
    invoices = []

    for idx, row in df.iterrows():
        try:
            inv_no    = str(row.get(col_map.get('invoice_no', ''), f'INV-{idx+1}')).strip()
            date      = str(row.get(col_map.get('date', ''), '')).strip()
            party     = str(row.get(col_map.get('party_name', ''), 'Unknown')).strip()
            if party in ('nan', '', 'None'): party = 'Unknown'
            gstin_raw = str(row.get(col_map.get('gstin', ''), '')).strip().upper()
            if gstin_raw in ('NAN', 'NONE', ''): gstin_raw = ''
            desc      = str(row.get(col_map.get('description', ''), '')).strip()

            taxable = to_float(row.get(col_map.get('taxable_value', ''), 0))
            cgst    = to_float(row.get(col_map.get('cgst', ''), 0))
            sgst    = to_float(row.get(col_map.get('sgst', ''), 0))
            igst    = to_float(row.get(col_map.get('igst', ''), 0))
            total   = to_float(row.get(col_map.get('total', ''), 0))

            if total == 0:
                total = taxable + cgst + sgst + igst
            if taxable == 0 and total > 0:
                taxable = round(total / 1.18, 2)

            gstin_valid   = validate_gstin(gstin_raw)
            is_interstate = igst > 0
            state         = get_state(gstin_raw) if gstin_valid else 'Unknown'
            supply_type   = 'B2B' if gstin_valid else 'B2C'

            # Classify supply nature
            desc_lower = (desc + ' ' + party).lower()
            if any(k in desc_lower for k in EXPORT_KEYWORDS):
                supply_nature = 'export'
            elif any(k in desc_lower for k in EXEMPT_KEYWORDS):
                supply_nature = 'exempt'
            else:
                supply_nature = 'taxable'

            invoices.append({
                'invoice_no':    inv_no,
                'date':          date,
                'party_name':    party,
                'gstin':         gstin_raw,
                'gstin_valid':   gstin_valid,
                'taxable_value': round(taxable, 2),
                'cgst':          round(cgst, 2),
                'sgst':          round(sgst, 2),
                'igst':          round(igst, 2),
                'total':         round(total, 2),
                'supply_type':   supply_type,
                'supply_nature': supply_nature,
                'is_interstate': is_interstate,
                'state':         state,
            })
        except Exception as e:
            pass

    return {'invoices': invoices, 'total': len(invoices)}


def parse_purchase_excel(filepath: str) -> dict:
    try:
        df = pd.read_excel(filepath, sheet_name=0)
    except Exception as e:
        return {'error': str(e), 'purchases': []}

    df = normalize_cols(df)
    col_map = map_columns(df)
    purchases = []

    for idx, row in df.iterrows():
        try:
            inv_no    = str(row.get(col_map.get('invoice_no', ''), f'PUR-{idx+1}')).strip()
            date      = str(row.get(col_map.get('date', ''), '')).strip()
            party     = str(row.get(col_map.get('party_name', ''), 'Unknown')).strip()
            if party in ('nan', '', 'None'): party = 'Unknown'
            gstin_raw = str(row.get(col_map.get('gstin', ''), '')).strip().upper()
            if gstin_raw in ('NAN', 'NONE', ''): gstin_raw = ''

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
            itc_eligible = gstin_valid  # ITC only if supplier has valid GSTIN

            purchases.append({
                'invoice_no':    inv_no,
                'date':          date,
                'party_name':    party,
                'gstin':         gstin_raw,
                'gstin_valid':   gstin_valid,
                'taxable_value': round(taxable, 2),
                'cgst':          round(cgst, 2),
                'sgst':          round(sgst, 2),
                'igst':          round(igst, 2),
                'total':         round(total, 2),
                'itc_eligible':  itc_eligible,
            })
        except Exception as e:
            pass

    return {'purchases': purchases, 'total': len(purchases)}


# ═══════════════════════════════════════════════════════════
#  TABLE GENERATORS
# ═══════════════════════════════════════════════════════════

def generate_table31(sales: list, transactions: list) -> dict:
    """
    Table 3.1 — Nature of Outward Supplies
    3.1(a) Taxable supplies
    3.1(b) Zero-rated (exports)
    3.1(c) Nil rated / exempt
    3.1(d) Non-GST supplies
    """
    taxable = [s for s in sales if s['supply_nature'] == 'taxable']
    exports  = [s for s in sales if s['supply_nature'] == 'export']
    exempt   = [s for s in sales if s['supply_nature'] == 'exempt']

    # From bank — if no sales Excel, estimate from CR transactions
    if not sales and transactions:
        total_cr = sum(t['amount'] for t in transactions
                       if t.get('type') == 'CR' and t.get('amount'))
        taxable_est = round(total_cr / 1.18, 2)
        cgst_est    = round(taxable_est * 0.09, 2)
        sgst_est    = round(taxable_est * 0.09, 2)
        return {
            'taxable': {
                'value': taxable_est, 'igst': 0,
                'cgst': cgst_est, 'sgst': sgst_est,
                'cess': 0, 'source': 'bank_estimate'
            },
            'zero_rated': {'value': 0, 'igst': 0, 'cess': 0},
            'nil_exempt': {'value': 0},
            'non_gst':    {'value': 0},
            'total_tax':  round(cgst_est + sgst_est, 2),
            'source':     'bank_estimate',
        }

    def sum_inv(lst, key):
        return round(sum(i[key] for i in lst), 2)

    taxable_val  = sum_inv(taxable, 'taxable_value')
    taxable_igst = sum_inv(taxable, 'igst')
    taxable_cgst = sum_inv(taxable, 'cgst')
    taxable_sgst = sum_inv(taxable, 'sgst')

    export_val   = sum_inv(exports, 'taxable_value')
    export_igst  = sum_inv(exports, 'igst')

    exempt_val   = sum_inv(exempt, 'taxable_value')

    total_tax = round(taxable_igst + taxable_cgst + taxable_sgst + export_igst, 2)

    return {
        'taxable': {
            'value': taxable_val,
            'igst':  taxable_igst,
            'cgst':  taxable_cgst,
            'sgst':  taxable_sgst,
            'cess':  0,
            'count': len(taxable),
        },
        'zero_rated': {
            'value': export_val,
            'igst':  export_igst,
            'cess':  0,
            'count': len(exports),
        },
        'nil_exempt': {
            'value': exempt_val,
            'count': len(exempt),
        },
        'non_gst': {'value': 0, 'count': 0},
        'total_tax':  total_tax,
        'total_value': round(taxable_val + export_val + exempt_val, 2),
        'source': 'invoice_data',
    }


def generate_table32(sales: list) -> list:
    """
    Table 3.2 — Inter-state supplies to unregistered / composition / UIN holders
    State-wise IGST breakup
    """
    state_map = defaultdict(lambda: {'taxable': 0, 'igst': 0, 'count': 0})

    for s in sales:
        if s['is_interstate'] and s['igst'] > 0:
            state = s['state']
            state_map[state]['taxable'] += s['taxable_value']
            state_map[state]['igst']    += s['igst']
            state_map[state]['count']   += 1

    return [
        {
            'state':   state,
            'taxable': round(v['taxable'], 2),
            'igst':    round(v['igst'], 2),
            'count':   v['count'],
        }
        for state, v in sorted(state_map.items(), key=lambda x: -x[1]['igst'])
    ]


def generate_table4(purchases: list, transactions: list) -> dict:
    """
    Table 4 — Eligible ITC
    4(A)(5): All other ITC (B2B purchases)
    """
    if not purchases and transactions:
        # Estimate ITC from bank debits with GST keywords
        gst_debits = [
            t for t in transactions
            if t.get('type') == 'DR' and t.get('amount')
            and any(k in (t.get('desc') or '').lower() for k in GST_KEYWORDS)
        ]
        est_igst = round(sum(t['amount'] for t in gst_debits) * 0.18 / 1.18, 2)
        return {
            'igst':         est_igst,
            'cgst':         0,
            'sgst':         0,
            'cess':         0,
            'total':        est_igst,
            'eligible_count': len(gst_debits),
            'ineligible':   [],
            'source':       'bank_estimate',
        }

    eligible   = [p for p in purchases if p['itc_eligible']]
    ineligible = [p for p in purchases if not p['itc_eligible']]

    itc_igst = round(sum(p['igst'] for p in eligible), 2)
    itc_cgst = round(sum(p['cgst'] for p in eligible), 2)
    itc_sgst = round(sum(p['sgst'] for p in eligible), 2)
    total_itc = round(itc_igst + itc_cgst + itc_sgst, 2)

    return {
        'igst':           itc_igst,
        'cgst':           itc_cgst,
        'sgst':           itc_sgst,
        'cess':           0,
        'total':          total_itc,
        'eligible_count': len(eligible),
        'ineligible':     ineligible[:10],
        'ineligible_count': len(ineligible),
        'source':         'purchase_data',
    }


def generate_table5(purchases: list) -> dict:
    """
    Table 5 — Exempt, Nil, Non-GST inward supplies
    """
    exempt_purchases = [
        p for p in purchases
        if not p['itc_eligible'] and p['taxable_value'] > 0
    ]

    return {
        'inter_state': {'taxable': 0, 'cgst_sgst': 0},
        'intra_state': {
            'taxable': round(sum(p['taxable_value'] for p in exempt_purchases), 2),
        },
        'count': len(exempt_purchases),
    }


def generate_table61(table31: dict, table4: dict) -> dict:
    """
    Table 6.1 — Tax Payable vs Paid
    GST ITC utilization rules (as per GST Act):
    Step 1: IGST ITC → offset IGST first
    Step 2: Remaining IGST ITC → offset CGST
    Step 3: Remaining IGST ITC → offset SGST
    Step 4: CGST ITC → offset CGST only
    Step 5: SGST ITC → offset SGST only
    CGST ITC cannot offset SGST and vice versa.
    """
    output_igst = round(table31['taxable'].get('igst', 0) + table31['zero_rated'].get('igst', 0), 2)
    output_cgst = round(table31['taxable'].get('cgst', 0), 2)
    output_sgst = round(table31['taxable'].get('sgst', 0), 2)
    total_output = round(output_igst + output_cgst + output_sgst, 2)

    itc_igst = table4.get('igst', 0)
    itc_cgst = table4.get('cgst', 0)
    itc_sgst = table4.get('sgst', 0)
    total_itc = round(itc_igst + itc_cgst + itc_sgst, 2)

    # ── Step 1: IGST ITC offsets IGST ──
    igst_after_itc = max(0, output_igst - itc_igst)
    igst_itc_used  = min(itc_igst, output_igst)
    igst_itc_remaining = round(itc_igst - igst_itc_used, 2)

    # ── Step 2: Remaining IGST ITC offsets CGST ──
    cgst_after_igst_itc = max(0, output_cgst - igst_itc_remaining)
    igst_used_for_cgst  = min(igst_itc_remaining, output_cgst)
    igst_itc_remaining2 = round(igst_itc_remaining - igst_used_for_cgst, 2)

    # ── Step 3: Remaining IGST ITC offsets SGST ──
    sgst_after_igst_itc = max(0, output_sgst - igst_itc_remaining2)
    igst_used_for_sgst  = min(igst_itc_remaining2, output_sgst)

    # ── Step 4: CGST ITC offsets remaining CGST ──
    net_cgst = max(0, round(cgst_after_igst_itc - itc_cgst, 2))

    # ── Step 5: SGST ITC offsets remaining SGST ──
    net_sgst = max(0, round(sgst_after_igst_itc - itc_sgst, 2))

    # ── IGST net payable ──
    net_igst = round(igst_after_itc, 2)

    net_payable = round(net_igst + net_cgst + net_sgst, 2)

    return {
        'output_igst':        output_igst,
        'output_cgst':        output_cgst,
        'output_sgst':        output_sgst,
        'output_cess':        0,
        'total_output':       total_output,
        'itc_igst':           round(itc_igst, 2),
        'itc_cgst':           round(itc_cgst, 2),
        'itc_sgst':           round(itc_sgst, 2),
        'total_itc':          total_itc,
        'igst_used_for_cgst': round(igst_used_for_cgst, 2),
        'igst_used_for_sgst': round(igst_used_for_sgst, 2),
        'net_igst':           net_igst,
        'net_cgst':           net_cgst,
        'net_sgst':           net_sgst,
        'net_payable':        net_payable,
        'cash_required':      net_payable,
    }


# ═══════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════

def run_gstr3b(
    transactions: list,
    sales_filepath: str = None,
    purchase_filepath: str = None,
) -> dict:

    # Parse files
    sales     = []
    purchases = []
    errors    = []

    if sales_filepath:
        s_result = parse_sales_excel(sales_filepath)
        if 'error' in s_result:
            errors.append(f'Sales Excel: {s_result["error"]}')
        else:
            sales = s_result['invoices']

    if purchase_filepath:
        p_result = parse_purchase_excel(purchase_filepath)
        if 'error' in p_result:
            errors.append(f'Purchase Excel: {p_result["error"]}')
        else:
            purchases = p_result['purchases']

    # Generate tables
    table31 = generate_table31(sales, transactions)
    table32 = generate_table32(sales)
    table4  = generate_table4(purchases, transactions)
    table5  = generate_table5(purchases)
    table61 = generate_table61(table31, table4)

    # Bank summary for reference
    total_bank_cr = round(sum(t['amount'] for t in transactions
                              if t.get('type') == 'CR' and t.get('amount')), 2)
    total_bank_dr = round(sum(t['amount'] for t in transactions
                              if t.get('type') == 'DR' and t.get('amount')), 2)

    # Month from transactions
    months = sorted(set(
        t['date'][:7] for t in transactions
        if t.get('date') and len(t.get('date', '')) >= 7
    ))

    return {
        'table31':          table31,
        'table32':          table32,
        'table4':           table4,
        'table5':           table5,
        'table61':          table61,
        'sales':            sales,
        'purchases':        purchases,
        'errors':           errors,
        'total_sales':      len(sales),
        'total_purchases':  len(purchases),
        'total_bank_cr':    total_bank_cr,
        'total_bank_dr':    total_bank_dr,
        'months':           months,
        'has_sales_data':   len(sales) > 0,
        'has_purchase_data':len(purchases) > 0,
        'filing_summary': {
            'total_output_tax': table61['total_output'],
            'total_itc':        table61['total_itc'],
            'net_payable':      table61['net_payable'],
            'filing_ready':     len(errors) == 0,
        }
    }
