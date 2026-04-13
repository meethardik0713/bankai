"""
core/gstr2b_recon.py
────────────────────
GSTR-2A/2B Reconciliation Engine
- GSTR-2B Excel (from GST portal) vs Purchase Register Excel
- Invoice-by-invoice matching
- ITC mismatch detection
- Missing invoices on both sides
- Net eligible ITC calculation
"""

import re
import pandas as pd
from collections import defaultdict

# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════

GSTIN_REGEX = re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z][Z][0-9A-Z]$')

MATCH_STATUS = {
    'MATCHED':           'Matched',
    'AMT_MISMATCH':      'Amount Mismatch',
    'GSTIN_MISMATCH':    'GSTIN Mismatch',
    'IN_2B_NOT_PR':      'In GSTR-2B only',
    'IN_PR_NOT_2B':      'In Purchase Register only',
    'PARTIAL':           'Partial Match',
}

# Tolerance for amount matching (±1%)
AMOUNT_TOLERANCE = 0.01


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def validate_gstin(gstin: str) -> bool:
    if not gstin or not isinstance(gstin, str):
        return False
    return bool(GSTIN_REGEX.match(gstin.strip().upper()))


def to_float(val) -> float:
    try:
        return round(float(str(val).replace(',', '').replace('₹', '').strip()), 2)
    except:
        return 0.0


def clean_str(val) -> str:
    s = str(val).strip()
    return '' if s in ('nan', 'None', '') else s


def normalize_cols(df):
    df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
    return df


def map_gstr2b_cols(df) -> dict:
    """Map GSTR-2B Excel columns — GST portal format."""
    col_map = {}
    for col in df.columns:
        if 'cgst' in col:
            col_map['cgst'] = col
        elif 'sgst' in col:
            col_map['sgst'] = col
        elif 'igst' in col:
            col_map['igst'] = col
        elif any(k in col for k in ['supplier_gstin', 'gstin_of_supplier', 'gstin']):
            col_map['supplier_gstin'] = col
        elif any(k in col for k in ['invoice_number', 'inv_no', 'invoice_no', 'bill_no']):
            col_map['invoice_no'] = col
        elif any(k in col for k in ['invoice_date', 'inv_date', 'date']):
            col_map['date'] = col
        elif any(k in col for k in ['taxable_value', 'taxable', 'basic', 'value']):
            col_map['taxable_value'] = col
        elif any(k in col for k in ['invoice_value', 'total', 'gross']):
            col_map['total'] = col
        elif any(k in col for k in ['supplier_name', 'trade_name', 'supplier', 'party']):
            col_map['supplier_name'] = col
        elif any(k in col for k in ['place_of_supply', 'pos', 'state']):
            col_map['place_of_supply'] = col
        elif any(k in col for k in ['itc_availability', 'itc_eligible', 'eligible']):
            col_map['itc_eligible'] = col
        elif any(k in col for k in ['reason', 'remark']):
            col_map['reason'] = col
    return col_map


def map_purchase_cols(df) -> dict:
    """Map Purchase Register Excel columns."""
    col_map = {}
    for col in df.columns:
        if 'cgst' in col:
            col_map['cgst'] = col
        elif 'sgst' in col:
            col_map['sgst'] = col
        elif 'igst' in col:
            col_map['igst'] = col
        elif any(k in col for k in ['gstin', 'supplier_gstin', 'vendor_gstin']):
            col_map['supplier_gstin'] = col
        elif any(k in col for k in ['invoice', 'inv_no', 'bill_no']):
            col_map['invoice_no'] = col
        elif any(k in col for k in ['date', 'inv_date']):
            col_map['date'] = col
        elif any(k in col for k in ['taxable', 'basic', 'value']):
            col_map['taxable_value'] = col
        elif any(k in col for k in ['total', 'gross', 'invoice_value']):
            col_map['total'] = col
        elif any(k in col for k in ['supplier', 'vendor', 'party', 'name']):
            col_map['supplier_name'] = col
    return col_map


# ═══════════════════════════════════════════════════════════
#  PARSERS
# ═══════════════════════════════════════════════════════════

def parse_gstr2b(filepath: str) -> dict:
    """Parse GSTR-2B Excel downloaded from GST portal."""
    try:
        # Try multiple sheets — GST portal exports have B2B in sheet 1
        try:
            df = pd.read_excel(filepath, sheet_name='B2B')
        except:
            try:
                df = pd.read_excel(filepath, sheet_name=0)
            except Exception as e:
                return {'error': str(e), 'invoices': []}
    except Exception as e:
        return {'error': str(e), 'invoices': []}

    df = normalize_cols(df)
    col_map = map_gstr2b_cols(df)
    invoices = []

    for idx, row in df.iterrows():
        try:
            inv_no      = clean_str(row.get(col_map.get('invoice_no', ''), f'2B-{idx+1}'))
            date        = clean_str(row.get(col_map.get('date', ''), ''))
            gstin       = clean_str(row.get(col_map.get('supplier_gstin', ''), '')).upper()
            sup_name    = clean_str(row.get(col_map.get('supplier_name', ''), 'Unknown'))
            pos         = clean_str(row.get(col_map.get('place_of_supply', ''), ''))
            itc_elig_raw= clean_str(row.get(col_map.get('itc_eligible', ''), 'Yes'))
            reason      = clean_str(row.get(col_map.get('reason', ''), ''))

            taxable = to_float(row.get(col_map.get('taxable_value', ''), 0))
            cgst    = to_float(row.get(col_map.get('cgst', ''), 0))
            sgst    = to_float(row.get(col_map.get('sgst', ''), 0))
            igst    = to_float(row.get(col_map.get('igst', ''), 0))
            total   = to_float(row.get(col_map.get('total', ''), 0))

            if total == 0:
                total = taxable + cgst + sgst + igst
            if taxable == 0 and total > 0:
                taxable = round(total / 1.18, 2)

            total_itc = round(cgst + sgst + igst, 2)
            itc_eligible = itc_elig_raw.lower() not in ('no', 'ineligible', 'blocked', '0', 'false')

            if not inv_no or taxable == 0:
                continue

            invoices.append({
                'invoice_no':    inv_no,
                'date':          date,
                'supplier_gstin': gstin,
                'supplier_name': sup_name if sup_name != 'Unknown' else gstin,
                'place_of_supply': pos,
                'taxable_value': taxable,
                'cgst':          cgst,
                'sgst':          sgst,
                'igst':          igst,
                'total':         total,
                'total_itc':     total_itc,
                'itc_eligible':  itc_eligible,
                'reason':        reason,
                'source':        '2B',
            })
        except Exception:
            pass

    return {
        'invoices':      invoices,
        'total':         len(invoices),
        'total_itc':     round(sum(i['total_itc'] for i in invoices if i['itc_eligible']), 2),
        'total_taxable': round(sum(i['taxable_value'] for i in invoices), 2),
    }


def parse_purchase_register(filepath: str) -> dict:
    """Parse Purchase Register / Books Excel."""
    try:
        df = pd.read_excel(filepath, sheet_name=0)
    except Exception as e:
        return {'error': str(e), 'invoices': []}

    df = normalize_cols(df)
    col_map = map_purchase_cols(df)
    invoices = []

    for idx, row in df.iterrows():
        try:
            inv_no   = clean_str(row.get(col_map.get('invoice_no', ''), f'PR-{idx+1}'))
            date     = clean_str(row.get(col_map.get('date', ''), ''))
            gstin    = clean_str(row.get(col_map.get('supplier_gstin', ''), '')).upper()
            sup_name = clean_str(row.get(col_map.get('supplier_name', ''), 'Unknown'))

            taxable = to_float(row.get(col_map.get('taxable_value', ''), 0))
            cgst    = to_float(row.get(col_map.get('cgst', ''), 0))
            sgst    = to_float(row.get(col_map.get('sgst', ''), 0))
            igst    = to_float(row.get(col_map.get('igst', ''), 0))
            total   = to_float(row.get(col_map.get('total', ''), 0))

            if total == 0:
                total = taxable + cgst + sgst + igst
            if taxable == 0 and total > 0:
                taxable = round(total / 1.18, 2)

            total_itc = round(cgst + sgst + igst, 2)

            if not inv_no or taxable == 0:
                continue

            invoices.append({
                'invoice_no':    inv_no,
                'date':          date,
                'supplier_gstin': gstin,
                'supplier_name': sup_name,
                'taxable_value': taxable,
                'cgst':          cgst,
                'sgst':          sgst,
                'igst':          igst,
                'total':         total,
                'total_itc':     total_itc,
                'source':        'PR',
            })
        except Exception:
            pass

    return {
        'invoices':      invoices,
        'total':         len(invoices),
        'total_itc':     round(sum(i['total_itc'] for i in invoices), 2),
        'total_taxable': round(sum(i['taxable_value'] for i in invoices), 2),
    }


# ═══════════════════════════════════════════════════════════
#  RECONCILIATION ENGINE
# ═══════════════════════════════════════════════════════════

def reconcile(gstr2b_invoices: list, pr_invoices: list) -> dict:
    """
    Match GSTR-2B invoices against Purchase Register.
    Matching key: Invoice No + Supplier GSTIN (primary)
    Fallback: Invoice No only, then Amount match
    """
    matched          = []
    amt_mismatch     = []
    gstin_mismatch   = []
    in_2b_not_pr     = []
    in_pr_not_2b     = []

    # Index purchase register
    pr_by_inv_gstin = {}
    pr_by_inv       = {}
    pr_used         = set()

    for i, inv in enumerate(pr_invoices):
        key1 = (inv['invoice_no'].upper(), inv['supplier_gstin'].upper())
        key2 = inv['invoice_no'].upper()
        pr_by_inv_gstin[key1] = i
        if key2 not in pr_by_inv:
            pr_by_inv[key2] = i

    for inv2b in gstr2b_invoices:
        inv_no_upper = inv2b['invoice_no'].upper()
        gstin_upper  = inv2b['supplier_gstin'].upper()

        # Try exact match: invoice + GSTIN
        key1 = (inv_no_upper, gstin_upper)
        if key1 in pr_by_inv_gstin:
            pr_idx = pr_by_inv_gstin[key1]
            pr_inv = pr_invoices[pr_idx]
            pr_used.add(pr_idx)

            # Check amount match
            diff = abs(inv2b['total_itc'] - pr_inv['total_itc'])
            tol  = max(inv2b['total_itc'], pr_inv['total_itc']) * AMOUNT_TOLERANCE

            if diff <= tol or diff <= 1:
                matched.append({
                    '2b': inv2b, 'pr': pr_inv,
                    'status': MATCH_STATUS['MATCHED'],
                    'itc_diff': 0,
                    'itc_to_claim': inv2b['total_itc'],
                })
            else:
                amt_mismatch.append({
                    '2b': inv2b, 'pr': pr_inv,
                    'status': MATCH_STATUS['AMT_MISMATCH'],
                    'itc_diff': round(inv2b['total_itc'] - pr_inv['total_itc'], 2),
                    'itc_to_claim': min(inv2b['total_itc'], pr_inv['total_itc']),
                })
            continue

        # Try invoice no only match
        if inv_no_upper in pr_by_inv:
            pr_idx = pr_by_inv[inv_no_upper]
            pr_inv = pr_invoices[pr_idx]
            pr_used.add(pr_idx)

            # GSTIN mismatch
            if pr_inv['supplier_gstin'] and gstin_upper and pr_inv['supplier_gstin'].upper() != gstin_upper:
                gstin_mismatch.append({
                    '2b': inv2b, 'pr': pr_inv,
                    'status': MATCH_STATUS['GSTIN_MISMATCH'],
                    'itc_diff': round(inv2b['total_itc'] - pr_inv['total_itc'], 2),
                    'itc_to_claim': 0,  # Don't claim — GSTIN mismatch is serious
                })
            else:
                diff = abs(inv2b['total_itc'] - pr_inv['total_itc'])
                tol  = max(inv2b['total_itc'], pr_inv['total_itc']) * AMOUNT_TOLERANCE
                if diff <= tol or diff <= 1:
                    matched.append({
                        '2b': inv2b, 'pr': pr_inv,
                        'status': MATCH_STATUS['MATCHED'],
                        'itc_diff': 0,
                        'itc_to_claim': inv2b['total_itc'],
                    })
                else:
                    amt_mismatch.append({
                        '2b': inv2b, 'pr': pr_inv,
                        'status': MATCH_STATUS['AMT_MISMATCH'],
                        'itc_diff': round(inv2b['total_itc'] - pr_inv['total_itc'], 2),
                        'itc_to_claim': min(inv2b['total_itc'], pr_inv['total_itc']),
                    })
            continue

        # Not found in PR
        in_2b_not_pr.append({
            '2b': inv2b, 'pr': None,
            'status': MATCH_STATUS['IN_2B_NOT_PR'],
            'itc_diff': inv2b['total_itc'],
            'itc_to_claim': inv2b['total_itc'] if inv2b['itc_eligible'] else 0,
        })

    # PR invoices not in 2B
    for i, pr_inv in enumerate(pr_invoices):
        if i not in pr_used:
            in_pr_not_2b.append({
                '2b': None, 'pr': pr_inv,
                'status': MATCH_STATUS['IN_PR_NOT_2B'],
                'itc_diff': -pr_inv['total_itc'],
                'itc_to_claim': 0,  # Can't claim — not in 2B
            })

    return {
        'matched':        matched,
        'amt_mismatch':   amt_mismatch,
        'gstin_mismatch': gstin_mismatch,
        'in_2b_not_pr':   in_2b_not_pr,
        'in_pr_not_2b':   in_pr_not_2b,
    }


# ═══════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════

def run_gstr2b_recon(gstr2b_filepath: str, pr_filepath: str) -> dict:
    # Parse both files
    gstr2b_data = parse_gstr2b(gstr2b_filepath)
    pr_data     = parse_purchase_register(pr_filepath)

    if 'error' in gstr2b_data:
        return {'error': f'GSTR-2B parse failed: {gstr2b_data["error"]}'}
    if 'error' in pr_data:
        return {'error': f'Purchase Register parse failed: {pr_data["error"]}'}

    gstr2b_invoices = gstr2b_data['invoices']
    pr_invoices     = pr_data['invoices']

    if not gstr2b_invoices and not pr_invoices:
        return {'error': 'No invoices found in either file. Check column headers.'}

    # Reconcile
    recon = reconcile(gstr2b_invoices, pr_invoices)

    matched        = recon['matched']
    amt_mismatch   = recon['amt_mismatch']
    gstin_mismatch = recon['gstin_mismatch']
    in_2b_not_pr   = recon['in_2b_not_pr']
    in_pr_not_2b   = recon['in_pr_not_2b']

    # ITC Summary
    itc_in_2b         = gstr2b_data['total_itc']
    itc_in_pr         = pr_data['total_itc']
    itc_matched       = round(sum(r['itc_to_claim'] for r in matched), 2)
    itc_mismatch_loss = round(sum(abs(r['itc_diff']) for r in amt_mismatch), 2)
    itc_gstin_blocked = round(sum(r['2b']['total_itc'] for r in gstin_mismatch), 2)
    itc_only_in_2b    = round(sum(r['itc_to_claim'] for r in in_2b_not_pr), 2)
    itc_only_in_pr    = round(sum(r['pr']['total_itc'] for r in in_pr_not_2b), 2)
    itc_claimable     = round(itc_matched + itc_only_in_2b, 2)
    itc_at_risk       = round(itc_mismatch_loss + itc_gstin_blocked + itc_only_in_pr, 2)

    match_rate = round(len(matched) / max(len(gstr2b_invoices), 1) * 100, 1)

    # Risk level
    if itc_at_risk == 0 and len(gstin_mismatch) == 0:
        risk_level = 'Low'
        risk_color = 'green'
    elif itc_at_risk < itc_in_2b * 0.1:
        risk_level = 'Medium'
        risk_color = 'yellow'
    else:
        risk_level = 'High'
        risk_color = 'red'

    return {
        'matched':            matched,
        'amt_mismatch':       amt_mismatch,
        'gstin_mismatch':     gstin_mismatch,
        'in_2b_not_pr':       in_2b_not_pr,
        'in_pr_not_2b':       in_pr_not_2b,

        # Counts
        'total_2b':           len(gstr2b_invoices),
        'total_pr':           len(pr_invoices),
        'matched_count':      len(matched),
        'amt_mismatch_count': len(amt_mismatch),
        'gstin_mismatch_count': len(gstin_mismatch),
        'in_2b_not_pr_count': len(in_2b_not_pr),
        'in_pr_not_2b_count': len(in_pr_not_2b),

        # ITC
        'itc_in_2b':          itc_in_2b,
        'itc_in_pr':          itc_in_pr,
        'itc_matched':        itc_matched,
        'itc_claimable':      itc_claimable,
        'itc_at_risk':        itc_at_risk,
        'itc_mismatch_loss':  itc_mismatch_loss,
        'itc_gstin_blocked':  itc_gstin_blocked,
        'itc_only_in_2b':     itc_only_in_2b,
        'itc_only_in_pr':     itc_only_in_pr,

        # Summary
        'match_rate':         match_rate,
        'risk_level':         risk_level,
        'risk_color':         risk_color,
        'filing_ready':       risk_level == 'Low',

        # Raw data for reference
        'gstr2b_total_taxable': gstr2b_data['total_taxable'],
        'pr_total_taxable':     pr_data['total_taxable'],
    }
