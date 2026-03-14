"""
Bank Statement Transaction Analyzer
────────────────────────────────────
Classifies bank transactions into: Salary, EMI/Loans,
NACH Bounces, and Frequent Transfers using pattern matching.

All patterns are server-side only — never exposed to the browser.
"""

import re
from datetime import datetime
from typing import Any


# ═══════════════════════════════════════════════════════════════════════
# 1. SALARY PATTERNS
# ═══════════════════════════════════════════════════════════════════════
# SALARY_KEYWORDS  – narrations that explicitly mention salary/wage credits
# COMPANY_PATTERNS – company types for inferred-salary detection
# SALARY_TRANSFER_MODE – salary usually arrives via NEFT/RTGS/IMPS
# LOAN_EXCLUDE_PATTERNS – block loan disbursements from being tagged as salary

SALARY_KEYWORDS = re.compile(
    '|'.join([
        r'salary',
        r'sal cr',
        r'sal/',
        r'monthly pay',
        r'payroll',
        r'stipend',
        r'wages',
        r'branch\s*imprest',
    ]),
    re.IGNORECASE,
)

COMPANY_PATTERNS = re.compile(
    '|'.join([
        r'pvt\.?\s*ltd', r'private\s+limited', r'\blimited\b', r'\bltd\b',
        r'\bllp\b', r'\bcorp\b', r'\binc\b',
        r'technologies', r'solutions', r'services', r'enterprises', r'consulting',
        r'infosys', r'wipro', r'tcs\b', r'hcl\b', r'cognizant', r'accenture',
        r'hospital', r'\bhosp\b',
    ]),
    re.IGNORECASE,
)

SALARY_TRANSFER_MODE = re.compile(
    r'^(neft|rtgs|imps)/|^ft\s*(neft|rtgs|imps)/',
    re.IGNORECASE,
)

LOAN_EXCLUDE_PATTERNS = re.compile(
    '|'.join([
        r'\bldr\b', r'loan', r'disb', r'lend', r'financ', r'fincorp', r'nbfc',
        r'capital', r'credit', r'commodit', r'trading', r'nidhi', r'chit\s*fund',
        r'micro\s*fin', r'pay\s*day', r'leasing', r'fincap', r'assignments',
        r'securities', r'metals',
        r'bajaj\s?fin', r'bajaj\s*housing', r'lic\s*housing',
        r'home\s*credit', r'capital\s*first', r'fullerton',
        r'tata\s?capital', r'piramal', r'shriram', r'sundaram', r'idfc\s*first',
        r'manappuram', r'muthoot',
        r'paysense', r'kreditbee', r'cashe', r'navi\b',
        r'money\s*tap', r'earlysalary', r'mpokket',
        r'protium', r'stashfin', r'kissht',
        r'ola\s*financial', r'pay\s*later', r'branch\s*(?:payment|online)',
        r'sampati', r'unifinz', r'chintamani', r'goldline', r'chinmay',
        r'rk\s*bansal', r'solomon', r'vaishali',
        r'd\.?pal', r'day\s*to\s*day', r'goodskill', r'easyfincare',
        r'agrim', r'naman\s*finlease', r'junoon', r'datta\s*finance',
        r'gagan\s*metals', r'gaganmetals', r'achiievers', r'sawalsha',
        r'woodland\s*securities', r'konark', r'kasar', r'tycoon',
        r'sashi\s*enterprises', r'sashienterprises',
        r'xpressloan', r'growing', r'sabharwal', r'sabkaloan',
        r'mahashakti', r'speedo\s*loans', r'salora', r'comero',
        r'loanpe', r'girdhar', r'fast\s*solutions\s*fin',
        r'uca\b', r'devmuni', r'ayaan\b',
        r'agarwal\s*assignments', r'digner', r'devashish', r'skyrise',
        r'bazarloan', r'tsb\s*finance', r'cashmypayment',
        r'loanhub', r'loanforcare', r'salary4sure', r'salarynow',
        r'bharatloan', r'gdl\s*leasing', r'agf\b',
        r'mahavira', r'innofinsolu', r'respo',
        r'akara', r'northern\s*arc', r'aman\s*fincap',
        r'auro\s*fin', r'fincfriend', r'citra', r'ava\s*fina',
    ]),
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════
# 2. EMI / LOAN PATTERNS
# ═══════════════════════════════════════════════════════════════════════
# EMI_PATTERNS     – generic EMI keywords + all known lender names
# EMI_UPI_STRICT   – stricter subset used for UPI transactions only
# NACH_PATTERNS    – any auto-debit (NACH/ECS/mandate), not just bounces
# ACH_NACH_PATTERN – narrations that start with ACH/NACH/ECS/SI
# LENDER_PATTERNS  – known lenders (superset of EMI_PATTERNS; includes
#                    generic finance keywords + extra lenders)

EMI_PATTERNS = re.compile(
    '|'.join([
        # Generic keywords
        r'\bemi\b', r'loan', r'lending', r'repayment', r'installment',
        # Major banks / HFCs
        r'bajaj\s?fin', r'bajaj\s?housing', r'lic\s?housing',
        r'home\s?credit', r'capital\s?first', r'fullerton',
        r'tata\s?capital', r'shriram', r'piramal', r'sundaram', r'idfc\s?first',
        # Gold-loan / traditional NBFCs
        r'manappuram', r'muthoot', r'fincap', r'nbfc',
        # Fintech / digital lenders
        r'paysense', r'kreditbee', r'cashe', r'navi\b',
        r'money\s?tap', r'earlysalary', r'mpokket',
        r'protium', r'stashfin', r'kissht',
        r'ola\s?financial', r'pay\s?later', r'branch\s?(?:payment|online)',
        # Regional / smaller NBFCs
        r'sampati', r'unifinz', r'chintamani', r'goldline', r'chinmay',
        r'rk\s?bansal', r'solomon', r'vaishali',
        r'd\.?pal', r'day\s?to\s?day', r'goodskill', r'easyfincare',
        r'agrim', r'naman\s?finlease', r'junoon', r'datta\s?finance',
        r'gagan\s?metals', r'gaganmetals', r'achiievers', r'sawalsha',
        r'woodland\s?securities', r'konark', r'kasar', r'tycoon',
        r'sashi\s?enterprises', r'sashienterprises',
        r'xpressloan', r'growing', r'sabharwal', r'sabkaloan',
        r'mahashakti', r'speedo\s?loans', r'salora', r'comero',
        r'loanpe', r'girdhar', r'fast\s?solutions\s?fin',
        r'uca\b', r'devmuni', r'ayaan\b',
        r'agarwal\s?assignments', r'digner', r'devashish', r'skyrise',
        r'bazarloan', r'tsb\s?finance', r'cashmypayment',
        r'loanhub', r'loanforcare', r'salary4sure', r'salarynow',
        r'bharatloan', r'gdl\s?leasing', r'agf\b',
        # Discovered from real statements
        r'mahavira\s?finlease', r'innofinsolu', r'respo\s?financial',
        r'akara\s?capital', r'northern\s?arc', r'aman\s?fincap',
        r'auro\s?fin', r'fincfriend', r'citra\s?financial', r'ava\s?fina',
    ]),
    re.IGNORECASE,
)

EMI_UPI_STRICT = re.compile(
    r'\bemi\b|loan|repayment|installment',
    re.IGNORECASE,
)

NACH_PATTERNS = re.compile(
    r'nach|ecs|mandate|auto.?debit|si/',
    re.IGNORECASE,
)

ACH_NACH_PATTERN = re.compile(
    r'^ach|^nach|^ecs|^si/',
    re.IGNORECASE,
)

LENDER_PATTERNS = re.compile(
    '|'.join([
        r'bajaj\s?fin', r'bajaj\s*housing', r'lic\s*housing',
        r'home\s*credit', r'capital\s*first', r'fullerton',
        r'tata\s?capital', r'piramal', r'shriram', r'sundaram', r'idfc\s*first',
        r'manappuram', r'muthoot',
        r'financ', r'fincorp', r'nbfc', r'fincap', r'leasing',
        r'paysense', r'kreditbee', r'cashe', r'navi\b',
        r'money\s*tap', r'earlysalary', r'mpokket',
        r'protium', r'stashfin', r'kissht',
        r'ola\s*financial', r'pay\s*later', r'branch\s*(?:payment|online)',
        r'afiloans', r'pawansut',
        r'sampati', r'unifinz', r'chintamani', r'goldline', r'chinmay',
        r'rk\s*bansal', r'solomon', r'vaishali',
        r'd\.?pal', r'day\s*to\s*day', r'goodskill', r'easyfincare',
        r'agrim', r'naman\s*finlease', r'junoon', r'datta\s*finance',
        r'gagan\s*metals', r'gaganmetals', r'achiievers', r'sawalsha',
        r'woodland\s*securities', r'konark', r'kasar', r'tycoon',
        r'sashi\s*enterprises', r'sashienterprises',
        r'xpressloan', r'growing', r'sabharwal', r'sabkaloan',
        r'mahashakti', r'speedo\s*loans', r'salora', r'comero',
        r'loanpe', r'girdhar', r'fast\s*solutions\s*fin',
        r'uca\b', r'devmuni', r'ayaan\b',
        r'agarwal\s*assignments', r'digner', r'devashish', r'skyrise',
        r'bazarloan', r'tsb\s*finance', r'cashmypayment',
        r'loanhub', r'loanforcare', r'salary4sure', r'salarynow',
        r'bharatloan', r'gdl\s*leasing', r'agf\b',
        r'mahavira', r'innofinsolu', r'respo',
        r'akara', r'northern\s*arc', r'aman\s*fincap',
        r'auro\s*fin', r'fincfriend', r'citra', r'ava\s*fina',
    ]),
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════
# 3. NACH BOUNCE PATTERNS
# ═══════════════════════════════════════════════════════════════════════
# Detects failed auto-debit (NACH/ECS) transactions — critical red flag.
#
#   HDFC:  "NACH RET", "NACH RETURN"
#   ICICI: "ECSRTN1_0402..."
#   SBI:   "ACH...RET", "NACH...FAIL"
#   Kotak: "Chrg: ECS Mandate"

NACH_BOUNCE_PATTERNS = re.compile(
    '|'.join([
        r'nach ret', r'nach return', r'nach bounce', r'nach.?fail',
        r'ecs.*ret', r'ecs return', r'ecsrtn',
        r'ach.*ret',
        r'mandate reject',
        r'insufficient',
        r'bounce',
        r'dishon',
        r'unpaid',
        r'return.*nach',
        r'ret\s*ch',
        r'nach.?ad.?rtn',
    ]),
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════
# 4. HELPER PATTERNS
# ═══════════════════════════════════════════════════════════════════════
# UPI_PATTERN    – identifies UPI transactions (varies by bank)
# ECOM_PATTERN   – ECOM-prefixed debits
# CHARGE_PATTERNS– bank fees/penalties (kept for internal use)
# _UPI_HANDLE    – extracts UPI handle (xxx@yyy) from narration
# _ACCOUNT_NUM   – extracts account/reference numbers for transfer grouping

UPI_PATTERN = re.compile(
    r'^upi[\s/]|^upiout/|^upi\s?in/',
    re.IGNORECASE,
)

ECOM_PATTERN = re.compile(r'^ecom', re.IGNORECASE)

CHARGE_PATTERNS = re.compile(
    '|'.join([
        r'\bcharges?\b', r'(?<!re)charge\b', r'\bfee\b', r'\bchrg\b',
        r'penalty', r'penal', r'interest.*debit', r'min.?bal', r'maint',
        r'non.?maint', r'sms\s*(?:charge|alert)', r'instaalert',
        r'atm.?(?:charge|fee|maint)', r'overdue', r'gst', r'cess',
        r'service tax', r'stamp duty', r'folio', r'annual', r'late.*fee',
        r'posdec.?chg', r'rtnchg', r'nach.?rtn.?chrg',
    ]),
    re.IGNORECASE,
)

_UPI_HANDLE = re.compile(r'[\w.\-]+@[\w.]+')
_ACCOUNT_NUM = re.compile(r'\b\d{9,16}\b')


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _get_narration(txn: dict) -> str:
    return (
        txn.get('narration')
        or txn.get('transactionNarration')
        or txn.get('reference')
        or txn.get('Narration')
        or ''
    )


def _get_type(txn: dict) -> str:
    return (txn.get('type') or txn.get('txnType') or txn.get('Type') or '').upper()


def _get_amount(txn: dict) -> float:
    raw = txn.get('amount') or txn.get('transactionAmount') or txn.get('Amount') or 0
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


def _get_date(txn: dict) -> str:
    return (
        txn.get('valueDate')
        or txn.get('transactionTimestamp')
        or txn.get('txnDate')
        or txn.get('Date')
        or ''
    )


def _extract_destination(narr: str) -> str:
    """Extract UPI handle or account number from a narration for grouping."""
    m = _UPI_HANDLE.search(narr)
    if m:
        return m.group().lower()
    m = _ACCOUNT_NUM.search(narr)
    if m:
        return m.group()
    # Fallback: 2nd segment of UPI/xxx/... format
    if re.match(r'upi', narr, re.IGNORECASE):
        parts = narr.split('/')
        if len(parts) >= 2:
            dest = parts[1].strip().lower()
            if dest and dest not in ('dr', 'cr', 'in', 'out'):
                return dest
    return ''


def _parse_date_obj(date_str: str):
    """Try to parse a date string into a datetime object."""
    if not date_str:
        return None
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return datetime.strptime(date_str[:len(fmt)], fmt)
        except (ValueError, TypeError):
            continue
    return None


# ═══════════════════════════════════════════════════════════════════════
# MAIN ANALYSIS FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def analyze_transactions(transactions: list[dict]) -> dict[str, Any]:
    """
    Analyze a list of bank transactions and classify them.

    Returns
    -------
    dict with keys:
        salary             : salary credits (≥ ₹20,000 only)
        emi_loans          : EMI payments + loan repayments (merged)
        nach_bounce        : NACH/ECS bounce transactions
        frequent_transfers : repeated transfers to same UPI/account
                             where total ≥ 40% of avg monthly salary
        charges            : bank charges / fees (kept for reference)
        summary            : totals and counts
    """

    # ── Pass 1: Pre-scan recurring company credits ───────────────
    company_credit_counts: dict[str, int] = {}

    for txn in transactions:
        narr = _get_narration(txn).lower()
        txn_type = _get_type(txn)
        amt = _get_amount(txn)

        if (
            txn_type == 'CREDIT'
            and amt >= 20000
            and COMPANY_PATTERNS.search(narr)
            and SALARY_TRANSFER_MODE.search(narr)
            and not LOAN_EXCLUDE_PATTERNS.search(narr)
        ):
            parts = narr.split('/')
            company_key = parts[2].strip() if len(parts) >= 3 else narr[:30]
            company_credit_counts[company_key] = company_credit_counts.get(company_key, 0) + 1

    # ── Pass 2: Classify each transaction ───────────────────────
    salary = []
    emi_loans = []
    nach_bounce = []
    charges = []

    total_credits = 0.0
    total_debits = 0.0
    credit_count = 0
    debit_count = 0

    for txn in transactions:
        narr_raw = _get_narration(txn)
        narr = narr_raw.lower()
        txn_type = _get_type(txn)
        amt = _get_amount(txn)
        date = _get_date(txn)

        row = {'date': date, 'narration': narr_raw, 'amount': amt}

        if txn_type == 'CREDIT':
            total_credits += amt
            credit_count += 1
        elif txn_type == 'DEBIT':
            total_debits += amt
            debit_count += 1

        # ── Salary (≥ ₹20,000 only) ──
        if txn_type == 'CREDIT' and amt >= 20000:
            if SALARY_KEYWORDS.search(narr):
                salary.append(row)
            elif (
                COMPANY_PATTERNS.search(narr)
                and SALARY_TRANSFER_MODE.search(narr)
                and not LOAN_EXCLUDE_PATTERNS.search(narr)
            ):
                parts = narr.split('/')
                company_key = parts[2].strip() if len(parts) >= 3 else narr[:30]
                if company_credit_counts.get(company_key, 0) >= 2:
                    salary.append(row)

        # ── NACH Bounces (highest priority, exit early) ──
        if NACH_BOUNCE_PATTERNS.search(narr):
            nach_bounce.append({**row, 'type': txn_type})
            continue

        if txn_type != 'DEBIT':
            continue

        is_ach_nach = bool(ACH_NACH_PATTERN.search(narr))
        is_lender = bool(LENDER_PATTERNS.search(narr))
        is_upi = bool(UPI_PATTERN.search(narr))
        is_ecom = bool(ECOM_PATTERN.search(narr))

        # ── EMI / Loans (merged) ──
        if is_lender and amt >= 500 and (is_ach_nach or is_ecom or SALARY_TRANSFER_MODE.search(narr) or is_upi):
            emi_loans.append(row)
        elif is_ach_nach and amt >= 500:
            emi_loans.append(row)
        elif amt >= 500 and (is_upi or is_ecom) and EMI_UPI_STRICT.search(narr):
            emi_loans.append(row)

        # ── Bank charges ──
        if CHARGE_PATTERNS.search(narr):
            charges.append(row)

    # ── Pass 3: Frequent Transfer Detection ─────────────────────
    # Estimate avg monthly salary from statement date range
    parsed_dates = [_parse_date_obj(_get_date(t)) for t in transactions]
    parsed_dates = [d for d in parsed_dates if d]
    if parsed_dates:
        delta_months = max(1, (max(parsed_dates).year - min(parsed_dates).year) * 12
                          + (max(parsed_dates).month - min(parsed_dates).month) + 1)
    else:
        delta_months = 3

    total_salary = sum(r['amount'] for r in salary)
    monthly_salary = total_salary / delta_months if total_salary > 0 else 0
    # 40% of avg monthly salary (only when monthly salary >= ₹20,000);
    # fall back to ₹5,000 when salary is undetected or below threshold
    transfer_threshold = (0.4 * monthly_salary) if monthly_salary >= 20000 else 5000

    # Build set of already-classified narrations to avoid double-flagging
    emi_loan_narrs = {r['narration'] for r in emi_loans}

    dest_groups: dict[str, dict] = {}
    for txn in transactions:
        narr_raw = _get_narration(txn)
        narr = narr_raw.lower()
        txn_type = _get_type(txn)
        amt = _get_amount(txn)
        date = _get_date(txn)

        if txn_type != 'DEBIT':
            continue
        if not UPI_PATTERN.search(narr):
            continue
        if narr_raw in emi_loan_narrs:
            continue
        if NACH_BOUNCE_PATTERNS.search(narr):
            continue

        dest = _extract_destination(narr)
        if not dest:
            continue

        if dest not in dest_groups:
            dest_groups[dest] = {'count': 0, 'total': 0.0, 'transactions': []}
        dest_groups[dest]['count'] += 1
        dest_groups[dest]['total'] += amt
        dest_groups[dest]['transactions'].append({'date': date, 'narration': narr_raw, 'amount': amt})

    frequent_transfers = []
    for dest, info in dest_groups.items():
        if info['count'] >= 2 and info['total'] >= transfer_threshold:
            pct = round((info['total'] / monthly_salary) * 100, 1) if monthly_salary > 0 else 0
            frequent_transfers.append({
                'destination': dest,
                'count': info['count'],
                'total': round(info['total'], 2),
                'pct_of_salary': pct,
                'transactions': info['transactions'],
            })

    frequent_transfers.sort(key=lambda x: x['total'], reverse=True)

    # ── Build summary ────────────────────────────────────────────
    total_emi_loans = sum(r['amount'] for r in emi_loans)
    total_charges = sum(r['amount'] for r in charges)

    return {
        'salary': salary,
        'emi_loans': emi_loans,
        'nach_bounce': nach_bounce,
        'frequent_transfers': frequent_transfers,
        'charges': charges,
        'summary': {
            'total_credits': total_credits,
            'total_debits': total_debits,
            'credit_count': credit_count,
            'debit_count': debit_count,
            'total_salary': total_salary,
            'salary_count': len(salary),
            'total_emi_loans': total_emi_loans,
            'emi_loans_count': len(emi_loans),
            'nach_bounce_count': len(nach_bounce),
            'frequent_transfers_count': len(frequent_transfers),
            'total_charges': total_charges,
            'charges_count': len(charges),
        },
    }
