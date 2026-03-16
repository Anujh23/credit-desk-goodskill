"""
PostgreSQL Database Module for Customer 360 Insight
Product-specific tables: {product}_disbursed and {product}_collection
"""
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import sql as psql
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

GRACE_PERIOD_DAYS = 3

# ── Connection Pool ──────────────────────────────────────────
_connection_pool = None

# Valid product name pattern (alphanumeric + underscore, 2-10 chars)
VALID_PRODUCT_RE = re.compile(r'^[A-Za-z0-9_]{2,10}$')

# ── Column Type Mapping ─────────────────────────────────────
COLUMN_TYPE_MAP = {
    # Amounts → NUMERIC
    'loan_amount': 'NUMERIC(12,2)', 'loan_amount_approved': 'NUMERIC(12,2)',
    'emi_amount': 'NUMERIC(12,2)', 'collected_amount': 'NUMERIC(12,2)',
    'disbursed_amount': 'NUMERIC(12,2)', 'processing_fee': 'NUMERIC(12,2)',
    'sanction_amount': 'NUMERIC(12,2)', 'net_salary': 'NUMERIC(12,2)',
    'monthly_gross_salary': 'NUMERIC(12,2)', 'total_obligation': 'NUMERIC(12,2)',
    'roi': 'NUMERIC(6,3)',
    # Integer counts / scores
    'cibil_score': 'INTEGER', 'emi_count': 'INTEGER', 'tenure': 'INTEGER',
    # Dates → DATE
    'repay_date': 'DATE', 'sanction_date': 'DATE', 'disbursal_date': 'DATE',
    'collected_date': 'DATE', 'invoice_date': 'DATE', 'approval_date': 'DATE',
}

DATE_COLUMNS = {k for k, v in COLUMN_TYPE_MAP.items() if v == 'DATE'}
NUMERIC_COLUMNS = {k for k, v in COLUMN_TYPE_MAP.items() if v.startswith('NUMERIC') or v == 'INTEGER'}


def _col_pg_type(col_clean: str) -> str:
    """Return the proper PostgreSQL column type for a known column, else TEXT."""
    return COLUMN_TYPE_MAP.get(col_clean.lower(), 'TEXT')


def _get_pool():
    """Get or create the connection pool."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor
        )
        logger.info("Database connection pool created (min=2, max=10)")
    return _connection_pool


def init_db():
    """Initialize database: create pool and required tables. Call this at app startup."""
    _get_pool()
    init_auth_tables()
    create_bank_statements_table()
    logger.info("Database initialized successfully")


def normalize_upload_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column headers from CSVs/Excel to match database schema across all products."""
    df = df.copy()
    df.columns = df.columns.str.strip()

    def _key(col: str) -> str:
        # Remove quotes, normalize spaces/underscores, lowercase for matching
        clean = col.strip().replace('"', '').replace("'", '')
        clean = clean.replace(' ', '_').replace('-', '_').lower()
        return clean

    # Canonical mappings - source columns from all 4 products map to standard DB columns
    canonical_map = {
        # ============ IDs ============
        'leadid': 'LeadID',
        'lead_id': 'LeadID',
        'loanno': 'Loan_No',
        'loan_no': 'Loan_No',
        'loan': 'Loan_No',
        
        # ============ Product Info (Generic file has this) ============
        'productname': 'Product_Name',
        'product_name': 'Product_Name',
        
        # ============ Personal Info ============
        'name': 'Name',
        'customername': 'Name',
        'customer_name': 'Name',
        'fullname': 'Name',
        'applicantname': 'Name',
        'applicant_name': 'Name',
        'email': 'Email',
        'mobile': 'Mobile',
        'mobileno': 'Mobile',
        'mobile_no': 'Mobile',
        'phone': 'Mobile',
        'phoneno': 'Mobile',
        'phonenumber': 'Mobile',
        'phone_number': 'Mobile',
        'contact': 'Mobile',
        'contactno': 'Contact_No',
        'contact_no': 'Contact_No',
        'pancard': 'Pancard',
        'pan': 'Pancard',
        'pancardno': 'Pancard',
        'pan_no': 'Pancard',
        'aadharno': 'Aadhar_No',
        'aadhar_no': 'Aadhar_No',
        'aadhar': 'Aadhar_No',
        'gender': 'Gender',
        'dob': 'DOB',
        'dateofbirth': 'DOB',
        'date_of_birth': 'DOB',
        'employed': 'Employed',
        
        # ============ Loan Details ============
        'branch': 'Branch',
        'loantype': 'Loan_Type',
        'loan_type': 'Loan_Type',
        'loanamount': 'Loan_Amount',
        'loan_amount': 'Loan_Amount',
        'loanamountapproved': 'Loan_Amount',
        'loan_amount_approved': 'Loan_Amount',
        'sanctionedamount': 'Loan_Amount',
        'sanctioned_amount': 'Loan_Amount',
        'disbursedamount': 'Loan_Amount',
        'disbursed_amount': 'Loan_Amount',
        'amount': 'Loan_Amount',
        'approvalamount': 'Loan_Amount',
        'approval_amount': 'Loan_Amount',
        'tenure': 'Tenure',
        'roi': 'ROI',
        'interestrate': 'ROI',
        'interest_rate': 'ROI',
        'repaydate': 'Repay_Date',
        'repay_date': 'Repay_Date',
        'repaymentdate': 'Repay_Date',
        'repayment_date': 'Repay_Date',
        
        # ============ Bank Details ============
        'accountno': 'Account_No',
        'account_no': 'Account_No',
        'accounttype': 'Account_Type',
        'account_type': 'Account_Type',
        'bankifsc': 'Bank_IFSC',
        'ifsc': 'Bank_IFSC',
        'bankifsccode': 'Bank_IFSC',
        'ifsc_code': 'Bank_IFSC',
        'bankname': 'Bank',
        'bank_name': 'Bank',
        'bank': 'Bank',
        'bankbranch': 'Bank_Branch',
        'bank_branch': 'Bank_Branch',
        'branchname': 'Bank_Branch',
        'chequeno': 'Cheque_No',
        'cheque_no': 'Cheque_No',
        'customerbankaccount': 'Customer_Bank_Account',
        'customer_bank_account': 'Customer_Bank_Account',
        
        # ============ Disbursal ============
        'disbursalreferenceno': 'Disbursal_Reference_No',
        'disbursal_refno': 'Disbursal_Reference_No',
        'disbursal_ref_no': 'Disbursal_Reference_No',
        'disbursalreference': 'Disbursal_Reference_No',
        'disbursaldate': 'Disbursal_Date',
        'disbursal_date': 'Disbursal_Date',
        'disbursedbybank': 'Disbursed_By_Bank',
        'disbursed_by_bank': 'Disbursed_By_Bank',
        'disbursaltime': 'Disbursal_Time',
        'disbursal_time': 'Disbursal_Time',
        'enachdetails': 'Enach_Details',
        'enach_details': 'Enach_Details',
        
        # ============ Fees & Charges ============
        'adminfee': 'Admin_Fee',
        'admin_fee': 'Admin_Fee',
        'platformfee': 'Platform_Fee',
        'platform_fee': 'Platform_Fee',
        'platformfee': 'Platform_Fee',
        'conveniencefee': 'Convenience_Fee',
        'convenience_fee': 'Convenience_Fee',
        'convininecefee': 'Convenience_Fee',  # typo fix
        'convininece_fee': 'Convenience_Fee',
        'creditriskanalysisfee': 'CreditRisk_Analysis_Fee',
        'creditrisk_analysis_fee': 'CreditRisk_Analysis_Fee',
        'creditriskfee': 'CreditRisk_Analysis_Fee',
        'creditrisk_analisys_fee': 'CreditRisk_Analysis_Fee',  # typo fix
        'creditriskanalisysfee': 'CreditRisk_Analysis_Fee',
        'cibilfee': 'Cibil_Fee',
        'cibil_fee': 'Cibil_Fee',
        'gstofadminfee': 'GST_On_Admin_Fee',
        'gst_of_admin_fee': 'GST_On_Admin_Fee',
        'gstonadminfee': 'GST_On_Admin_Fee',
        'gst_on_admin': 'GST_On_Admin_Fee',
        'gst_fee': 'GST_On_Admin_Fee',
        'gst': 'GST_On_Admin_Fee',
        
        # ============ Financial ============
        'monthlyincome': 'Monthly_Income',
        'monthly_income': 'Monthly_Income',
        'monthlyobligation': 'Monthly_Obligation',
        'monthly_obligation': 'Monthly_Obligation',
        'cibil': 'Cibil',
        'cibilscore': 'Cibil',
        'cibil_score': 'Cibil',
        'cibill': 'Cibil',
        
        # ============ Collection: Amounts ============
        'collectedamount': 'Collected_Amount',
        'collected_amount': 'Collected_Amount',
        'collectionamount': 'Collected_Amount',
        'collection_amount': 'Collected_Amount',
        'repaymentamount': 'Collected_Amount',
        'repayment_amount': 'Collected_Amount',
        'principalamount': 'Principal_Amount',
        'principal_amount': 'Principal_Amount',
        'interestamount': 'Interest_Amount',
        'interest_amount': 'Interest_Amount',
        'penaltyamount': 'Penalty_Amount',
        'penalty_amount': 'Penalty_Amount',
        'penalty': 'Penalty_Amount',
        'discountamount': 'Discount_Amount',
        'discount_amount': 'Discount_Amount',
        'settlementamount': 'Settlement_Amount',
        'settlement_amount': 'Settlement_Amount',
        'tilldateamount': 'Till_Date_Amount',
        'till_date_amount': 'Till_Date_Amount',
        'excessamount': 'Excess_Amount',
        'excess_amount': 'Excess_Amount',
        'refundamount': 'Refund_Amount',
        'refund_amount': 'Refund_Amount',
        
        # ============ Collection: Waive/Discount ============
        'waiveoff': 'Waive_Off',
        'waive_off': 'Waive_Off',
        'waveoff': 'Waive_Off',  # typo fix
        'wave_off': 'Waive_Off',
        
        # ============ Collection: References & Mode ============
        'collectedmode': 'Collected_Mode',
        'collectionmode': 'Collected_Mode',
        'collection_mode': 'Collected_Mode',
        'collected_date': 'Collected_Date',
        'collectiondate': 'Collected_Date',
        'collection_date': 'Collected_Date',
        'referenceno': 'Reference_No',
        'reference_no': 'Reference_No',
        'utr': 'UTR',
        'collectionsources': 'Collection_Source',
        'collection_source': 'Collection_Source',
        
        # ============ Collection: Refunds ============
        'refundtype': 'Refund_Type',
        'refund_type': 'Refund_Type',
        'refunddate': 'Refund_Date',
        'refund_date': 'Refund_Date',
        
        # ============ Status & Meta ============
        'status': 'Status',
        'remark': 'Remark',
        'remarks': 'Remark',
        'state': 'State',
        'utmsource': 'UTM_Source',
        'utm_source': 'UTM_Source',
        'leadcomingdate': 'Lead_Coming_Date',
        'lead_coming_date': 'Lead_Coming_Date',
        'lead_date': 'Lead_Coming_Date',
        'leadscomingdate': 'Lead_Coming_Date',
        'leadcases': 'Lead_Cases',
        'cases': 'Lead_Cases',
        'formno': 'Form_No',
        'form_no': 'Form_No',
        'redflag': 'Red_Flag',
        'red_flag': 'Red_Flag',
        'pdtype': 'PD_Type',
        'pd_type': 'PD_Type',
        'disbursaltype': 'Disbursal_Type',
        'disbursal_type': 'Disbursal_Type',
        'freshrepeat': 'Fresh_Repeat',
        'fresh_repeat': 'Fresh_Repeat',
        'fresh/repeat': 'Fresh_Repeat',
        'invoiceno': 'Invoice_No',
        'invoice_no': 'Invoice_No',
        'invoicedate': 'Invoice_Date',
        'invoice_date': 'Invoice_Date',
        'approvaldate': 'Approval_Date',
        'approval_date': 'Approval_Date',
        
        # ============ Processing ============
        'creditby': 'Credit_By',
        'credit_by': 'Credit_By',
        'pdby': 'PD_By',
        'pd_by': 'PD_By',
    }

    rename_map = {}
    for col in df.columns:
        k = _key(col)
        if k in canonical_map:
            rename_map[col] = canonical_map[k]

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


@contextmanager
def get_db_connection():
    """Context manager for PostgreSQL database connections using the connection pool."""
    p = _get_pool()
    conn = None
    try:
        conn = p.getconn()
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            p.putconn(conn)


def get_table_name(product: str, table_type: str) -> str:
    """Generate table name for product-specific table."""
    return f"{product.lower()}_{table_type}"


def sanitize_column_name(col: str) -> str:
    """Sanitize column name for PostgreSQL."""
    return col.strip().replace('"', '').replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')[:60]


def create_product_tables(product: str, disbursed_columns: List[str], collection_columns: List[str]) -> None:
    """Create disbursed and collection tables for a specific product with all CSV columns."""
    disbursed_table = get_table_name(product, 'disbursed')
    collection_table = get_table_name(product, 'collection')

    def build_columns(col_list: List[str]) -> List[str]:
        cols = ["id SERIAL PRIMARY KEY"]
        for col in col_list:
            col_clean = sanitize_column_name(col)
            if col_clean.lower() != 'id':
                cols.append(f'"{col_clean}" {_col_pg_type(col_clean)}')
        cols.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        return cols

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            disbursed_cols = build_columns(disbursed_columns)
            collection_cols = build_columns(collection_columns)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {disbursed_table} (
                    {', '.join(disbursed_cols)}
                )
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {collection_table} (
                    {', '.join(collection_cols)}
                )
            """)

            # Commit table creation first so later rollbacks don't remove the tables.
            conn.commit()

            # Add unique constraint for duplicate prevention
            try:
                cur.execute(f"""
                    ALTER TABLE {disbursed_table} 
                    ADD CONSTRAINT {disbursed_table}_lead_loan_unique 
                    UNIQUE ("LeadID", "Loan_No")
                """)
                conn.commit()
            except psycopg2.Error:
                conn.rollback()  # Constraint already exists

            try:
                cur.execute(f"""
                    ALTER TABLE {collection_table} 
                    ADD CONSTRAINT {collection_table}_lead_loan_unique 
                    UNIQUE ("LeadID", "Loan_No")
                """)
                conn.commit()
            except psycopg2.Error:
                conn.rollback()  # Constraint already exists
            logger.info(f"Created tables: {disbursed_table}, {collection_table}")


def insert_dataframe(cur, df: pd.DataFrame, table_name: str) -> tuple:
    """Insert DataFrame records into a PostgreSQL table with bulk upsert (update on duplicate)."""
    if df.empty:
        return 0, 0

    # Normalize columns for insertion
    columns = []
    col_index_map = {}  # Map column name to its index in values
    for idx, col in enumerate(df.columns):
        col_clean = sanitize_column_name(col)
        if col_clean.lower() != 'id':
            columns.append(f'"{col_clean}"')
            col_index_map[col_clean] = len(columns) - 1

    if not columns:
        return 0, 0

    # Build ON CONFLICT update clause (exclude LeadID and Loan_No from update)
    update_cols = [col for col in columns if col not in ['"LeadID"', '"Loan_No"']]
    update_clause = ', '.join([f'{col} = EXCLUDED.{col}' for col in update_cols])

    # Prepare data as list of tuples for bulk insert
    data_tuples = []
    for _, row in df.iterrows():
        values = []
        for col in df.columns:
            col_clean = sanitize_column_name(col)
            if col_clean.lower() != 'id':
                val = row.get(col, '')
                if pd.isna(val):
                    values.append(None)
                elif col_clean.lower() in DATE_COLUMNS:
                    parsed = parse_date_flexible(val)
                    values.append(parsed.strftime('%Y-%m-%d') if parsed else None)
                elif col_clean.lower() in NUMERIC_COLUMNS:
                    try:
                        cleaned = str(val).strip().replace(',', '')
                        values.append(float(cleaned) if cleaned else None)
                    except (ValueError, TypeError):
                        values.append(None)
                else:
                    values.append(str(val).strip())
        data_tuples.append(tuple(values))

    inserted_count = 0
    updated_count = 0
    batch_size = 1000  # Process in batches of 1000 rows
    
    # Get indices for LeadID and Loan_No for deduplication
    leadid_idx = col_index_map.get('LeadID')
    loan_no_idx = col_index_map.get('Loan_No')
    
    logger.info(f"Inserting {len(data_tuples)} rows into {table_name} with columns: {columns}")

    try:
        # Process in batches for better performance and error handling
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            
            # Deduplicate within batch: keep last occurrence of each (LeadID, Loan_No)
            if leadid_idx is not None and loan_no_idx is not None:
                seen = {}
                deduplicated = []
                for idx, row in enumerate(batch):
                    key = (row[leadid_idx], row[loan_no_idx])
                    seen[key] = idx
                # Keep only the last occurrence of each key
                keep_indices = set(seen.values())
                deduplicated = [batch[idx] for idx in sorted(keep_indices)]
                if len(deduplicated) < len(batch):
                    logger.info(f"Deduplicated batch from {len(batch)} to {len(deduplicated)} rows")
                batch = deduplicated
            
            if not batch:
                continue
            
            # Use execute_values for bulk insert with ON CONFLICT
            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES %s
                ON CONFLICT ("LeadID", "Loan_No") DO UPDATE SET
                    {update_clause}
            """
            
            try:
                execute_values(cur, query, batch, page_size=len(batch))
                inserted_count += len(batch)
                logger.info(f"Batch {i//batch_size + 1}: Inserted/updated {len(batch)} rows")
            except psycopg2.Error as e:
                error_msg = str(e)
                logger.warning(f"Batch insert error at batch {i//batch_size + 1}: {error_msg}")
                
                # If it's a duplicate key error, fall back to row-by-row
                if "ON CONFLICT" in error_msg or "duplicate" in error_msg.lower():
                    logger.info(f"Falling back to row-by-row insert for batch {i//batch_size + 1}")
                    for row_idx, row_values in enumerate(batch):
                        try:
                            cur.execute(f"""
                                INSERT INTO {table_name} ({', '.join(columns)})
                                VALUES ({', '.join(['%s'] * len(columns))})
                                ON CONFLICT ("LeadID", "Loan_No") DO UPDATE SET
                                    {update_clause}
                            """, row_values)
                            inserted_count += 1
                        except psycopg2.Error as row_e:
                            logger.debug(f"Row {i + row_idx} failed: {row_e}")
                            continue
                else:
                    # Re-raise if it's a different error
                    raise

    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        raise

    logger.info(f"Completed: {inserted_count} rows processed for {table_name}")
    return inserted_count, 0


def align_dataframe_columns(df: pd.DataFrame, existing_cols: List[str]) -> pd.DataFrame:
    """Align DataFrame columns to match existing table columns."""
    if df.empty:
        return df
    
    df = df.copy()
    
    # Check for duplicate columns in input
    if len(df.columns) != len(set(df.columns)):
        logger.warning(f"Duplicate columns found in input: {df.columns.tolist()}")
        df = df.loc[:, ~df.columns.duplicated()]
    
    # Build mapping from normalized names to original df column names
    df_col_map = {}
    for col in df.columns:
        normalized = sanitize_column_name(col).lower()
        if normalized not in df_col_map:
            df_col_map[normalized] = col
    
    # Build mapping of existing table columns to df columns
    matched_cols = {}
    unmatched_cols = []
    
    for existing_col in existing_cols:
        existing_normalized = existing_col.lower()
        if existing_normalized in df_col_map:
            matched_cols[existing_col] = df_col_map[existing_normalized]
        else:
            unmatched_cols.append(existing_col)
    
    logger.info(f"Aligning: {len(matched_cols)} matched, {len(unmatched_cols)} missing")
    
    # Build the aligned dataframe column by column
    aligned_data = {}
    
    # Add matched columns
    for table_col, df_col in matched_cols.items():
        aligned_data[table_col] = df[df_col].values
    
    # Add missing columns with None values
    for col in unmatched_cols:
        aligned_data[col] = [None] * len(df)
    
    # Create new DataFrame
    new_df = pd.DataFrame(aligned_data, index=df.index)
    
    return new_df


def process_uploaded_files(disbursed_df: pd.DataFrame, collection_df: pd.DataFrame, product: str) -> Dict[str, Any]:
    """Process uploaded CSV files and insert into product-specific PostgreSQL tables."""
    disbursed_df = normalize_upload_columns(disbursed_df)
    collection_df = normalize_upload_columns(collection_df)
    
    # Standardize date columns to YYYY-MM-DD format
    date_columns = ['Disbursal_Date', 'Repay_Date', 'Collected_Date', 'Lead_Coming_Date', 
                   'DOB', 'Invoice_Date', 'Approval_Date', 'Refund_Date']
    
    for col in date_columns:
        disbursed_df = standardize_date_column(disbursed_df, col)
        collection_df = standardize_date_column(collection_df, col)

    disbursed_table = get_table_name(product, 'disbursed')
    collection_table = get_table_name(product, 'collection')

    # Check if tables exist and get their columns
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (disbursed_table,))
            disbursed_exists = cur.fetchone()['exists']
            
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (collection_table,))
            collection_exists = cur.fetchone()['exists']
    
    if disbursed_exists and collection_exists:
        # Tables exist - get their actual column names
        logger.info(f"Tables exist for {product}, using existing schema")
        existing_disbursed_cols = [c['column_name'] for c in get_table_columns(disbursed_table) if c['column_name'] != 'id']
        existing_collection_cols = [c['column_name'] for c in get_table_columns(collection_table) if c['column_name'] != 'id']
        
        # Align uploaded data columns to existing table columns
        disbursed_df = align_dataframe_columns(disbursed_df, existing_disbursed_cols)
        collection_df = align_dataframe_columns(collection_df, existing_collection_cols)
    else:
        # Create new tables
        create_product_tables(product, list(disbursed_df.columns), list(collection_df.columns))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            disbursed_table = get_table_name(product, 'disbursed')
            collection_table = get_table_name(product, 'collection')

            disbursed_inserted, disbursed_updated = insert_dataframe(cur, disbursed_df, disbursed_table)
            collection_inserted, collection_updated = insert_dataframe(cur, collection_df, collection_table)

            conn.commit()
            logger.info(f"{product}: {disbursed_inserted} inserted/{disbursed_updated} updated disbursed, {collection_inserted} inserted/{collection_updated} updated collection")

    return {
        'success': True,
        'product': product,
        'disbursed_inserted': disbursed_inserted,
        'disbursed_updated': disbursed_updated,
        'collection_inserted': collection_inserted,
        'collection_updated': collection_updated
    }


def list_products() -> List[str]:
    """Get list of products by finding all tables ending with _disbursed."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%_disbursed'
                ORDER BY table_name
            """)
            tables = cur.fetchall()
            return [row['table_name'].replace('_disbursed', '').upper() for row in tables]


def get_table_columns(table_name: str) -> List[Dict[str, Any]]:
    """Get all columns for a specific table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cur.fetchall()
            return [
                {
                    'column_name': row['column_name'],
                    'data_type': row['data_type'],
                    'is_nullable': row['is_nullable'],
                    'quoted_name': f'"{row["column_name"]}"'
                }
                for row in columns
            ]


def parse_date_flexible(date_val):
    """Parse date string in multiple formats and return standardized YYYY-MM-DD format."""
    if not date_val:
        return None
    
    if isinstance(date_val, datetime):
        return date_val.date() if hasattr(date_val, 'date') else date_val
    
    date_str = str(date_val).strip()
    if not date_str:
        return None
    
    # Try DD-MM-YYYY format first (common in Indian data)
    try:
        parsed_date = datetime.strptime(date_str, '%d-%m-%Y').date()
        return parsed_date
    except ValueError:
        pass
    
    # Try YYYY-MM-DD format
    try:
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        return parsed_date
    except ValueError:
        pass
    
    # Try DD/MM/YYYY format
    try:
        parsed_date = datetime.strptime(date_str, '%d/%m/%Y').date()
        return parsed_date
    except ValueError:
        pass
    
    # If all fail, log and return None
    logger.warning(f"Could not parse date: {date_str}")
    return None


def standardize_date_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Standardize date column to DD-MM-YYYY format."""
    if column_name not in df.columns:
        return df
    
    df = df.copy()
    df[column_name] = df[column_name].apply(lambda x: 
        parse_date_flexible(x).strftime('%d-%m-%Y') if parse_date_flexible(x) else None
    )
    return df


def determine_payment_status(collected_date: Optional[datetime], repay_date: Optional[datetime], status: str = None) -> str:
    """Determine payment status based on collection and repayment dates."""
    # Check for preclose status first
    if status and status.lower() == 'payday preclose':
        return "PRECLOSE"
    
    # Parse dates flexibly
    collected_date = parse_date_flexible(collected_date)
    repay_date = parse_date_flexible(repay_date)
    
    if not collected_date or not repay_date:
        return "NOT_COLLECTED"
    
    if collected_date < repay_date:
        return "EARLY"
    elif collected_date == repay_date:
        return "ON_TIME"
    elif (collected_date - repay_date).days <= GRACE_PERIOD_DAYS:
        return "GRACE_PERIOD"
    else:
        return "LATE"


def search_customer(pan: str = None, name: str = None, mobile: str = None, case_sensitive: bool = False) -> Dict[str, Any]:
    """Search records by PAN, Name, or Mobile across all product tables in PostgreSQL."""
    products = list_products()
    all_results = []
    
    # Check if at least one search parameter is provided
    if not any([pan, name, mobile]):
        return {
            'success': False,
            'error': 'At least one search parameter (PAN, Name, or Mobile) is required',
            'total_records': 0,
            'records': []
        }

    # Build WHERE clause once (shared across all product sub-queries)
    where_conditions = []
    base_params = []
    if pan:
        where_conditions.append('LOWER(d."Pancard") = LOWER(%s)')
        base_params.append(pan)
    if name:
        where_conditions.append('LOWER(d."Name") LIKE LOWER(%s)')
        base_params.append(f'%{name}%')
    if mobile:
        where_conditions.append('LOWER(d."Mobile") = LOWER(%s)')
        base_params.append(mobile)
    where_clause = ' OR '.join(where_conditions)

    # Build a single UNION ALL query across all product tables
    union_parts = []
    all_params = []
    for product in products:
        if not VALID_PRODUCT_RE.match(product):
            logger.warning(f"Skipping invalid product name: {product}")
            continue
        d_table = get_table_name(product, 'disbursed')
        c_table = get_table_name(product, 'collection')
        # Determine loan amount column (varies by product)
        d_cols = [c['column_name'] for c in get_table_columns(d_table)]
        if 'Loan_Amount' in d_cols:
            loan_amt_expr = 'd."Loan_Amount"'
        elif 'Loan_Amount_Approved' in d_cols:
            loan_amt_expr = 'd."Loan_Amount_Approved"'
        else:
            loan_amt_expr = 'NULL'
        union_parts.append(f"""
            (SELECT d."LeadID", d."Loan_No", d."Name", d."Mobile", d."Pancard",
                    d."DOB", d."Gender", d."Email", d."Aadhar_No",
                    d."Loan_Type", d."Branch", d."Tenure", d."ROI",
                    {loan_amt_expr} AS "Loan_Amount",
                    d."Repay_Date", d."Disbursal_Date", d."Cibil",
                    d."Monthly_Income", d."Status",
                    c."Collected_Date" AS "Collected_Date",
                    c."Collected_Amount" AS "Collected_Amount",
                    c."Status" AS "Collection_Status",
                    %s AS "_product"
             FROM {d_table} d
             LEFT JOIN {c_table} c ON d."LeadID" = c."LeadID" AND d."Loan_No" = c."Loan_No"
             WHERE {where_clause})
        """)
        all_params.append(product)
        all_params.extend(base_params)

    if not union_parts:
        return {'success': True, 'pan': pan, 'name': name, 'mobile': mobile, 'total_records': 0, 'records': []}

    with get_db_connection() as conn:
        try:
            with conn.cursor() as cur:
                query = ' UNION ALL '.join(union_parts) + ' ORDER BY "Repay_Date"'
                cur.execute(query, tuple(all_params))

                for row in cur.fetchall():
                    result = dict(row)
                    result['RepayDate'] = result.get('Repay_Date')
                    result['CollectionDate'] = result.get('Collected_Date')
                    result['Loan_Amount'] = result.get('Loan_Amount') or result.get('Loan_Amount_Approved') or result.get('LoanAmount')
                    result['PaymentStatus'] = determine_payment_status(
                        result.get('Collected_Date'),
                        result.get('Repay_Date'),
                        result.get('Collection_Status')
                    )
                    result['Product'] = result.pop('_product', '').upper()
                    all_results.append(result)
        except psycopg2.Error as e:
            logger.warning(f"Error in customer search: {e}")

    return {
        'success': True,
        'pan': pan,
        'name': name,
        'mobile': mobile,
        'total_records': len(all_results),
        'records': all_results
    }


def calculate_behavior_score(pan: str = None, name: str = None, mobile: str = None) -> Dict[str, Any]:
    """Calculate customer behavior score from disbursed + collection history."""
    result = search_customer(pan=pan, name=name, mobile=mobile)
    if not result.get('success') or not result.get('records'):
        return {'success': False, 'error': 'Customer not found', 'records': 0}

    records = result['records']
    total_loans = len(records)
    from datetime import date

    # --- 1. Payment Timeliness (0-10) ---
    status_counts = {'EARLY': 0, 'ON_TIME': 0, 'GRACE_PERIOD': 0, 'LATE': 0, 'NOT_COLLECTED': 0, 'PRECLOSE': 0}
    for r in records:
        st = r.get('PaymentStatus', 'NOT_COLLECTED')
        status_counts[st] = status_counts.get(st, 0) + 1

    collected = total_loans - status_counts['NOT_COLLECTED']
    timeliness_pending = False
    if collected > 0:
        good = status_counts['EARLY'] + status_counts['ON_TIME'] + status_counts['PRECLOSE']
        ok = status_counts['GRACE_PERIOD']
        bad = status_counts['LATE']
        timeliness = min(10, round((good * 10 + ok * 6 + bad * 2) / collected, 1))
    else:
        # All loans are NOT_COLLECTED — new customer, give neutral score
        timeliness = 5
        timeliness_pending = True

    # --- 2. CIBIL Score (0-10) ---
    cibil_values = [int(r.get('Cibil') or 0) for r in records if r.get('Cibil')]
    avg_cibil = round(sum(cibil_values) / len(cibil_values)) if cibil_values else 0
    if avg_cibil >= 751: cibil_pts = 10
    elif avg_cibil >= 701: cibil_pts = 8
    elif avg_cibil >= 651: cibil_pts = 6
    elif avg_cibil >= 601: cibil_pts = 4
    elif avg_cibil >= 550: cibil_pts = 2
    else: cibil_pts = 1

    # --- 3. Age Score (0-10) from DOB ---
    first = records[0]
    dob_str = first.get('DOB')
    age = None
    age_pts = 5  # neutral default
    if dob_str:
        dob_date = parse_date_flexible(dob_str)
        if dob_date:
            today = date.today()
            age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
            if 21 <= age <= 30: age_pts = 8
            elif 31 <= age <= 40: age_pts = 10
            elif 41 <= age <= 50: age_pts = 7
            elif age > 50: age_pts = 5
            else: age_pts = 5
    age_detail = f'Age: {age} yrs' if age else 'DOB not available'

    # --- 4. Obligation Ratio (0-10) ---
    incomes = []
    obligations = []
    for r in records:
        inc = r.get('Monthly_Income')
        if inc:
            try: incomes.append(float(inc))
            except (ValueError, TypeError): pass
        obl = r.get('Monthly_Obligation')
        if obl:
            try: obligations.append(float(obl))
            except (ValueError, TypeError): pass

    avg_income = round(sum(incomes) / len(incomes)) if incomes else 0
    avg_obligation = round(sum(obligations) / len(obligations)) if obligations else 0
    obligation_pts = 5  # neutral default
    obligation_detail = 'No data'
    if avg_income > 0:
        ratio = avg_obligation / avg_income
        ratio_pct = round(ratio * 100)
        if ratio < 0.30: obligation_pts = 10
        elif ratio < 0.50: obligation_pts = 8
        elif ratio < 0.70: obligation_pts = 5
        else: obligation_pts = 2
        obligation_detail = f'{ratio_pct}% of income'

    # --- Weighted Final Score ---
    weights = {
        'timeliness': 0.30,
        'cibil': 0.30,
        'age': 0.15,
        'obligation': 0.25,
    }
    scores = {
        'timeliness': timeliness,
        'cibil': cibil_pts,
        'age': age_pts,
        'obligation': obligation_pts,
    }
    final_score = round(sum(scores[k] * weights[k] for k in weights), 1)

    # --- Risk Label ---
    if final_score >= 8: risk = 'Low Risk'
    elif final_score >= 6: risk = 'Medium Risk'
    elif final_score >= 4: risk = 'High Risk'
    else: risk = 'Very High Risk'

    timeliness_detail = 'Pending (new customer)' if timeliness_pending else f"Early: {status_counts['EARLY']}, On-time: {status_counts['ON_TIME']}, Late: {status_counts['LATE']}"

    return {
        'success': True,
        'customer': {
            'name': first.get('Name', ''),
            'pan': first.get('Pancard', ''),
            'mobile': first.get('Mobile', ''),
            'cibil': avg_cibil,
            'age': age,
            'dob': dob_str,
        },
        'total_loans': total_loans,
        'behavior_score': final_score,
        'max_score': 10.0,
        'risk_label': risk,
        'breakdown': {
            'payment_timeliness': {'score': timeliness, 'detail': timeliness_detail},
            'cibil_score': {'score': cibil_pts, 'detail': f'Avg CIBIL: {avg_cibil}'},
            'age_score': {'score': age_pts, 'detail': age_detail},
            'obligation_ratio': {'score': obligation_pts, 'detail': obligation_detail},
        },
        'payment_summary': status_counts,
    }


def run_read_only_query(query: str) -> Dict[str, Any]:
    """Execute SQL query on PostgreSQL database - SELECT only."""
    # Normalize query for validation
    normalized = query.strip().upper()
    
    # Block dangerous commands
    forbidden_keywords = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'TRUNCATE', 'ALTER', 'GRANT', 'REVOKE']
    for keyword in forbidden_keywords:
        if keyword in normalized:
            raise ValueError(f"{keyword} commands are not allowed. Only SELECT queries are permitted.")
    
    # Ensure query starts with SELECT
    if not normalized.startswith('SELECT'):
        raise ValueError("Only SELECT queries are allowed.")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            
            if cur.description:
                records = cur.fetchall()
                return {
                    'success': True,
                    'total_records': len(records),
                    'records': [dict(row) for row in records]
                }
            else:
                return {
                    'success': True,
                    'total_records': 0,
                    'records': [],
                    'message': 'Query executed successfully'
                }


# Authentication and Logging Functions

def create_users_table():
    """Create users table for authentication."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'users'
                )
            """)
            table_exists = cur.fetchone()['exists']
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            """)
            conn.commit()
            if table_exists:
                logger.info("Users table verified (already exists)")
            else:
                logger.info("Users table created")


def create_activity_logs_table():
    """Create activity logs table for tracking user actions."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'activity_logs'
                )
            """)
            table_exists = cur.fetchone()['exists']
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) NOT NULL,
                    action VARCHAR(50) NOT NULL,
                    details TEXT,
                    ip_address VARCHAR(45),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            if table_exists:
                logger.info("Activity logs table verified (already exists)")
            else:
                logger.info("Activity logs table created")


def init_auth_tables():
    """Initialize authentication tables."""
    create_users_table()
    create_activity_logs_table()


def create_user(username: str, password: str, role: str = 'user') -> bool:
    """Create a new user with hashed password."""
    import hashlib
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO users (username, password_hash, role)
                    VALUES (%s, %s, %s)
                """, (username, password_hash, role))
                conn.commit()
                logger.info(f"User created: {username} (role: {role})")
                return True
            except psycopg2.IntegrityError:
                logger.warning(f"Username already exists: {username}")
                return False


def admin_create_user(username: str, password: str, role: str = 'user', created_by: str = None) -> Dict[str, Any]:
    """Admin creates a new user with generated password. Returns the generated password."""
    import hashlib
    import secrets
    import string
    
    # Generate random password if not provided
    if not password:
        password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(10))
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO users (username, password_hash, role)
                    VALUES (%s, %s, %s)
                """, (username, password_hash, role))
                conn.commit()
                
                # Log admin activity
                log_activity(created_by or 'admin', "CREATE_USER", f"Created user: {username} (role: {role})")
                
                logger.info(f"Admin created user: {username} (role: {role})")
                return {
                    'success': True,
                    'username': username,
                    'password': password,
                    'role': role,
                    'message': f"User '{username}' created successfully with password: {password}"
                }
            except psycopg2.IntegrityError:
                logger.warning(f"Username already exists: {username}")
                return {
                    'success': False,
                    'error': f"Username '{username}' already exists"
                }


def get_all_users() -> List[Dict[str, Any]]:
    """Get all users (for admin)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, role, created_at, last_login
                FROM users
                ORDER BY created_at DESC
            """)
            return [dict(row) for row in cur.fetchall()]


def delete_user(username: str, deleted_by: str = None) -> bool:
    """Delete a user (admin only)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE username = %s", (username,))
            conn.commit()
            
            if cur.rowcount > 0:
                log_activity(deleted_by or 'admin', "DELETE_USER", f"Deleted user: {username}")
                logger.info(f"User deleted: {username}")
                return True
            return False


def reset_user_password(username: str, new_password: str = None, reset_by: str = None) -> Dict[str, Any]:
    """Reset user password (admin only). Returns the new password."""
    import hashlib
    import secrets
    import string
    
    # Generate random password if not provided
    if not new_password:
        new_password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(10))
    
    password_hash = hashlib.sha256(new_password.encode()).hexdigest()
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET password_hash = %s
                WHERE username = %s
            """, (password_hash, username))
            conn.commit()
            
            if cur.rowcount > 0:
                log_activity(reset_by or 'admin', "RESET_PASSWORD", f"Reset password for: {username}")
                logger.info(f"Password reset for user: {username}")
                return {
                    'success': True,
                    'username': username,
                    'new_password': new_password,
                    'message': f"Password reset for '{username}'. New password: {new_password}"
                }
            return {
                'success': False,
                'error': f"User '{username}' not found"
            }


def verify_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Verify user credentials and return user info."""
    import hashlib
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, role, created_at
                FROM users
                WHERE username = %s AND password_hash = %s
            """, (username, password_hash))
            
            user = cur.fetchone()
            if user:
                # Update last login
                cur.execute("""
                    UPDATE users SET last_login = CURRENT_TIMESTAMP
                    WHERE username = %s
                """, (username,))
                conn.commit()
                return dict(user)
            return None


def log_activity(username: str, action: str, details: str = None, ip_address: str = None):
    """Log user activity."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO activity_logs (username, action, details, ip_address)
                VALUES (%s, %s, %s, %s)
            """, (username, action, details, ip_address))
            conn.commit()
            logger.info(f"Activity logged: {username} - {action}")


def get_user_logs(username: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Get activity logs for a user or all users."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if username:
                cur.execute("""
                    SELECT * FROM activity_logs
                    WHERE username = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (username, limit))
            else:
                cur.execute("""
                    SELECT * FROM activity_logs
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (limit,))
            
            return [dict(row) for row in cur.fetchall()]


# ─── Bank Statement Storage ─────────────────────────────────

def create_bank_statements_table():
    """Create bank_statements table for storing fetched AA statements."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'bank_statements'
                )
            """)
            table_exists = cur.fetchone()['exists']

            cur.execute("""
                CREATE TABLE IF NOT EXISTS bank_statements (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200),
                    mobile VARCHAR(20),
                    bank_name VARCHAR(200),
                    account_number VARCHAR(100),
                    account_type VARCHAR(50),
                    statement_from DATE,
                    statement_to DATE,
                    summary JSONB,
                    transactions JSONB,
                    transaction_count INTEGER DEFAULT 0,
                    created_by VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

            if not table_exists:
                try:
                    cur.execute("""
                        CREATE INDEX idx_bank_stmt_name ON bank_statements (LOWER(customer_name));
                        CREATE INDEX idx_bank_stmt_mobile ON bank_statements (mobile);
                    """)
                    conn.commit()
                except psycopg2.Error:
                    conn.rollback()
                logger.info("bank_statements table created")
            else:
                logger.info("bank_statements table verified (already exists)")


def save_bank_statement(account_data: Dict[str, Any], created_by: str = None) -> Dict[str, Any]:
    """Save a fetched bank statement to the database."""
    txns = account_data.get('transactions', [])

    # Determine date range from transactions
    dates = []
    for txn in txns:
        date_str = txn.get('valueDate') or txn.get('transactionTimestamp') or txn.get('txnDate', '')
        if date_str:
            try:
                dates.append(datetime.strptime(date_str[:10], '%Y-%m-%d').date())
            except ValueError:
                pass

    stmt_from = min(dates) if dates else None
    stmt_to = max(dates) if dates else None

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Check for duplicate (same account + same date range)
            cur.execute("""
                SELECT id FROM bank_statements
                WHERE account_number = %s AND bank_name = %s
                AND statement_from = %s AND statement_to = %s
            """, (
                account_data.get('maskedAccNumber', ''),
                account_data.get('fipId', ''),
                stmt_from, stmt_to
            ))
            existing = cur.fetchone()

            if existing:
                cur.execute("""
                    UPDATE bank_statements SET
                        customer_name = %s, mobile = %s,
                        summary = %s, transactions = %s, transaction_count = %s,
                        created_by = %s, created_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (
                    account_data.get('holderName', ''),
                    account_data.get('mobile', ''),
                    psycopg2.extras.Json(account_data.get('summary', {})),
                    psycopg2.extras.Json(txns),
                    len(txns),
                    created_by,
                    existing['id']
                ))
                conn.commit()
                stmt_id = existing['id']
                logger.info(f"Updated bank statement id={stmt_id}")
                return {'success': True, 'id': stmt_id, 'action': 'updated'}
            else:
                cur.execute("""
                    INSERT INTO bank_statements
                        (customer_name, mobile, bank_name, account_number,
                         account_type, statement_from, statement_to, summary,
                         transactions, transaction_count, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    account_data.get('holderName', ''),
                    account_data.get('mobile', ''),
                    account_data.get('fipId', ''),
                    account_data.get('maskedAccNumber', ''),
                    account_data.get('accountType', ''),
                    stmt_from, stmt_to,
                    psycopg2.extras.Json(account_data.get('summary', {})),
                    psycopg2.extras.Json(txns),
                    len(txns),
                    created_by
                ))
                stmt_id = cur.fetchone()['id']
                conn.commit()
                logger.info(f"Saved bank statement id={stmt_id}")
                return {'success': True, 'id': stmt_id, 'action': 'created'}


def search_bank_statements(name: str = None, mobile: str = None) -> List[Dict[str, Any]]:
    """Search saved bank statements by customer name or mobile. Returns metadata only."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            conditions = []
            params = []

            if name:
                conditions.append("LOWER(customer_name) LIKE LOWER(%s)")
                params.append(f"%{name}%")
            if mobile:
                conditions.append("mobile LIKE %s")
                params.append(f"%{mobile}%")

            if not conditions:
                return []

            where_clause = " OR ".join(conditions)

            cur.execute(f"""
                SELECT id, customer_name, mobile, bank_name, account_number,
                       account_type, statement_from, statement_to,
                       transaction_count, created_by, created_at
                FROM bank_statements
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT 50
            """, tuple(params))

            results = []
            for row in cur.fetchall():
                r = dict(row)
                if r.get('statement_from'):
                    r['statement_from'] = r['statement_from'].strftime('%d-%m-%Y')
                if r.get('statement_to'):
                    r['statement_to'] = r['statement_to'].strftime('%d-%m-%Y')
                if r.get('created_at'):
                    r['created_at'] = r['created_at'].strftime('%d-%m-%Y %H:%M')
                results.append(r)
            return results


def get_bank_statement(statement_id: int) -> Optional[Dict[str, Any]]:
    """Load a full bank statement by ID (including transactions)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bank_statements WHERE id = %s", (statement_id,))
            row = cur.fetchone()
            if not row:
                return None
            r = dict(row)
            if r.get('statement_from'):
                r['statement_from'] = r['statement_from'].strftime('%d-%m-%Y')
            if r.get('statement_to'):
                r['statement_to'] = r['statement_to'].strftime('%d-%m-%Y')
            if r.get('created_at'):
                r['created_at'] = r['created_at'].strftime('%d-%m-%Y %H:%M')
            return r


def delete_bank_statement(statement_id: int) -> bool:
    """Delete a saved bank statement."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bank_statements WHERE id = %s", (statement_id,))
            conn.commit()
            return cur.rowcount > 0


