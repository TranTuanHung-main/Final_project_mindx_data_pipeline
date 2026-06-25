"""
Supermarket Sales ETL Pipeline
===============================
DAG thực hiện quy trình ETL hoàn chỉnh cho dữ liệu Supermarket Sales:
  1. Tạo bảng trong PostgreSQL
  2. Load dữ liệu thô từ file Excel vào raw table
  3. Clean & validate dữ liệu
  4. Load dữ liệu đã clean vào data warehouse
  5. Kiểm tra chất lượng dữ liệu (Data Quality Check)

"""

import os
import logging
import time
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import urllib.parse

# ============================================================
# CẤU HÌNH
# ============================================================
POSTGRES_CONN_ID = 'my_postgres_db'
RAW_TABLE = 'raw_supermarket_sales'
WAREHOUSE_TABLE = 'warehouse_supermarket_sales'
DATA_FILE = '/opt/airflow/data/supermarket_sales.xlsx'

logger = logging.getLogger(__name__)

def get_postgres_engine():
    """Tạo SQLAlchemy engine trực tiếp từ connection config, tránh lỗi option __extra__."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_connection(POSTGRES_CONN_ID)
    login = urllib.parse.quote_plus(conn.login) if conn.login else ""
    password = urllib.parse.quote_plus(conn.password) if conn.password else ""
    conn_uri = f"postgresql+psycopg2://{login}:{password}@{conn.host}:{conn.port}/{conn.schema or ''}"
    return create_engine(conn_uri)

# ============================================================
# DEFAULT ARGS
# ============================================================
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# ============================================================
# TASK 1: TẠO BẢNG
# ============================================================
def create_tables(**kwargs):
    """Tạo raw table và warehouse table trong PostgreSQL nếu chưa tồn tại."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("TASK 1: BẮT ĐẦU TẠO BẢNG")
    logger.info("=" * 60)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_raw_sql = f"""
    DROP TABLE IF EXISTS {RAW_TABLE};
    CREATE TABLE {RAW_TABLE} (
        invoice_id TEXT,
        branch TEXT,
        city TEXT,
        customer_type TEXT,
        gender TEXT,
        product_line TEXT,
        unit_price NUMERIC,
        quantity INTEGER,
        tax_5 NUMERIC,
        total NUMERIC,
        date TEXT,
        time TEXT,
        payment TEXT,
        cogs NUMERIC,
        gross_margin_percentage NUMERIC,
        gross_income NUMERIC,
        rating NUMERIC
    );
    """

    create_wh_sql = f"""
    DROP TABLE IF EXISTS {WAREHOUSE_TABLE};
    CREATE TABLE {WAREHOUSE_TABLE} (
        invoice_id TEXT PRIMARY KEY,
        branch TEXT NOT NULL,
        city TEXT NOT NULL,
        customer_type TEXT,
        gender TEXT,
        product_line TEXT NOT NULL,
        unit_price NUMERIC NOT NULL,
        quantity INTEGER NOT NULL,
        tax_5 NUMERIC,
        total NUMERIC NOT NULL,
        date DATE NOT NULL,
        time TIME,
        payment TEXT,
        cogs NUMERIC,
        gross_income NUMERIC,
        rating NUMERIC,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    logger.info("Đang tạo bảng raw: %s", RAW_TABLE)
    hook.run(create_raw_sql)
    logger.info("✅ Đã tạo bảng raw thành công")

    logger.info("Đang tạo bảng warehouse: %s", WAREHOUSE_TABLE)
    hook.run(create_wh_sql)
    logger.info("✅ Đã tạo bảng warehouse thành công")

    elapsed = round(time.time() - start_time, 2)
    logger.info("⏱️  Task hoàn thành trong %s giây", elapsed)


# ============================================================
# TASK 2: LOAD DỮ LIỆU THÔ VÀO RAW TABLE
# ============================================================
def load_csv_to_raw(**kwargs):
    """Đọc file Excel và load nguyên bản vào raw table (không transform)."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("TASK 2: LOAD DỮ LIỆU THÔ VÀO RAW TABLE")
    logger.info("=" * 60)

    # Kiểm tra file tồn tại
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu: {DATA_FILE}")

    file_size = os.path.getsize(DATA_FILE)
    logger.info("📂 File: %s (%.2f KB)", DATA_FILE, file_size / 1024)

    # Đọc file Excel
    logger.info("Đang đọc file Excel...")
    df = pd.read_excel(DATA_FILE, engine='openpyxl')
    logger.info("📊 Số dòng đọc được: %d", len(df))
    logger.info("📊 Số cột: %d", len(df.columns))
    logger.info("📊 Các cột: %s", list(df.columns))

    # Chuẩn hóa tên cột
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('%', '')
        .str.replace('-', '_')
    )
    df = df.rename(columns={'tax_5_': 'tax_5'})
    logger.info("📊 Tên cột sau chuẩn hóa: %s", list(df.columns))

    # Load vào raw table
    engine = get_postgres_engine()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    logger.info("Đang load dữ liệu vào bảng %s...", RAW_TABLE)
    df.to_sql(RAW_TABLE, con=engine, if_exists='append', index=False)

    # Verify
    row_count = hook.get_first(f"SELECT COUNT(*) FROM {RAW_TABLE}")[0]
    logger.info("✅ Đã load %d dòng vào bảng %s", row_count, RAW_TABLE)

    # Lưu metadata qua XCom
    kwargs['ti'].xcom_push(key='raw_row_count', value=row_count)

    elapsed = round(time.time() - start_time, 2)
    logger.info("⏱️  Task hoàn thành trong %s giây", elapsed)


# ============================================================
# TASK 3: CLEAN & VALIDATE DỮ LIỆU
# ============================================================
def clean_validate(**kwargs):
    """Đọc từ raw table, clean và validate dữ liệu."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("TASK 3: CLEAN & VALIDATE DỮ LIỆU")
    logger.info("=" * 60)

    engine = get_postgres_engine()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Đọc dữ liệu từ raw table
    logger.info("Đang đọc dữ liệu từ bảng %s...", RAW_TABLE)
    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", con=engine)
    total_before = len(df)
    logger.info("📊 Tổng số dòng từ raw: %d", total_before)

    # --- BƯỚC 1: Loại bỏ bản ghi trùng lặp ---
    duplicates = df.duplicated(subset=['invoice_id']).sum()
    df = df.drop_duplicates(subset=['invoice_id'], keep='first')
    logger.info("🔍 Loại bỏ %d bản ghi trùng lặp (theo invoice_id)", duplicates)

    # --- BƯỚC 2: Loại bỏ giá trị null ở các cột quan trọng ---
    critical_cols = ['invoice_id', 'branch', 'product_line', 'total', 'date']
    null_before = df[critical_cols].isnull().sum()
    logger.info("🔍 Null values trước khi clean:")
    for col, count in null_before.items():
        if count > 0:
            logger.warning("   ⚠️  Cột '%s' có %d giá trị null", col, count)

    rows_before_dropna = len(df)
    df = df.dropna(subset=critical_cols)
    dropped_null = rows_before_dropna - len(df)
    logger.info("🔍 Loại bỏ %d dòng có null ở cột quan trọng", dropped_null)

    # --- BƯỚC 3: Validate giá trị hợp lệ ---
    invalid_quantity = (df['quantity'] <= 0).sum()
    invalid_total = (df['total'] <= 0).sum()
    df = df[(df['quantity'] > 0) & (df['total'] > 0)]
    logger.info("🔍 Loại bỏ %d dòng có quantity <= 0", invalid_quantity)
    logger.info("🔍 Loại bỏ %d dòng có total <= 0", invalid_total)

    # --- BƯỚC 4: Chuyển đổi kiểu dữ liệu ---
    logger.info("Đang chuyển đổi kiểu dữ liệu...")
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype(int)
    df['total'] = pd.to_numeric(df['total'], errors='coerce')

    # --- BƯỚC 5: Loại bỏ cột không cần thiết ---
    if 'gross_margin_percentage' in df.columns:
        df = df.drop(columns=['gross_margin_percentage'])
        logger.info("🔍 Đã loại bỏ cột 'gross_margin_percentage' (giá trị không đổi)")

    total_after = len(df)
    total_removed = total_before - total_after
    logger.info("=" * 40)
    logger.info("📊 KẾT QUẢ CLEAN & VALIDATE:")
    logger.info("   Tổng dòng ban đầu:   %d", total_before)
    logger.info("   Tổng dòng bị loại:   %d", total_removed)
    logger.info("   Tổng dòng còn lại:   %d", total_after)
    logger.info("   Tỷ lệ dữ liệu sạch: %.1f%%", (total_after / total_before * 100) if total_before > 0 else 0)
    logger.info("=" * 40)

    # Lưu metadata qua XCom
    kwargs['ti'].xcom_push(key='clean_row_count', value=total_after)
    kwargs['ti'].xcom_push(key='removed_row_count', value=total_removed)

    # Lưu dataframe đã clean tạm thời vào staging
    df.to_sql('staging_supermarket_sales', con=engine, if_exists='replace', index=False)
    logger.info("✅ Đã lưu dữ liệu sạch vào staging table")

    elapsed = round(time.time() - start_time, 2)
    logger.info("⏱️  Task hoàn thành trong %s giây", elapsed)


# ============================================================
# TASK 4: LOAD DỮ LIỆU VÀO DATA WAREHOUSE
# ============================================================
def load_to_warehouse(**kwargs):
    """Load dữ liệu đã clean từ staging vào warehouse table."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("TASK 4: LOAD DỮ LIỆU VÀO DATA WAREHOUSE")
    logger.info("=" * 60)

    engine = get_postgres_engine()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Đọc từ staging
    logger.info("Đang đọc dữ liệu từ staging table...")
    df = pd.read_sql("SELECT * FROM staging_supermarket_sales", con=engine)
    logger.info("📊 Số dòng cần load: %d", len(df))

    # Load vào warehouse
    logger.info("Đang load vào bảng %s...", WAREHOUSE_TABLE)
    df.to_sql(WAREHOUSE_TABLE, con=engine, if_exists='append', index=False)

    # Verify
    wh_count = hook.get_first(f"SELECT COUNT(*) FROM {WAREHOUSE_TABLE}")[0]
    logger.info("✅ Đã load %d dòng vào bảng warehouse", wh_count)

    # Cleanup staging
    hook.run("DROP TABLE IF EXISTS staging_supermarket_sales")
    logger.info("🧹 Đã xóa staging table")

    # Lưu metadata qua XCom
    kwargs['ti'].xcom_push(key='warehouse_row_count', value=wh_count)

    elapsed = round(time.time() - start_time, 2)
    logger.info("⏱️  Task hoàn thành trong %s giây", elapsed)


# ============================================================
# TASK 5: DATA QUALITY CHECK
# ============================================================
def data_quality_check(**kwargs):
    """Kiểm tra chất lượng dữ liệu trong warehouse."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("TASK 5: DATA QUALITY CHECK")
    logger.info("=" * 60)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ti = kwargs['ti']

    # Lấy metadata từ XCom
    raw_count = ti.xcom_pull(task_ids='load_csv_to_raw', key='raw_row_count')
    clean_count = ti.xcom_pull(task_ids='clean_validate', key='clean_row_count')
    wh_count = ti.xcom_pull(task_ids='load_to_warehouse', key='warehouse_row_count')

    logger.info("📊 TỔNG QUAN PIPELINE:")
    logger.info("   Raw table:       %s dòng", raw_count)
    logger.info("   Sau clean:       %s dòng", clean_count)
    logger.info("   Warehouse table: %s dòng", wh_count)

    checks_passed = 0
    checks_total = 0

    # CHECK 1: Warehouse không rỗng
    checks_total += 1
    actual_wh_count = hook.get_first(f"SELECT COUNT(*) FROM {WAREHOUSE_TABLE}")[0]
    if actual_wh_count > 0:
        logger.info("✅ CHECK 1 PASSED: Warehouse có %d dòng (> 0)", actual_wh_count)
        checks_passed += 1
    else:
        logger.error("❌ CHECK 1 FAILED: Warehouse rỗng!")

    # CHECK 2: Không có null ở cột quan trọng
    checks_total += 1
    null_check = hook.get_first(f"""
        SELECT COUNT(*) FROM {WAREHOUSE_TABLE}
        WHERE invoice_id IS NULL
           OR branch IS NULL
           OR product_line IS NULL
           OR total IS NULL
           OR date IS NULL
    """)[0]
    if null_check == 0:
        logger.info("✅ CHECK 2 PASSED: Không có null ở các cột quan trọng")
        checks_passed += 1
    else:
        logger.error("❌ CHECK 2 FAILED: Có %d dòng null ở cột quan trọng", null_check)

    # CHECK 3: Không có duplicate invoice_id
    checks_total += 1
    dup_check = hook.get_first(f"""
        SELECT COUNT(*) FROM (
            SELECT invoice_id FROM {WAREHOUSE_TABLE}
            GROUP BY invoice_id
            HAVING COUNT(*) > 1
        ) AS dups
    """)[0]
    if dup_check == 0:
        logger.info("✅ CHECK 3 PASSED: Không có invoice_id trùng lặp")
        checks_passed += 1
    else:
        logger.error("❌ CHECK 3 FAILED: Có %d invoice_id trùng lặp", dup_check)

    # CHECK 4: Total và quantity phải > 0
    checks_total += 1
    invalid_values = hook.get_first(f"""
        SELECT COUNT(*) FROM {WAREHOUSE_TABLE}
        WHERE total <= 0 OR quantity <= 0
    """)[0]
    if invalid_values == 0:
        logger.info("✅ CHECK 4 PASSED: Tất cả total và quantity > 0")
        checks_passed += 1
    else:
        logger.error("❌ CHECK 4 FAILED: Có %d dòng với total/quantity <= 0", invalid_values)

    # CHECK 5: Sample data
    checks_total += 1
    sample = hook.get_records(f"SELECT branch, COUNT(*), ROUND(SUM(total)::numeric, 2) FROM {WAREHOUSE_TABLE} GROUP BY branch ORDER BY branch")
    logger.info("📊 Doanh thu theo chi nhánh:")
    for row in sample:
        logger.info("   Branch %s: %d giao dịch, tổng doanh thu: %s", row[0], row[1], row[2])
    checks_passed += 1
    logger.info("✅ CHECK 5 PASSED: Sample data hợp lệ")

    # Kết quả tổng hợp
    logger.info("=" * 40)
    logger.info("📊 KẾT QUẢ: %d/%d checks passed", checks_passed, checks_total)
    logger.info("=" * 40)

    if checks_passed < checks_total:
        raise ValueError(f"Data quality check FAILED: {checks_passed}/{checks_total} passed")

    logger.info("🎉 TẤT CẢ DATA QUALITY CHECKS ĐỀU PASSED!")

    elapsed = round(time.time() - start_time, 2)
    logger.info("⏱️  Task hoàn thành trong %s giây", elapsed)


# ============================================================
# ĐỊNH NGHĨA DAG
# ============================================================
with DAG(
    dag_id='supermarket_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline cho dữ liệu Supermarket Sales - MindX DPA Final Project',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'supermarket', 'mindx'],
) as dag:

    task_create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )

    task_load_raw = PythonOperator(
        task_id='load_csv_to_raw',
        python_callable=load_csv_to_raw,
    )

    task_clean = PythonOperator(
        task_id='clean_validate',
        python_callable=clean_validate,
    )

    task_load_wh = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse,
    )

    task_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )

    # Pipeline Flow
    task_create_tables >> task_load_raw >> task_clean >> task_load_wh >> task_quality_check