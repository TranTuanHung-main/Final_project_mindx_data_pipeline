# 🛒 Supermarket Sales ETL Pipeline

> **MindX DPA - Final Project**
> Xây dựng pipeline Airflow hoàn chỉnh chạy trên Docker

## 📋 Mô tả dự án

Pipeline ETL (Extract - Transform - Load) tự động xử lý dữ liệu bán hàng siêu thị:
- **Extract**: Đọc dữ liệu từ file CSV
- **Transform**: Clean, validate, chuyển đổi kiểu dữ liệu
- **Load**: Lưu vào PostgreSQL Data Warehouse
- **Quality Check**: Kiểm tra chất lượng dữ liệu tự động

## 📁 Cấu trúc thư mục

```
mindx_data_pipeline/
├── dags/
│   └── supermarket_etl.py      # DAG chính - pipeline ETL
├── data/
│   └── supermarket_sales.csv   # File dữ liệu nguồn
├── sql/
│   └── sample_queries.sql       # Các query mẫu để kiểm tra kết quả
├── config/                      # Airflow config (nếu cần)
├── logs/                        # Airflow logs (tự sinh)
├── plugins/                     # Airflow plugins (nếu cần)
├── docker-compose.yaml          # Cấu hình Docker services
├── Dockerfile                   # Custom Airflow image
├── requirements.txt             # Python dependencies
├── .env                         # Biến môi trường
├── .gitignore                   # Git ignore rules
└── README.md                    # Tài liệu dự án
```

## 🔄 Pipeline Workflow

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  create_tables   │────▶│ load_csv_to_raw  │────▶│  clean_validate  │
│                  │     │                  │     │                  │
│ Tạo raw table &  │     │ Đọc CSV file     │     │ - Drop nulls     │
│ warehouse table  │     │ Load vào raw     │     │ - Remove dupes   │
│ trong PostgreSQL │     │ table nguyên bản │     │ - Validate values│
└──────────────────┘     └──────────────────┘     │ - Convert types  │
                                                  └────────┬─────────┘
                                                           │
                         ┌──────────────────┐     ┌────────▼─────────┐
                         │ data_quality_    │◀────│ load_to_         │
                         │ check            │     │ warehouse        │
                         │                  │     │                  │
                         │ - Row count > 0  │     │ Load dữ liệu    │
                         │ - No nulls       │     │ sạch vào         │
                         │ - No duplicates  │     │ warehouse table  │
                         │ - Valid values   │     │                  │
                         └──────────────────┘     └──────────────────┘
```

## 🚀 Hướng dẫn chạy

### Bước 1: Khởi động Docker Desktop

Đảm bảo Docker Desktop đang chạy trên máy.

### Bước 2: Build và khởi động Airflow

```bash
# Build custom image
docker compose build

# Khởi chạy tất cả services
docker compose up -d

# Kiểm tra trạng thái
docker compose ps
```

### Bước 3: Truy cập Airflow UI

- URL: **http://localhost:8080**
- Username: `airflow`
- Password: `airflow`

### Bước 4: Tạo Postgres Connection

Trong Airflow UI:
1. Vào **Admin** → **Connections**
2. Click **+** (Add a new record)
3. Điền thông tin:
   - **Connection Id**: `my_postgres_db`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Schema**: `airflow`
   - **Login**: `airflow`
   - **Password**: `airflow`
   - **Port**: `5432`
4. Click **Save**

### Bước 5: Trigger DAG

1. Tìm DAG `supermarket_etl_pipeline` trong danh sách
2. Bật toggle **ON** (unpause)
3. Click **Trigger DAG** (nút play ▶)
4. Theo dõi các task chạy trong **Graph View**

### Bước 6: Kiểm tra kết quả

```bash
# Truy cập PostgreSQL trong container
docker compose exec postgres psql -U airflow -d airflow

# Chạy query kiểm tra
SELECT COUNT(*) FROM raw_supermarket_sales;
SELECT COUNT(*) FROM warehouse_supermarket_sales;

# Xem sample data
SELECT * FROM warehouse_supermarket_sales LIMIT 5;
```

## 📊 Dữ liệu

**Dataset**: Supermarket Sales (từ Data Bank)

| Cột | Mô tả |
|-----|--------|
| invoice_id | Mã hóa đơn |
| branch | Chi nhánh (A, B, C) |
| city | Thành phố |
| customer_type | Loại khách hàng (Member/Normal) |
| gender | Giới tính |
| product_line | Dòng sản phẩm |
| unit_price | Đơn giá |
| quantity | Số lượng |
| tax_5 | Thuế 5% |
| total | Tổng tiền |
| date | Ngày giao dịch |
| time | Giờ giao dịch |
| payment | Phương thức thanh toán |
| cogs | Giá vốn |
| gross_income | Lợi nhuận gộp |
| rating | Đánh giá khách hàng |

## 🛠️ Công nghệ sử dụng

- **Apache Airflow 2.8.1** - Orchestration
- **PostgreSQL 13** - Database
- **Docker & Docker Compose** - Containerization
- **Python 3** + pandas - Data processing
- **Redis** - Celery broker (CeleryExecutor)

## 📝 Logging

Mỗi task trong pipeline đều có logging chi tiết:
- Số dòng đọc/ghi
- Số dòng bị loại và lý do
- Thời gian xử lý từng task
- Kết quả data quality check
- Metadata được truyền giữa các task qua XCom

Xem logs tại: **Airflow UI** → Click vào task → **Logs**

## 🧹 Dừng và dọn dẹp

```bash
# Dừng tất cả services
docker compose down

# Dừng và xóa volumes (xóa database)
docker compose down -v
```
