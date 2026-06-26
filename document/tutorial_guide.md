# GIÁO TRÌNH HƯỚNG DẪN CHI TIẾT DỰ ÁN CUỐI KỲ DPA
## Hệ Thống Pipeline ETL Tự Động Hóa Dữ Liệu Bán Hàng Siêu Thị (Supermarket Sales)
---

Chào bạn! Tài liệu này được biên soạn dành riêng cho bạn với mục tiêu giúp bạn nắm rõ từ gốc rễ: **Chúng ta đang làm cái gì? Tại sao phải làm thế? Từng dòng code hoạt động ra sao? Và làm sao để thuyết trình thuyết phục nhất trước hội đồng chấm điểm.**

Hãy đọc kỹ từng chương dưới đây. Bạn có thể mở trực tiếp tài liệu này ra hoặc in ra để chuẩn bị cho buổi bảo vệ.

---

## MỤC LỤC
* **CHƯƠNG 1**: Bản chất dự án - Chúng ta đang làm gì và tại sao cần làm thế?
* **CHƯƠNG 2**: Các khái niệm cốt lõi (ETL, Data Warehouse, Orchestration)
* **CHƯƠNG 3**: Cấu trúc thư mục dự án và vai trò của từng File
* **CHƯƠNG 4**: Giải thích chi tiết mã nguồn DAG (supermarket_etl.py)
* **CHƯƠNG 5**: Giải thích hệ thống Docker và Cơ sở dữ liệu PostgreSQL
* **CHƯƠNG 6**: Hướng dẫn vận hành và kịch bản thuyết trình mẫu (Từng bước demo)

---

## CHƯƠNG 1: BẢN CHẤT DỰ ÁN - CHÚNG TA ĐANG LÀM GÌ VÀ TẠI SAO CẦN LÀM THẾ?

### 1. Vấn đề thực tế của doanh nghiệp
Hãy tưởng tượng bạn là Giám đốc Công nghệ (CTO) hoặc Kỹ sư dữ liệu (Data Engineer) của một chuỗi siêu thị lớn có 3 chi nhánh tại Yangon, Naypyitaw và Mandalay. 
Mỗi ngày, các hệ thống máy tính tại quầy thu ngân ghi nhận hàng ngàn giao dịch bán hàng và xuất ra các file dữ liệu dạng thô (CSV). 

Nếu bạn muốn trả lời các câu hỏi kinh doanh như:
* *Chi nhánh nào đang có doanh thu cao nhất?*
* *Nhóm sản phẩm nào bán chạy nhất?*
* *Khách hàng thành viên (Member) chi tiêu nhiều hơn khách hàng thường (Normal) bao nhiêu?*

Bạn không thể bắt các nhà phân tích dữ liệu (Data Analyst) mở thủ công từng file Excel thô ra rồi tự tính toán bằng tay hay viết các hàm Excel phức tạp mỗi ngày được. Dữ liệu thô thường có nhiều lỗi: dòng trống (null), mã hóa đơn trùng lặp, số lượng nhập âm do lỗi hệ thống gõ nhầm của thu ngân... 

### 2. Yêu cầu của đề bài và giải pháp của chúng ta
Đề bài yêu cầu xây dựng một **Pipeline Airflow hoàn chỉnh chạy trên Docker** để thực hiện:
1. **Load dữ liệu thô** từ file CSV vào một bảng tạm (bảng Raw).
2. **Làm sạch và kiểm tra (Clean/Validate)** dữ liệu để loại bỏ lỗi.
3. **Lưu trữ dữ liệu sạch** vào kho dữ liệu tập trung (Data Warehouse).
4. **Tự động kiểm tra chất lượng dữ liệu** (Data Quality Checks) và ghi nhật ký hoạt động (Logging).
5. Đảm bảo toàn bộ hệ thống này khởi chạy tự động chỉ bằng **Docker**.

**Giải pháp của chúng ta:**
Chúng ta xây dựng một hệ thống ETL tự động hóa hoàn toàn. Thay vì làm thủ công, hệ thống này được thiết lập lịch trình (ví dụ: chạy vào lúc 12h đêm hàng ngày). Nó tự động quét thư mục chứa file bán hàng của ngày hôm đó, nạp vào hệ thống, làm sạch các dòng lỗi, đẩy vào cơ sở dữ liệu phân tích tập trung, tự động kiểm tra xem dữ liệu nạp vào có bị lỗi hay thiếu không, và gửi báo cáo về trạng thái chạy (Thành công hay Thất bại).

---

## CHƯƠNG 2: CÁC KHÁI NIỆM CỐT LÕI (DÀNH CHO NGƯỜI MỚI)

Để trả lời trôi chảy các câu hỏi chất vấn của thầy cô, bạn cần nắm rõ 4 khái niệm sau:

### 1. Data Pipeline (Đường ống dữ liệu)
Là một chuỗi các bước xử lý dữ liệu liên tiếp. Dữ liệu đi vào đầu đường ống ở dạng thô (Raw) và đi ra ở cuối đường ống dưới dạng thông tin có ích, đã được làm sạch và sẵn sàng để phân tích.

### 2. ETL (Extract - Transform - Load)
Đây là quy trình kinh điển trong kỹ nghệ dữ liệu:
* **Extract (Trích xuất)**: Lấy dữ liệu từ các nguồn khác nhau. Trong dự án của chúng ta, nguồn dữ liệu là file `supermarket_sales.csv`.
* **Transform (Biến đổi/Làm sạch)**: Lọc bỏ dòng lỗi, định dạng lại định dạng ngày/giờ, tính toán thêm các chỉ số phụ.
* **Load (Tải lên)**: Ghi dữ liệu đã được làm sạch vào kho lưu trữ cuối cùng (PostgreSQL Data Warehouse) để lưu trữ lâu dài.

### 3. Data Warehouse (Kho dữ liệu)
Khác với các cơ sở dữ liệu giao dịch hàng ngày (OLTP) vốn liên tục cập nhật/xóa từng dòng hóa đơn khi khách mua hàng, **Data Warehouse** là một cơ sở dữ liệu được tối ưu hóa cho mục đích phân tích (OLAP). Dữ liệu trong Warehouse là dữ liệu lịch sử, mang tính nhất quán cao, cực kỳ sạch sẽ và được tổ chức tốt để phục vụ việc chạy các báo cáo thống kê phức tạp với tốc độ nhanh nhất.

### 4. Orchestration & Workflow Management (Quản lý luồng công việc)
Khi hệ thống có hàng chục hay hàng trăm bước xử lý dữ liệu phụ thuộc lẫn nhau (ví dụ: Bước B chỉ được chạy khi Bước A hoàn thành thành công), ta cần một công cụ điều phối. **Apache Airflow** chính là "nhạc trưởng" làm nhiệm vụ điều phối này. Nó quản lý các công việc dưới dạng một **DAG** (Directed Acyclic Graph - Đồ thị định hướng không chu trình).

---

## CHƯƠNG 3: CẤU TRÚC THƯ MỤC DỰ ÁN VÀ VAI TRÒ CỦA TỪNG FILE

Khi trình bày dự án, bạn nên mở cấu trúc thư mục ra và giải thích ngắn gọn vai trò của từng thành phần:

```
mindx_data_pipeline/
├── dags/
│   └── supermarket_etl.py      # Bộ não điều khiển: File Python định nghĩa DAG Airflow và logic ETL
├── data/
│   └── supermarket_sales.csv   # File dữ liệu bán hàng thực tế (CSV nguồn)
├── sql/
│   └── sample_queries.sql       # Các câu lệnh SQL mẫu để truy vấn kiểm tra dữ liệu sau khi chạy
├── docker-compose.yaml          # File cấu hình Docker: Khởi chạy tất cả các dịch vụ (Airflow, Postgres, Redis)
├── Dockerfile                   # File xây dựng môi trường: Tạo một hệ điều hành thu nhỏ chứa đầy đủ thư viện Python cần thiết
├── requirements.txt             # Danh sách thư viện Python cần cài đặt (pandas, sqlalchemy)
├── .env                         # Lưu trữ các tham số hệ thống cấu hình cho Docker
└── .gitignore                   # Quy định những thư mục/file rác nào không đẩy lên GitHub
```

### Chi tiết vai trò của các file chính:
* **`Dockerfile`**: Do hình ảnh Airflow mặc định từ Internet không cài sẵn thư viện xử lý dữ liệu nâng cao, file này hướng dẫn Docker cài thêm `pandas` và `sqlalchemy` ngay từ lúc khởi động.
* **`docker-compose.yaml`**: Đây là bản vẽ kiến trúc. Nó bảo Docker tạo ra 6 máy ảo container chạy cùng lúc:
  1. `postgres`: Cơ sở dữ liệu PostgreSQL lưu trữ dữ liệu bán hàng của ta.
  2. `redis`: Bộ nhớ đệm trung gian quản lý hàng đợi các công việc xử lý dữ liệu.
  3. `airflow-webserver`: Cung cấp giao diện đồ họa trực quan trên trình duyệt (cổng 8080).
  4. `airflow-scheduler`: Bộ não giám sát thời gian chạy và ra lệnh thực thi các task.
  5. `airflow-worker`: Người công nhân thực sự chạy code Python trong DAG.
  6. `airflow-triggerer`: Hỗ trợ chạy các tác vụ bất đồng bộ dài hạn.

---

## CHƯƠNG 4: GIẢI THÍCH CHI TIẾT MÃ NGUỒN DAG (supermarket_etl.py)

Đây là file quan trọng nhất của toàn bộ dự án. Thầy cô sẽ yêu cầu bạn mở file này và giải thích code. Dưới đây là phân tích chi tiết từng khối code:

### 1. Phần khai báo Thư viện và Cấu hình
```python
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
```
* **`pandas`**: Thư viện mạnh mẽ nhất của Python dùng để thao tác với bảng dữ liệu (đọc CSV, lọc dữ liệu, xóa dòng trống...).
* **`PostgresHook`**: Công cụ của Airflow giúp kết nối an toàn với cơ sở dữ liệu PostgreSQL mà không cần lộ mật khẩu trong code.
* **`PythonOperator`**: Một công cụ của Airflow dùng để biến các hàm Python thông thường thành các Task (nhiệm vụ) chạy tuần tự trong pipeline.

```python
POSTGRES_CONN_ID = 'my_postgres_db'
RAW_TABLE = 'raw_supermarket_sales'
WAREHOUSE_TABLE = 'warehouse_supermarket_sales'
DATA_FILE = '/opt/airflow/data/supermarket_sales.csv'
```
* Các biến cấu hình định nghĩa tên kết nối cơ sở dữ liệu, tên bảng chứa dữ liệu thô (`RAW_TABLE`), bảng kho lưu trữ cuối (`WAREHOUSE_TABLE`), và đường dẫn đến file CSV trong Docker container.

### 2. Hàm sửa lỗi Kết nối (get_postgres_engine)
```python
def get_postgres_engine():
    """Tạo SQLAlchemy engine trực tiếp từ connection config, tránh lỗi option __extra__."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_connection(POSTGRES_CONN_ID)
    login = urllib.parse.quote_plus(conn.login) if conn.login else ""
    password = urllib.parse.quote_plus(conn.password) if conn.password else ""
    conn_uri = f"postgresql+psycopg2://{login}:{password}@{conn.host}:{conn.port}/{conn.schema or ''}"
    return create_engine(conn_uri)
```
* **Tại sao cần hàm này?** Trong Airflow 2.8, kết nối PostgreSQL mặc định đôi khi tự thêm các tham số lạ (`__extra__`) khiến driver kết nối Python bị crash. Hàm này trích xuất thủ công các thông tin cơ bản: Tên đăng nhập (`login`), mật khẩu (`password`), máy chủ (`host`), cổng (`port`), tên database (`schema`) để tạo ra một đường dẫn kết nối sạch sẽ, đảm bảo pipeline chạy ổn định 100%.

---

### 3. Task 1: Khởi tạo các Bảng (create_tables)
```python
def create_tables(**kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # Câu lệnh SQL tạo bảng Raw và bảng Warehouse...
```
* **Bảng thô (raw_supermarket_sales)**: Định nghĩa tất cả các cột ở dạng dữ liệu cơ bản như `TEXT` để nạp dữ liệu vào nhanh nhất mà không lo bị lỗi sai định dạng ngày tháng hay số học.
* **Bảng Warehouse (warehouse_supermarket_sales)**: Định nghĩa các ràng buộc chặt chẽ hơn như:
  * `invoice_id TEXT PRIMARY KEY`: Đảm bảo mã hóa đơn không được trùng lặp và không được trống.
  * Các trường tiền tệ, số lượng chuyển thành `NUMERIC`, `INTEGER NOT NULL` để đảm bảo tính toàn vẹn của dữ liệu số học.
  * `date DATE NOT NULL`: Ép kiểu ngày tháng đúng chuẩn.
  * `loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`: Tự động lưu thời gian dữ liệu được nạp vào kho.

---

### 4. Task 2: Trích xuất và Nạp dữ liệu thô (load_csv_to_raw)
```python
def load_csv_to_raw(**kwargs):
    # Đọc file CSV
    df = pd.read_csv(DATA_FILE)
    
    # Chuẩn hóa tên cột
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('%', '').str.replace('-', '_')
    df = df.rename(columns={'tax_5_': 'tax_5'})
    
    # Ghi vào raw table
    df.to_sql(RAW_TABLE, con=engine, if_exists='append', index=False)
```
* **Ý nghĩa**: Đọc file CSV gốc vào một đối tượng bảng trong Python (DataFrame). Chuẩn hóa tên cột (ví dụ: `Invoice ID` chuyển thành `invoice_id`, loại bỏ các ký tự trống và ký hiệu đặc biệt `%`, `-` để tránh lỗi khi thao tác trong cơ sở dữ liệu).
* Lệnh `to_sql` dùng để ghi toàn bộ dữ liệu thô vừa đọc được vào cơ sở dữ liệu PostgreSQL (bảng `raw_supermarket_sales`).
* Hàm `ti.xcom_push` lưu lại số dòng đã nạp để chuyển tiếp thông tin này cho các task sau kiểm tra (cơ chế truyền tin nội bộ XCom của Airflow).

---

### 5. Task 3: Làm sạch và Validate dữ liệu (clean_validate)
```python
def clean_validate(**kwargs):
    # Đọc dữ liệu từ raw table
    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", con=engine)
    
    # Loại bỏ bản ghi trùng lặp
    df = df.drop_duplicates(subset=['invoice_id'], keep='first')
    
    # Loại bỏ null ở các cột quan trọng
    df = df.dropna(subset=['invoice_id', 'branch', 'product_line', 'total', 'date'])
    
    # Validate giá trị dương
    df = df[(df['quantity'] > 0) & (df['total'] > 0)]
    
    # Chuyển đổi kiểu dữ liệu
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    # Lưu vào staging table
    df.to_sql('staging_supermarket_sales', con=engine, if_exists='replace', index=False)
```
* **Bước lọc trùng lặp**: Loại bỏ các dòng hóa đơn có cùng `invoice_id`, chỉ giữ lại dòng đầu tiên xuất hiện.
* **Bước lọc Null**: Dùng hàm `dropna()` loại bỏ các dòng bị trống thông tin ở các trường cốt lõi.
* **Bước kiểm tra logic số học**: Loại bỏ các dòng có số lượng mua hàng (`quantity`) hoặc tổng tiền hóa đơn (`total`) nhỏ hơn hoặc bằng 0 (lỗi hệ thống).
* **Định dạng dữ liệu**: Chuyển cột ngày tháng từ dạng chữ (`TEXT`) sang chuẩn ngày tháng thực tế để có thể sắp xếp hoặc lọc theo thời gian sau này.
* Lưu dữ liệu đã làm sạch vào một bảng tạm trung gian gọi là **Staging Table** (`staging_supermarket_sales`).

---

### 6. Task 4: Tải dữ liệu vào Kho (load_to_warehouse)
```python
def load_to_warehouse(**kwargs):
    df = pd.read_sql("SELECT * FROM staging_supermarket_sales", con=engine)
    df.to_sql(WAREHOUSE_TABLE, con=engine, if_exists='append', index=False)
    # Xóa bảng staging tạm thời...
```
* Đọc dữ liệu cực kỳ sạch sẽ từ bảng Staging và ghi đè/nạp nối đuôi vào bảng lưu trữ chính của kho dữ liệu (`warehouse_supermarket_sales`). 
* Dọn dẹp hệ thống bằng cách xóa bảng staging tạm thời nhằm tối ưu hóa dung lượng database.

---

### 7. Task 5: Kiểm định Chất lượng Dữ liệu (data_quality_check)
```python
def data_quality_check(**kwargs):
    # Kiểm tra 1: Số dòng trong Warehouse > 0
    # Kiểm tra 2: Không có dòng nào bị Null ở cột quan trọng
    # Kiểm tra 3: Không có invoice_id bị lặp lại
    # Kiểm tra 4: Đảm bảo toàn bộ tổng tiền hóa đơn > 0
    # Kiểm tra 5: Truy vấn in ra doanh thu theo từng chi nhánh để báo cáo
```
* **Tại sao cần bước này?** Trong một hệ thống xử lý dữ liệu lớn, việc hệ thống chạy báo thành công (Success) không đồng nghĩa với việc dữ liệu nạp vào là chính xác. Task này đóng vai trò chốt chặn cuối cùng. Nếu phát hiện dữ liệu vi phạm bất kỳ quy tắc chất lượng nào (ví dụ: kho dữ liệu bị trống rỗng, hoặc có mã hóa đơn bị lặp), task này sẽ tự động ném ra lỗi (Raise Error) để dừng pipeline và đánh dấu trạng thái lỗi (Failed), cảnh báo cho kỹ sư dữ liệu xử lý kịp thời.

---

### 8. Định nghĩa luồng chạy (Workflow Flow)
```python
task_create_tables >> task_load_raw >> task_clean >> task_load_wh >> task_quality_check
```
* Ký hiệu `>>` định nghĩa thứ tự chạy tuần tự của các tác vụ:
  Tạo bảng xong mới nạp dữ ➔ Nạp dữ liệu xong mới làm sạch ➔ Làm sạch xong mới nạp vào kho ➔ Cuối cùng là chạy kiểm tra chất lượng.

---

## CHƯƠNG 5: HỆ THỐNG DOCKER VÀ CƠ SỞ DỮ LIỆU POSTGRESQL

Một trong những câu hỏi thầy cô hay hỏi nhất là: **"Nhìn bảng dữ liệu ở đâu?"** hoặc **"Airflow lưu trữ dữ liệu ở chỗ nào?"**. Bạn cần hiểu rõ bản chất sau để trả lời:

### 1. Airflow UI không hiển thị bảng dữ liệu!
Airflow **chỉ là nhạc trưởng điều phối**. Giao diện đồ họa của Airflow (cổng 8080) chỉ hiển thị trạng thái chạy của các task (xanh lá cây là thành công, đỏ là thất bại, cam là đang thử lại), thời gian chạy, sơ đồ thiết kế pipeline và nhật ký xử lý dữ liệu (Logs). **Nó không phải là một công cụ xem bảng dữ liệu.**

### 2. Dữ liệu thực tế nằm trong PostgreSQL!
PostgreSQL là hệ quản trị cơ sở dữ liệu được khởi chạy trong một container Docker độc lập (tên dịch vụ là `postgres`).
Để xem dữ liệu thực tế đã được nạp vào, chúng ta phải kết nối và truy vấn trực tiếp vào database này.

### 3. Cách truy cập xem dữ liệu
Trong buổi thuyết trình, bạn có thể thực hiện 1 trong 2 cách sau để trình diễn dữ liệu cho thầy xem:

#### Cách 1: Sử dụng Terminal (Nhanh và thể hiện trình độ chuyên môn)
Mở một cửa sổ Terminal (PowerShell hoặc Command Prompt) trên máy tính của bạn và gõ dòng lệnh sau để truy cập trực tiếp vào cơ sở dữ liệu bên trong container:

```bash
docker compose exec postgres psql -U airflow -d airflow
```
* Giải thích câu lệnh cho thầy: *"Lệnh này yêu cầu Docker cho phép chúng ta chạy trình quản lý cơ sở dữ liệu `psql` dưới tên tài khoản `airflow` (`-U airflow`) trong database tên là `airflow` (`-d airflow`) nằm ở container tên là `postgres`"*.

Sau khi ấn Enter, bạn sẽ thấy giao diện dòng lệnh của Postgres xuất hiện (`airflow=#`). Tại đây bạn có thể gõ các câu lệnh SQL để hiển thị dữ liệu:
* Xem danh sách các bảng đang có: `\dt`
* Xem 5 dòng dữ liệu đầu tiên trong kho: 
  ```sql
  SELECT invoice_id, branch, city, total, date FROM warehouse_supermarket_sales LIMIT 5;
  ```
* Thoát ra ngoài terminal: `\q` hoặc gõ `exit`.

#### Cách 2: Kết nối bằng các công cụ đồ họa (DBeaver, pgAdmin)
Nếu bạn có cài đặt các phần mềm quản lý database như DBeaver hoặc pgAdmin trên máy tính:
1. Tạo một kết nối mới định dạng **PostgreSQL**.
2. Nhập thông tin kết nối:
   * **Host**: `localhost` (vì Docker đang map cổng ra máy thật của bạn)
   * **Port**: `5432`
   * **Database**: `airflow`
   * **Username**: `airflow`
   * **Password**: `airflow`
3. Kết nối thành công, bạn sẽ thấy cấu trúc thư mục chứa các bảng `raw_supermarket_sales` và `warehouse_supermarket_sales` hiện ra trực quan như Excel.

---

## CHƯƠNG 6: KỊCH BẢN THUYẾT TRÌNH VÀ DEMO MẪU (TỪNG BƯỚC)

Dưới đây là kịch bản thuyết trình chi tiết từng bước mà bạn có thể nói theo để tạo ấn tượng mạnh mẽ với giáo viên hướng dẫn và hội đồng chấm điểm:

### Bước 1: Giới thiệu dự án (1 - 2 phút)
> *"Kính thưa các thầy cô, em xin phép trình bày dự án cuối khóa học MindX DPA của mình. Đề tài của em là xây dựng một pipeline ETL hoàn chỉnh để tự động hóa việc thu thập, làm sạch và lưu trữ dữ liệu bán hàng của chuỗi siêu thị Supermarket Sales chạy trên môi trường Docker. Mục tiêu của hệ thống này là giải phóng sức lao động thủ công của doanh nghiệp, tự động hóa toàn bộ việc xử lý các file dữ liệu thô phát sinh hàng ngày và cung cấp một kho dữ liệu sạch, đáng tin cậy phục vụ cho việc phân tích kinh doanh."*

### Bước 2: Giới thiệu Kiến trúc & Công nghệ (1 - 2 phút)
> *"Hệ thống của em được vận hành hoàn toàn trên nền tảng Docker Container để đảm bảo hệ thống có thể triển khai trên bất kỳ máy chủ nào mà không lo bị lỗi tương thích môi trường. Hệ thống bao gồm 3 thành phần công nghệ chính:*
> 1. *Apache Airflow: Dùng làm công cụ điều phối, lên lịch và giám sát luồng dữ liệu.*
> 2. *PostgreSQL: Đóng vai trò là hệ quản trị cơ sở dữ liệu lưu trữ kho dữ liệu (Data Warehouse).*
> 3. *Pandas & Python: Thư viện lập trình được sử dụng để lọc trùng, xử lý giá trị trống (null) và làm sạch dữ liệu thô."*

### Bước 3: Hướng dẫn Khởi chạy Hệ thống (Demo thực tế)
*(Bạn mở terminal tại thư mục dự án và bắt đầu gõ lệnh)*

> *"Để khởi chạy hệ thống, em chỉ cần chạy duy nhất một dòng lệnh của Docker Compose:*
> ```bash
> docker compose up -d
> ```
> *Docker sẽ tự động build image và khởi động các dịch vụ ngầm."*

*(Bạn mở trình duyệt và truy cập địa chỉ `http://localhost:8080`)*

> *"Sau khi hệ thống khởi động xong, em truy cập vào giao diện Web UI trực quan của Airflow tại cổng 8080. Như thầy cô có thể thấy, giao diện quản lý rất gọn gàng và không có các DAG ví dụ làm rối mắt nhờ việc em đã tắt cấu hình load examples trong cấu hình Docker."*

---

### Bước 4: Demo chạy thử Pipeline
*(Bạn chỉ vào kết nối trong Connection)*

> *"Trước khi chạy, em đã cấu hình một kết nối có tên là `my_postgres_db` thuộc loại Postgres để Airflow có thể giao tiếp trực tiếp với cơ sở dữ liệu lưu trữ."*

*(Bạn bật ON DAG và click **Trigger DAG (▶)**)*

> *"Bây giờ em xin phép kích hoạt chạy thử pipeline thủ công. Luồng xử lý dữ liệu của em bao gồm 5 bước cụ thể:*
> 1. **create_tables**: *Hệ thống tự động kiểm tra và khởi tạo các cấu trúc bảng cần thiết.*
> 2. **load_csv_to_raw**: *Đọc file CSV nguồn từ thư mục dữ liệu, chuẩn hóa các tiêu đề cột sang viết thường để tránh lỗi chính tả và nạp toàn bộ 1000 dòng dữ liệu gốc vào bảng tạm raw.*
> 3. **clean_validate**: *Đọc dữ liệu thô từ bảng raw, tiến hành lọc bỏ các hóa đơn trùng lặp, lọc các dòng trống (null) ở cột khóa chính, kiểm tra điều kiện logic nếu số lượng mua hoặc tổng tiền bé hơn hoặc bằng 0 thì loại bỏ ngay, sau đó lưu tạm vào staging.*
> 4. **load_to_warehouse**: *Đẩy toàn bộ dữ liệu cực kỳ sạch từ staging vào Data Warehouse chính thức và dọn dẹp bảng staging tạm.*
> 5. **data_quality_check**: *Đây là chốt chặn cuối cùng kiểm tra tự động xem dữ liệu ghi vào kho có bị trống không, có bị trùng lặp khóa chính không, và in ra một số thống kê doanh thu cơ bản.*
>
> *Như thầy cô thấy, cả 5 vòng tròn task đều đã chuyển sang màu xanh lá cây, báo hiệu toàn bộ pipeline đã chạy thành công tốt đẹp."*

---

### Bước 5: Trình diễn Kết quả truy vấn dữ liệu (Query Data)
*(Bạn mở Terminal và gõ lệnh truy cập Postgres)*

> *"Để chứng minh dữ liệu đã vào kho thành công và sạch sẽ, em xin phép truy cập trực tiếp vào container PostgreSQL để chạy các câu lệnh SQL truy vấn:"*
> ```bash
> docker compose exec postgres psql -U airflow -d airflow
> ```

*(Bạn chạy câu lệnh kiểm tra số dòng)*
> ```sql
> SELECT 'raw_supermarket_sales' AS table_name, COUNT(*) AS row_count FROM raw_supermarket_sales
> UNION ALL
> SELECT 'warehouse_supermarket_sales', COUNT(*) FROM warehouse_supermarket_sales;
> ```
> *Kết quả cho thấy cả bảng thô và bảng kho lưu trữ đều ghi nhận đủ 1000 dòng dữ liệu sạch.*

*(Bạn chạy câu lệnh tính doanh thu theo chi nhánh)*
> ```sql
> SELECT branch, city, COUNT(*) AS total_transactions, ROUND(SUM(total)::numeric, 2) AS total_revenue FROM warehouse_supermarket_sales GROUP BY branch, city ORDER BY total_revenue DESC;
> ```
> *Đây là bảng kết quả tổng hợp doanh thu giúp doanh nghiệp đưa ra quyết định: Chi nhánh C tại Yangon đang mang lại doanh thu cao nhất với hơn 110,568.71 USD trên tổng số 328 giao dịch.*

---

### Bước 6: Kết luận và Hỏi đáp (1 phút)
> *"Hệ thống pipeline này hoàn toàn có thể chạy tự động theo lịch đặt trước (ví dụ chạy hàng ngày lúc nửa đêm). Nó giúp tự động hóa hoàn toàn quy trình xử lý dữ liệu cho doanh nghiệp, đảm bảo dữ liệu luôn chính xác và nhất quán. Em xin phép hoàn thành phần trình bày của mình tại đây và rất mong nhận được những câu hỏi góp ý từ các thầy cô trong hội đồng. Em xin chân thành cảm ơn!"*

---
Chúc bạn học tập thật tốt và tự tin đạt điểm số cao nhất với dự án này!
