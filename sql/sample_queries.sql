-- ============================================================
-- SAMPLE QUERIES - Supermarket Sales Data Warehouse
-- Dùng để demo khi thuyết trình
-- ============================================================

-- 1. Kiểm tra số lượng bản ghi raw vs warehouse
SELECT 'raw_supermarket_sales' AS table_name, COUNT(*) AS row_count
FROM raw_supermarket_sales
UNION ALL
SELECT 'warehouse_supermarket_sales', COUNT(*)
FROM warehouse_supermarket_sales;

-- 2. Tổng doanh thu theo chi nhánh (Branch)
SELECT
    branch,
    city,
    COUNT(*) AS total_transactions,
    ROUND(SUM(total)::numeric, 2) AS total_revenue,
    ROUND(AVG(total)::numeric, 2) AS avg_revenue_per_transaction
FROM warehouse_supermarket_sales
GROUP BY branch, city
ORDER BY total_revenue DESC;

-- 3. Top 5 dòng sản phẩm có doanh thu cao nhất
SELECT
    product_line,
    COUNT(*) AS total_sold,
    ROUND(SUM(total)::numeric, 2) AS total_revenue,
    ROUND(AVG(rating)::numeric, 2) AS avg_rating
FROM warehouse_supermarket_sales
GROUP BY product_line
ORDER BY total_revenue DESC
LIMIT 5;

-- 4. Phân tích theo loại khách hàng
SELECT
    customer_type,
    gender,
    COUNT(*) AS total_transactions,
    ROUND(SUM(total)::numeric, 2) AS total_spent,
    ROUND(AVG(total)::numeric, 2) AS avg_spent
FROM warehouse_supermarket_sales
GROUP BY customer_type, gender
ORDER BY total_spent DESC;

-- 5. Phân tích theo phương thức thanh toán
SELECT
    payment,
    COUNT(*) AS total_transactions,
    ROUND(SUM(total)::numeric, 2) AS total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM warehouse_supermarket_sales
GROUP BY payment
ORDER BY total_revenue DESC;

-- 6. Doanh thu theo tháng (trend)
SELECT
    TO_CHAR(date, 'YYYY-MM') AS month,
    COUNT(*) AS total_transactions,
    ROUND(SUM(total)::numeric, 2) AS monthly_revenue
FROM warehouse_supermarket_sales
GROUP BY TO_CHAR(date, 'YYYY-MM')
ORDER BY month;

-- 7. Top 10 giao dịch có giá trị cao nhất
SELECT
    invoice_id,
    branch,
    product_line,
    quantity,
    total,
    date
FROM warehouse_supermarket_sales
ORDER BY total DESC
LIMIT 10;

-- 8. Kiểm tra data quality - null values
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(invoice_id) AS null_invoice_id,
    COUNT(*) - COUNT(branch) AS null_branch,
    COUNT(*) - COUNT(total) AS null_total,
    COUNT(*) - COUNT(date) AS null_date
FROM warehouse_supermarket_sales;
