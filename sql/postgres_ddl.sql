
-- Limpa tabelas existentes (caso ja existam)
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- ----------------------------------------------------------------------------
-- TABELA: customers
-- ----------------------------------------------------------------------------

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    email VARCHAR(200) UNIQUE NOT NULL,
    phone VARCHAR(50),
    registration_date TIMESTAMP,
    birth_date DATE,
    gender CHAR(1),
    preferred_language VARCHAR(10),
    address JSONB,
    CONSTRAINT chk_email_format CHECK (email LIKE '%@%'),
    CONSTRAINT chk_gender CHECK (gender IN ('M', 'F', 'O'))
);

-- Indices adicionais
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_registration_date ON customers(registration_date);

-- Comentarios
COMMENT ON TABLE customers IS 'Tabela de clientes da plataforma';
COMMENT ON COLUMN customers.customer_id IS 'Identificador unico do cliente';
COMMENT ON COLUMN customers.email IS 'Email do cliente (unico e normalizado)';


-- ----------------------------------------------------------------------------
-- TABELA: orders
-- ----------------------------------------------------------------------------

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_email VARCHAR(200) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(50),
    payment_method VARCHAR(50),
    rating INTEGER,
    review_date TIMESTAMP,
    CONSTRAINT chk_amount CHECK (amount > 0),
    CONSTRAINT chk_rating CHECK (rating BETWEEN 1 AND 5),
    CONSTRAINT fk_orders_customer 
        FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Indices para otimizacao de queries
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_customer_email ON orders(customer_email);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);


-- ----------------------------------------------------------------------------
-- QUERY 1: Total de ventas por cliente
-- ----------------------------------------------------------------------------

SELECT 
    c.customer_id,
    c.name AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.amount), 0) AS total_spent,
    COALESCE(AVG(o.amount), 0) AS avg_order_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_spent DESC;


-- ----------------------------------------------------------------------------
-- QUERY 2: Numero de ordenes por pais
-- ----------------------------------------------------------------------------

SELECT 
    c.address->>'country' AS country,
    COUNT(o.order_id) AS num_orders,
    SUM(o.amount) AS total_revenue,
    AVG(o.amount) AS avg_ticket,
    COUNT(DISTINCT c.customer_id) AS num_customers
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE c.address->>'country' IS NOT NULL
GROUP BY c.address->>'country'
ORDER BY num_orders DESC;


-- ----------------------------------------------------------------------------
-- QUERY 3: Ticket promedio
-- ----------------------------------------------------------------------------

SELECT 
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_ticket,
    MIN(amount) AS min_ticket,
    MAX(amount) AS max_ticket,
    STDDEV(amount) AS stddev_ticket,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_ticket
FROM orders;


-- Top 10 Clientes por Valor Gasto
SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) AS num_orders,
    SUM(o.amount) AS total_spent,
    AVG(o.rating) AS avg_rating
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 10;


-- Analise de Avaliacoes (Ratings)
SELECT 
    rating,
    COUNT(*) AS num_orders,
    ROUND(CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS NUMERIC), 2) AS percentage,
    AVG(amount) AS avg_order_value
FROM orders
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY rating DESC;


-- Evolucao Temporal de Pedidos (por mes)
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*) AS num_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_ticket,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;


-- Analise de Status dos Pedidos
SELECT 
    status,
    COUNT(*) AS num_orders,
    SUM(amount) AS total_amount,
    ROUND(CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS NUMERIC), 2) AS percentage
FROM orders
GROUP BY status
ORDER BY num_orders DESC;


-- View: Resumo de Clientes
CREATE OR REPLACE VIEW vw_customer_summary AS
SELECT 
    c.customer_id,
    c.name,
    c.email,
    c.registration_date,
    c.address->>'country' AS country,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.amount), 0) AS total_spent,
    COALESCE(AVG(o.amount), 0) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    MIN(o.order_date) AS first_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email, c.registration_date, c.address;


-- View: Metricas de Negocio (KPIs)
CREATE OR REPLACE VIEW vw_business_metrics AS
SELECT 
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT o.customer_id) AS active_customers,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_revenue,
    AVG(o.amount) AS avg_ticket,
    ROUND(CAST(COUNT(DISTINCT o.customer_id) AS NUMERIC) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 4) AS conversion_rate
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;

