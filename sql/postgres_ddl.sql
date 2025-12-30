-- ============================================================================
-- SCRIPT SQL - E-COMMERCE DATABASE
-- ============================================================================
-- Descricao: Criacao de tabelas e queries de analise
-- Database: PostgreSQL
-- Autor: Data Engineer Challenge
-- ============================================================================

-- ============================================================================
-- PARTE 1: DDL - CRIACAO DAS TABELAS
-- ============================================================================

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

-- Comentarios
COMMENT ON TABLE orders IS 'Tabela de pedidos/transacoes';
COMMENT ON COLUMN orders.order_id IS 'Identificador unico do pedido';
COMMENT ON COLUMN orders.customer_id IS 'Referencia ao cliente (FK)';
COMMENT ON COLUMN orders.amount IS 'Valor do pedido';
COMMENT ON COLUMN orders.rating IS 'Avaliacao do cliente (1-5)';


-- ============================================================================
-- PARTE 2: VERIFICACAO DA ESTRUTURA
-- ============================================================================

-- Verifica tabelas criadas
SELECT 
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'public'
    AND table_name IN ('customers', 'orders')
ORDER BY table_name;

-- Verifica colunas da tabela customers
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'customers'
ORDER BY ordinal_position;

-- Verifica colunas da tabela orders
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'orders'
ORDER BY ordinal_position;

-- Verifica constraints (PKs, FKs, Checks)
SELECT
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'public'
    AND tc.table_name IN ('customers', 'orders')
ORDER BY tc.table_name, tc.constraint_type;


-- ============================================================================
-- PARTE 3: QUERIES DE ANALISE (CONFORME DESAFIO)
-- ============================================================================

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


-- ============================================================================
-- PARTE 4: QUERIES ADICIONAIS (ANALISES EXTRAS)
-- ============================================================================

-- Analise de Clientes sem Pedidos
SELECT 
    customer_id,
    name,
    email,
    registration_date,
    EXTRACT(DAY FROM CURRENT_TIMESTAMP - registration_date) AS days_since_registration
FROM customers
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders)
ORDER BY registration_date DESC;


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


-- Pedidos por Forma de Pagamento
SELECT 
    payment_method,
    COUNT(*) AS num_orders,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    ROUND(CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS NUMERIC), 2) AS percentage
FROM orders
GROUP BY payment_method
ORDER BY num_orders DESC;


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


-- ============================================================================
-- PARTE 5: VIEWS UTEIS PARA DASHBOARDS
-- ============================================================================

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


-- ============================================================================
-- PARTE 6: FUNCOES UTEIS
-- ============================================================================

-- Funcao: Calcula CLV (Customer Lifetime Value) de um cliente
CREATE OR REPLACE FUNCTION calculate_clv(p_customer_id VARCHAR)
RETURNS NUMERIC AS $$
DECLARE
    v_clv NUMERIC;
BEGIN
    SELECT COALESCE(SUM(amount), 0)
    INTO v_clv
    FROM orders
    WHERE customer_id = p_customer_id;
    
    RETURN v_clv;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- FIM DO SCRIPT
-- ============================================================================