# 🛒 Projeto ETL - Plataforma de E-commerce

Pipeline de dados moderno usando arquitetura **Medallion (Bronze, Silver, Gold)** para transformar dados operacionais em insights de negócio.

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Tecnologias](#-tecnologias)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Como Executar](#-como-executar)
- [Camadas de Dados](#-camadas-de-dados)
- [Modelo de Dados](#-modelo-de-dados)
- [Consultas SQL](#-consultas-sql)
- [Decisões Técnicas](#-decisões-técnicas)
- [Logs e Monitoramento](#-logs-e-monitoramento)
- [Troubleshooting](#-troubleshooting)

---

## 🎯 Visão Geral

Este projeto implementa um pipeline ETL completo que:
- ✅ Extrai dados de múltiplas fontes JSON
- ✅ Limpa e normaliza os dados seguindo padrões de qualidade
- ✅ Modela dados em um Data Warehouse PostgreSQL
- ✅ Expõe valor através de consultas SQL analíticas
- ✅ Pode ser executado múltiplas vezes sem duplicação (idempotente)

---

## 🏗️ Arquitetura

Arquitetura Medallion**:

```
┌─────────────────────────────────────────────────────────────┐
│                    FONTES DE DADOS (JSON)                   │
│  customers | transactions | reviews | tickets | inventory   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw)                       │
│  • Ingestão sem transformação                               │
│  • Formato: Parquet                                         │
│  • Versionamento por timestamp                              │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned)                    │
│  • Limpeza e normalização                                   │
│  • Validação de qualidade                                   │
│  • Deduplicação                                             │
│  • Formato: Parquet                                         │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              GOLD LAYER (Business/Analytics)                │
│  • Modelo dimensional (PostgreSQL)                          │
│  • Tabelas: customers, orders                               │
│  • Relacionamentos (FK/PK)                                  │
│  • Pronto para consumo analítico                            │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    📊 CONSULTAS SQL                         │
│  • Total de vendas por cliente                              │
│  • Número de pedidos por país                               │
│  • Ticket médio                                             │
└─────────────────────────────────────────────────────────────┘
```

---

##  Tecnologias

| Categoria | Tecnologia | Versão | Justificativa |
|-----------|-----------|--------|---------------|
| **Linguagem** | Python | 3.8+ | Padrão da indústria para Data Engineering |
| **Banco de Dados** | PostgreSQL | 15 | RDBMS robusto, open-source |
| **Containerização** | Docker | - | Ambiente isolado e reproduzível |
| **Manipulação de Dados** | Pandas | 2.1+ | Biblioteca líder para transformação de dados |
| **ORM/Conexão** | SQLAlchemy | 2.0+ | Abstração robusta para banco de dados |
| **Formato Intermediário** | Parquet | - | Formato colunar eficiente |
| **Logs** | Python logging | - | Rastreabilidade e debugging |

---

## 📦 Pré-requisitos

### Software necessário:

- **Docker & Docker Compose** (recomendado)
  - [Instalação Docker Desktop](https://www.docker.com/products/docker-desktop/)
  
- **Python 3.8+**
  ```bash
  python --version
  ```

- **pip** (gerenciador de pacotes Python)
  ```bash
  pip --version
  ```

### Opcional:
- **Git** (para clonar o repositório)
---

## 🚀 Instalação

### 1. Clonar o repositório (ou baixar os arquivos)

```bash
git clone <url-do-repositorio>
cd PITZ-data_engineer_project
```

### 2. Configurar ambiente Python

```bash
# Criar ambiente virtual (recomendado)
python -m venv venv

# Ativar ambiente virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```

### 3. Configurar PostgreSQL com Docker

```bash
# Subir o container PostgreSQL
docker compose up -d

# Verificar se está rodando
docker ps
```

### 4. Verificar arquivo .env

O arquivo `.env` deve conter:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce
DB_USER=postgres
DB_PASSWORD=postgres
```

---

## 📁 Estrutura do Projeto

```
PITZ-data_engineer_project/
│
├── .env                                    # Configurações do banco
├── docker-compose.yml                      # Definição do container PostgreSQL
├── requirements.txt                        # Dependências Python
│
├── bronze_extraction.py                    #  Camada Bronze - Extração
├── transformations.py                #  Camada Silver - Limpeza 
├── postgres_loader.py                         #  Camada Gold - Carregamento 
│
├── postgres_ddl.sql                         # Consultas SQL de valor
├── README.md                               # Este arquivo
│
├── data/                                   # Diretório de dados 
│   ├── bronze/                                # Dados brutos em Parquet
│   ├── silver/                             # Dados limpos em Parquet
│   
```

---

##  Como Executar

### Execução Completa do Pipeline

```bash
# 1. Camada Bronze - Extração
python src/extraction.py

# 2. Camada Silver - Transformação
python src/transformations.py

# 3. Camada Gold - Carregamento no PostgreSQL
python src/postgres_loader.py

```

### Execução das Consultas SQL

```bash
# Conectar ao PostgreSQL
docker exec -it ecommerce_postgres psql -U postgres -d ecommerce

# Dentro do psql, executar as consultas do arquivo sql_queries.sql
\i sql_queries.sql

# Ou executar consultas individuais:
SELECT * FROM customers LIMIT 5;
```


---

## Camadas de Dados

### 🥉 Bronze Layer (Raw Data)

**Objetivo:** Ingestão de dados brutos sem transformação

**Características:**
- Preserva dados originais exatamente como recebidos
- Formato: Parquet (compressão eficiente)
- Versionamento por timestamp
- Serve como fonte única da verdade

**Validações:**
-  Arquivo existe e é válido JSON
-  Estrutura básica do JSON é válida
-  Não valida conteúdo dos campos

**Saída:** `data/bronze/{entity}.parquet`

---

###  Silver Layer (Cleaned Data)

**Objetivo:** Limpeza, normalização e validação de qualidade

**Transformações aplicadas:**

1. **Emails:**
   - Conversão para lowercase
   - Remoção de espaços (trim)
   - Validação de formato (`@` presente)

2. **Datas:**
   - Conversão para formato padrão ISO 8601 (YYYY-MM-DD)
   - Suporte a múltiplos formatos de entrada:
     - `2024-01-15T14:32:00Z`
     - `2024-01-15`
     - `15/01/2024`
     - `15/01/2024 14:32:00`

3. **Valores Monetários:**
   - Conversão para tipo numérico (float/decimal)
   - Remoção de caracteres especiais
   - Validação de valores negativos


5. **Relacionamentos:**
   - Vinculação de orders com customers via email
   - Validação de integridade referencial

**Critérios de Descarte:**

| Entidade | Critério de Descarte | Justificativa |
|----------|---------------------|---------------|
| **Customers** | Email inválido ou vazio | Campo obrigatório para identificação |
| **Customers** | customer_id vazio | Chave primária obrigatória |
| **Orders** | transaction_id vazio | Chave primária obrigatória |
| **Orders** | amount inválido/negativo | Valor de pedido deve ser positivo |
| **Orders** | status inválido | Apenas: completed, pending, cancelled, refunded |
| **Orders** | customer_id não encontrado | Integridade referencial |

**Saída:** `data/silver/{entity}.parquet`

---

### 🥇Gold Layer (Business/Analytics)

**Objetivo:** Modelo dimensional otimizado para análise

**Modelo de Dados:**

```sql
customers
├── customer_id (PK)
├── name
├── email (UNIQUE)
├── phone
├── registration_date
├── country
├── city
└── created_at

orders
├── transaction_id (PK)
├── customer_id (FK → customers)
├── payment_method
├── amount
├── currency
├── payment_date
├── status
└── created_at
```

**Características:**
- Chaves primárias e estrangeiras
- Índices para performance
- Constraints de integridade
- Suporta re-execução sem duplicação (UPSERT)

---

## 🗄️ Modelo de Dados

### Tabela: `customers`

| Coluna | Tipo | Restrições | Descrição |
|--------|------|-----------|-----------|
| customer_id | VARCHAR(20) | PRIMARY KEY | Identificador único do cliente |
| name | VARCHAR(200) | - | Nome completo |
| email | VARCHAR(200) | UNIQUE, NOT NULL | Email (normalizado) |
| phone | VARCHAR(50) | - | Telefone de contato |
| registration_date | DATE | - | Data de cadastro |
| country | VARCHAR(100) | - | País (normalizado) |
| city | VARCHAR(100) | - | Cidade |
| created_at | TIMESTAMP | DEFAULT NOW() | Timestamp de inserção |

### Tabela: `orders`

| Coluna | Tipo | Restrições | Descrição |
|--------|------|-----------|-----------|
| transaction_id | VARCHAR(20) | PRIMARY KEY | Identificador único da ordem |
| customer_id | VARCHAR(20) | FK, NOT NULL | Referência ao cliente |
| payment_method | VARCHAR(50) | - | Método de pagamento |
| amount | DECIMAL(10,2) | NOT NULL | Valor da ordem |
| currency | VARCHAR(10) | - | Moeda (MXN, ARS, BRL, etc.) |
| payment_date | DATE | - | Data do pagamento |
| status | VARCHAR(20) | NOT NULL | Status da ordem |
| created_at | TIMESTAMP | DEFAULT NOW() | Timestamp de inserção |

### Relacionamentos

```
customers (1) ──< (N) orders
   ↑                    │
   └─── customer_id ────┘
```

**Cardinalidade:** Um cliente pode ter múltiplos pedidos (1:N)

**Integridade Referencial:** 
- `ON DELETE CASCADE` → Se um cliente for deletado, seus pedidos também são
- Foreign Key garante que todo pedido tem um cliente válido

---

## 📊 Consultas SQL

### 1. Total de Vendas por Cliente

**Objetivo:** Identificar os clientes mais valiosos (VIP)

```sql
SELECT 
    c.customer_id,
    c.name,
    c.email,
    c.country,
    COUNT(o.transaction_id) AS total_orders,
    SUM(CASE WHEN o.status = 'completed' THEN o.amount ELSE 0 END) AS total_sales,
    ROUND(AVG(CASE WHEN o.status = 'completed' THEN o.amount END), 2) AS avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email, c.country
ORDER BY total_sales DESC;
```

---

### 2. Número de Pedidos por País

**Objetivo:** Análise geográfica de operações

```sql
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(o.transaction_id) AS total_orders,
    COUNT(CASE WHEN o.status = 'completed' THEN 1 END) AS completed_orders,
    SUM(CASE WHEN o.status = 'completed' THEN o.amount ELSE 0 END) AS total_revenue,
    ROUND(
        100.0 * COUNT(CASE WHEN o.status = 'completed' THEN 1 END) / 
        NULLIF(COUNT(o.transaction_id), 0), 
        2
    ) AS conversion_rate_pct
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.country IS NOT NULL
GROUP BY c.country
ORDER BY total_revenue DESC;
```
---

### 3. Ticket Médio

**Objetivo:** Entender valor médio de compra

```sql
-- Ticket médio geral
SELECT 
    ROUND(AVG(amount), 2) AS avg_ticket,
    ROUND(MIN(amount), 2) AS min_ticket,
    ROUND(MAX(amount), 2) AS max_ticket,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_revenue
FROM orders
WHERE status = 'completed';

-- Ticket médio por método de pagamento
SELECT 
    payment_method,
    COUNT(*) AS order_count,
    ROUND(AVG(amount), 2) AS avg_ticket,
    SUM(amount) AS total_revenue
FROM orders
WHERE status = 'completed'
GROUP BY payment_method
ORDER BY avg_ticket DESC;
```

---

##  Decisões Técnicas

### 1. Por que Pandas em vez de PySpark?

**Decisão:** Usar Pandas

**Justificativa:**
- Volume de dados é pequeno (< 1 GB)
- Simplicidade e curva de aprendizado
- Biblioteca madura e bem documentada
- Suficiente para o escopo do projeto

**Quando migrar para PySpark:**
- Dados > 10 GB
- Necessidade de processamento distribuído
- Operações complexas de window functions

---

### 2. Por que Parquet em vez de CSV?

**Decisão:** Usar Parquet nas camadas Bronze e Silver

**Justificativa:**
- Formato colunar → melhor compressão (60-80% menor que CSV)
- Preserva tipos de dados
- Leitura mais rápida (especialmente com filtros)
- Schema embedding


---

### 3. Por que SQLAlchemy em vez de psycopg2 puro?

**Decisão:** Usar SQLAlchemy

**Justificativa:**
- Abstração de banco (fácil trocar MySQL, PostgreSQL, etc.)
- ORM opcional (podemos usar SQL puro também)
- Melhor tratamento de transações
- Padrão da indústria

---

### 4. Por que arquitetura Medallion?

**Decisão:** Bronze → Silver → Gold

**Justificativa:**
- **Rastreabilidade:** Sempre podemos voltar aos dados brutos
- **Flexibilidade:** Regras de limpeza podem mudar
- **Performance:** Processar incrementalmente
- **Governança:** Camadas claras de responsabilidade

---

### 5. Idempotência: Como garantir?

**Problema:** Pipeline pode ser executado múltiplas vezes

**Soluções implementadas:**

```sql
-- 1. TRUNCATE + INSERT (simples, mas perde histórico)
TRUNCATE TABLE customers;
INSERT INTO customers VALUES (...);


---

##  Logs e Monitoramento

### Níveis de Log

```python
logging.DEBUG    # Detalhes técnicos para debugging
logging.INFO     # Progresso normal do pipeline
logging.WARNING  # Alertas (ex: dados faltantes, mas não crítico)
logging.ERROR    # Erros que impedem parte do processo
logging.CRITICAL # Falhas catastróficas
```


