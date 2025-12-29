"""
SILVER LAYER - Data Transformation
===================================
Transforma dados da Bronze para Silver conforme requisitos:
- Cria tabela CUSTOMERS a partir de customers_master
- Cria tabela ORDERS combinando reviews + transactions
- Relaciona orders com customers via customer_id ---> email
"""

import pandas as pd
import logging
from pathlib import Path


# CONFIGURAÇÃO DE LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('silver_transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# CONFIGURAÇÃO DE DIRETÓRIOS
BRONZE_DIR = Path('data') / 'bronze'
SILVER_DIR = Path('data') / 'silver'
SILVER_DIR.mkdir(parents=True, exist_ok=True)


# FUNÇÕES AUXILIARES

def load_bronze_table(table_name: str) -> pd.DataFrame:
    """Carrega tabela da camada Bronze."""
    filepath = BRONZE_DIR / f"{table_name}.parquet"
    
    try:
        logger.info(f"Carregando {table_name} da Bronze...")
        df = pd.read_parquet(filepath)
        logger.info(f"{len(df)} registros carregados")
        return df
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao carregar {table_name}: {e}")
        return pd.DataFrame()


def save_to_silver(df: pd.DataFrame, table_name: str) -> None:
    """Salva DataFrame na camada Silver."""
    if df.empty:
        logger.warning(f"DataFrame {table_name} está vazio. Não será salvo.")
        return
    
    filepath = SILVER_DIR / f"{table_name}.parquet"
    
    try:
        df.to_parquet(filepath, index=False, engine='pyarrow')
        logger.info(f"Dados salvos: {filepath}")
        logger.info(f"Total: {len(df)} registros x {len(df.columns)} colunas\n")
    except Exception as e:
        logger.error(f"Erro ao salvar {table_name}: {e}\n")


# TRANSFORMAÇÃO: CUSTOMERS

def transform_customers() -> pd.DataFrame:
    """
    Transforma dados de customers.
    
    Transformações:
    - Normaliza emails (lowercase, trim)
    - Remove duplicatas por customer_id
    - Remove registros inválidos
    - Converte datas
    """
    logger.info("=" * 70)
    logger.info("TRANSFORMANDO CUSTOMERS")
    logger.info("=" * 70)
    
    # 1. Carregar dados
    df = load_bronze_table('customers')
    if df.empty:
        return df
    
    initial_count = len(df)
    logger.info(f"Colunas: {list(df.columns)}")
    
    # 2. Padronizar nomes de colunas
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    
    # 3. Validar campos obrigatórios
    if 'customer_id' not in df.columns:
        logger.error("ERRO: customer_id não encontrado!")
        return pd.DataFrame()
    
    if 'email' not in df.columns:
        logger.error("ERRO: email não encontrado!")
        return pd.DataFrame()
    
    # 4. Remover registros com customer_id ou email nulos
    df = df[df['customer_id'].notna()]
    df = df[df['email'].notna()]
    logger.info(f"Registros após remover nulos: {len(df)}")
    
    # 5. Normalizar email (lowercase, trim)
    df['email'] = df['email'].astype(str).str.lower().str.strip()
    
    # 6. Validar formato de email
    df = df[df['email'].str.contains('@', na=False)]
    df = df[df['email'] != '']
    df = df[df['email'] != 'nan']
    logger.info(f"Registros após validar emails: {len(df)}")
    
    # 7. Remover duplicatas por customer_id
    df_before = len(df)
    df = df.drop_duplicates(subset=['customer_id'], keep='first')
    duplicates = df_before - len(df)
    if duplicates > 0:
        logger.warning(f"{duplicates} customers duplicados removidos")
    
    # 8. Converter datas
    date_columns = ['registration_date', 'birth_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Coluna '{col}' convertida para datetime")
    
    # 9. Limpar campos de texto
    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        if col not in ['customer_id', 'email']:  # já foram tratados
            df[col] = df[col].astype(str).str.strip()
    
    logger.info(f"Total final: {len(df)} customers")
    logger.info(f"Registros removidos: {initial_count - len(df)}\n")
    
    return df


# TRANSFORMAÇÃO: ORDERS
# ============================================================================

def transform_orders(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria tabela ORDERS combinando reviews + transactions.
    
    Processo:
    1. Une reviews de janeiro e fevereiro
    2. Faz JOIN com transactions via transaction_id
    3. Faz JOIN com customers via customer_id para pegar email
    4. Valida e limpa dados
    """
    logger.info("=" * 70)
    logger.info("TRANSFORMANDO ORDERS (REVIEWS + TRANSACTIONS)")
    logger.info("=" * 70)
    
    # 1. Carregar reviews de janeiro e fevereiro
    reviews_jan = load_bronze_table('reviews_jan')
    reviews_feb = load_bronze_table('reviews_feb')
    
    if reviews_jan.empty or reviews_feb.empty:
        logger.error("ERRO: Não foi possível carregar reviews")
        return pd.DataFrame()
    
    # 2. Unir reviews
    reviews = pd.concat([reviews_jan, reviews_feb], ignore_index=True)
    logger.info(f"Total de reviews unificadas: {len(reviews)}")
    logger.info(f"Colunas reviews: {list(reviews.columns)}")
    
    # 3. Padronizar colunas
    reviews.columns = reviews.columns.str.lower().str.strip().str.replace(' ', '_')
    
    # 4. Carregar transactions
    transactions = load_bronze_table('transactions')
    if transactions.empty:
        logger.error("ERRO: Não foi possível carregar transactions")
        return pd.DataFrame()
    
    transactions.columns = transactions.columns.str.lower().str.strip().str.replace(' ', '_')
    logger.info(f"Colunas transactions: {list(transactions.columns)}")
    
    # 5. JOIN reviews + transactions via transaction_id
    logger.info("\nFazendo JOIN reviews + transactions via transaction_id...")
    orders = reviews.merge(
        transactions,
        on='transaction_id',
        how='inner',
        suffixes=('_review', '_payment')
    )
    logger.info(f"Orders após JOIN: {len(orders)}")
    
    if orders.empty:
        logger.error("ERRO: Nenhuma correspondência entre reviews e transactions!")
        return pd.DataFrame()
    
    # 6. JOIN com customers para pegar email
    logger.info("\nFazendo JOIN com customers para pegar email...")
    
    # Criar lookup de customer_id → email
    customer_lookup = customers_df[['customer_id', 'email']].copy()
    
    orders = orders.merge(
        customer_lookup,
        on='customer_id',
        how='inner'
    )
    logger.info(f"Orders após JOIN com customers: {len(orders)}")
    
    # 7. Selecionar e renomear colunas relevantes
    logger.info("\nSelecionando colunas finais...")
    
    # Identificar colunas disponíveis
    columns_to_keep = {
        'transaction_id': 'order_id',
        'customer_id': 'customer_id',
        'email': 'customer_email',
        'amount': 'amount',
        'currency': 'currency',
        'payment_date': 'order_date',
        'status': 'status',
        'payment_method': 'payment_method',
        'rating': 'rating',
        'review_date': 'review_date'
    }
    
    # Manter apenas colunas que existem
    final_columns = {}
    for original, new_name in columns_to_keep.items():
        if original in orders.columns:
            final_columns[original] = new_name
    
    orders = orders[list(final_columns.keys())].rename(columns=final_columns)
    logger.info(f"Colunas finais: {list(orders.columns)}")
    
    # 8. Converter datas
    if 'order_date' in orders.columns:
        orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
        logger.info("Coluna 'order_date' convertida para datetime")
    
    if 'review_date' in orders.columns:
        orders['review_date'] = pd.to_datetime(orders['review_date'], errors='coerce')
    
    # 9. Converter amount para numérico (se necessário)
    if 'amount' in orders.columns:
        if orders['amount'].dtype == 'object':
            orders['amount'] = orders['amount'].str.replace('$', '').str.replace(',', '')
            orders['amount'] = pd.to_numeric(orders['amount'], errors='coerce')
            logger.info("Coluna 'amount' convertida para numérico")
    
    # 10. Validar valores
    initial_count = len(orders)
    
    # Remove orders com amount inválido
    if 'amount' in orders.columns:
        orders = orders[orders['amount'] > 0]
        logger.info(f"Orders com amount > 0: {len(orders)}")
    
    # Remove orders sem email
    if 'customer_email' in orders.columns:
        orders = orders[orders['customer_email'].notna()]
        orders = orders[orders['customer_email'] != '']
        logger.info(f"Orders com email válido: {len(orders)}")
    
    # 11. Remover duplicatas
    df_before = len(orders)
    orders = orders.drop_duplicates(subset=['order_id'], keep='first')
    duplicates = df_before - len(orders)
    if duplicates > 0:
        logger.warning(f"{duplicates} orders duplicadas removidas")
    
    logger.info(f"\nTotal final: {len(orders)} orders")
    logger.info(f"Registros removidos: {initial_count - len(orders)}\n")
    
    return orders


# RELATÓRIO DE VALIDAÇÃO
# ============================================================================

def generate_validation_report(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> None:
    """Gera relatório de validação dos dados."""
    logger.info("\n" + "=" * 70)
    logger.info("RELATÓRIO DE VALIDAÇÃO")
    logger.info("=" * 70)
    
    # Customers
    logger.info("\nCUSTOMERS:")
    logger.info(f"  Total: {len(customers_df)}")
    logger.info(f"  Customer IDs únicos: {customers_df['customer_id'].nunique()}")
    logger.info(f"  Emails únicos: {customers_df['email'].nunique()}")
    
    # Orders
    logger.info("\nORDERS:")
    logger.info(f"  Total: {len(orders_df)}")
    
    if 'order_id' in orders_df.columns:
        logger.info(f"  Order IDs únicos: {orders_df['order_id'].nunique()}")
    
    if 'customer_email' in orders_df.columns:
        logger.info(f"  Customers com orders: {orders_df['customer_email'].nunique()}")
    
    if 'amount' in orders_df.columns:
        logger.info(f"  Valor total: ${orders_df['amount'].sum():,.2f}")
        logger.info(f"  Ticket médio: ${orders_df['amount'].mean():,.2f}")
        logger.info(f"  Valor mínimo: ${orders_df['amount'].min():,.2f}")
        logger.info(f"  Valor máximo: ${orders_df['amount'].max():,.2f}")
    
    # Relacionamento
    if 'customer_email' in orders_df.columns:
        logger.info("\nRELACIONAMENTO:")
        customers_with_orders = orders_df['customer_email'].nunique()
        customers_without_orders = len(customers_df) - customers_with_orders
        logger.info(f"  Customers com orders: {customers_with_orders}")
        logger.info(f"  Customers sem orders: {customers_without_orders}")
    
    logger.info("=" * 70)


# ORQUESTRAÇÃO PRINCIPAL
# ============================================================================

def transform_all():
    """Executa todas as transformações."""
    logger.info("=" * 70)
    logger.info("INICIANDO TRANSFORMAÇÃO - SILVER LAYER")
    logger.info("=" * 70)
    logger.info(f"Origem: {BRONZE_DIR}")
    logger.info(f"Destino: {SILVER_DIR}")
    logger.info("=" * 70)
    
    # 1. Transformar customers
    customers_df = transform_customers()
    
    if customers_df.empty:
        logger.error("ERRO: Falha ao transformar customers. Abortando.")
        return
    
    # 2. Transformar orders (depende de customers)
    orders_df = transform_orders(customers_df)
    
    if orders_df.empty:
        logger.error("ERRO: Falha ao transformar orders. Abortando.")
        return
    
    # 3. Gerar relatório
    generate_validation_report(customers_df, orders_df)
    
    # 4. Salvar na Silver
    save_to_silver(customers_df, 'customers')
    save_to_silver(orders_df, 'orders')
    
    logger.info("\n" + "=" * 70)
    logger.info("TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
    logger.info("=" * 70)
    logger.info(f"Arquivos criados em: {SILVER_DIR}")
    logger.info("  - customers.parquet")
    logger.info("  - orders.parquet")
    logger.info("=" * 70)


# EXECUÇÃO PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    transform_all()