"""
SILVER LAYER - Data Transformation & Validation
================================================
Transforma dados Bronze → Silver seguindo arquitetura Medallion:
- Validação de qualidade rigorosa
- Padronização e limpeza
- Documentação de rejeições
"""

import pandas as pd
import logging
import sys
import io
from pathlib import Path
from typing import Dict
from datetime import datetime


# ============================================================================
# CONFIGURAÇÃO DE ENCODING (FIX WINDOWS)
# ============================================================================

# Força UTF-8 no Windows para suportar emojis
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('silver_transformation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BRONZE_DIR = Path('data') / 'bronze'
SILVER_DIR = Path('data') / 'silver'
REJECTED_DIR = Path('data') / 'rejected'

SILVER_DIR.mkdir(parents=True, exist_ok=True)
REJECTED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# UTILITÁRIOS
# ============================================================================

def load_bronze_table(table_name: str) -> pd.DataFrame:
    """Carrega tabela da Bronze com tratamento de erro."""
    filepath = BRONZE_DIR / f"{table_name}.parquet"
    
    try:
        logger.info(f" Carregando {table_name}...")
        df = pd.read_parquet(filepath)
        logger.info(f"   {len(df):,} registros carregados")
        return df
    except FileNotFoundError:
        logger.error(f" Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao carregar {table_name}: {e}")
        return pd.DataFrame()


def save_to_silver(df: pd.DataFrame, table_name: str) -> None:
    """Salva DataFrame validado na Silver."""
    if df.empty:
        logger.warning(f"DataFrame {table_name} vazio - não será salvo")
        return
    
    filepath = SILVER_DIR / f"{table_name}.parquet"
    
    try:
        df.to_parquet(filepath, index=False, engine='pyarrow')
        logger.info(f"{table_name}.parquet salvo: {len(df):,} registros × {len(df.columns)} colunas")
    except Exception as e:
        logger.error(f"Erro ao salvar {table_name}: {e}")


def save_rejected_records(df: pd.DataFrame, table_name: str, reason: str) -> None:
    """Salva registros rejeitados para auditoria."""
    if df.empty:
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = REJECTED_DIR / f"{table_name}_rejected_{timestamp}.parquet"
    
    df['rejection_reason'] = reason
    df['rejection_timestamp'] = datetime.now()
    
    try:
        df.to_parquet(filepath, index=False)
        logger.warning(f"{len(df)} registros rejeitados salvos em: {filepath.name}")
    except Exception as e:
        logger.error(f"Erro ao salvar rejeitados: {e}")


# ============================================================================
# TRANSFORMAÇÃO: CUSTOMERS
# ============================================================================

def transform_customers() -> pd.DataFrame:
    """
    Transforma e valida customers seguindo qualidade Medallion.
    
    Validações:
    - Campos obrigatórios não-nulos
    - Formato de email válido
    - Remoção de duplicatas
    - Padronização de dados
    """
    logger.info("\n" + "="*70)
    logger.info("TRANSFORMANDO CUSTOMERS")
    logger.info("="*70)
    
    # 1. Carregar dados
    df = load_bronze_table('customers')
    if df.empty:
        return df
    
    initial_count = len(df)
    
    # 2. Padronizar colunas
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    logger.info(f"Colunas: {list(df.columns)}")
    
    # 3. VALIDAÇÃO: Campos obrigatórios existem?
    required_fields = ['customer_id', 'email']
    missing_fields = [f for f in required_fields if f not in df.columns]
    
    if missing_fields:
        logger.error(f"ERRO CRÍTICO: Campos obrigatórios ausentes: {missing_fields}")
        return pd.DataFrame()
    
    # 4. VALIDAÇÃO: customer_id não-nulo e único
    null_ids = df[df['customer_id'].isna()]
    if not null_ids.empty:
        save_rejected_records(null_ids, 'customers', 'customer_id_null')
        df = df[df['customer_id'].notna()]
        logger.warning(f"{len(null_ids)} registros com customer_id nulo rejeitados")
    
    # 5. VALIDAÇÃO: email não-nulo
    null_emails = df[df['email'].isna()]
    if not null_emails.empty:
        save_rejected_records(null_emails, 'customers', 'email_null')
        df = df[df['email'].notna()]
        logger.warning(f"{len(null_emails)} registros com email nulo rejeitados")
    
    # 6. Normalização de email
    df['email'] = df['email'].astype(str).str.lower().str.strip()
    
    # 7. VALIDAÇÃO: formato de email
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    invalid_emails = df[~df['email'].str.match(email_pattern, na=False)]
    
    if not invalid_emails.empty:
        save_rejected_records(invalid_emails, 'customers', 'email_invalid_format')
        df = df[df['email'].str.match(email_pattern, na=False)]
        logger.warning(f"{len(invalid_emails)} emails com formato inválido rejeitados")
    
    # 8. VALIDAÇÃO: customer_id duplicado
    duplicates = df[df.duplicated(subset=['customer_id'], keep=False)]
    if not duplicates.empty:
        save_rejected_records(duplicates[duplicates.duplicated(subset=['customer_id'], keep='first')], 
                            'customers', 'customer_id_duplicate')
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        logger.warning(f"{len(duplicates) - len(df)} customer_id duplicados removidos (mantido primeiro)")
    
    # 9. Conversão de datas
    date_columns = ['registration_date', 'birth_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            null_dates = df[col].isna().sum()
            if null_dates > 0:
                logger.info(f"   {col}: {null_dates} datas inválidas convertidas para NULL")
    
    # 10. Validação de datas lógicas
    if 'birth_date' in df.columns:
        future_births = df[df['birth_date'] > datetime.now()]
        if not future_births.empty:
            logger.warning(f"{len(future_births)} birth_date no futuro detectadas")
    
    # 11. Limpeza de texto
    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        if col not in ['customer_id', 'email']:
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(['', 'nan', 'null', 'None'], None)
    
    # 12. Resumo final
    removed_count = initial_count - len(df)
    removal_rate = (removed_count / initial_count * 100) if initial_count > 0 else 0
    
    logger.info(f"\n RESULTADO:")
    logger.info(f"Registros iniciais: {initial_count:,}")
    logger.info(f"Registros válidos:  {len(df):,}")
    logger.info(f"Removidos:          {removed_count:,} ({removal_rate:.2f}%)")
    logger.info(f"Customer IDs únicos: {df['customer_id'].nunique():,}")
    logger.info(f"Emails únicos:       {df['email'].nunique():,}")
    
    return df


# ============================================================================
# TRANSFORMAÇÃO: ORDERS
# ============================================================================

def transform_orders(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria tabela ORDERS com validações Medallion.
    
    Processo:
    1. Une reviews (janeiro + fevereiro)
    2. JOIN com transactions via transaction_id
    3. JOIN com customers via customer_id
    4. Validações de integridade e qualidade
    """
    logger.info("\n" + "="*70)
    logger.info("TRANSFORMANDO ORDERS")
    logger.info("="*70)
    
    # 1. Carregar e unir reviews
    reviews_jan = load_bronze_table('reviews_jan')
    reviews_feb = load_bronze_table('reviews_feb')
    
    if reviews_jan.empty or reviews_feb.empty:
        logger.error("Erro ao carregar reviews")
        return pd.DataFrame()
    
    reviews = pd.concat([reviews_jan, reviews_feb], ignore_index=True)
    reviews.columns = reviews.columns.str.lower().str.strip().str.replace(' ', '_')
    logger.info(f"Reviews unificadas: {len(reviews):,}")
    
    # 2. Carregar transactions
    transactions = load_bronze_table('transactions')
    if transactions.empty:
        logger.error("Erro ao carregar transactions")
        return pd.DataFrame()
    
    transactions.columns = transactions.columns.str.lower().str.strip().str.replace(' ', '_')
    logger.info(f"Transactions: {len(transactions):,}")
    
    # 3. VALIDAÇÃO: transaction_id não-nulo
    reviews = reviews[reviews['transaction_id'].notna()]
    transactions = transactions[transactions['transaction_id'].notna()]
    
    # 4. JOIN reviews + transactions
    logger.info("JOIN reviews ⟷ transactions...")
    orders = reviews.merge(
        transactions,
        on='transaction_id',
        how='inner',
        suffixes=('_review', '_transaction')
    )
    logger.info(f"   {len(orders):,} correspondências encontradas")
    
    if orders.empty:
        logger.error("Nenhuma correspondência entre reviews e transactions!")
        return pd.DataFrame()
    
    # 5. VALIDAÇÃO: customer_id não-nulo
    null_customer_ids = orders[orders['customer_id'].isna()]
    if not null_customer_ids.empty:
        save_rejected_records(null_customer_ids, 'orders', 'customer_id_null')
        orders = orders[orders['customer_id'].notna()]
    
    # 6. JOIN com customers
    logger.info("JOIN orders ⟷ customers...")
    customer_lookup = customers_df[['customer_id', 'email']].rename(columns={'email': 'customer_email'})
    
    orders = orders.merge(customer_lookup, on='customer_id', how='inner')
    logger.info(f"   {len(orders):,} orders com customer válido")
    
    # 7. Selecionar colunas finais
    column_mapping = {
        'transaction_id': 'order_id',
        'customer_id': 'customer_id',
        'customer_email': 'customer_email',
        'amount': 'amount',
        'currency': 'currency',
        'payment_date': 'order_date',
        'status': 'status',
        'payment_method': 'payment_method',
        'rating': 'rating',
        'review_date': 'review_date'
    }
    
    available_columns = {k: v for k, v in column_mapping.items() if k in orders.columns}
    orders = orders[list(available_columns.keys())].rename(columns=available_columns)
    
    # 8. Conversão de tipos
    if 'order_date' in orders.columns:
        orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
    
    if 'review_date' in orders.columns:
        orders['review_date'] = pd.to_datetime(orders['review_date'], errors='coerce')
    
    if 'amount' in orders.columns:
        if orders['amount'].dtype == 'object':
            orders['amount'] = orders['amount'].str.replace('$', '').str.replace(',', '')
        orders['amount'] = pd.to_numeric(orders['amount'], errors='coerce')
    
    if 'rating' in orders.columns:
        orders['rating'] = pd.to_numeric(orders['rating'], errors='coerce')
    
    # 9. VALIDAÇÃO: amount válido
    invalid_amounts = orders[(orders['amount'].isna()) | (orders['amount'] <= 0)]
    if not invalid_amounts.empty:
        save_rejected_records(invalid_amounts, 'orders', 'amount_invalid')
        orders = orders[(orders['amount'].notna()) & (orders['amount'] > 0)]
        logger.warning(f"{len(invalid_amounts)} orders com amount inválido rejeitadas")
    
    # 10. VALIDAÇÃO CRÍTICA: order_date obrigatório (NOT NULL no PostgreSQL)
    null_order_dates = orders[orders['order_date'].isna()]
    if not null_order_dates.empty:
        logger.warning(f"ATENÇÃO: {len(null_order_dates)} orders com order_date NULL serão rejeitadas:")
        for _, row in null_order_dates.head(5).iterrows():
            logger.warning(f"   - {row.get('order_id', 'N/A')}: {row.get('customer_email', 'N/A')}")
        
        save_rejected_records(null_order_dates, 'orders', 'order_date_null')
        orders = orders[orders['order_date'].notna()]
        logger.warning(f"{len(null_order_dates)} orders com order_date NULL rejeitadas")
    
    # 11. VALIDAÇÃO: order_id duplicado
    duplicates = orders[orders.duplicated(subset=['order_id'], keep=False)]
    if not duplicates.empty:
        save_rejected_records(duplicates[duplicates.duplicated(subset=['order_id'], keep='first')],
                            'orders', 'order_id_duplicate')
        orders = orders.drop_duplicates(subset=['order_id'], keep='first')
        logger.warning(f"{len(duplicates) - len(orders)} order_id duplicados removidos")
    
    # 12. VALIDAÇÃO: datas lógicas
    if 'order_date' in orders.columns and 'review_date' in orders.columns:
        # Remove NaT do review_date para comparação
        orders_with_review = orders[orders['review_date'].notna()]
        invalid_dates = orders_with_review[orders_with_review['review_date'] < orders_with_review['order_date']]
        if not invalid_dates.empty:
            logger.warning(f"{len(invalid_dates)} orders com review_date < order_date")
    
    # 13. Resumo final
    logger.info(f"\n RESULTADO:")
    logger.info(f"   Orders válidas:      {len(orders):,}")
    logger.info(f"   Order IDs únicos:    {orders['order_id'].nunique():,}")
    logger.info(f"   Customers únicos:    {orders['customer_email'].nunique():,}")
    if 'amount' in orders.columns:
        logger.info(f"   Valor total:         ${orders['amount'].sum():,.2f}")
        logger.info(f"   Ticket médio:        ${orders['amount'].mean():,.2f}")
    
    return orders


# ============================================================================
# VALIDAÇÃO DE QUALIDADE
# ============================================================================

def validate_data_quality(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> Dict:
    """Valida qualidade dos dados transformados."""
    logger.info("\n" + "="*70)
    logger.info(" VALIDAÇÃO DE QUALIDADE")
    logger.info("="*70)
    
    quality_report = {
        'timestamp': datetime.now(),
        'customers': {},
        'orders': {},
        'integrity': {}
    }
    
    # Customers
    quality_report['customers'] = {
        'total_records': len(customers_df),
        'unique_ids': customers_df['customer_id'].nunique(),
        'unique_emails': customers_df['email'].nunique(),
        'null_check': customers_df.isnull().sum().to_dict()
    }
    
    # Orders
    quality_report['orders'] = {
        'total_records': len(orders_df),
        'unique_ids': orders_df['order_id'].nunique(),
        'null_check': orders_df.isnull().sum().to_dict()
    }
    
    # Integridade referencial
    customers_with_orders = orders_df['customer_email'].nunique()
    customers_without_orders = len(customers_df) - customers_with_orders
    
    quality_report['integrity'] = {
        'customers_with_orders': customers_with_orders,
        'customers_without_orders': customers_without_orders,
        'orphan_orders': 0  # Garantido pelo inner join
    }
    
    logger.info(f"\n CUSTOMERS:")
    logger.info(f"   Total:          {quality_report['customers']['total_records']:,}")
    logger.info(f"   IDs únicos:     {quality_report['customers']['unique_ids']:,}")
    logger.info(f"   Emails únicos:  {quality_report['customers']['unique_emails']:,}")
    
    logger.info(f"\n ORDERS:")
    logger.info(f"   Total:          {quality_report['orders']['total_records']:,}")
    logger.info(f"   IDs únicos:     {quality_report['orders']['unique_ids']:,}")
    
    logger.info(f"\n INTEGRIDADE:")
    logger.info(f"   Customers com orders:     {customers_with_orders:,}")
    logger.info(f"   Customers sem orders:     {customers_without_orders:,}")
    logger.info(f"   Orders órfãs:             0 (garantido)")
    
    # Verificação de campos NULL críticos
    logger.info(f"\n CAMPOS CRÍTICOS (NULL check):")
    if 'order_date' in orders_df.columns:
        null_order_dates = orders_df['order_date'].isna().sum()
        logger.info(f"   order_date NULL:  {null_order_dates} (deve ser 0!)")
        if null_order_dates > 0:
            logger.error(f"ERRO: order_date tem valores NULL!")
    
    return quality_report


# ============================================================================
# ORQUESTRAÇÃO
# ============================================================================

def transform_all():
    """Executa pipeline completo de transformação."""
    logger.info("\n" + "="*70)
    logger.info("INICIANDO TRANSFORMAÇÃO - SILVER LAYER")
    logger.info("="*70)
    logger.info(f"Bronze:   {BRONZE_DIR}")
    logger.info(f"Silver:   {SILVER_DIR}")
    logger.info(f"Rejected: {REJECTED_DIR}")
    logger.info("="*70)
    
    start_time = datetime.now()
    
    # 1. Transformar customers
    customers_df = transform_customers()
    if customers_df.empty:
        logger.error("FALHA: Customers vazio")
        return
    
    # 2. Transformar orders
    orders_df = transform_orders(customers_df)
    if orders_df.empty:
        logger.error("FALHA: Orders vazio")
        return
    
    # 3. Validar qualidade
    quality_report = validate_data_quality(customers_df, orders_df)
    
    # 4. Salvar na Silver
    save_to_silver(customers_df, 'customers')
    save_to_silver(orders_df, 'orders')
    
    # 5. Resumo final
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "="*70)
    logger.info("TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
    logger.info("="*70)
    logger.info(f"Tempo de execução: {elapsed_time:.2f}s")
    logger.info(f" Arquivos criados em: {SILVER_DIR}")
    logger.info("  customers.parquet")
    logger.info("  orders.parquet")
    logger.info(f" Registros rejeitados salvos em: {REJECTED_DIR}")
    logger.info("="*70)


# ============================================================================
# EXECUÇÃO
# ============================================================================

if __name__ == "__main__":
    transform_all()