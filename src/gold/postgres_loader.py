"""
GOLD LAYER - PostgreSQL Data Loader
====================================
Carrega dados Silver → PostgreSQL (Gold Layer)
- Processo idempotente (truncate + load)
- Validação de integridade pós-carga
- Sem transformações (dados já validados na Silver)
"""

import pandas as pd
import logging
import json
from pathlib import Path
from sqlalchemy import create_engine
import psycopg2


# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('postgres_loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SILVER_DIR = Path('data') / 'silver'

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce',
    'user': 'postgres',
    'password': 'postgres'
}


# ============================================================================
# CONEXÃO
# ============================================================================

def get_connection():
    """Cria conexão com PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Conexão PostgreSQL estabelecida")
        return conn
    except Exception as e:
        logger.error(f"Erro de conexão: {e}")
        return None


def get_engine():
    """Cria engine SQLAlchemy."""
    try:
        conn_string = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(conn_string)
        logger.info("SQLAlchemy engine criada")
        return engine
    except Exception as e:
        logger.error(f"Erro ao criar engine: {e}")
        return None


# ============================================================================
# CARREGAMENTO
# ============================================================================

def load_parquet(table_name: str) -> pd.DataFrame:
    """Carrega arquivo Parquet da Silver."""
    filepath = SILVER_DIR / f"{table_name}.parquet"
    
    try:
        logger.info(f"Carregando {table_name}...")
        df = pd.read_parquet(filepath)
        logger.info(f"   {len(df):,} registros carregados")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar {table_name}: {e}")
        return pd.DataFrame()


def truncate_table(conn, table_name: str) -> None:
    """Limpa tabela para carga idempotente."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        conn.commit()
        cursor.close()
        logger.info(f"Tabela {table_name} truncada")
    except Exception as e:
        logger.error(f"Erro ao truncar {table_name}: {e}")
        conn.rollback()


def prepare_address_json(df: pd.DataFrame) -> pd.DataFrame:
    """Converte coluna address para JSON válido."""
    if 'address' not in df.columns:
        return df
    
    def convert_to_json(value):
        if pd.isna(value) or value in ['null', 'None', '', 'nan']:
            return None
        
        if isinstance(value, str):
            try:
                # Tenta converter string Python dict para JSON
                import ast
                addr_dict = ast.literal_eval(value)
                return json.dumps(addr_dict)
            except:
                return None
        elif isinstance(value, dict):
            return json.dumps(value)
        else:
            return None
    
    df['address'] = df['address'].apply(convert_to_json)
    return df


def validate_orders_before_load(df: pd.DataFrame) -> pd.DataFrame:
    """
    VALIDAÇÃO CRÍTICA: Remove orders com order_date NULL.
    
    O PostgreSQL exige order_date NOT NULL, mas a Silver pode ter
    registros onde order_date é NULL (dados da Bronze).
    """
    initial_count = len(df)
    
    # Remove orders sem order_date
    if 'order_date' in df.columns:
        null_dates = df[df['order_date'].isna()]
        
        if not null_dates.empty:
            logger.warning(f"ATENÇÃO: {len(null_dates)} orders com order_date NULL serão removidas:")
            for _, row in null_dates.iterrows():
                logger.warning(f"   - {row.get('order_id', 'N/A')}: {row.get('customer_email', 'N/A')}")
            
            df = df[df['order_date'].notna()]
    
    removed = initial_count - len(df)
    if removed > 0:
        logger.warning(f"{removed} orders removidas por falta de order_date")
    
    return df


def load_customers(df: pd.DataFrame, engine) -> None:
    """Carrega customers no PostgreSQL - dados já validados na Silver."""
    if df.empty:
        logger.warning("Customers vazio")
        return
    
    logger.info("\n" + "="*70)
    logger.info("CARREGANDO CUSTOMERS")
    logger.info("="*70)
    
    try:
        # Converte address para JSON (única transformação necessária para PostgreSQL)
        df_load = prepare_address_json(df.copy())
        
        # Carrega direto - dados já estão limpos e validados
        df_load.to_sql(
            'customers',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"SUCESSO: {len(df_load):,} customers carregados")
        
    except Exception as e:
        logger.error(f"Erro ao carregar customers: {e}")
        raise


def load_orders(df: pd.DataFrame, engine) -> None:
    """Carrega orders no PostgreSQL - dados já validados na Silver."""
    if df.empty:
        logger.warning("Orders vazio")
        return
    
    logger.info("\n" + "="*70)
    logger.info("CARREGANDO ORDERS")
    logger.info("="*70)
    
    try:
        # VALIDAÇÃO CRÍTICA: Remove orders sem order_date
        df_validated = validate_orders_before_load(df.copy())
        
        if df_validated.empty:
            logger.error("Nenhuma order válida para carregar!")
            return
        
        # Carrega direto - dados já estão limpos e validados
        df_validated.to_sql(
            'orders',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"SUCESSO: {len(df_validated):,} orders carregadas")
        
    except Exception as e:
        logger.error(f"Erro ao carregar orders: {e}")
        raise


# ============================================================================
# VALIDAÇÃO
# ============================================================================

def verify_integrity(conn) -> bool:
    """Valida integridade dos dados carregados."""
    logger.info("\n" + "="*70)
    logger.info("VALIDAÇÃO DE INTEGRIDADE")
    logger.info("="*70)
    
    try:
        cursor = conn.cursor()
        
        # Conta registros
        cursor.execute("SELECT COUNT(*) FROM customers;")
        customers_count = cursor.fetchone()[0]
        logger.info(f"Customers: {customers_count:,}")
        
        cursor.execute("SELECT COUNT(*) FROM orders;")
        orders_count = cursor.fetchone()[0]
        logger.info(f"Orders: {orders_count:,}")
        
        # Verifica integridade referencial
        cursor.execute("""
            SELECT COUNT(*) 
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
        """)
        orphan_orders = cursor.fetchone()[0]
        
        if orphan_orders > 0:
            logger.error(f"{orphan_orders} orders órfãs encontradas!")
            cursor.close()
            return False
        else:
            logger.info("Integridade referencial OK")
        
        # Testa queries principais
        logger.info("\nTestando queries de análise...")
        
        # Query 1: Total por cliente
        cursor.execute("""
            SELECT c.customer_id, 
                   c.email,
                   COALESCE(SUM(o.amount), 0) as total_spent,
                   COUNT(o.order_id) as total_orders
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.email
            LIMIT 5;
        """)
        logger.info("   Query 1: Total por cliente - OK")
        
        # Query 2: Pedidos por país
        cursor.execute("""
            SELECT c.address->>'country' as country, 
                   COUNT(o.order_id) as total_orders
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE c.address->>'country' IS NOT NULL
            GROUP BY country
            LIMIT 5;
        """)
        logger.info("   Query 2: Pedidos por país - OK")
        
        # Query 3: Métricas gerais
        cursor.execute("""
            SELECT 
                COUNT(*) as total_orders,
                AVG(amount) as ticket_medio,
                MAX(amount) as maior_compra,
                MIN(amount) as menor_compra,
                SUM(amount) as total_vendas
            FROM orders;
        """)
        metrics = cursor.fetchone()
        
        if metrics[0] > 0:  # Se tem orders
            logger.info(f"\nMÉTRICAS:")
            logger.info(f"   Total orders:  {metrics[0]:,}")
            logger.info(f"   Ticket médio:  ${metrics[1]:.2f}")
            logger.info(f"   Maior compra:  ${metrics[2]:.2f}")
            logger.info(f"   Menor compra:  ${metrics[3]:.2f}")
            logger.info(f"   Total vendas:  ${metrics[4]:,.2f}")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"Erro na validação: {e}")
        return False


# ============================================================================
# ORQUESTRAÇÃO
# ============================================================================

def load_all():
    """Executa carga completa Silver → PostgreSQL."""
    logger.info("\n" + "="*70)
    logger.info("INICIANDO CARGA - GOLD LAYER")
    logger.info("="*70)
    logger.info(f"Database: {DB_CONFIG['database']}")
    logger.info(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    logger.info("="*70)
    
    # Conectar
    conn = get_connection()
    if not conn:
        return
    
    engine = get_engine()
    if not engine:
        return
    
    try:
        # 1. Truncar tabelas (ordem importa - FK!)
        logger.info("\nLimpando tabelas...")
        truncate_table(conn, 'orders')      # Primeiro (tem FK)
        truncate_table(conn, 'customers')   # Depois
        
        # 2. Carregar customers
        customers_df = load_parquet('customers')
        if customers_df.empty:
            logger.error("Customers vazio - abortando")
            return
        
        load_customers(customers_df, engine)
        
        # 3. Carregar orders
        orders_df = load_parquet('orders')
        if orders_df.empty:
            logger.error("Orders vazio - abortando")
            return
        
        load_orders(orders_df, engine)
        
        # 4. Validar integridade
        if not verify_integrity(conn):
            logger.error("Validação falhou!")
            return
        
        # Sucesso!
        logger.info("\n" + "="*70)
        logger.info("CARGA CONCLUÍDA COM SUCESSO!")
        logger.info("="*70)
        logger.info("\nPróximos passos:")
        logger.info("   1. Executar queries SQL de análise")
        logger.info("   2. Criar visualizações/dashboards")
        logger.info("   3. Configurar monitoramento")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"Erro durante carga: {e}")
        conn.rollback()
    
    finally:
        if conn:
            conn.close()
            logger.info("Conexão fechada")
        if engine:
            engine.dispose()


# ============================================================================
# EXECUÇÃO
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("CONFIGURAÇÃO DO BANCO DE DADOS")
    print("="*70)
    print(f"  Host:     {DB_CONFIG['host']}")
    print(f"  Port:     {DB_CONFIG['port']}")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  User:     {DB_CONFIG['user']}")
    print(f"  Password: {'*' * len(DB_CONFIG['password'])}")
    print("="*70)
    
    response = input("\nCredenciais corretas? (s/n): ")
    
    if response.lower() == 's':
        load_all()
    else:
        print("\nConfigure DB_CONFIG no código e execute novamente.")
        print("="*70)