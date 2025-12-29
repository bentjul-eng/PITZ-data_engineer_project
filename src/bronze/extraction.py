"""
BRONZE LAYER - Data Extraction
Extrai dados brutos dos arquivos JSON sem aplicar transformações.
Salva em formato Parquet para processamento posterior.
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime


# CONFIGURAÇÃO DE LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bronze_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# CONFIGURAÇÃO DE DIRETÓRIOS

# Seguindo arquitetura Medallion: data/ contém os dados, src/ contém o código
# raw -> bronze -> silver -> gold

JSON_DIR = Path('data') / 'raw' / 'json_files'  # Onde estão seus JSONs (origem)
BRONZE_DIR = Path('data') / 'bronze'             # Onde salvar os Parquets (destino)
BRONZE_DIR.mkdir(parents=True, exist_ok=True)    # Cria a pasta se não existir


# FUNÇÕES DE EXTRAÇÃO

def extract_json_to_dataframe(filepath: Path, entity_name: str) -> pd.DataFrame:
    """
    Extrai dados de um arquivo JSON e converte para DataFrame.
    
    Args:
        filepath: Caminho completo do arquivo JSON (Path object)
        entity_name: Nome da entidade (para logging)
    
    Returns:
        DataFrame com os dados brutos
    """
    try:
        logger.info(f"Extraindo dados de {filepath.name}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        
        logger.info(f"{len(df)} registros extraídos de {entity_name}")
        logger.info(f"Colunas: {list(df.columns)}")
        
        return df
    
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON em {filepath.name}: {e}")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Erro inesperado ao processar {filepath.name}: {e}")
        return pd.DataFrame()


def save_to_bronze(df: pd.DataFrame, entity_name: str) -> None:
    """
    Salva DataFrame na camada Bronze em formato Parquet.
    SOBRESCREVE o arquivo se já existir (evita duplicação).
    
    Args:
        df: DataFrame a ser salvo
        entity_name: Nome da entidade
    """
    if df.empty:
        logger.warning(f"DataFrame {entity_name} está vazio. Não será salvo.")
        return
    
    # SEM timestamp - sempre mesmo nome (sobrescreve)
    filename = BRONZE_DIR / f"{entity_name}.parquet"
    
    try:
        # Verifica se arquivo já existe
        if filename.exists():
            logger.info(f"Sobrescrevendo arquivo existente: {filename.name}")
        
        df.to_parquet(filename, index=False, engine='pyarrow')
        logger.info(f"Dados salvos em: {filename}")
        logger.info(f"Tamanho: {len(df)} registros x {len(df.columns)} colunas\n")
    
    except Exception as e:
        logger.error(f"Erro ao salvar {entity_name}: {e}\n")


def extract_all_sources():
    """
    Extrai dados de todas as fontes JSON disponíveis.
    """
    logger.info("=" * 70)
    logger.info("INICIANDO EXTRAÇÃO - BRONZE LAYER")
    logger.info("=" * 70)
    logger.info(f"Diretório de origem: {JSON_DIR}")
    logger.info(f"Diretório de destino: {BRONZE_DIR}")
    logger.info("=" * 70)
    
    # Dicionário com mapeamento: nome_entidade -> arquivo_json
    sources = {
        'competitor_pricing': 'competitor_pricing.json',
        'reviews_feb': 'customer_reviews_feb.json',
        'reviews_jan': 'customer_reviews_jan.json',
        'tickets': 'customer_support_tickets.json',
        'customers': 'customers_master.json',
        'email_sends': 'email_marketing_sends.json',
        'inventory_feb': 'inventory_adjustments_feb.json',
        'inventory_jan': 'inventory_adjustments_jan.json',
        'campaigns': 'marketing_campaigns_q1.json',
        'transactions': 'payment_transactions.json'
    }
    
    extraction_summary = []
    
    for entity_name, json_file in sources.items():
        logger.info(f"\nProcessando: {entity_name}")
        logger.info("-" * 70)
        
        # Constrói o caminho completo do arquivo
        filepath = JSON_DIR / json_file
        
        # Verifica se o arquivo existe antes de tentar ler
        if not filepath.exists():
            logger.error(f"Arquivo não encontrado: {filepath}")
            extraction_summary.append({
                'entity': entity_name,
                'records': 0,
                'columns': 0,
                'status': 'Não encontrado'
            })
            continue
        
        df = extract_json_to_dataframe(filepath, entity_name)
        
        if not df.empty:
            save_to_bronze(df, entity_name)
            extraction_summary.append({
                'entity': entity_name,
                'records': len(df),
                'columns': len(df.columns),
                'status': 'Sucesso'
            })
        else:
            extraction_summary.append({
                'entity': entity_name,
                'records': 0,
                'columns': 0,
                'status': 'Falhou'
            })
    
    # Resumo da extração
    logger.info("\n" + "=" * 70)
    logger.info("RESUMO DA EXTRAÇÃO")
    logger.info("=" * 70)
    
    summary_df = pd.DataFrame(extraction_summary)
    logger.info(f"\n{summary_df.to_string(index=False)}")
    
    total_records = summary_df['records'].sum()
    success_count = len(summary_df[summary_df['status'] == 'Sucesso'])
    failed_count = len(summary_df[summary_df['status'].str.contains('Falhou|encontrado')])
    
    logger.info("\n" + "=" * 70)
    logger.info(f"EXTRAÇÃO CONCLUÍDA")
    logger.info(f"Total de entidades processadas: {len(sources)}")
    logger.info(f"Extrações bem-sucedidas: {success_count}")
    logger.info(f"Extrações com falha: {failed_count}")
    logger.info(f"Total de registros extraídos: {total_records:,}")
    logger.info("=" * 70)


# EXECUÇÃO PRINCIPAL


if __name__ == "__main__":
    extract_all_sources()