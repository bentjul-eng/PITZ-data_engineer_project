"""
SCRIPT DE DIAGNÓSTICO
======================
Verifica as colunas reais dos arquivos Bronze para debug.
"""

import pandas as pd
from pathlib import Path
import json


# DIRETÓRIOS
JSON_DIR = Path('data') / 'raw' / 'json_files'
BRONZE_DIR = Path('data') / 'bronze'


def inspect_json_file(filename: str):
    """Inspeciona estrutura do arquivo JSON."""
    filepath = JSON_DIR / filename
    
    print(f"\n{'='*70}")
    print(f"ARQUIVO: {filename}")
    print(f"{'='*70}")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list) and len(data) > 0:
            # Pega primeiro registro
            first_record = data[0]
            
            print(f"Total de registros: {len(data)}")
            print(f"\nColunas encontradas ({len(first_record)}):")
            for i, col in enumerate(first_record.keys(), 1):
                print(f"  {i}. '{col}'")
            
            print(f"\nPrimeiro registro (exemplo):")
            for key, value in first_record.items():
                # Trunca valores longos
                val_str = str(value)[:50]
                print(f"  {key}: {val_str}")
        
        else:
            print("ERRO: Arquivo vazio ou formato inesperado")
    
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado")
    except Exception as e:
        print(f"ERRO: {e}")


def inspect_parquet_file(filename: str):
    """Inspeciona estrutura do arquivo Parquet."""
    filepath = BRONZE_DIR / filename
    
    print(f"\n{'='*70}")
    print(f"ARQUIVO PARQUET: {filename}")
    print(f"{'='*70}")
    
    try:
        df = pd.read_parquet(filepath)
        
        print(f"Total de registros: {len(df)}")
        print(f"\nColunas encontradas ({len(df.columns)}):")
        for i, col in enumerate(df.columns, 1):
            dtype = df[col].dtype
            null_count = df[col].isnull().sum()
            print(f"  {i}. '{col}' (tipo: {dtype}, nulos: {null_count})")
        
        print(f"\nPrimeira linha (exemplo):")
        first_row = df.iloc[0].to_dict()
        for key, value in first_row.items():
            val_str = str(value)[:50]
            print(f"  {key}: {val_str}")
        
        # Busca por colunas que contenham 'email'
        print(f"\nColunas que contêm 'email' (case-insensitive):")
        email_cols = [col for col in df.columns if 'email' in col.lower()]
        if email_cols:
            for col in email_cols:
                print(f"  ✓ {col}")
        else:
            print("  ✗ NENHUMA coluna com 'email' encontrada")
    
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado")
    except Exception as e:
        print(f"ERRO: {e}")


def main():
    """Executa diagnóstico completo."""
    print("\n" + "="*70)
    print("DIAGNÓSTICO DE ESTRUTURA DOS DADOS")
    print("="*70)
    
    # Inspeciona JSONs principais
    print("\n\n>>> INSPECIONANDO ARQUIVOS JSON ORIGINAIS <<<")
    inspect_json_file('customers_master.json')
    inspect_json_file('payment_transactions.json')
    
    # Inspeciona Parquets da Bronze
    print("\n\n>>> INSPECIONANDO ARQUIVOS PARQUET DA BRONZE <<<")
    inspect_parquet_file('customers.parquet')
    inspect_parquet_file('transactions.parquet')
    
    print("\n" + "="*70)
    print("DIAGNÓSTICO CONCLUÍDO")
    print("="*70)


if __name__ == "__main__":
    main()